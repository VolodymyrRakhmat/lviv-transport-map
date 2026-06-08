import requests, zipfile, time, json, firebase_admin, smtplib, math
from email.mime.text import MIMEText
from io import BytesIO
from firebase_admin import credentials, db
from datetime import datetime
from google.transit import gtfs_realtime_pb2
import pandas as pd

# ================== НАЛАШТУВАННЯ ПОШТИ ==================
EMAIL_FROM = "vova1.rakhmat@gmail.com"
EMAIL_TO = "vova1.rakhmat@gmail.com"
EMAIL_PASSWORD = "vzclefgugmwsrxhg"

def send_email_alert(new_unknown):
    if not new_unknown:
        return
    subject = f"🚨 Нові неідентифіковані ТЗ: {len(new_unknown)}"
    body = "Знайдено нові неідентифіковані борти, які зараз на лінії:\n\n"
    for vid, info in new_unknown.items():
        body += f"ID: {vid} | Маршрут: {info.get('route', 'Невідомо')} | Час: {info.get('time', '')}\n"
    
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = EMAIL_FROM
    msg['To'] = EMAIL_TO

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(EMAIL_FROM, EMAIL_PASSWORD)
            server.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())
        print(f"✅ Лист надіслано ({len(new_unknown)} нових невідомих)")
    except Exception as e:
        print(f"❌ Помилка відправки листа: {e}")

# =======================================================

DB_URL = 'https://lviv-transport-web-default-rtdb.firebaseio.com/'
STATIC_URL = "http://track.ua-gis.com/gtfs/lviv/static.zip"
REALTIME_URL = "http://track.ua-gis.com/gtfs/lviv/vehicle_position"
CERT_FILE = "serviceAccountKey.json"
MAPPING_FILE = r"C:\Users\Volodymyr\lviv-transport-web\public\mapping.json"

if not firebase_admin._apps:
    firebase_admin.initialize_app(credentials.Certificate(CERT_FILE), {'databaseURL': DB_URL})

def get_routes_map():
    try:
        r = requests.get(STATIC_URL, timeout=30)
        z = zipfile.ZipFile(BytesIO(r.content))
        with z.open('routes.txt') as f:
            routes = pd.read_csv(f)
            return {str(row['route_id']).strip(): str(row['route_short_name']).strip() for _, row in routes.iterrows()}
    except Exception as e:
        print(f"Помилка завантаження routes.txt: {e}")
        return {}

def load_mapping():
    try:
        with open(MAPPING_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Не вдалося завантажити mapping.json: {e}")
        return {}

def haversine(lat1, lon1, lat2, lon2):
    R = 6371000  # метри
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def run():
    routes_map = get_routes_map()
    ref_live = db.reference('vehicles')
    ref_alerts = db.reference('alerts')
    ref_history = db.reference('history')

    trolley_numbers = ['22', '23', '24', '25', '30', '31', '32', '33', '38']

    last_history_save = 0
    HISTORY_INTERVAL = 300          # 5 хвилин
    last_cleanup = 0
    CLEANUP_INTERVAL = 60

    last_alerted = set()
    prev_state = {}                 # raw_id -> стан для контролю запису

    STATIONARY_THRESHOLD = 8        # ~1.5 хвилини для невідомих
    CHANGE_THRESHOLD_METERS = 20

    while True:
        try:
            vehicle_mapping = load_mapping()
            res = requests.get(REALTIME_URL, timeout=(10, 60))
            
            if res.status_code == 200:
                feed = gtfs_realtime_pb2.FeedMessage()
                feed.ParseFromString(res.content)

                current_ts = int(time.time())
                current_time_str = datetime.now().strftime("%H:%M:%S")

                # ==================== CLEANUP ====================
                if current_ts - last_cleanup >= CLEANUP_INTERVAL and len(feed.entity) > 0:
                    existing = ref_live.get() or {}
                    feed_ids = {str(getattr(e.vehicle.vehicle, 'id', '')).strip() 
                              for e in feed.entity if e.HasField('vehicle')}

                    to_delete = []
                    for veh_id, data in existing.items():
                        if veh_id not in feed_ids:
                            last_upd = int(data.get('last_updated', 0))
                            if current_ts - last_upd > 180:
                                to_delete.append(veh_id)

                    for vid in to_delete:
                        ref_live.child(vid).delete()

                    if to_delete:
                        print(f"[ОЧИЩЕННЯ] Видалено привидів: {len(to_delete)}")
                    last_cleanup = current_ts
                # =================================================

                payload = {}
                alerts_payload = {}
                new_unknown_this_cycle = {}

                for entity in feed.entity:
                    if not entity.HasField('vehicle'):
                        continue

                    v = entity.vehicle
                    raw_id = str(getattr(v.vehicle, 'id', '')).strip()
                    if not raw_id:
                        continue

                    r_id = str(v.trip.route_id).strip()
                    name = routes_map.get(r_id, r_id)
                    is_identified = raw_id in vehicle_mapping

                    name_low = name.lower()
                    if name in trolley_numbers or 'трол' in name_low or 'тр' in name_low:
                        v_type = 'trolley'
                    elif 'трам' in name_low or name.startswith('Т'):
                        v_type = 'tram'
                    else:
                        v_type = 'bus'

                    current_lat = float(v.position.latitude)
                    current_lng = float(v.position.longitude)
                    current_speed = round(v.position.speed * 3.6, 1) if v.position.HasField('speed') else 0
                    stop = v.stop_id if v.HasField('stop_id') else "В дорозі"
                    gtfs_timestamp = v.timestamp if v.HasField('timestamp') else current_ts

                    # === Логіка зменшення дублювання ===
                    prev = prev_state.get(raw_id, {})
                    stationary_count = prev.get('stationary_count', 0)

                    moved = True
                    if prev:
                        dist = haversine(prev['lat'], prev['lng'], current_lat, current_lng)
                        if dist < CHANGE_THRESHOLD_METERS and abs(current_speed - prev.get('speed', 0)) < 2:
                            stationary_count += 1
                            moved = False
                        else:
                            stationary_count = 0

                    prev_state[raw_id] = {
                        'lat': current_lat,
                        'lng': current_lng,
                        'speed': current_speed,
                        'stationary_count': stationary_count,
                        'last_save_ts': prev.get('last_save_ts', 0)
                    }

                    # Рішення про запис
                    should_update_live = True
                    if not is_identified:                                 # Невідомі
                        if stationary_count >= STATIONARY_THRESHOLD and current_speed < 3:
                            should_update_live = False
                    else:                                                 # Ідентифіковані
                        if stationary_count >= 4 and current_speed < 3:
                            should_update_live = (current_ts - prev.get('last_save_ts', 0) > 60)

                    vehicle_data = {
                        'r': name,
                        'type': v_type,
                        'lat': current_lat,
                        'lng': current_lng,
                        'p': vehicle_mapping.get(raw_id, raw_id),
                        'raw_p': raw_id,
                        'b': float(v.position.bearing) if v.position.HasField('bearing') else 0.0,
                        's': current_speed,
                        'stop': stop,
                        'has_bort': is_identified,
                        'timestamp': current_time_str,
                        'last_updated': gtfs_timestamp,
                        'updated_at': datetime.now().isoformat()
                    }

                    if should_update_live:
                        payload[raw_id] = vehicle_data
                        prev_state[raw_id]['last_save_ts'] = current_ts

                    if not is_identified:
                        alerts_payload[raw_id] = {'id': raw_id, 'route': name, 'time': current_time_str}
                        if (current_speed > 3 or not v.HasField('stop_id')) and raw_id not in last_alerted:
                            new_unknown_this_cycle[raw_id] = alerts_payload[raw_id]
                            last_alerted.add(raw_id)

                # Запис в Firebase
                if payload:
                    ref_live.update(payload)

                ref_alerts.set(alerts_payload)

                if new_unknown_this_cycle:
                    send_email_alert(new_unknown_this_cycle)

                print(f"[{current_time_str}] Оновлено {len(payload)} ТЗ | Невідомих: {len(alerts_payload)} | Всього в prev_state: {len(prev_state)}")

            time.sleep(11)

        except Exception as e:
            print(f"Помилка: {e}")
            time.sleep(10)

if __name__ == "__main__":
    run()