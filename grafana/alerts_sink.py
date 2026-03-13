from kafka import KafkaConsumer
import psycopg2
import json
from datetime import datetime

conn = psycopg2.connect(
    host='localhost', port=5433,
    dbname='airflow', user='airflow', password='airflow'
)
cur = conn.cursor()

cur.execute('DROP TABLE IF EXISTS realtime_alerts')
cur.execute('''CREATE TABLE realtime_alerts (
    id SERIAL PRIMARY KEY,
    tower_id BIGINT,
    operator TEXT,
    area BIGINT,
    lat FLOAT,
    lon FLOAT,
    alert_type TEXT,
    signal_strength FLOAT,
    latency_ms FLOAT,
    connected_users INT,
    call_drop_rate FLOAT,
    created_at TIMESTAMP DEFAULT NOW()
)''')
conn.commit()
print('Table created, listening for alerts...')

consumer = KafkaConsumer(
    'regional_alerts',
    bootstrap_servers='localhost:9092',
    group_id='grafana-alerts-group',
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for msg in consumer:
    alert = msg.value
    cur.execute('''INSERT INTO realtime_alerts
        (tower_id, operator, area, lat, lon, alert_type,
         signal_strength, latency_ms, connected_users, call_drop_rate)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
        (
            alert.get('tower_id'),
            alert.get('operator'),
            alert.get('area'),
            alert.get('lat'),
            alert.get('lon'),
            alert.get('alert_type'),
            alert.get('signal_strength'),
            alert.get('latency_ms'),
            alert.get('connected_users'),
            alert.get('call_drop_rate')
        )
    )
    conn.commit()
    print(f"Alert saved: {alert.get('alert_type')} - Tower {alert.get('tower_id')}")