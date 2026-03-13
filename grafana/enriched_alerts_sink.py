import json
import logging
import time
from kafka import KafkaConsumer
import psycopg2

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

DB_CONFIG = dict(host='localhost', port=5433, dbname='airflow', user='airflow', password='airflow')

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

cur.execute('DROP TABLE IF EXISTS enriched_alerts')
cur.execute('''CREATE TABLE enriched_alerts (
    id SERIAL PRIMARY KEY,
    tower_id BIGINT,
    operator TEXT,
    area BIGINT,
    alert_type TEXT,
    weather_impact TEXT,
    conclusion TEXT,
    rain FLOAT,
    wind_speed FLOAT,
    cloud_cover INT,
    created_at TIMESTAMP DEFAULT NOW()
)''')
conn.commit()
log.info('Table enriched_alerts ready')

consumer = KafkaConsumer(
    'enriched_alerts',
    bootstrap_servers='localhost:9092',
    group_id='grafana-enriched-group',
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

log.info('Listening for enriched alerts...')
for msg in consumer:
    alert = msg.value
    try:
        cur.execute('''INSERT INTO enriched_alerts
            (tower_id, operator, area, alert_type, weather_impact, conclusion, rain, wind_speed, cloud_cover)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)''',
            (
                alert.get('tower_id'),
                alert.get('operator'),
                alert.get('area'),
                alert.get('alert_type'),
                alert.get('weather_impact'),
                alert.get('conclusion'),
                alert.get('rain'),
                alert.get('wind_speed'),
                alert.get('cloud_cover')
            )
        )
        conn.commit()
        log.info(f"Saved: {alert.get('alert_type')} | {alert.get('weather_impact')} | Tower {alert.get('tower_id')}")
    except Exception as e:
        log.error(f'Error: {e}')
        conn.rollback()