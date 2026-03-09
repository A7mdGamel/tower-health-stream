import pandas as pd
import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

df = pd.read_csv('data/towers_clean.csv')
towers = df.to_dict('records')
print(f"Loaded {len(towers)} towers")


def is_peak_hour():
    hour = datetime.now().hour
    return (7 <= hour <= 9) or (17 <= hour <= 19)


def generate_event(tower):
    peak = is_peak_hour()
    anomaly = random.random() < 0.05

    signal = random.uniform(-90, -60)
    latency = random.uniform(20, 60)
    users = random.randint(50, 300)
    drop_rate = random.uniform(0.01, 0.05)

    if peak:
        signal -= random.uniform(5, 15)
        latency += random.uniform(10, 40)
        users += random.randint(100, 400)

    if anomaly:
        signal -= random.uniform(20, 40)
        latency += random.uniform(50, 150)
        drop_rate += random.uniform(0.1, 0.3)

    return {
        "tower_id":        tower["tower_id"],
        "operator":        tower["operator"],
        "radio":           tower["radio"],
        "lat":             tower["lat"],
        "lon":             tower["lon"],
        "area":            tower["area"],
        "timestamp":       datetime.now(timezone.utc).isoformat(),
        "signal_strength": round(signal, 2),
        "latency_ms":      round(latency, 2),
        "connected_users": users,
        "call_drop_rate":  round(drop_rate, 4),
        "is_peak_hour":    peak,
        "is_anomaly":      anomaly
    }


print("Simulator started...")

while True:
    for tower in towers:
        event = generate_event(tower)
        producer.send('tower_events', value=event)

    producer.flush()
    print(f"[{datetime.now(timezone.utc).isoformat()}] Sent {len(towers)} events")
    time.sleep(5)