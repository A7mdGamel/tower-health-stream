import json
import time
import urllib.request
from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer(
    'regional_alerts',
    bootstrap_servers='localhost:9092',
    group_id='weather-enrichment-group',
    auto_offset_reset='latest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_weather(lat, lon):
    try:
        url = (
            f"https://api.open-meteo.com/v1/forecast"
            f"?latitude={lat}&longitude={lon}"
            f"&current=temperature_2m,wind_speed_10m,rain,cloud_cover"
            f"&timezone=Africa%2FCairo"
        )
        with urllib.request.urlopen(url, timeout=5) as resp:
            data = json.loads(resp.read())
            current = data.get("current", {})
            return {
                "temperature": current.get("temperature_2m"),
                "wind_speed":  current.get("wind_speed_10m"),
                "rain":        current.get("rain"),
                "cloud_cover": current.get("cloud_cover")
            }
    except Exception:
        return {
            "temperature": None,
            "wind_speed":  None,
            "rain":        None,
            "cloud_cover": None
        }

print("Weather enrichment started...")

for msg in consumer:
    alert = msg.value
    lat = alert.get("lat")
    lon = alert.get("lon")

    if lat and lon:
        weather = get_weather(lat, lon)
        alert["weather"] = weather
        producer.send('enriched_alerts', value=alert)
        print(f"Enriched tower {alert.get('tower_id')} | temp={weather['temperature']}°C | wind={weather['wind_speed']}km/h")

    time.sleep(0.1)