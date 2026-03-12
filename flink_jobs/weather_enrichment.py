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

def get_weather_impact(weather):
    rain        = weather.get("rain") or 0
    wind_speed  = weather.get("wind_speed") or 0
    cloud_cover = weather.get("cloud_cover") or 0

    if rain > 5 or wind_speed > 40:
        return "HIGH"
    elif rain > 1 or wind_speed > 20 or cloud_cover > 80:
        return "MEDIUM"
    else:
        return "LOW"

print("Weather enrichment started...")

for msg in consumer:
    alert = msg.value
    lat = alert.get("lat")
    lon = alert.get("lon")

    if lat and lon:
        weather = get_weather(lat, lon)
        weather["weather_impact"] = get_weather_impact(weather)
        alert["weather"] = weather

        if weather["weather_impact"] == "LOW":
            alert["conclusion"] = "Technical issue — not weather related"
        elif weather["weather_impact"] == "MEDIUM":
            alert["conclusion"] = "Weather may be a partial cause"
        else:
            alert["conclusion"] = "Weather is the main cause of the issue"

        producer.send('enriched_alerts', value=alert)
        print(
            f"Tower {alert.get('tower_id')} | "
            f"{alert.get('alert_type')} | "
            f"temp={weather['temperature']}°C | "
            f"wind={weather['wind_speed']}km/h | "
            f"impact={weather['weather_impact']} | "
            f"{alert['conclusion']}"
        )

    time.sleep(0.1)