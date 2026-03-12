from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import urllib.request
import json
import os

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_weather_batch():
    towers_file = '/opt/airflow/data/towers_clean.csv'
    output_file = '/opt/airflow/data/weather_latest.csv'

    if not os.path.exists(towers_file):
        print(f"Towers file not found: {towers_file}")
        return

    df = pd.read_csv(towers_file)

    # Sample 100 towers per run to avoid API rate limits
    sample = df.sample(min(100, len(df)))

    results = []
    for _, row in sample.iterrows():
        lat = row['lat']
        lon = row['lon']
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
                results.append({
                    "tower_id":    row['tower_id'],
                    "operator":    row['operator'],
                    "lat":         lat,
                    "lon":         lon,
                    "temperature": current.get("temperature_2m"),
                    "wind_speed":  current.get("wind_speed_10m"),
                    "rain":        current.get("rain"),
                    "cloud_cover": current.get("cloud_cover"),
                    "fetched_at":  datetime.utcnow().isoformat(),
                })
        except Exception as e:
            print(f"Failed for tower {row['tower_id']}: {e}")

    weather_df = pd.DataFrame(results)
    weather_df.to_csv(output_file, index=False)
    print(f"Saved weather data for {len(results)} towers to {output_file}")

with DAG(
    dag_id='weather_batch',
    default_args=default_args,
    description='Hourly weather batch fetch for sampled towers',
    schedule_interval='0 * * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['weather', 'batch'],
) as dag:

    fetch = PythonOperator(
        task_id='fetch_weather_batch',
        python_callable=fetch_weather_batch,
    )