from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def refresh_towers():
    source = '/opt/airflow/data/towers_raw.csv'
    output = '/opt/airflow/data/towers_clean.csv'

    if not os.path.exists(source):
        print(f"Source file not found: {source}")
        return

    df = pd.read_csv(source, low_memory=False)
    df.columns = df.columns.str.strip()

    # Filter Egypt towers only
    df = df[df['mcc'] == 602]

    # Remove unknown operators
    df = df[~df['net'].isin([10, 11])]

    # Map operator names
    operator_map = {1: 'Orange', 2: 'Vodafone', 3: 'e&', 4: 'WE'}
    df['operator'] = df['net'].map(operator_map)

    # Rename columns
    df = df.rename(columns={
        'cell': 'tower_id',
        'lon': 'lon',
        'lat': 'lat',
        'range': 'range_m',
        'radio': 'radio',
        'area': 'area',
    })

    df = df[['tower_id', 'operator', 'radio', 'lat', 'lon', 'area']]
    df = df.dropna(subset=['lat', 'lon'])
    df.to_csv(output, index=False)
    print(f"Saved {len(df)} towers to {output}")

with DAG(
    dag_id='opencellid_refresh',
    default_args=default_args,
    description='Monthly refresh of OpenCelliD tower data',
    schedule_interval='0 0 1 * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['data', 'opencellid'],
) as dag:

    refresh = PythonOperator(
        task_id='refresh_towers',
        python_callable=refresh_towers,
    )