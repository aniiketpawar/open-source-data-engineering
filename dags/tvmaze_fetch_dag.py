# /dags/tvmaze_fetch_dag.py

import json
import os
from datetime import datetime

from airflow.datasets import Dataset
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator

# --- PATHS UPDATED ---
# The paths now point to the standardized location /opt/spark-data/
# which is shared across the Airflow and Spark containers.
raw_shows_dataset = Dataset("file:///opt/spark-data/raw/tv_shows.json")
RAW_DATA_DIR = "/opt/spark-data/raw"

def _save_shows(ti) -> None:
    """
    Saves the fetched TV shows data to a JSON file.
    """
    shows = ti.xcom_pull(task_ids=['get_shows'])
    
    if not shows or not shows[0]:
        raise ValueError("No data received from the API.")

    os.makedirs(RAW_DATA_DIR, exist_ok=True)

    execution_date_str = ti.execution_date.strftime('%Y-%m-%d')
    output_filepath = f"{RAW_DATA_DIR}/tv_shows_{execution_date_str}.json"
    
    print(f"Saving shows to: {output_filepath}")

    with open(output_filepath, "w") as f:
        json.dump(json.loads(shows[0]), f, indent=4)


with DAG(
    dag_id="tvmaze_fetch_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["data_engineering", "tvmaze"],
) as dag:

    is_api_available = HttpSensor(
        task_id="is_api_available",
        http_conn_id="tvmaze_api",
        endpoint="shows?page=1",
    )

    get_shows = SimpleHttpOperator(
        task_id="get_shows",
        http_conn_id="tvmaze_api",
        endpoint="shows?page=1",
        method="GET",
        response_filter=lambda response: response.text,
    )

    save_shows = PythonOperator(
        task_id="save_shows",
        python_callable=_save_shows,
        outlets=[raw_shows_dataset],
    )

    is_api_available >> get_shows >> save_shows
