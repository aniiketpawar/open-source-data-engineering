# /dags/spark_process_dag.py

from datetime import datetime

from airflow.datasets import Dataset
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

raw_shows_dataset = Dataset("file:///opt/spark-data/raw/tv_shows.json")

S3_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

# --- FINAL FIX ---
# Add the spark.master configuration to the conf dictionary.
# This is the correct way to specify the master for the Spark job.
S3_CONFIG = {
    "spark.master": "spark://spark-master:7077",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "admin",
    "spark.hadoop.fs.s3a.secret.key": "password",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
}

with DAG(
    dag_id="spark_process_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=[raw_shows_dataset],
    catchup=False,
    tags=["data_engineering", "spark"],
) as dag:
    
    submit_spark_job = SparkSubmitOperator(
        task_id="submit_spark_job",
        application="/opt/spark-apps/process_shows.py",
        conn_id="spark_default",
        verbose=True,
        packages=S3_PACKAGES,
        conf=S3_CONFIG,
    )
