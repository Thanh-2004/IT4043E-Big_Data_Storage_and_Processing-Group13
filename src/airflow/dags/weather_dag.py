from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import socket
import os

# 1. FORCE the correct connection string (overrides UI defaults)
os.environ["AIRFLOW_CONN_SPARK_STANDALONE"] = "spark://spark-master-svc.default.svc.cluster.local:7077"

# 2. Get Airflow's IP for the callback (Network Fix)
airflow_ip = socket.gethostbyname(socket.gethostname())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

with DAG(
    'weather_bigdata_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    submit_etl = SparkSubmitOperator(
        task_id='submit_etl_job',
        conn_id='spark_standalone',
        application='/opt/airflow/spark_jobs/weather_etl.py',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0',
        conf={
            "spark.submit.deployMode": "client",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.driver.host": airflow_ip,
            "spark.jars.ivy": "/tmp/.ivy_clean"
        },
        verbose=True
    )