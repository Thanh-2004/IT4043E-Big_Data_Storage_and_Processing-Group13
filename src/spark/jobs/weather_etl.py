from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Default settings for all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'weather_bigdata_pipeline',
    default_args=default_args,
    description='Submit Weather ETL to Spark Cluster',
    schedule_interval=None,  # Manual trigger for now
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['spark', 'weather'],
) as dag:

    # Task: Submit the Spark Job
    submit_etl = SparkSubmitOperator(
        task_id='submit_etl_job',
        # This connection ID comes from your env var AIRFLOW_CONN_SPARK_STANDALONE
        conn_id='spark_standalone', 
        
        # Path where the script lives INSIDE the Airflow container (via volume mount)
        application='/opt/airflow/spark_jobs/weather_etl.py',
        
        # Dependencies (Kafka + Mongo Connectors)
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0',
        
        # Spark Configuration
        conf={
            "spark.master": "spark://spark-master-svc.default.svc.cluster.local:7077",
            "spark.submit.deployMode": "client", # Run driver in the Airflow pod (easier logs)
            "spark.driver.bindAddress": "0.0.0.0"
        },
        verbose=True
    )

    submit_etl