import json
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define the function that acts as the "Client"
def submit_job_via_rest():
    # 1. Define the endpoint (Service Name inside K8s)
    # port 6066 is the REST Submission API
    url = "http://spark-master-svc.default.svc.cluster.local:6066/v1/submissions/create"
    
    # 2. Define the payload (The "Shopping List")
    payload = {
        "action": "CreateSubmissionRequest",
        "appArgs": [],
        "appResource": "file:/opt/spark/jobs/spark_test.py", # Path inside the SPARK container
        "clientSparkVersion": "3.5.0",
        "mainClass": "org.apache.spark.deploy.SparkSubmit",
        "environmentVariables": {"SPARK_ENV_LOADED": "1"},
        "sparkProperties": {
            "spark.jars.packages": "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1", # Pre-loading Mongo for later
            "spark.driver.supervise": "false",
            "spark.app.name": "AirflowRestTest",
            "spark.submit.deployMode": "cluster", # Driver runs on Worker, not Master
            "spark.master": "spark://spark-master-svc:7077"
        }
    }

    # 3. Send the request
    headers = {'Content-Type': 'application/json;charset=UTF-8'}
    print(f"Sending request to {url}...")
    
    try:
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        print("Response Code:", response.status_code)
        print("Response Body:", response.text)
        
        # Check if Spark accepted it
        if response.status_code != 200:
            raise Exception(f"Submission failed: {response.text}")
            
        response_json = response.json()
        if not response_json.get('success'):
            raise Exception(f"Spark rejected the job: {response_json}")
            
        print(f"Job submitted successfully! Submission ID: {response_json.get('submissionId')}")
        
    except requests.exceptions.ConnectionError:
        raise Exception("Could not connect to Spark Master. Is the service 'spark-master-svc' running in 'default' namespace?")

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0
}

with DAG('01_spark_rest_test',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    trigger_spark = PythonOperator(
        task_id='trigger_spark_pi',
        python_callable=submit_job_via_rest
    )