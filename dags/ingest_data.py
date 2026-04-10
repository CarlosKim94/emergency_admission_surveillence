from datetime import datetime
from airflow import DAG # For defining the DAG
from airflow.operators.bash import BashOperator # For running bash commands
from airflow.operators.python import PythonOperator # For running Python functions
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator
import pandas as pd

# --- CONFIGURATION ---
PROJECT_ID = "emergency-admission-492214"
BUCKET = "emergency-admission-data"
GCP_CONN_ID = "google_cloud_default" # The name in Airflow UI
REGION = "europe-west1"

# Data URL
DATA_URL = "https://raw.githubusercontent.com/robert-koch-institut/Daten_der_Notaufnahmesurveillance/refs/heads/main/Notaufnahmesurveillance_Zeitreihen_Syndrome.tsv" 
FILENAME = "emergency_data_{{ ds }}.parquet"
LOCAL_PATH = "/opt/airflow/" + FILENAME
CLUSTER_NAME = "emergency-spark-cluster"
PYSPARK_JOB_PATH = f"gs://{BUCKET}/code/transform.py"
# ---------------------

def upload_to_gcs(bucket, object_name, local_file):
    """
    Uploads the local file to GCS Bucket
    """
    # This automatically uses the connection in the Airflow UI
    hook = GCSHook(gcp_conn_id='google_cloud_default')
    print(f"Uploading {local_file} to gs://{bucket}/{object_name}")
    hook.upload(
        bucket_name=bucket,
        object_name=object_name,
        filename=local_file
    )

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="ingest_emergency_data_v2",
    schedule_interval="@weekly",
    default_args=default_args,
    catchup=False,
    tags=['emergency-surveillence'],
) as dag:

    # Task 1: Download and convert to Parquet (saving space/cost)
    download_task = BashOperator(
        task_id="download_and_convert",
        # Using a python one-liner to download and convert to parquet immediately
        bash_command=f"python3 -c \"import pandas as pd; df = pd.read_csv('{DATA_URL}', sep='\\t'); df.to_parquet('{LOCAL_PATH}')\" "
    )

    # Task 2: Upload to GCS
    upload_task = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": "raw/" + FILENAME,
            "local_file": LOCAL_PATH,
        },
    )

    # Task 3: Load to BigQuery as raw back up
    load_to_bq_task = GCSToBigQueryOperator(
        task_id='load_to_bq_raw',
        bucket='emergency-admission-data',
        # Path to the file we just uploaded
        source_objects=["raw/emergency_data_{{ ds }}.parquet"],
        # Table name will be 'admissions_raw' inside your dataset
        destination_project_dataset_table=f"{PROJECT_ID}.emergency_admission_data.admissions_raw",
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE', # Replaces data if you re-run for the same day
        autodetect=True,                   # BigQuery will figure out the columns
        gcp_conn_id='google_cloud_default'
    )

    # Task 4: Spark Task
    spark_transform = DataprocSubmitPySparkJobOperator(
        task_id="spark_transform",
        main=PYSPARK_JOB_PATH,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        # This JAR allows Spark to talk to BigQuery
        dataproc_jars=["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"], 
        gcp_conn_id='google_cloud_default'
    )

    # Task 5: Cleanup local file to keep disk space
    cleanup_task = BashOperator(
        task_id="cleanup_local",
        bash_command=f"rm {LOCAL_PATH}"
    )

    download_task >> upload_task >> load_to_bq_task >> spark_transform >> cleanup_task