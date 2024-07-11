from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage

# Define constants
PROJECT_ID = 'your-project-id'
BUCKET_NAME = 'your-bucket-name'
LOCATION = 'US'
CMEK_KEY_NAME = 'projects/your-project-id/locations/global/keyRings/your-key-ring/cryptoKeys/your-key'

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'create_gcs_bucket_with_cmek',
    default_args=default_args,
    description='A simple DAG to create a GCS bucket with CMEK',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 1),
    catchup=False,
)

def apply_cmek_to_bucket(bucket_name, cmek_key_name):
    """Applies CMEK to an existing GCS bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    bucket.default_kms_key_name = cmek_key_name
    bucket.patch()

# Task to create a GCS bucket
create_bucket = GCSCreateBucketOperator(
    task_id='create_bucket',
    bucket_name=BUCKET_NAME,
    storage_class='STANDARD',
    location=LOCATION,
    project_id=PROJECT_ID,
    dag=dag,
)

# Task to apply CMEK to the bucket
apply_cmek = PythonOperator(
    task_id='apply_cmek_to_bucket',
    python_callable=apply_cmek_to_bucket,
    op_args=[BUCKET_NAME, CMEK_KEY_NAME],
    dag=dag,
)

# Set task dependencies
create_bucket >> apply_cmek
