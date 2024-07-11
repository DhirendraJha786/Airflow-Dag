from datetime import datetime, timedelta
from airflow import DAG
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

def create_bucket_with_cmek(bucket_name, cmek_key_name, location, project_id):
    """Creates a GCS bucket with CMEK."""
    storage_client = storage.Client(project=project_id)
    bucket = storage.Bucket(storage_client, name=bucket_name)
    bucket.location = location
    bucket.iam_configuration.uniform_bucket_level_access_enabled = True  # Optional: Enable uniform bucket-level access
    bucket.default_kms_key_name = cmek_key_name
    bucket.create()

# Task to create a GCS bucket with CMEK
create_bucket = PythonOperator(
    task_id='create_bucket_with_cmek',
    python_callable=create_bucket_with_cmek,
    op_args=[BUCKET_NAME, CMEK_KEY_NAME, LOCATION, PROJECT_ID],
    dag=dag,
)

# In this case, there is no need for an additional task to apply CMEK as it's done during bucket creation
