from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.utils.dates import days_ago

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Define the DAG
with DAG(
    dag_id='create_bq_dataset',
    default_args=default_args,
    schedule_interval=None,  # Set the schedule interval as needed
    catchup=False,
) as dag:

    # Task to create a BigQuery dataset
    create_bq_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_bq_dataset',
        dataset_id='your_dataset_id',  # Replace with your dataset ID
        project_id='your_project_id',  # Replace with your GCP project ID
        location='US',  # Specify the dataset location
    )

    create_bq_dataset
