import time
import pandas as pd
from google.cloud import storage
from google.cloud import spanner


def _gcs_to_spanner(bucket_name, file_name, spanner_instance_id, database_id, table_name):

   # Initialize the GCS and Spanner clients
   storage_client = storage.Client()
   spanner_client = spanner.Client()
   # Get the bucket and the blob (file)
   bucket = storage_client.bucket(bucket_name)
   blob = bucket.blob(file_name)
   # Download the CSV file content as a string
   csv_content = blob.download_as_text()
   # Read the CSV content into a DataFrame
   df = pd.read_csv(pd.compat.StringIO(csv_content))
   # Connect to the Spanner instance and database
   instance = spanner_client.instance(spanner_instance_id)
   database = instance.database(database_id)
   # Process the DataFrame and insert rows into Spanner
   def insert_rows(transaction):
       for index, row in df.iterrows():
           transaction.insert(
               table=table_name,
               columns=list(df.columns),
               values=[tuple(row)]
           )
   # Execute the insert operation in a Spanner transaction
   database.run_in_transaction(insert_rows)
   print("CSV data has been processed and inserted into Spanner.")
