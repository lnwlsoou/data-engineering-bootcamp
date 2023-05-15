from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
import requests
import csv

# Import modules regarding GCP service account, BigQuery, and GCS 
# Your code here
import json
import os
import sys

from google.api_core import exceptions
from google.cloud import storage
from google.oauth2 import service_account


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""

    keyfile = "/opt/airflow/dags/effective-sonar-384416-9d9b0d1f4d72.json"
    service_account_info = json.load(open(keyfile))
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    project_id = "effective-sonar-384416"

    storage_client = storage.Client(
        project=project_id,
        credentials=credentials,
    )
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )



def _extract_data(ds):
    # date = "2021-02-10"
    table_name = "events"

    API_URL = "http://34.87.139.82:8000"
    DATA_FOLDER = f"/opt/airflow/dags/data/{table_name}"
    uri = f"{API_URL}/{table_name}/?created_at={ds}"
    print("Request", uri)

    response = requests.get(uri)
    data = response.json()

    if len(data):
        with open(f"{DATA_FOLDER}/{table_name}-{ds}.csv", "w") as f:
            writer = csv.writer(f)
            header = data[0].keys()
            writer.writerow(header)

            for each in data:
                writer.writerow(each.values())


def _load_data_to_gcs(ds):
    bucket_name = "deb-bootcamp-100038"
    source_file_name = f"/opt/airflow/dags/data/{table_name}/{table_name}-{ds}.csv"
    destination_blob_name = f"deb-bootcamp-100038/greenery/{table_name}/{ds}/{table_name}.csv"
    upload_blob(bucket_name, source_file_name, destination_blob_name)


def _load_data_from_gcs_to_bigquery():
    # Your code below
    pass


default_args = {
    "owner": "airflow",
		"start_date": timezone.datetime(2023, 5, 1),  # Set an appropriate start date here
}
with DAG(
    dag_id="greenery_events_data_pipeline",  # Replace xxx with the data name
    default_args=default_args,
    schedule=None,  # Set your schedule here
    catchup=False,
    tags=["DEB", "2023", "greenery"],
):

    # Extract data from Postgres, API, or SFTP
    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=_extract_data,
        op_kwargs={
            "ds": "{{ ds }}"
        }
    )

    # Load data to GCS
    load_data_to_gcs = PythonOperator(
        task_id="load_data_to_gcs",
        python_callable=_load_data_to_gcs,
        op_kwargs={
            "ds": "{{ ds }}"
        }
    )

    # Load data from GCS to BigQuery
    load_data_from_gcs_to_bigquery = PythonOperator(
        task_id="load_data_from_gcs_to_bigquery",
        python_callable=_load_data_from_gcs_to_bigquery,
    )

    # Task dependencies
    extract_data >> load_data_to_gcs >> load_data_from_gcs_to_bigquery