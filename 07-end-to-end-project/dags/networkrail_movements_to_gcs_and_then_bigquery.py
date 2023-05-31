import csv
import json
import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone

from google.cloud import bigquery, storage
from google.oauth2 import service_account


DAGS_FOLDER = "/opt/airflow/dags"
BUSINESS_DOMAIN = "networkrail"
DATA = "movements"
LOCATION = "asia-southeast1"
PROJECT_ID = "effective-sonar-384416"
GCS_BUCKET = "deb-bootcamp-100038"
BIGQUERY_DATASET = "networkrail"
KEYFILE = "effective-sonar-384416-9b7cda7f9d30-bigquery-and-gcs.json"


def load_google_credential():
    service_account_info = json.load(open(f"{DAGS_FOLDER}/{KEYFILE}"))
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    return credentials


def _extract_data(**context):
    ds = context["data_interval_start"].to_date_string()

    # Use PostgresHook to query data from Postgres database
    pg_hook = PostgresHook(
        postgres_conn_id="networkrail_postgres_conn",
        schema="networkrail"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = f"""
        select * from movements where date(actual_timestamp) = '{ds}'
    """
    cursor.execute(sql)
    rows = cursor.fetchall()

    if rows:
        with open(f"{DAGS_FOLDER}/movements_metadata.json") as f:
            metadata = json.load(f)
        header = metadata.keys()
        with open(f"{DAGS_FOLDER}/{DATA}-{ds}.csv", "w") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            for row in rows:
                logging.info(row)
                writer.writerow(row)

        return "load_data_to_gcs"
    else:
        return "do_nothing"



def _load_data_to_gcs(**context):
    ds = context["data_interval_start"].to_date_string()

    storage_client = storage.Client(
        project=PROJECT_ID,
        credentials=load_google_credential(),
    )
    bucket = storage_client.bucket(GCS_BUCKET)

    destination_blob_name = f"{BUSINESS_DOMAIN}/{DATA}/{ds}/{DATA}.csv"
    blob = bucket.blob(destination_blob_name)

    source_file_name = f"{DAGS_FOLDER}/{DATA}-{ds}.csv"
    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )
    


def _load_data_from_gcs_to_bigquery(**context):
    ds = context["data_interval_start"].to_date_string()

    source_blob_name = f"{BUSINESS_DOMAIN}/{DATA}/{ds}/{DATA}.csv"
    gsutil_uri = f"gs://{GCS_BUCKET}/{source_blob_name}"

    bq_client = bigquery.Client(
        credentials=load_google_credential(),
    )
    table_id = f"{PROJECT_ID}.{BUSINESS_DOMAIN}.{DATA}${ds.replace('-', '')}"

    with open(f"{DAGS_FOLDER}/movements_metadata.json") as f:
        metadata = json.load(f)
    map_type = {
        "STRING": bigquery.enums.SqlTypeNames.STRING,
        "TIMESTAMP": bigquery.enums.SqlTypeNames.TIMESTAMP,
        "INTEGER": bigquery.enums.SqlTypeNames.INTEGER,
        "BOOLEAN": bigquery.enums.SqlTypeNames.BOOLEAN,
    }
    bigquery_schema = []
    for column, data_type in metadata.items():
        map_type[data_type]
        sh = bigquery.SchemaField(column,  map_type[data_type])
        bigquery_schema.append(sh)

    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        schema=bigquery_schema,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="actual_timestamp",
        ),
    )

    load_job = bq_client.load_table_from_uri(
        gsutil_uri, table_id, job_config=job_config
    )
    load_job.result()

    print(
        f"File {gsutil_uri} uploaded to bigquery {table_id} success"
    )




default_args = {
    "owner": "sopap",
    "start_date": timezone.datetime(2023, 5, 1),
}
with DAG(
    dag_id="networkrail_movements_to_gcs_and_then_bigquery",
    default_args=default_args,
    schedule="@hourly",  # Set the schedule here
    catchup=False,
    tags=["DEB", "2023", "networkrail"],
    max_active_runs=3,
):

    # Start
    start = EmptyOperator(task_id="start")

    # Extract data from NetworkRail Postgres Database
    extract_data = BranchPythonOperator(
        task_id="extract_data", 
        python_callable=_extract_data
        )

    # Do nothing
    do_nothing = EmptyOperator(task_id="do_nothing")

    # Load data to GCS
    load_data_to_gcs = PythonOperator(
        task_id="load_data_to_gcs",
        python_callable=_load_data_to_gcs
        )

    # Load data from GCS to BigQuery
    load_data_from_gcs_to_bigquery = PythonOperator(
        task_id="load_data_from_gcs_to_bigquery",
        python_callable=_load_data_from_gcs_to_bigquery
        )

    # End
    end = EmptyOperator(task_id="end", trigger_rule="one_success")

    # Task dependencies
    start >> extract_data >> load_data_to_gcs >> load_data_from_gcs_to_bigquery >> end
    extract_data >> do_nothing >> end