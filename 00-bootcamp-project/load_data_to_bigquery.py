import configparser
import json
import os
from datetime import datetime

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


def get_credentials():
    '''Get credential from file'''
    keyfile = os.environ.get("KEYFILE_PATH")
    with open(keyfile, "r") as f:
        service_account_info = json.load(f)
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    return credentials


def create_job_config(_meta_data):
    ''' crate job config for load data to bigquery'''
    if _meta_data["partitioning"]:
        time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=_meta_data["partitioning"],
        )
    else:
        time_partitioning = None

    _job_config = bigquery.LoadJobConfig(
        skip_leading_rows=0,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
        time_partitioning=time_partitioning
        # clustering_fields=["first_name", "last_name"],
    )
    return _job_config


parser = configparser.ConfigParser()
parser.read("pipeline.conf")
project_id = parser.get("bigquery_config", "project_id")
dataset = parser.get("bigquery_config", "dataset")

credentials = get_credentials()
client = bigquery.Client(project_id, credentials)

with open("metadata.json", "r") as f:
    tables_meta = json.load(f)

for file_name, meta_data in tables_meta.items():

    file_path = f"data/{file_name}.csv"
    df = pd.read_csv(file_path, parse_dates=meta_data["parse_dates"])
    df.info()

    table_id = f"{project_id}.{dataset}.{file_name}"
    job_config = create_job_config(meta_data)
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")