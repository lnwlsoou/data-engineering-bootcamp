import configparser
import csv

import requests


parser = configparser.ConfigParser()
parser.read("pipeline.conf")
host = parser.get("api_config", "host")
port = parser.get("api_config", "port")

API_URL = f"http://{host}:{port}"
DATA_FOLDER = "data-api"

tables_name = [
    "addresses",
    "events",
    "order-items",
    "orders",
    "products",
    "promos",
    "users",
]

date = "2021-02-10"
for table_name in tables_name:
    try:
        # uri = f"{API_URL}/{table_name}/?created_at={date}"
        uri = f"{API_URL}/{table_name}/"
        print("Request", uri)
        response = requests.get(uri)
        data = response.json()
        with open(f"{DATA_FOLDER}/{table_name}.csv", "w") as f:
            writer = csv.writer(f)
            header = data[0].keys()
            writer.writerow(header)

            for each in data:
                writer.writerow(each.values())
    except Exception as e:
        print(f"{table_name} error: {e}")
