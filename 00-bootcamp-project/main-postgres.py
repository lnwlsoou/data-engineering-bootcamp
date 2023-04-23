import csv
import configparser

import psycopg2


parser = configparser.ConfigParser()
parser.read("pipeline.conf")
dbname = parser.get("postgres_config", "database")
user = parser.get("postgres_config", "username")
password = parser.get("postgres_config", "password")
host = parser.get("postgres_config", "host")
port = parser.get("postgres_config", "port")

conn_str = f"dbname={dbname} user={user} password={password} host={host} port={port}"
conn = psycopg2.connect(conn_str)
cursor = conn.cursor()

DATA_FOLDER = "data-postgres"

tables_meta = {
    "addresses": ["address_id", "address", "zipcode", "state", "country"],
    "events": ["event_id", "session_id", "page_url", "created_at", "event_type", "user", "order", "product"],
    "order_items": ["order_id", "product_id", "quantity"],
    "orders": ["order_id", "created_at", "order_cost", "shipping_cost", "order_total", "tracking_id", "shipping_service", "estimated_delivery_at", "delivered_at", "status", "user", "promo", "address"],
    "products": ["product_id", "name", "price", "inventory"],
    "promos": ["promo_id", "discount", "status"],
    "users": ["user_id", "first_name", "last_name", "email", "phone_number", "created_at", "updated_at", "address_id"],
}

date = "2021-02-10"
for table_name, header in tables_meta.items():
    with open(f"{DATA_FOLDER}/{table_name}.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(header)

        query = f"select * from {table_name}"
        cursor.execute(query)

        results = cursor.fetchall()
        for each in results:
            writer.writerow(each)