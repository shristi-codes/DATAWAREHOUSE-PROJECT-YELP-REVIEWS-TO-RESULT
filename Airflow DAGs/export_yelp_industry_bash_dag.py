from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
import csv

# === MongoDB Config ===
MONGO_URI = "mongodb+srv://shristikumar:Datawarehouse123@datawarehouseproject.jqpbsqk.mongodb.net"
DB_NAME = "Datawarehouse_Project"
COLLECTION_NAME = "Yelp_Industry_Only"

# === File & GCP Info ===
LOCAL_FILE = "/tmp/yelp_industry_only.csv"
BUCKET_NAME = "dw_final_project"
GCP_BLOB_NAME = "exports/yelp_industry_only.csv"
GCP_UPLOAD_CMD = f"gsutil cp {LOCAL_FILE} gs://{BUCKET_NAME}/{GCP_BLOB_NAME}"

# === Export Function ===
def export_to_csv():
    print(f"🔁 Exporting {COLLECTION_NAME} to {LOCAL_FILE}")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    with open(LOCAL_FILE, "w", newline='', encoding='utf-8') as csvfile:
        writer = None
        cursor = collection.find().sort("_id")

        for i, doc in enumerate(cursor, start=1):
            doc.pop('_id', None)
            if writer is None:
                writer = csv.DictWriter(csvfile, fieldnames=doc.keys())
                writer.writeheader()
            writer.writerow(doc)

            if i % 1000 == 0:
                print(f"✅ Processed {i} rows...")

    print(f"✅ Export complete: {i} total rows")

# === DAG Setup ===
default_args = {
    'owner': 'abhishek',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='export_yelp_industry_csv_to_gcp',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['mongo', 'bash', 'gsutil'],
) as dag:

    export_csv = PythonOperator(
        task_id='export_csv_from_mongo',
        python_callable=export_to_csv,
        execution_timeout=timedelta(minutes=3)
    )

    upload_to_gcp = BashOperator(
        task_id='upload_csv_to_gcp',
        bash_command=GCP_UPLOAD_CMD
    )

    export_csv >> upload_to_gcp

