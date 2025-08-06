from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
from google.cloud import storage
import csv
import signal

# MongoDB Atlas connection
MONGO_URI = "mongodb+srv://shristikumar:Datawarehouse123@datawarehouseproject.jqpbsqk.mongodb.net"
DB_NAME = "Datawarehouse_Project"
COLLECTION_NAME = "Yelp_Industry_Only"

# GCP
BUCKET_NAME = "dw_final_project"
LOCAL_FILE = "/tmp/yelp_industry_only.csv"
GCP_BLOB_NAME = "exports/yelp_industry_only.csv"

# Timeout class for GCP upload
class TimeoutException(Exception):
    pass

def handler(signum, frame):
    raise TimeoutException("GCP upload timed out!")

def export_yelp_industry_only():
    print(f"🔁 Starting export of collection: {COLLECTION_NAME}")

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

        print(f"✅ Export complete: {i} rows written")

    # Upload to GCP with timeout
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(60)  # 60-second timeout

    try:
        print(f"📤 Starting upload to GCP: {LOCAL_FILE} → {GCP_BLOB_NAME}")
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(GCP_BLOB_NAME)
        blob.upload_from_filename(LOCAL_FILE)
        signal.alarm(0)  # cancel the timeout
        print(f"✅ Upload complete: {GCP_BLOB_NAME}")
    except TimeoutException:
        print("❌ Upload timed out after 60 seconds")
        raise
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        raise

# Airflow DAG setup
default_args = {
    'owner': 'abhishek',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='export_yelp_industry_to_gcp',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['mongo', 'gcp', 'export'],
) as dag:

    export_task = PythonOperator(
        task_id='export_yelp_industry_only',
        python_callable=export_yelp_industry_only,
        execution_timeout=timedelta(minutes=5)  # Airflow-level timeout safeguard
    )

