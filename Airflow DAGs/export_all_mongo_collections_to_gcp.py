from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pymongo import MongoClient
from google.cloud import storage
import csv
import os

# MongoDB Atlas connection
MONGO_URI = "mongodb+srv://shristikumar:Datawarehouse123@datawarehouseproject.jqpbsqk.mongodb.net"
DB_NAME = "Datawarehouse_Project"

# GCP bucket name
BUCKET_NAME = "dw_final_project"

# Function to export MongoDB collection in chunks and upload to GCP
def export_and_upload(collection_name, file_name, gcp_blob_name):
    print(f"🔁 Exporting full {collection_name} to {file_name}")

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[collection_name]

    batch_size = 5000
    total_docs = collection.count_documents({})
    written_rows = 0

    with open(file_name, "w", newline='', encoding='utf-8') as csvfile:
        writer = None

        for batch_index in range(0, total_docs, batch_size):
            batch = collection.find().sort("_id").skip(batch_index).limit(batch_size)
            rows = []

            for doc in batch:
                doc.pop('_id', None)
                rows.append(doc)

            if not rows:
                break

            if writer is None:
                writer = csv.DictWriter(csvfile, fieldnames=rows[0].keys())
                writer.writeheader()

            writer.writerows(rows)
            written_rows += len(rows)
            print(f"✅ Wrote {written_rows} rows so far from {collection_name}")

    # Upload to GCP
    gcp = storage.Client()
    bucket = gcp.bucket(BUCKET_NAME)
    blob = bucket.blob(gcp_blob_name)
    blob.upload_from_filename(file_name)
    print(f"✅ Uploaded {file_name} to GCP as {gcp_blob_name}")

default_args = {
    'owner': 'abhishek',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='export_all_mongo_collections_to_gcp',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['mongo', 'gcp', 'export'],
) as dag:

    collections = {
        'Yelp_Review_With_Sentiment': (
            '/tmp/yelp_review_with_sentiment.csv',
            'exports/yelp_review_with_sentiment.csv'
        ),
        'Yelp_Industry_Only': (
            '/tmp/yelp_industry_only.csv',
            'exports/yelp_industry_only.csv'
        ),
        'Yelp_Checkins_Industry_Combined': (
            '/tmp/yelp_checkins_combined.csv',
            'exports/yelp_checkins_combined.csv'
        ),
    }

    for collection, (file_path, blob_name) in collections.items():
        PythonOperator(
            task_id=f'export_upload_{collection.lower()}',
            python_callable=export_and_upload,
            op_args=[collection, file_path, blob_name]
        )

