from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime
import csv

def load_combined_csv_to_mongo():
    filepath = r'/Users/abhishekkumar/Downloads/yelp_review_dataset_business and review combined_csv.csv'

    with open(filepath, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        batch = []
        batch_size = 1000
        count = 0

        mongo_hook = MongoHook(conn_id='mongo_default')
        collection = mongo_hook.get_collection('Yelp_Business_Review_Combined', 'Datawarehouse_Project')

        for row in reader:
            batch.append(row)
            if len(batch) == batch_size:
                collection.insert_many(batch)
                count += len(batch)
                print(f"Inserted batch of {len(batch)} — Total so far: {count}")
                batch = []

        # insert any leftovers
        if batch:
            collection.insert_many(batch)
            count += len(batch)
            print(f"Inserted final batch of {len(batch)} — Total inserted: {count}")

default_args = {
    'owner': 'abhishek',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='load_combined_review_business_csv_to_mongo',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['mongo', 'etl', 'combined'],
) as dag:

    load_combined_csv = PythonOperator(
        task_id='load_combined_csv_file',
        python_callable=load_combined_csv_to_mongo
    )
