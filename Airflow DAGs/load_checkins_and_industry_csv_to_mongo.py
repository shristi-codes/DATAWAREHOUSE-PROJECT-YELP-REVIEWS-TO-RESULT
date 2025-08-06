from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime
import csv

def load_csv_to_mongo(filepath, collection_name):
    with open(filepath, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        batch = []
        batch_size = 1000
        count = 0

        mongo_hook = MongoHook(conn_id='mongo_default')
        collection = mongo_hook.get_collection(collection_name, 'Datawarehouse_Project')

        for row in reader:
            batch.append(row)
            if len(batch) == batch_size:
                collection.insert_many(batch)
                count += len(batch)
                print(f"Inserted batch of {len(batch)} — Total so far: {count}")
                batch = []

        if batch:
            collection.insert_many(batch)
            count += len(batch)
            print(f"Inserted final batch of {len(batch)} — Total inserted: {count}")

def load_checkins():
    filepath = r'/Users/abhishekkumar/Downloads/Yelp JSON/yelp_business_checkins_transformed.csv'
    load_csv_to_mongo(filepath, 'Yelp_Checkins_Industry_Combined')

def load_industry():
    filepath = r'/Users/abhishekkumar/Downloads/Yelp JSON/Yelp_Industry_Dataset.csv'
    load_csv_to_mongo(filepath, 'Yelp_Industry_Only')

default_args = {
    'owner': 'abhishek',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='load_checkins_and_industry_csv_to_mongo',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'mongo', 'csv'],
) as dag:

    load_checkins_task = PythonOperator(
        task_id='load_checkins_csv',
        python_callable=load_checkins
    )

    load_industry_task = PythonOperator(
        task_id='load_industry_csv',
        python_callable=load_industry
    )

    load_checkins_task >> load_industry_task

