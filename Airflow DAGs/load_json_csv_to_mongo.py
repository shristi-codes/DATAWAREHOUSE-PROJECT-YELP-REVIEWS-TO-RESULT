from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime
import json
import csv

def load_json_to_mongo():
    filepath = '/Users/abhishekkumar/Downloads/Yelp JSON/yelp_dataset/yelp_academic_dataset_checkin.json'
    
    with open(filepath, 'r') as f:
        documents = [json.loads(line) for line in f]

    mongo_hook = MongoHook(conn_id='mongo_default')
    collection = mongo_hook.get_collection('Yelp_Review_Dataset', 'Datawarehouse_Project')
    collection.insert_many(documents)
    print(f"Inserted {len(documents)} JSON records.")

def load_csv_to_mongo():
    filepath = '/Users/abhishekkumar/Downloads/yelp_review_dataset_csv.csv'

    with open(filepath, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        documents = list(reader)

    mongo_hook = MongoHook(conn_id='mongo_default')
    collection = mongo_hook.get_collection('Yelp_Review_Dataset', 'Datawarehouse_Project')
    collection.insert_many(documents)
    print(f"Inserted {len(documents)} CSV records.")

default_args = {
    'owner': 'abhishek',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='load_json_csv_to_mongo',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'mongo'],
) as dag:

    load_json = PythonOperator(
        task_id='load_json_file',
        python_callable=load_json_to_mongo
    )

    load_csv = PythonOperator(
        task_id='load_csv_file',
        python_callable=load_csv_to_mongo
    )

    load_json >> load_csv

