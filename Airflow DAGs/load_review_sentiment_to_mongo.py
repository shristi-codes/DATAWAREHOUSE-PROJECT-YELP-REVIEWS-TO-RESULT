from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime
import csv

def load_sentiment_csv_to_mongo():
    filepath = r'/Users/abhishekkumar/Downloads/Yelp JSON/yelp_review_dataset_with_sentiment.csv'
    batch_size = 1000

    with open(filepath, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        batch = []
        count = 0

        mongo_hook = MongoHook(conn_id='mongo_default')
        collection = mongo_hook.get_collection('Yelp_Review_With_Sentiment', 'Datawarehouse_Project')

        for row in reader:
            batch.append(row)
            if len(batch) == batch_size:
                collection.insert_many(batch)
                count += len(batch)
                print(f"Inserted batch — Total so far: {count}")
                batch = []

        if batch:
            collection.insert_many(batch)
            count += len(batch)
            print(f"Inserted final batch — Total inserted: {count}")

default_args = {
    'owner': 'abhishek',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='load_review_sentiment_to_mongo',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['mongo', 'sentiment'],
) as dag:

    load_sentiment_task = PythonOperator(
        task_id='load_sentiment_csv',
        python_callable=load_sentiment_csv_to_mongo
    )

