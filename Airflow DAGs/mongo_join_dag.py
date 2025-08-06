from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pymongo import MongoClient

def run_mongo_merge():
    client = MongoClient("mongodb://localhost:27017")
    db = client["Datawarehouse_Project"]
    reviews = db["Yelp_Review_Dataset"]

    pipeline = [
        { "$lookup": {
            "from": "Business Dataset",  # EXACT name as in MongoDB
            "localField": "business_id",
            "foreignField": "business_id",
            "as": "business_info"
        }},
        { "$unwind": "$business_info" },
        { "$merge": {
            "into": "Joined_Yelp_Businesses",
            "whenMatched": "merge",
            "whenNotMatched": "insert"
        }}
    ]

    reviews.aggregate(pipeline)
    print("✅ Merge complete: data written to 'Joined_Yelp_Businesses'")

default_args = {
    'owner': 'abhishek',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id="mongo_merge_reviews_and_businesses",
    default_args=default_args,
    description="Join reviews with businesses and store merged result",
    schedule=None,  # run manually!
    catchup=False,
) as dag:

    merge_task = PythonOperator(
        task_id="join_and_merge_mongo_data",
        python_callable=run_mongo_merge
    )
