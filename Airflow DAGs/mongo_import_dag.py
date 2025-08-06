from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Default arguments for the DAG
default_args = {
    'owner': 'abhishek',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='mongo_json_import',
    default_args=default_args,
    description='Import Yelp JSON file into MongoDB using mongoimport',
    schedule_interval=None,  # Set to None for manual trigger
    catchup=False,
) as dag:

    # Task: Run mongoimport to load JSON into MongoDB
    import_task = BashOperator(
        task_id='import_json_to_mongodb',
        bash_command="""
        mongoimport --uri="mongodb://localhost:27017" \
        --db=Datawarehouse_Project \
        --collection=Yelp_Dataset \
        --file="/Users/abhishekkumar/Downloads/Yelp JSON/yelp_reviews_500000_datasets.json"
        """
    )

