from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'abhishek',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='mongo_business_import',
    default_args=default_args,
    description='Import Yelp Business JSON into MongoDB',
    schedule=None,
    catchup=False,
) as dag:

    import_businesses = BashOperator(
        task_id='import_businesses_to_mongodb',
        bash_command="""
        mongoimport --uri="mongodb://localhost:27017" \
        --db=Datawarehouse_Project \
        --collection=Business Dataset \
        --file="/Users/abhishekkumar/Downloads/Yelp JSON/Final_yelp_business_with_industries_V3.json"
        """
    )

