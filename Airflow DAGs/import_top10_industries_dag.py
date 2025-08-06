from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'abhishek',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='import_top10_industry_csv_to_mongodb',
    default_args=default_args,
    description='Import Top 10 Industries CSV into MongoDB for Compass',
    schedule=None,  # Trigger manually
    catchup=False,
) as dag:

    import_csv = BashOperator(
        task_id='import_csv_to_mongodb',
        bash_command="""
        mongoimport --uri="mongodb://localhost:27017" \
        --db=Datawarehouse_Project \
        --collection=Top10_Industry_Reviews \
        --type=csv \
        --headerline \
        --file="/Users/abhishekkumar/Downloads/yelp_reviews_top_10_industries.csv"
        """
    )
