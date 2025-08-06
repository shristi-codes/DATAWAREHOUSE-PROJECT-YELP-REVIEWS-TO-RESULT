from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from textblob import TextBlob
import pandas as pd

default_args = {
    'start_date': datetime(2024, 1, 1),
}

def process_sentiment():
    input_path = '/Users/abhishekkumar/Downloads/Yelp JSON/yelp_review_dataset_csv.csv'
    output_path = '/Users/abhishekkumar/Downloads/Yelp JSON/yelp_review_dataset_with_sentiment.csv'

    # Step 1: Load the dataset
    df = pd.read_csv(input_path)

    # Step 2: Compute sentiment score
    def get_sentiment(text):
        try:
            return TextBlob(str(text)).sentiment.polarity
        except:
            return 0.0  # Fallback if something breaks

    df['sentiment_score'] = df['text'].astype(str).apply(get_sentiment)

    # Step 3: Drop the large 'text' column
    df.drop(columns=['text'], inplace=True)

    # Step 4: Save the compact version
    df.to_csv(output_path, index=False)
    print("✅ Sentiment added and review text removed.")

with DAG(
    'process_yelp_sentiment',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    sentiment_task = PythonOperator(
        task_id='compute_sentiment_and_drop_text',
        python_callable=process_sentiment
    )

