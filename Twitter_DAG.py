import csv
import pandas as pd
from textblob import TextBlob
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

#python_callable to move the data from GCS bucket to LocalFileSystem
def copy_to_local():
    conn = GCSHook(gcp_conn_id = 'gcp_conn')
    bucket = 'egen-twitter-bucket'
    file_item_list = conn.list(bucket_name = bucket)
    with open('combined.csv', 'w') as file:
        writer = csv.writer(file)
        for item in file_item_list:
            raw_data = conn.download(bucket_name = bucket, object_name = item)
            data = raw_data.decode('utf-8')
            data_list = data.split(',')
            writer.writerow(data_list)
            conn.delete(bucket_name = bucket, object_name = item)

class TweetDataTransformation:
    """
    Provides sentiment analysis to the tweet text based on TextBlob
    """
    # polarity analysis
    def getTextPolarity(self,txt):
        return TextBlob(txt).sentiment.polarity

    # negative, neutral, positive analysis
    def getTextAnalysis(self,a):
        if a < 0:
            return "Negative"
        elif a == 0:
            return "Neutral"
        else:
            return "Positive"

#python_callable to clean the twitter tweet data
def data_cleaner():
    transform = TweetDataTransformation()
    df = pd.read_csv('combined.csv', header = None, error_bad_lines=False)
    df.iloc[:, 2] = df.iloc[:, 2].str.lower()
    df.iloc[:, 2] = df.iloc[:, 2].str.replace(r'https?://.+\b|.+\.com|www\..+|.+\.in', '')# Remove urls
    df.iloc[:, 2] = df.iloc[:, 2].str.replace('@', '').replace('#', '').replace('_', ' ')# Remove hashtags and others
    df.iloc[:, 2] = df.iloc[:, 2].str.replace(r'[^\x00-\x7F]+', '')# Remove emojies
    df.iloc[:, 2] = df.iloc[:, 2].str.replace(r'[^A-Za-z0-9 ]', '')# Remove mentions
    df.iloc[:, 3] = df.iloc[:, 3].str.extract(r'<.+>([\w\s]+)<.+>', expand = False) # Extract source
    df['Score'] = df.iloc[:, 2].apply(transform.getTextPolarity).apply(transform.getTextAnalysis)# Gets sentiment of tweet
    df = df.drop_duplicates(subset=df.columns[2], keep="first") # Remove duplicates
    df.iloc[:, 0] = df.iloc[:, 0].astype(int)
    df.to_csv('transformed_combined.csv', index = False, header = None)



with DAG("My_Twitter_DAG", start_date = days_ago(1), schedule_interval='*/2 * * * *', catchup = False) as dag:

    start_operator_task = DummyOperator(task_id='Begin_execution', dag=dag)

    gcs_to_local = PythonOperator(
        task_id = 'gcs_to_local',
        python_callable = copy_to_local
    )

    local_transformations = PythonOperator(
        task_id = 'local_transformations',
        python_callable = data_cleaner
    )

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id = 'upload_to_gcs',
        gcp_conn_id = 'gcp_conn',
        src = 'transformed_combined.csv',
        dst = 'transformed_combined.csv',
        bucket = 'trial-egen-twitter'
    )


    gcs_to_big_query = GCSToBigQueryOperator(
        task_id = 'gcs_to_big_query',
        google_cloud_storage_conn_id = 'gcp_conn',
        bigquery_conn_id = 'gcp_conn',
        bucket = 'trial-egen-twitter',
        source_objects = ['transformed_combined.csv'],
        destination_project_dataset_table = 'twitter_tweets.tweets_data',
        schema_fields = [
            {'name':'tweet_id', 'type':'INTEGER', 'mode':'REQUIRED'},
            {'name':'tweet_created_at', 'type':'STRING', 'mode':'NULLABLE'},
            {'name':'text', 'type':'STRING', 'mode':'NULLABLE'},
            {'name':'source', 'type':'STRING', 'mode':'NULLABLE'},
            {'name':'reply_count', 'type':'INTEGER', 'mode':'NULLABLE'},
            {'name':'retweet_count', 'type':'INTEGER', 'mode':'NULLABLE'},
            {'name':'favorite_count', 'type':'INTEGER', 'mode':'NULLABLE'},
            {'name':'user_id', 'type':'INTEGER', 'mode':'NULLABLE'},
            {'name':'user_name', 'type':'STRING', 'mode':'NULLABLE'},
            {'name':'user_location', 'type':'STRING', 'mode':'NULLABLE'},
            {'name':'user_followers_count', 'type':'INTEGER', 'mode':'NULLABLE'},
            {'name':'user_friends_count', 'type':'INTEGER', 'mode':'NULLABLE'},
            {'name':'user_listed_count', 'type':'INTEGER', 'mode':'NULLABLE'},
            {'name':'user_favourites_count', 'type':'INTEGER', 'mode':'NULLABLE'},
            {'name':'user_statuses_count', 'type':'INTEGER', 'mode':'NULLABLE'},
            {'name':'profile_created_at', 'type':'STRING', 'mode':'NULLABLE'},
            {'name': 'tweet_score', 'type': 'STRING', 'mode': 'NULLABLE'}
        ],
        write_disposition = 'WRITE_APPEND',allow_quoted_newlines = True
    )

    end_operator_task = DummyOperator(task_id='Stop_execution', dag=dag)

    start_operator_task >> gcs_to_local >> local_transformations >> upload_to_gcs >> gcs_to_big_query >> end_operator_task