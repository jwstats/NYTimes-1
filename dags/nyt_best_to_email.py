from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator

import json
from datetime import datetime

default_args = {
    'start_date': datetime(2021, 1, 1)
}

with DAG(dag_id='nyt_best_to_email',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='db_postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS best_sellers (
                book_id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                author TEXT NOT NULL,
                bestsellers_date TEXT NOT NULL,
                published_date TEXT NOT NULL,
                list_name TEXT NOT NULL,
                rank TEXT NOT NULL,
                rank_last_week TEXT NOT NULL,
                amazon_product_url TEXT NOT NULL,
                description TEXT NOT NULL
            );
            '''
    )

    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='nyt_api',
        endpoint=''
    )

    extract_list = SimpleHttpOperator(
        task_id='extract_list',
        http_conn_id='nyt_api',
        method='GET',
        log_response=True
    )

    create_table >> is_api_available >> extract_list

    
    


    
