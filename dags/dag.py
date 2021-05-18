from datetime import timedelta
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine

from airflow import DAG
import subprocess
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging

import pandas as pd
import json


host = "postgres_data"
database = "testDB"
user = "me"
password = "1234"
port = '5432'
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

def extractData():
    try:
        import psycopg2
    except:
        subprocess.check_call(['pip', 'install', 'psycopg2-binary'])
        import psycopg2
    
    try:
        from sqlalchemy import create_engine, select
    except:
        subprocess.check_call(['pip', 'install', 'sqlalchemy'])
        from sqlalchemy import create_engine , select

    df = pd.read_sql("SELECT * FROM users2020;", engine)
    df.to_csv('/tmp/users2020.csv')

def convertToJSON():
    df = pd.read_csv("/tmp/users2020.csv")
    df.to_json('/tmp/users2020.json')

def import_json():
    from pymongo import MongoClient

    client = MongoClient('mongo:27016', username='root', password='example')
    db = client["Assignment_1"]
    Collection = db["users2020"]

    # Loading or Opening the json file
    with open('/tmp/users2020.json') as file:
        file_data = json.load(file)
        Collection.insert_many(file_data)
# LOGGER = logging.getLogger("airflow.task")
default_args = {"owner": "airflow"}

with DAG(
        dag_id='assignment',
        default_args=default_args,
        start_date=days_ago(2),
        dagrun_timeout=timedelta(minutes=60),
        tags=['ETL'],
) as dag:

    extractData = PythonOperator(
        task_id='extractData',
        python_callable=extractData,
        provide_context=True,
        dag=dag
    )

    toJSON = PythonOperator(
        task_id='csvtojson',
        python_callable=convertToJSON,
        provide_context=True,
        dag=dag
    )
    installPyMongoDB = BashOperator(
        task_id='install_pymongodb',
        bash_command='pip install pymongo'
    )

    loadData = PythonOperator(
        task_id='import_json_to_mongo',
        python_callable=import_json,
        provide_context=True,
        dag=dag
    )
    
    extractData >> toJSON >> installPyMongoDB >> loadData