
import airflow
import datetime
import pandas as pd
from pymongo import MongoClient
import requests
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.utils.task_group import TaskGroup
import pandas as pd
import csv
import pandas as pd


cases_deaths='https://covid19.who.int/WHO-COVID-19-global-data.csv'
vaccinations='https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations.csv'
government_measures='https://raw.githubusercontent.com/OxCGRT/covid-policy-dataset/main/data/OxCGRT_compact_national_v1.csv'

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(seconds=10),
}

dag = DAG('covid_data_dag_mongo', start_date=airflow.utils.dates.days_ago(0), default_args=default_args, schedule_interval='@daily')

def download_cases_deaths():
    response = requests.get(cases_deaths)
    with open('/opt/airflow/dags/mongoDB/cases_deaths.csv', 'wb') as f:
        f.write(response.content)

def download_vaccinations():
    response = requests.get(vaccinations)
    with open('/opt/airflow/dags/mongoDB/vaccinations.csv', 'wb') as f:
        f.write(response.content)

def download_government_measures():
    response = requests.get(government_measures)
    with open('/opt/airflow/dags/mongoDB/government_measures.csv', 'wb') as f:
        f.write(response.content)

def _create_cases_deaths_query(previous_epoch: int, output_folder: str):
    df = pd.read_csv('/opt/airflow/dags/mongoDB/cases_deaths.csv')
    client = MongoClient('mongodb://mongo:27017/')
    db = client['covid_data']
    collection = db['cases_deaths']
    collection.insert_many(df.to_dict('records'))

def _create_government_measures_query(previous_epoch: int, output_folder: str):
    df = pd.read_csv('/opt/airflow/dags/mongoDB/government_measures.csv')
    df = df[['CountryName', 'CountryCode', 'RegionName', 'RegionCode', 'Jurisdiction', 'Date', 'StringencyIndex_Average', 'GovernmentResponseIndex_Average', 'ContainmentHealthIndex_Average', 'EconomicSupportIndex']]
    client = MongoClient('mongodb://mongo:27017/')
    db = client['covid_data']
    collection = db['government_measures']
    collection.insert_many(df.to_dict('records'))

def _create_vaccinations_query(previous_epoch: int, output_folder: str):
    df = pd.read_csv('/opt/airflow/dags/MongoDB/vaccinations.csv')
    df.fillna(0, inplace=True)
    client = MongoClient('mongodb://mongo:27017/')
    db = client['covid_data']
    collection = db['vaccinations']
    collection.insert_many(df.to_dict('records'))

download_cases_deaths = PythonOperator(
    task_id='download_cases_deaths',
    dag=dag,
    python_callable=download_cases_deaths,
    op_kwargs={},
    trigger_rule='all_success',
    depends_on_past=False,
)

download_vaccinations = PythonOperator(
    task_id='download_vaccinations',
    dag=dag,
    python_callable=download_vaccinations,
    op_kwargs={},
    trigger_rule='all_success',
    depends_on_past=False,
)

download_government_measures = PythonOperator(
    task_id='download_government_measures',
    python_callable=download_government_measures,
    dag=dag
)

create_cases_deaths_query_operator = PythonOperator(
    task_id='create_cases_deaths_query_operator',
    dag=dag,
    python_callable=_create_cases_deaths_query,
    op_kwargs={
        'previous_epoch': '{{ prev_execution_date.int_timestamp }}',
        'output_folder': '/opt/airflow/dags',
    },
    trigger_rule='all_success',
    depends_on_past=False,
)

create_vaccinations_query_operator = PythonOperator(
    task_id='create_vaccinations_query_operator',
    dag=dag,
    python_callable=_create_vaccinations_query,
    op_kwargs={
        'previous_epoch': '{{ prev_execution_date.int_timestamp }}',
        'output_folder': '/opt/airflow/dags',
    },
    trigger_rule='all_success',
    depends_on_past=False,
)

create_government_measures_query_operator = PythonOperator(
    task_id='create_government_measures_query_operator',
    dag=dag,
    python_callable=_create_government_measures_query,
    op_kwargs={
        'previous_epoch': '{{ prev_execution_date.int_timestamp }}',
        'output_folder': '/opt/airflow/dags',
    },
    trigger_rule='all_success',
    depends_on_past=False,
)

def print_vaccinations():
    hook = MongoHook(conn_id="mongo_default")
    conn = hook.get_conn()
    db = conn['covid_data']
    vaccinations = db['vaccinations']
    results = vaccinations.find().limit(100)
    with open('/opt/airflow/dags/speriamo.csv', 'a') as csvfile:
        writer = csv.writer(csvfile)
        for result in results:
            writer.writerow(result.values())
    conn.close()

print_vaccinations_operator = PythonOperator(
    task_id='print_vaccinations_operator',
    dag=dag,
    python_callable=print_vaccinations,
)

download_cases_deaths >> create_cases_deaths_query_operator
download_vaccinations >> create_vaccinations_query_operator
download_government_measures >> create_government_measures_query_operator
create_cases_deaths_query_operator >> print_vaccinations_operator

