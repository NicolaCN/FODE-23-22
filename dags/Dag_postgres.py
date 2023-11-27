import airflow
import datetime
import pandas as pd
import requests
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import csv
import pandas as pd
import wbgapi as wb
import numpy as np
import sys
import os
from airflow.utils.task_group import TaskGroup
from sqlalchemy import create_engine
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from airflow.utils.task_group import TaskGroup
#sys.path.insert(0,os.path.abspath(os.path.dirname("Utils.py")))
#from Utils import _download_cases_deaths, _download_population_data, _download_vaccinations, _download_government_measures, _download_location_table, _create_time_csv, _wrangle_cases_deaths, _wrangle_vaccinations, _wrangle_government_measures, _wrangle_population_data, _wrangle_location_data
from pymongo import MongoClient

location_csv = 'https://raw.githubusercontent.com/lukes/ISO-3166-Countries-with-Regional-Codes/master/all/all.csv'
population_data='https://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=csv'
cases_deaths='https://covid19.who.int/WHO-COVID-19-global-data.csv'
vaccinations='https://storage.googleapis.com/covid19-open-data/v3/vaccinations.csv'
#vaccinations='https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations.csv'
government_measures='https://raw.githubusercontent.com/OxCGRT/covid-policy-dataset/main/data/OxCGRT_compact_national_v1.csv'


default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(seconds=10),
    'catchup': False,
    "depends_on_past": False,
}

dag = DAG('covid_data_dag_postgres_plis', default_args=default_args, schedule_interval='@daily')


def _create_time_csv():
    start_date = f"{2019}-01-01"
    end_date = f"{2023}-12-31"
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    data = {
        'Date': date_range,
        'Week': date_range.isocalendar().week,
        'Month': date_range.month,
        'Trimester': (date_range.month - 1) // 3 + 1,
        'Semester': (date_range.month <= 6).astype(int) + 1,
        'Year': date_range.year
        }
    
    df = pd.DataFrame(data)
    df.to_csv('/opt/airflow/dags/postgres/time_table.csv', index=False)

def _download_location_table():
    response = requests.get(location_csv)
    with open('/opt/airflow/dags/postgres/location.csv', 'wb') as f:
        f.write(response.content)
    
def _download_cases_deaths():
    response = requests.get(cases_deaths)
    with open('/opt/airflow/dags/postgres/cases_deaths.csv', 'wb') as f:
        f.write(response.content)

def _download_vaccinations():
    response = requests.get(vaccinations)
    with open('/opt/airflow/dags/postgres/vaccinations.csv', 'wb') as f:
        f.write(response.content)

def _download_government_measures():
    response = requests.get(government_measures)
    with open('/opt/airflow/dags/postgres/government_measures.csv', 'wb') as f:
        f.write(response.content)
        
# Population data from the World Bank API, used to calculate the per capita metrics 
# for every table (e.g. total_vaccinations_per_hundred, people_vaccinated_per_hundred, etc.)         
def _download_population_data():
    data = wb.data.DataFrame('SP.POP.TOTL', labels=True, time=range(2019, 2023))
    
    #Manually add the population for 2023 and set it equal to the population of 2022
    data["YR2023"] = data["YR2022"]
    
    data_reshaped = pd.melt(data, id_vars=['Country'], var_name='Year', value_name='population')
    data_reshaped['Year'] = data_reshaped['Year'].apply(lambda year: int(year[2:]))
    
    data_reshaped.to_csv('/opt/airflow/dags/postgres/population_data.csv', index=False)
    
def csv_to_json(filename, header=None):
    data = pd.read_csv(filename, header=header)
    return data.to_dict('records')

def _store_location_csv():
    # Code to store location.csv in MongoDB
    client = MongoClient('mongodb://mongo:27017/')
    db = client["covid"] 
    db.drop_collection('location')
    collection = db['location']
    collection.insert_many(csv_to_json(location_csv, 0))
    
    '''with open('/opt/airflow/dags/postgres/location.csv', 'r') as f:
        # Read the CSV file
        data = f.read()
        # Insert the data into the collection
        collection.insert_many(data)'''

def _store_population_data():
    data = wb.data.DataFrame('SP.POP.TOTL', labels=True, time=range(2019, 2023))
    
    client = MongoClient('mongodb://mongo:27017/')
    db = client["covid"]
    db.drop_collection('population_data')
    collection = db['population_data']
    collection.insert_many(data.to_dict('records'))

# For big files it might be better to first store
def _store_cases_deaths():
    client = MongoClient('mongodb://mongo:27017/')
    db = client["covid"]
    db.drop_collection('cases_deaths')
    collection = db['cases_deaths']
    collection.insert_many(csv_to_json(cases_deaths, 0))
    

def _store_vaccinations():
    df = pd.read_csv(vaccinations).iloc[:, :9]
    json = df.to_dict('records')
    client = MongoClient('mongodb://mongo:27017/')
    db = client["covid"]
    db.drop_collection('vaccinations')
    collection = db['vaccinations']
    collection.insert_many(json)
    

def _store_government_measures():
    client = MongoClient('mongodb://mongo:27017/')
    db = client["covid"]
    db.drop_collection('government_measures')
    collection = db['government_measures']
    collection.insert_many(csv_to_json(government_measures, 0))
    
def _pull_location_csv():
    # Code to pull location.csv from MongoDB
    client = MongoClient('mongodb://mongo:27017/')
    db = client["covid"] 
    collection = db['location']
    data = pd.DataFrame(list(collection.find())).drop('_id', axis=1)
    data.to_csv('/opt/airflow/dags/files/location.csv', index=False)
    
def _pull_population_data():
    client = MongoClient('mongodb://mongo:27017/')
    db = client["covid"]
    collection = db['population_data']
    data = pd.DataFrame(list(collection.find())).drop('_id', axis=1)
    data.to_csv('/opt/airflow/dags/files/population_data.csv', index=False)

def _pull_cases_deaths():
    client = MongoClient('mongodb://mongo:27017/')
    db = client["covid"]
    collection = db['cases_deaths']
    data = pd.DataFrame(list(collection.find())).drop('_id', axis=1)
    data.to_csv('/opt/airflow/dags/files/cases_deaths.csv', index=False)
    
def _pull_vaccinations():
    client = MongoClient('mongodb://mongo:27017/')
    db = client["covid"]
    collection = db['vaccinations']
    data = pd.DataFrame(list(collection.find())).drop('_id', axis=1)
    data.to_csv('/opt/airflow/dags/files/vaccinations.csv', index=False)

def _pull_government_measures():
    client = MongoClient('mongodb://mongo:27017/')
    db = client["covid"]
    collection = db['government_measures']
    data = pd.DataFrame(list(collection.find())).drop('_id', axis=1)
    data.to_csv('/opt/airflow/dags/files/government_measures.csv', index=False)

def format_date(df: pd.DataFrame, date_col) -> pd.DataFrame:
    print("Formatting date…")
    df[date_col] = pd.to_datetime(df[date_col], format="%Y-%m-%d")
    return df

def discard_rows(df, columns):
    print("Discarding rows…")
    # For all rows where new_cases or new_deaths is negative, we keep the cumulative value but set
    # the daily change to NA. This also sets the 7-day rolling average to NA for the next 7 days.
    for col in columns:
        df.loc[df[col] < 0, col] = np.nan
    
    return df

def _inject_growth(df, prefix, periods):
    cases_colname = "%s_cases" % prefix
    deaths_colname = "%s_deaths" % prefix
    cases_growth_colname = "%s_pct_growth_cases" % prefix
    deaths_growth_colname = "%s_pct_growth_deaths" % prefix

    df[[cases_colname, deaths_colname]] = (
        df[["Country", "New_cases", "New_deaths"]]
        .groupby("Country")[["New_cases", "New_deaths"]]
        .rolling(window=periods, min_periods=periods - 1, center=False)
        .sum()
        .reset_index(level=0, drop=True)
    )
    df[[cases_growth_colname, deaths_growth_colname]] = (
        df[["Country", cases_colname, deaths_colname]]
        .groupby("Country")[[cases_colname, deaths_colname]]
        .pct_change(periods=periods, fill_method=None)
        .round(3)
        .replace([np.inf, -np.inf], pd.NA)
        * 100
    )
    
    return df
    
def _inject_population(df, date_col):
    'dags\postgres\population_data.csv'
    df_population = pd.read_csv('/opt/airflow/dags/files/population_data.csv')
    # Extract year from Date_reported column and create a new column Year
    df['Year'] = pd.DatetimeIndex([date_col]).year
    # 
    # Merge the two dataframes based on Country and Year columns
    df_merged = pd.merge(df, df_population, how='left', on=['Country', 'Year'])
    df_merged.drop('Year', axis=1, inplace=True)
    
    return df_merged

def _per_capita(df, measures):
    for measure in measures:
        pop_measure = measure + "_per_million"
        series = df[measure] / (df["population"] / 1e6)
        df[pop_measure] = series.round(decimals=3)
    #df = drop_population(df)
    return df

def convert_iso_code(df, iso_col):
    converting_table = pd.read_csv('/opt/airflow/dags/files/location.csv')
    converting_table = converting_table[['alpha-2', 'alpha-3']]
    merged = pd.merge(df, converting_table, how='left', left_on=[iso_col], right_on=['alpha-2'])
    
    merged.drop([iso_col, 'alpha-2'], axis=1, inplace=True)
    merged.rename(columns={'alpha-3': 'Country_code'}, inplace=True)
    return merged

def _wrangle_cases_deaths():
    df = pd.read_csv('/opt/airflow/dags/files/cases_deaths.csv')
    
    df = format_date(df, date_col="Date_reported")
    df = discard_rows(df, ["New_cases", "New_deaths"])
    df = _inject_growth(df, 'Weekly', 7)   
    df = _inject_population(df, date_col="Date_reported")
    df = _per_capita(df, ["New_cases", "New_deaths", "Cumulative_cases", 
                          "Cumulative_deaths", "Weekly_cases", "Weekly_deaths",])
    df = convert_iso_code(df, iso_col='Country_code')
    
    
    df.to_csv('/opt/airflow/dags/files/cases_deaths.csv', index=False)

def _inject_growth_vacc(df, prefix, periods):
    vaccinated_colname = "%s_persons_vaccinated" % prefix
    fully_vaccinated_colname = "%s_persons_fully_vaccinated" % prefix
    vaccine_doses_colname = "%s_vaccine_doses_administered" % prefix
    vaccinated_growth_colname = "%s_pct_growth_persons_vaccinated" % prefix
    fully_vaccinated_growth_colname = "%s_pct_growth_persons_fully_vaccinated" % prefix
    vaccine_doses_growth_colname = "%s_pct_growth_vaccine_doses_administered" % prefix
    cases_growth_colname = "%s_pct_growth_cases" % prefix
    deaths_growth_colname = "%s_pct_growth_deaths" % prefix

    df[[vaccinated_colname, fully_vaccinated_colname, vaccine_doses_colname]] = (
        df[["location_key", "new_persons_vaccinated", "new_persons_fully_vaccinated", "new_vaccine_doses_administered"]]
        .groupby("location_key")[["new_persons_vaccinated", "new_persons_fully_vaccinated", "new_vaccine_doses_administered"]]
        .rolling(window=periods, min_periods=periods - 1, center=False)
        .sum()
        .reset_index(level=0, drop=True)
    )
    df[[cases_growth_colname, deaths_growth_colname]] = (
        df[["location_key", vaccinated_colname, fully_vaccinated_colname, vaccine_doses_colname]]
        .groupby("location_key")[[vaccinated_colname, fully_vaccinated_colname, vaccine_doses_colname]]
        .pct_change(periods=periods, fill_method=None)
        .round(3)
        .replace([np.inf, -np.inf], pd.NA)
        * 100
    )
    
    return df

def rollup(df):
    # Discard rows where location_key is null
    df.dropna(subset = ["location_key"], inplace=True)
    
    # Discard rows where location_key contains "_", i.e. regional/province data
    df.drop(df[df["location_key"].str.contains("_")].index, inplace=True)
    
    return df

def _wrangle_vaccinations():
    df = pd.read_csv('/opt/airflow/dags/files/vaccinations.csv')
    df = df.iloc[:, :9]     
    df = format_date(df, date_col="date")
    df = discard_rows(df, ["new_persons_vaccinated", "new_persons_fully_vaccinated", "new_vaccine_doses_administered"])
    df = rollup(df) 
    df = _inject_growth_vacc(df, 'Weekly', 7)
    df = _inject_population(df, date_col="date")
    df = _per_capita(df, ["new_persons_vaccinated", "new_persons_fully_vaccinated", "new_vaccine_doses_administered" 
                          "weekly_persons_vaccinated", "weekly_persons_fully_vaccinated", "weekly_vaccine_doses_administered"])
    df = convert_iso_code(df, iso_col='location_key')
    
    #df.to_csv('/opt/airflow/dags/postgres/vaccinations_wrangled.csv', index=False)

def _wrangle_government_measures():
    df = pd.read_csv('/opt/airflow/dags/files/government_measures.csv')
    # Apply data wrangling here
    
    # Mantain only the columns of interest
    df = df[['CountryName', 'CountryCode', 'Jurisdiction', 'Date', 'StringencyIndex_Average', 'GovernmentResponseIndex_Average', 'ContainmentHealthIndex_Average', 'EconomicSupportIndex']]
    
    
    df.to_csv('/opt/airflow/dags/files/government_measures.csv', index=False)

def _wrangle_population_data():
    data = pd.read_csv('/opt/airflow/dags/files/population_data.csv')
    
    data["YR2023"] = data["YR2022"]
    
    data_reshaped = pd.melt(data, id_vars=['Country'], var_name='Year', value_name='population')
    data_reshaped['Year'] = data_reshaped['Year'].apply(lambda year: int(year[2:]))
    
    data_reshaped.to_csv('/opt/airflow/dags/postgres/population_data.csv', index=False)
    #df.to_csv('/opt/airflow/dags/postgres/population_data_wrangled.csv', index=False)
    
def _wrangle_location_data():
    df = pd.read_csv('/opt/airflow/dags/files/location.csv')
    # Apply data wrangling here
    # ...
    #df.to_csv('/opt/airflow/dags/postgres/location_wrangled.csv', index=False)

def _create_joint_table():
    df = pd.read_csv('/opt/airflow/dags/postgres/joint_table.csv')
    df.set_index(['Country_code', 'Date_reported'], inplace=True)
    pd.to_sql('joint_table', con=PostgresHook(postgres_conn_id='postgres_default').get_sqlalchemy_engine(), if_exists='replace', index=True, index_label=['Country', 'Date_reported'])

def _join_tables():
    df_vaccinations = pd.read_csv('vaccinations.csv')
    df_cases_deaths = pd.read_csv('cases_deaths_wrangled.csv')
    df_government_measures = pd.read_csv('government_measures.csv')
    merged_df = pd.merge(df_cases_deaths, df_vaccinations, left_on=['Country_code', 'Date_repotred'], right_on=['iso_code', 'date'])
    merged_df = pd.merge(merged_df, df_government_measures, left_on=['Country_code', 'Date_repotred'], right_on=['CountryCode', 'Date'])
    with open('/opt/airflow/dags/postgres/joint_table.csv', 'wb') as f:
        f.write(merged_df.to_csv(index=False).encode('utf-8'))
  
def _upload_cases_deaths():
    conn_string = 'postgresql://airflow:airflow@postgres:5432/postgres'
    db = create_engine(conn_string)
    conn = db.connect()
    df = pd.read_csv('/opt/airflow/dags/postgres/cases_deaths_wrangled.csv')
    df.to_sql('cases_deaths', con=conn, if_exists='replace', index=False)

def _upload_vaccinations():
    conn_string = 'postgresql://airflow:airflow@postgres:5432/postgres'
    db = create_engine(conn_string)
    conn = db.connect()
    df = pd.read_csv('/opt/airflow/dags/postgres/vaccinations.csv')
    df.to_sql('vaccinations', con=conn, if_exists='replace', index=False)
    
def _upload_government_measures():
    conn_string = 'postgresql://airflow:airflow@postgres:5432/postgres'
    db = create_engine(conn_string)
    conn = db.connect()
    df = pd.read_csv('/opt/airflow/dags/postgres/government_measures_wrangled.csv')
    df.to_sql('government_measures', con=conn, if_exists='replace', index=False)
    
def _upload_time():
    conn_string = 'postgresql://airflow:airflow@postgres:5432/postgres'
    db = create_engine(conn_string)
    conn = db.connect()
    df = pd.read_csv('/opt/airflow/dags/postgres/time_table.csv')
    df.to_sql('time_table', con=conn, if_exists='replace', index=False)
    
def _upload_location():
    conn_string = 'postgresql://airflow:airflow@postgres:5432/postgres'
    db = create_engine(conn_string)
    conn = db.connect()
    df = pd.read_csv('/opt/airflow/dags/postgres/location.csv')
    df.to_sql('location', con=conn, if_exists='replace', index=False)
  
with TaskGroup("ingestion", dag=dag) as ingestion:
    
    ingestion_start = DummyOperator(
        task_id='ingestion_start',
        dag=dag,
    )
    
    '''# Download the cases_deaths.csv file from the WHO website
    download_cases_deaths = PythonOperator(
        task_id='download_cases_deaths',
        dag=dag,
        python_callable=_download_cases_deaths,
        op_kwargs={},
        trigger_rule='all_success',
        depends_on_past=False,
    )

    # Downlaod the vaccinations.csv file from the OWID GitHub repository
    download_vaccinations = PythonOperator(
        task_id='download_vaccinations',
        dag=dag,
        python_callable=_download_vaccinations,
        op_kwargs={},
        trigger_rule='all_success',
        depends_on_past=False,
    )

    # Download the government_measures.csv file from the OxCGRT GitHub repository
    download_government_measures = PythonOperator(
        task_id='download_government_measures',
        dag=dag,
        python_callable=_download_government_measures,
        op_kwargs={},
        trigger_rule='all_success',
        depends_on_past=False,
    )

    # Download the population_data.csv file from the World Bank API
    download_population_data = PythonOperator(
        task_id='download_population_data',
        dag=dag,
        python_callable=_download_population_data,
        op_kwargs={},
        trigger_rule='all_success',
        depends_on_past=False,
    )

    # Download the location.csv file from the GitHub repository
    download_location_data = PythonOperator(
        task_id='download_location_data',
        dag=dag,
        python_callable=_download_location_table,
        op_kwargs={},
        trigger_rule='all_success',
        depends_on_past=False,
    )'''
    
    # Store the location_csv in MongoDB
    store_location_csv = PythonOperator(
        task_id='store_location_csv',
        dag=dag,
        python_callable=_store_location_csv,
        op_kwargs={},
        trigger_rule='all_success',
        depends_on_past=False,
    )

    # Store the population_data in MongoDB
    store_population_data = PythonOperator(
        task_id='store_population_data',
        dag=dag,
        python_callable=_store_population_data,
        op_kwargs={},
        trigger_rule='all_success',
        depends_on_past=False,
    )

    # Store the cases_deaths in MongoDB
    store_cases_deaths = PythonOperator(
        task_id='store_cases_deaths',
        dag=dag,
        python_callable=_store_cases_deaths,
        op_kwargs={},
        trigger_rule='all_success',
        depends_on_past=False,
    )

    # Store the vaccinations in MongoDB
    store_vaccinations = PythonOperator(
        task_id='store_vaccinations',
        dag=dag,
        python_callable=_store_vaccinations,
        op_kwargs={},
        trigger_rule='all_success',
        depends_on_past=False,
    )

    # Store the government_measures in MongoDB
    store_government_measures = PythonOperator(
        task_id='store_government_measures',
        dag=dag,
        python_callable=_store_government_measures,
        op_kwargs={},
        trigger_rule='all_success',
        depends_on_past=False,
    )

    ingestion_end = DummyOperator(
        task_id='ingestion_end',
        dag=dag,
        trigger_rule='all_success'
    )
    
with TaskGroup("staging", dag=dag) as staging:    
    
    staging_start = DummyOperator(
        task_id='staging_start',
        dag=dag,
    )

    staging_end = DummyOperator(
        task_id='staging_end',
        dag=dag,
        trigger_rule='all_success'
    )
    
    # Pull data from Mongo for location_csv
    pull_location_csv = PythonOperator(
        task_id='pull_location_csv',
        dag=dag,
        python_callable=_pull_location_csv,
        op_kwargs={},
        trigger_rule='all_success',
        depends_on_past=False,
    )
    
    # Pull data from Mongo for population_data
    pull_population_data = PythonOperator(
        task_id='pull_population_data',
        dag=dag,
        python_callable=_pull_population_data,
        op_kwargs={},
        trigger_rule='all_success',
        depends_on_past=False,
    )
    
    # Pull data from Mongo for cases_deaths
    pull_cases_deaths = PythonOperator(
        task_id='pull_cases_deaths',
        dag=dag,
        python_callable=_pull_cases_deaths,
        op_kwargs={},
        trigger_rule='all_success',
        depends_on_past=False,
    )
    
    # Pull data from Mongo for vaccinations
    pull_vaccinations = PythonOperator(
        task_id='pull_vaccinations',
        dag=dag,
        python_callable=_pull_vaccinations,
        op_kwargs={},
        trigger_rule='all_success',
        depends_on_past=False,
    )
    
    # Pull data from Mongo for government_measures
    pull_government_measures = PythonOperator(
        task_id='pull_government_measures',
        dag=dag,
        python_callable=_pull_government_measures,
        op_kwargs={},
        trigger_rule='all_success',
        depends_on_past=False,
    )
        
    # Create the time_table.csv file with the time dimensions
    create_time_csv = PythonOperator(
        task_id='create_time_csv',
        python_callable=_create_time_csv,
        dag=dag,
    )

    wrangle_cases_deaths_task = PythonOperator(
        task_id='wrangle_cases_deaths',
        python_callable=_wrangle_cases_deaths,
        dag=dag,
    )

    wrangle_vaccinations_task = PythonOperator(
        task_id='wrangle_vaccinations',
        python_callable=_wrangle_vaccinations,
        dag=dag,
    )

    wrangle_government_measures_task = PythonOperator(
        task_id='wrangle_government_measures',
        python_callable=_wrangle_government_measures,
        dag=dag,
    )

    wrangle_population_data_task = PythonOperator(
        task_id='wrangle_population_data',
        python_callable=_wrangle_population_data,
        dag=dag,
    )

    wrangle_location_data_task = PythonOperator(
        task_id='wrangle_location_data',
        python_callable=_wrangle_location_data,
        dag=dag,
    )
    
    create_time_table = PostgresOperator(
        task_id='create_time_table',
        dag=dag,
        postgres_conn_id='postgres_default',
        sql='sql/create_time_table.sql',
        trigger_rule='all_success'
    )
    
    create_location_table = PostgresOperator(
        task_id='create_location_table',
        dag=dag,
        postgres_conn_id='postgres_default',
        sql='sql/create_location_table.sql',
        trigger_rule='all_success'
    )
    
    create_cases_deaths_table = PostgresOperator(
        task_id='create_cases_deaths_table',
        dag=dag,
        postgres_conn_id='postgres_default',
        sql='sql/create_cases_deaths_table.sql',
    )
    
    create_vaccinations_table = PostgresOperator(
        task_id='create_vaccinations_table',
        postgres_conn_id='postgres_default',
        sql='sql/create_vaccinations_table.sql',
        dag=dag,
    )
    
    create_government_measures_table = PostgresOperator(
        task_id='create_government_measures_table',
        postgres_conn_id='postgres_default',
        sql='sql/create_government_measures_table.sql',
        dag=dag
    )
    
    
    upload_cases_deaths = PythonOperator(
        task_id='upload_cases_deaths',
        python_callable=_upload_cases_deaths,
        dag=dag,
    )

    upload_vaccinations = PythonOperator(
        task_id='upload_vaccinations',
        python_callable=_upload_vaccinations,
        dag=dag,
    )

    upload_government_measures = PythonOperator(
        task_id='upload_government_measures',
        python_callable=_upload_government_measures,
        dag=dag,
    )

    upload_time = PythonOperator(
        task_id='upload_time',
        python_callable=_upload_time,
        dag=dag,
    )
    
    upload_location = PythonOperator(
        task_id='upload_location',
        python_callable=_upload_location,
        dag=dag,
    )
    
    
    
'''create_joint_table = PythonOperator(
        task_id='create_joint_table',
        python_callable=_create_joint_table,
        dag=dag,
        trigger_rule='all_success'
)'''
    
ingestion_start >> [store_location_csv, store_population_data, store_cases_deaths, store_vaccinations, store_government_measures]
[store_location_csv, store_population_data, store_cases_deaths, store_vaccinations, store_government_measures] >> ingestion_end
ingestion_end >> staging_start

staging_start >> [pull_location_csv, pull_population_data, pull_cases_deaths, pull_vaccinations, pull_government_measures]
[pull_cases_deaths, wrangle_population_data_task] >> wrangle_cases_deaths_task >> create_cases_deaths_table >> upload_cases_deaths
[pull_vaccinations, wrangle_population_data_task] >> wrangle_vaccinations_task >> create_vaccinations_table >> upload_vaccinations
pull_government_measures >> wrangle_government_measures_task >> create_government_measures_table >> upload_government_measures
pull_location_csv >> wrangle_location_data_task >> create_location_table >> upload_location
pull_population_data >> wrangle_population_data_task 
staging_start >> create_time_csv >> create_time_table >> upload_time

[upload_cases_deaths, upload_vaccinations, upload_government_measures, upload_location, upload_time] >> staging_end
 