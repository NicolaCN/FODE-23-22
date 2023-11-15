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
#sys.path.insert(0,os.path.abspath(os.path.dirname("Utils.py")))
#from Utils import _download_cases_deaths, _download_population_data, _download_vaccinations, _download_government_measures, _download_location_table, _create_time_csv, _wrangle_cases_deaths, _wrangle_vaccinations, _wrangle_government_measures, _wrangle_population_data, _wrangle_location_data

location_csv = 'https://raw.githubusercontent.com/lukes/ISO-3166-Countries-with-Regional-Codes/master/all/all.csv'
population_data='https://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=csv'
cases_deaths='https://covid19.who.int/WHO-COVID-19-global-data.csv'
vaccinations='https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations.csv'
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

def format_date(df: pd.DataFrame) -> pd.DataFrame:
    print("Formatting date…")
    df["Date_reported"] = pd.to_datetime(df["Date_reported"], format="%Y-%m-%d")
    return df

def discard_rows(df):
    print("Discarding rows…")
    # For all rows where new_cases or new_deaths is negative, we keep the cumulative value but set
    # the daily change to NA. This also sets the 7-day rolling average to NA for the next 7 days.
    df.loc[df.New_cases < 0, "New_cases"] = np.nan
    df.loc[df.New_deaths < 0, "New_deaths"] = np.nan
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
    
def _inject_population(df):
    'dags\postgres\population_data.csv'
    df_population = pd.read_csv('/opt/airflow/dags/postgres/population_data.csv')
    # Extract year from Date_reported column and create a new column Year
    df['Year'] = pd.DatetimeIndex(df['Date_reported']).year
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

def convert_iso_code(df):
    converting_table = pd.read_csv('/opt/airflow/dags/postgres/location.csv')
    converting_table = converting_table[['alpha-2', 'alpha-3']]
    merged = pd.merge(df, converting_table, how='left', left_on=['Country_code'], right_on=['alpha-2'])
    
    merged.drop(['Country_code', 'alpha-2'], axis=1, inplace=True)
    merged.rename(columns={'alpha-3': 'Country_code'}, inplace=True)
    return merged

def _wrangle_cases_deaths():
    df = pd.read_csv('/opt/airflow/dags/postgres/cases_deaths.csv')
    
    df = format_date(df)
    df = discard_rows(df)
    df = _inject_growth(df, 'Weekly', 7)   
    df = _inject_population(df)
    df = _per_capita(df, ["New_cases", "New_deaths", "Cumulative_cases", 
                          "Cumulative_deaths", "Weekly_cases", "Weekly_deaths",])
    df = convert_iso_code(df)
    
    
    df.to_csv('/opt/airflow/dags/postgres/cases_deaths_wrangled.csv', index=False)

def _wrangle_vaccinations():
    df = pd.read_csv('/opt/airflow/dags/postgres/vaccinations.csv')
    
    
    #df.to_csv('/opt/airflow/dags/postgres/vaccinations_wrangled.csv', index=False)

def _wrangle_government_measures():
    df = pd.read_csv('/opt/airflow/dags/postgres/government_measures.csv')
    # Apply data wrangling here
    
    # Mantain only the columns of interest
    df = df[['CountryName', 'CountryCode', 'Jurisdiction', 'Date', 'StringencyIndex_Average', 'GovernmentResponseIndex_Average', 'ContainmentHealthIndex_Average', 'EconomicSupportIndex']]
    
    
    df.to_csv('/opt/airflow/dags/postgres/government_measures_wrangled.csv', index=False)

def _wrangle_population_data():
    df = pd.read_csv('/opt/airflow/dags/postgres/population_data.csv')
    # Apply data wrangling here
    # ...
    #df.to_csv('/opt/airflow/dags/postgres/population_data_wrangled.csv', index=False)
    
def _wrangle_location_data():
    df = pd.read_csv('/opt/airflow/dags/postgres/location.csv')
    # Apply data wrangling here
    # ...
    #df.to_csv('/opt/airflow/dags/postgres/location_wrangled.csv', index=False)

with TaskGroup("ingestion", dag=dag) as ingestion:
    
    ingestion_start = DummyOperator(
        task_id='ingestion_start',
        dag=dag,
    )
    
    # Download the cases_deaths.csv file from the WHO website
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
    
    join_tables = DummyOperator(
        task_id='join_tables',
        dag=dag,
        trigger_rule='all_success'
    )
    
    create_time_table = DummyOperator(
        task_id='create_time_table',
        dag=dag,
        trigger_rule='all_success'
    )
    
    create_location_table = DummyOperator(
        task_id='create_location_table',
        dag=dag,
        trigger_rule='all_success'
    )
    
    create_joint_table = DummyOperator(
        task_id='create_joint_table',
        dag=dag,
        trigger_rule='all_success'
    )
    
ingestion_start >> [download_cases_deaths, download_population_data, download_location_data, download_government_measures, download_location_data, download_vaccinations]
[download_cases_deaths, download_population_data, download_location_data, download_government_measures, download_location_data] >> ingestion_end 
ingestion_end >> staging_start
staging_start >> [create_time_csv, wrangle_cases_deaths_task, wrangle_vaccinations_task, wrangle_government_measures_task, wrangle_population_data_task, wrangle_location_data_task]
[wrangle_cases_deaths_task, wrangle_vaccinations_task, wrangle_government_measures_task, wrangle_population_data_task] >> join_tables
wrangle_location_data_task >> create_location_table
create_time_csv >> create_time_table 
join_tables >> create_joint_table
[create_time_table, create_location_table, create_joint_table] >> staging_end



 