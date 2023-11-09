import airflow
import datetime
import pandas as pd
from pymongo import MongoClient
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

dag = DAG('covid_data_dag_postgres', start_date=airflow.utils.dates.days_ago(0), default_args=default_args, schedule_interval='@daily')

def download_cases_deaths():
    #url = 'https://covid19.who.int/WHO-COVID-19-global-data.csv'
    response = requests.get(cases_deaths)
    with open('/opt/airflow/dags/postgres/cases_deaths.csv', 'wb') as f:
        f.write(response.content)

def download_vaccinations():
    #url = 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations.csv'
    response = requests.get(vaccinations)
    with open('/opt/airflow/dags/postgres/vaccinations.csv', 'wb') as f:
        f.write(response.content)

def download_government_measures():
    #url = 'https://raw.githubusercontent.com/OxCGRT/covid-policy-dataset/main/data/OxCGRT_compact_national_v1.csv'
    response = requests.get(government_measures)
    with open('/opt/airflow/dags/postgres/government_measures.csv', 'wb') as f:
        f.write(response.content)

# Create the file cases_deaths_inserts.sql with the SQL query to insert the data into the database
def _create_cases_deaths_query(previous_epoch: int, output_folder: str):
    df = pd.read_csv('/opt/airflow/dags/postgres/cases_deaths.csv')
    with open("/opt/airflow/dags/postgres/cases_deaths_inserts.sql", "w") as f:
        df_iterable = df.iterrows()

        f.write(
            "DROP TABLE IF EXISTS cases_deaths;\n"
            "CREATE TABLE cases_deaths (\n"
            "id SERIAL PRIMARY KEY,\n"
            "date_reported DATE,\n"
            "country_code VARCHAR(10),\n"
            "country VARCHAR(100),\n"
            "who_region VARCHAR(100),\n"
            "new_cases INTEGER,\n"
            "cumulative_cases INTEGER,\n"
            "new_deaths INTEGER,\n"
            "cumulative_deaths INTEGER\n"
            ");\n"
        )
        
        
        for index, row in df_iterable:
            id = index
            date_reported = row['Date_reported']
            country_code = row['Country_code']
            # If the country name contains a single quote, replace it with two single quotes 
            # (the apostrophe is a reserved character in SQL)
            country = row['Country'].replace("'", "''")
            who_region = row['WHO_region']
            new_cases = row['New_cases']
            cumulative_cases = row['Cumulative_cases']
            new_deaths = row['New_deaths']
            cumulative_deaths = row['Cumulative_deaths']

            f.write(
                "INSERT INTO cases_deaths VALUES ("
                f"'{id}', '{date_reported}', '{country_code}', '{country}', '{who_region}', {new_cases}, {cumulative_cases}, {new_deaths}, {cumulative_deaths}"
                ");\n"
            )
            
            # Just for debugging purposes, I limit the number of records to 100
            if index == 100:
                break

        f.close()
        

def _create_government_measures_query(previous_epoch: int, output_folder: str):
    df = pd.read_csv('/opt/airflow/dags/postgres/government_measures.csv')
    df = df[['CountryName', 'CountryCode', 'RegionName', 'RegionCode', 'Jurisdiction', 'Date', 'StringencyIndex_Average', 'GovernmentResponseIndex_Average', 'ContainmentHealthIndex_Average', 'EconomicSupportIndex']]
    
    with open("/opt/airflow/dags/postgres/government_measures_inserts.sql", "w") as f:
        df_iterable = df.iterrows()

        f.write(
            "DROP TABLE IF EXISTS government_measures;\n"
            "CREATE TABLE government_measures (\n"
            "id SERIAL PRIMARY KEY,\n"
            "country_name VARCHAR(100),\n"
            "country_code VARCHAR(10),\n"
            "region_name VARCHAR(100),\n"
            "region_code VARCHAR(10),\n"
            "jurisdiction VARCHAR(100),\n"
            "date DATE,\n"
            "stringency_index_average FLOAT,\n"
            "government_response_index_average FLOAT,\n"
            "containment_health_index_average FLOAT,\n"
            "economic_support_index FLOAT\n"
            ");\n"
        )
        
        for index, row in df_iterable:
            id = index
            country_name = row['CountryName'].replace("'", "''")
            country_code = row['CountryCode']
            region_name = row['RegionName'] if pd.notnull(row['RegionName']) else None
            region_code = row['RegionCode'] if pd.notnull(row['RegionCode']) else None
            jurisdiction = row['Jurisdiction'].replace("'", "''") if pd.notnull(row['Jurisdiction']) else None
            date = row['Date']
            stringency_index_average = row['StringencyIndex_Average'] if pd.notnull(row['StringencyIndex_Average']) else None
            government_response_index_average = row['GovernmentResponseIndex_Average'] if pd.notnull(row['GovernmentResponseIndex_Average']) else None
            containment_health_index_average = row['ContainmentHealthIndex_Average'] if pd.notnull(row['ContainmentHealthIndex_Average']) else None
            economic_support_index = row['EconomicSupportIndex'] if pd.notnull(row['EconomicSupportIndex']) else None

            f.write(
                "INSERT INTO government_measures VALUES ("
                f"'{id}', '{country_name}', '{country_code}', '{region_name}', '{region_code}', '{jurisdiction}', '{date}', {stringency_index_average}, {government_response_index_average}, {containment_health_index_average}, {economic_support_index}"
                ");\n"
            )
            
            # Just for debugging purposes, I limit the number of records to 100
            if index == 100:
                break

        f.close()
        
# Create the file vaccinations_inserts.sql with the SQL query to insert the data into the database   
def _create_vaccinations_query(previous_epoch: int, output_folder: str):
    df = pd.read_csv('/opt/airflow/dags/postgres/vaccinations.csv')
    df.fillna(0, inplace=True)
    with open("/opt/airflow/dags/postgres/vaccinations_inserts.sql", "w") as f:
        df_iterable = df.iterrows()
        
        f.write(
            "DROP TABLE IF EXISTS vaccinations;\n"
            "CREATE TABLE vaccinations (\n"
            "id SERIAL PRIMARY KEY,\n"
            "date_ DATE,\n"
            "location_ VARCHAR(100),\n"
            "iso_code VARCHAR(10),\n"
            "total_vaccinations INTEGER,\n"
            "people_vaccinated INTEGER,\n"
            "people_fully_vaccinated INTEGER,\n"
            "daily_vaccinations_raw INTEGER,\n"
            "daily_vaccinations INTEGER,\n"
            "total_vaccinations_per_hundred FLOAT,\n"
            "people_vaccinated_per_hundred FLOAT,\n"
            "people_fully_vaccinated_per_hundred FLOAT,\n"
            "daily_vaccinations_per_million INTEGER,\n"
            "daily_people_vaccinated INTEGER,\n"
            "daily_people_vaccinated_per_hundred FLOAT\n"
            ");\n"
        )
        
        for index, row in df_iterable:
            id = index
            date = row['date']
            location = row['location']
            iso_code = row['iso_code']
            total_vaccinations = row['total_vaccinations']
            people_vaccinated = row['people_vaccinated']
            people_fully_vaccinated = row['people_fully_vaccinated']
            daily_vaccinations_raw = row['daily_vaccinations_raw']
            daily_vaccinations = row['daily_vaccinations']
            total_vaccinations_per_hundred = row['total_vaccinations_per_hundred']
            people_vaccinated_per_hundred = row['people_vaccinated_per_hundred']
            people_fully_vaccinated_per_hundred = row['people_fully_vaccinated_per_hundred']
            daily_vaccinations_per_million = row['daily_vaccinations_per_million']
            daily_people_vaccinated = row['daily_people_vaccinated']
            daily_people_vaccinated_per_hundred = row['daily_people_vaccinated_per_hundred']

            f.write(
                "INSERT INTO vaccinations VALUES ("
                f"'{id}', '{date}', '{location}', '{iso_code}', {total_vaccinations}, {people_vaccinated}, {people_fully_vaccinated}, {daily_vaccinations_raw}, {daily_vaccinations}, {total_vaccinations_per_hundred}, {people_vaccinated_per_hundred}, {people_fully_vaccinated_per_hundred}, {daily_vaccinations_per_million}, {daily_people_vaccinated}, {daily_people_vaccinated_per_hundred}"
                ");\n"
            )
            
            if index == 100:
                break

        f.close()
        

# Download the cases_deaths.csv file from the WHO website
download_cases_deaths = PythonOperator(
    task_id='download_cases_deaths',
    dag=dag,
    python_callable=download_cases_deaths,
    op_kwargs={},
    trigger_rule='all_success',
    depends_on_past=False,
)

# Downlaod the vaccinations.csv file from the OWID GitHub repository
download_vaccinations = PythonOperator(
    task_id='download_vaccinations',
    dag=dag,
    python_callable=download_vaccinations,
    op_kwargs={},
    trigger_rule='all_success',
    depends_on_past=False,
)

# Download the government_measures.csv file from the OxCGRT GitHub repository
download_government_measures = PythonOperator(
    task_id='download_government_measures',
    python_callable=download_government_measures,
    dag=dag
)

# Create the cases_deaths_inserts.sql file with the SQL query to insert the data into the database
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

# Create the vaccinations_inserts.sql file with the SQL query to insert the data into the database
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

# Create the government_measures_inserts.sql file with the SQL query to insert the data into the database
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

# Create the cases_deaths table in the database and insert the data
create_cases_deaths_table = PostgresOperator(
    task_id='create_cases_deaths_table',
    dag=dag,
    postgres_conn_id='postgres_default',
    sql='/postgres/cases_deaths_inserts.sql',
)

# Create the vaccinations table in the database and insert the data
create_vaccinations_table = PostgresOperator(
    task_id='create_vaccinations_table',
    postgres_conn_id='postgres_default',
    sql='/postgres/vaccinations_inserts.sql',
    dag=dag
)

# Create the government_measures table in the database and insert the data
create_government_measures_table = PostgresOperator(
    task_id='create_government_measures_table',
    postgres_conn_id='postgres_default',
    sql='/postgres/government_measures_inserts.sql',
    dag=dag
)


def print_vaccinations():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM vaccinations;")
    results = cursor.fetchall()

    with open('/opt/airflow/dags/postgres/speriamo.csv', 'a') as csvfile:
        #transform results to string
        results = str(results)
        csvfile.write(results)
        #writer = csv.writer(csvfile)
        #writer.writerows(results)
    cursor.close()
    
# Print the first 100 records of the vaccinations table 
# (to see if the data has been correctly inserted)
print_vaccinations_operator = PythonOperator(
    task_id='print_vaccinations_operator',
    dag=dag,
    python_callable=print_vaccinations,
)


download_cases_deaths >> create_cases_deaths_query_operator >> create_cases_deaths_table >> print_vaccinations_operator
download_vaccinations >> create_vaccinations_query_operator >> create_vaccinations_table 
download_government_measures >> create_government_measures_query_operator >> create_government_measures_table 


 