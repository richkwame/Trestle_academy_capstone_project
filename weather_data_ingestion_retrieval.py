# the codes below:
#Create the weather table in PostgreSQL if it does not exist.
#Fetch data from OpenWeatherMap API and handle any potential errors.
#Send an email notification in case of an error.
#Ingest the fetched data into the weather table in PostgreSQL.
from airflow import DAG
from airflow.operators.
import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta
import requests
import psycopg2
from psycopg2 import sql, OperationalError

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='A simple DAG to fetch weather data and store it in PostgreSQL with error handling and notification',
    schedule_interval='*/30 * * * *',
)

def create_weather_table():
    try:
        conn = psycopg2.connect(
            dbname="airflow",
            user="airflow",
            password="airflow",
            host="postgres"
        )
        cur = conn.cursor()
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS weather (
            id SERIAL PRIMARY KEY,
            city VARCHAR(50),
            temperature FLOAT,
            description VARCHAR(100),
            timestamp TIMESTAMP
        );
        '''
        cur.execute(create_table_query)
        conn.commit()
        cur.close()
        conn.close()
    except OperationalError as e:
        raise ValueError(f"Error creating weather table: {e}")

def fetch_weather_data():
    try:
        api_key = '8c39c9e3bc47282ae68de3634b955314'
        city = 'London'
        url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric'
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        weather_data = {
            'city': data['name'],
            'temperature': data['main']['temp'],
            'description': data['weather'][0]['description'],
            'timestamp': datetime.now()
        }
        return weather_data
    except requests.RequestException as e:
        raise ValueError(f"Error fetching weather data: {e}")

def store_weather_data(ti):
    weather_data = ti.xcom_pull(task_ids='fetch_weather_data_task')
    if not weather_data:
        raise ValueError('No weather data fetched')

    try:
        conn = psycopg2.connect(
            dbname="airflow",
            user="airflow",
            password="airflow",
            host="postgres"
        )
        cur = conn.cursor()
        insert_query = sql.SQL('''
        INSERT INTO weather (city, temperature, description, timestamp) 
        VALUES (%s, %s, %s, %s);
        ''')
        cur.execute(insert_query, (
            weather_data['city'],
            weather_data['temperature'],
            weather_data['description'],
            weather_data['timestamp']
        ))
        conn.commit()
        cur.close()
        conn.close()
    except OperationalError as e:
        raise ValueError(f"Error storing weather data: {e}")

# Tasks
create_table_task = PythonOperator(
    task_id='create_weather_table_task',
    python_callable=create_weather_table,
    dag=dag,
)

fetch_weather_data_task = PythonOperator(
    task_id='fetch_weather_data_task',
    python_callable=fetch_weather_data,
    dag=dag,
)

store_weather_data_task = PythonOperator(
    task_id='store_weather_data_task',
    python_callable=store_weather_data,
    provide_context=True,
    dag=dag,
)

send_email_task = EmailOperator(
    task_id='send_email_task',
    to='rowusu094@gmail.com',
    subject='Airflow Weather Data Pipeline Failed',
    html_content='The weather data pipeline has failed. Please check the logs for more details.',
    trigger_rule='one_failed',
    dag=dag,
)

# Task dependencies
create_table_task >> fetch_weather_data_task >> store_weather_data_task
fetch_weather_data_task >> send_email_task
