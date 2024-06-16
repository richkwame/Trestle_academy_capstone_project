from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import requests
import psycopg2
from psycopg2 import OperationalError

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 16, 10, 0, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='A DAG to fetch weather data and store it in PostgreSQL with dynamic table creation, error handling, and notification',
    schedule_interval='*/30 * * * *',
)

def fetch_weather_data(**kwargs):
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
            'humidity': data['main']['humidity'],
            'pressure': data['main']['pressure'],
            'wind_speed': data['wind']['speed'],
            'timestamp': datetime.fromtimestamp(data['dt']).strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Push data to XCom
        kwargs['ti'].xcom_push(key='weather_data', value=weather_data)
        
    except requests.RequestException as e:
        raise ValueError(f"Error fetching weather data: {e}")

def create_weather_table():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS real_weather_data (
        city VARCHAR(255),
        temperature FLOAT,
        description VARCHAR(255),
        humidity INTEGER,
        pressure INTEGER,
        wind_speed FLOAT,
        timestamp TIMESTAMP
    );
    """

    try:
        conn = psycopg2.connect(
            dbname="airflow",
            user="airflow",
            password="airflow",
            host="postgres"
        )
        cur = conn.cursor()
        cur.execute(create_table_query)
        conn.commit()
        cur.close()
        conn.close()
    except OperationalError as e:
        raise ValueError(f"Error creating weather table: {e}")

def store_weather_data(**kwargs):
    weather_data = kwargs['ti'].xcom_pull(key='weather_data', task_ids='fetch_weather_data_task')
    if not weather_data:
        raise ValueError('No weather data fetched')

    columns = ', '.join(weather_data.keys())
    values = ', '.join([f"%({key})s" for key in weather_data.keys()])
    insert_query = f"INSERT INTO real_weather_data ({columns}) VALUES ({values});"

    try:
        conn = psycopg2.connect(
            dbname="airflow",
            user="airflow",
            password="airflow",
            host="postgres"
        )
        cur = conn.cursor()
        cur.execute(insert_query, weather_data)
        conn.commit()
        cur.close()
        conn.close()
    except OperationalError as e:
        raise ValueError(f"Error storing weather data: {e}")

# Tasks
fetch_weather_data_task = PythonOperator(
    task_id='fetch_weather_data_task',
    python_callable=fetch_weather_data,
    provide_context=True,
    dag=dag,
)

create_table_task = PythonOperator(
    task_id='create_weather_table_task',
    python_callable=create_weather_table,
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
fetch_weather_data_task >> create_table_task >> store_weather_data_task
fetch_weather_data_task >> send_email_task
