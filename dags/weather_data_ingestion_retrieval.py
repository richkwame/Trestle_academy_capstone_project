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
    'start_date': datetime(2024, 6, 24, 1, 40, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# List of cities
cities = ['Kumasi', 'Accra', 'Takoradi', 'Cape Coast', 'Tema', 'Obuasi', 'Sunyani', 'Konongo', 'Koforidua', 'Techiman']

# DAG definition
dag = DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='A DAG to fetch weather data for multiple cities and store it in PostgreSQL with dynamic table creation, error handling, and notification',
    schedule_interval='*/30 * * * *',
)

def fetch_weather_data(city, **kwargs):
    try:
        api_key = '8c39c9e3bc47282ae68de3634b955314'
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
        kwargs['ti'].xcom_push(key=f'weather_data_{city}', value=weather_data)
        
    except requests.RequestException as e:
        raise ValueError(f"Error fetching weather data for {city}: {e}")

def create_weather_table(city):
    # Ensure the table name is SQL-compliant
    table_name = f"weather_data_{city.replace(' ', '_').lower()}"
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
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
        raise ValueError(f"Error creating weather table for {city}: {e}")

def store_weather_data(city, **kwargs):
    weather_data = kwargs['ti'].xcom_pull(key=f'weather_data_{city}', task_ids=f'fetch_weather_data_task_{city.replace(" ", "_").lower()}')
    if not weather_data:
        raise ValueError(f'No weather data fetched for {city}')

    # Ensure the table name is SQL-compliant
    table_name = f"weather_data_{city.replace(' ', '_').lower()}"
    columns = ', '.join(weather_data.keys())
    values = ', '.join([f"%({key})s" for key in weather_data.keys()])
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values});"

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
        raise ValueError(f"Error storing weather data for {city}: {e}")

for city in cities:
    city_safe = city.replace(' ', '_').lower()
    
    fetch_weather_data_task = PythonOperator(
        task_id=f'fetch_weather_data_task_{city_safe}',
        python_callable=fetch_weather_data,
        op_kwargs={'city': city},
        dag=dag,
    )

    create_table_task = PythonOperator(
        task_id=f'create_weather_table_task_{city_safe}',
        python_callable=create_weather_table,
        op_kwargs={'city': city},
        dag=dag,
    )

    store_weather_data_task = PythonOperator(
        task_id=f'store_weather_data_task_{city_safe}',
        python_callable=store_weather_data,
        op_kwargs={'city': city},
        dag=dag,
    )

    send_email_task = EmailOperator(
        task_id=f'send_email_task_{city_safe}',
        to='rowusu094@gmail.com',
        subject=f'Airflow Weather Data Pipeline Failed for {city}',
        html_content=f'The weather data pipeline for {city} has failed. Please check the logs for more details.',
        trigger_rule='one_failed',
        dag=dag,
    )

    fetch_weather_data_task >> create_table_task >> store_weather_data_task
    fetch_weather_data_task >> send_email_task
