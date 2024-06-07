def retrieve_weather_data():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    records = pg_hook.get_records("SELECT * FROM weather_data")
    for record in records:
        print(record)

retrieve_task = PythonOperator(
    task_id='retrieve_weather_data',
    python_callable=retrieve_weather_data,
    dag=dag,
)

store_task >> retrieve_task
