from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pyodbc
import json
import os

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'weather_etl_v2',
    default_args=default_args,
    description='An ETL pipeline to fetch weather data and load into SQL Server',
    schedule_interval='@once',
)

# Define the directory to store JSON files within the container
data_dir = '/opt/airflow/tmp'
os.makedirs(data_dir, exist_ok=True)

def extract_weather_data():
    api_key = os.getenv('WEATHER_API_KEY', '<api+key_goes_here>')
    city = 'London'
    url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}'
    response = requests.get(url)
    data = response.json()
    
    with open(os.path.join(data_dir, 'weather_data.json'), 'w') as f:
        json.dump(data, f)

def transform_weather_data():
    with open(os.path.join(data_dir, 'weather_data.json'), 'r') as f:
        data = json.load(f)
    
    transformed_data = {
        'city': data['name'],
        'temperature': data['main']['temp'],
        'weather': data['weather'][0]['description'],
        'humidity': data['main']['humidity'],
        'pressure': data['main']['pressure'],
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    with open(os.path.join(data_dir, 'transformed_weather_data.json'), 'w') as f:
        json.dump(transformed_data, f)

def load_weather_data():
    with open('/opt/airflow/tmp/transformed_weather_data.json', 'r') as f:
        data = json.load(f)
    
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=host.docker.internal;'
        'DATABASE=<database name>;'
        'UID=<user id>;'
        'PWD=<password for server>'
    )
    cursor = conn.cursor()
    
    insert_query = """
    INSERT INTO weather (city, temperature, weather, humidity, pressure, timestamp)
    VALUES (?, ?, ?, ?, ?, ?)
    """
    
    cursor.execute(insert_query, (
        data['city'], data['temperature'], data['weather'], data['humidity'], data['pressure'], data['timestamp']
    ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    os.remove('/opt/airflow/tmp/weather_data.json')
    os.remove('/opt/airflow/tmp/transformed_weather_data.json')


# Define tasks
extract_task = PythonOperator(
    task_id='extract_weather_data',
    python_callable=extract_weather_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_weather_data',
    python_callable=transform_weather_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_weather_data',
    python_callable=load_weather_data,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task
