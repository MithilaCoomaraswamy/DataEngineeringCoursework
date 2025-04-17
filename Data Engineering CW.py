from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta
import requests
import json
import boto3
import time
import logging
import os

# --- Default DAG Configuration ---
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

RAW_FILE = '/tmp/weather_raw.json'
TRANSFORMED_FILE = '/tmp/weather_transformed.json'


# --- Helpers ---
def get_valid_city():
    city = Variable.get("city_name", default_var="").strip()
    if not city:
        logging.warning("City name variable is empty. Defaulting to 'London'.")
        return "London"
    return city


# --- Validation Task (NEW) ---
def validate_vars():
    api_key = Variable.get("openweather_api_key", default_var=None)
    if not api_key or not api_key.strip():
        logging.error("Missing API key! Please set 'openweather_api_key' in Airflow Variables.")
        raise AirflowFailException("Missing API key for OpenWeatherMap.")

    city = Variable.get("city_name", default_var="").strip()
    if not city:
        logging.warning("No city name provided. Defaulting to 'London'.")


# --- Extract Task ---
def extract():
    try:
        API_KEY = Variable.get("openweather_api_key", default_var=None)
        if not API_KEY or not API_KEY.strip():
            raise ValueError("API key is missing or invalid.")

        city_string = Variable.get("city_name", default_var="London")
        if not city_string.strip():
            city_string = "London"
            logging.warning("City name is empty. Defaulting to 'London'.")

        cities = [c.strip() for c in city_string.split(",") if c.strip()]
        results = []

        for city in cities:
            url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}"
            for attempt in range(5):
                try:
                    logging.info(f"Requesting weather data for: {city} (Attempt {attempt+1})")
                    response = requests.get(url, timeout=10)

                    if response.status_code == 401:
                        logging.error("Invalid API key (401). Aborting extract task.")
                        raise ValueError("Invalid API key. Aborting extract task.")

                    elif response.status_code == 404:
                        logging.warning(f"City '{city}' not found (404). Skipping.")
                        break

                    elif response.status_code != 200:
                        logging.warning(f"Unexpected status {response.status_code} for {city}. Skipping.")
                        break

                    results.append(response.json())
                    break

                except requests.exceptions.Timeout:
                    logging.warning(f"Timeout when fetching {city}. Retrying...")
                    time.sleep(2 ** attempt)
                except Exception as e:
                    logging.error(f"Unexpected error for {city}: {e}")
                    break

        if not results:
            raise ValueError("No valid weather data retrieved.")

        if os.path.exists(RAW_FILE):
            with open(RAW_FILE, 'r') as f:
                existing_data = json.load(f)
        else:
            existing_data = []

        existing_data.extend(results)

        with open(RAW_FILE, 'w') as f:
            json.dump(existing_data, f)

        logging.info(f"Fetched weather data for {len(results)} city(ies).")

    except Exception as e:
        logging.error(f"Extract task failed: {str(e)}")
        raise


# --- Transform Task ---
def transform():
    try:
        if not os.path.exists(RAW_FILE):
            raise FileNotFoundError(f"Missing raw data file: {RAW_FILE}")

        with open(RAW_FILE, 'r') as f:
            raw_data_list = json.load(f)

        transformed = []

        for raw_data in raw_data_list:
            try:
                transformed.append({
                    "city": raw_data.get("name", "Unknown"),
                    "temperature": raw_data["main"]["temp"],
                    "humidity": raw_data["main"]["humidity"],
                    "weather_description": raw_data["weather"][0]["description"],
                    "timestamp": datetime.utcnow().isoformat()
                })
            except KeyError as e:
                logging.warning(f"Missing key in record for city {raw_data.get('name', 'Unknown')}: {e}")

        if not transformed:
            raise ValueError("No valid transformed records found.")

        if os.path.exists(TRANSFORMED_FILE):
            with open(TRANSFORMED_FILE, 'r') as f:
                existing_data = json.load(f)
        else:
            existing_data = []

        existing_data.extend(transformed)

        with open(TRANSFORMED_FILE, 'w') as f:
            json.dump(existing_data, f)

        logging.info(f"Transformed {len(transformed)} record(s).")
        logging.info(json.dumps(transformed, indent=2))

    except Exception as e:
        logging.error(f"Transform task failed: {str(e)}")
        raise


# --- Load Task ---
def load():
    bucket_name = 'my-coursework-bucket'
    s3_file = 'weather-data.json'

    try:
        if not os.path.exists(TRANSFORMED_FILE):
            raise FileNotFoundError(f"Missing transformed data file: {TRANSFORMED_FILE}")

        with open(TRANSFORMED_FILE, 'rb') as f:
            s3 = boto3.client('s3')
            s3.upload_fileobj(f, bucket_name, s3_file)

        logging.info(f"Uploaded file to s3://{bucket_name}/{s3_file}")

        with open(TRANSFORMED_FILE, 'r') as f:
            loaded_data = json.load(f)
            logging.info("Loaded data to S3:")
            logging.info(json.dumps(loaded_data, indent=2))

    except boto3.exceptions.S3UploadFailedError as e:
        logging.error(f"S3 upload failed: {e}")
        raise
    except Exception as e:
        logging.error(f"Load task failed: {str(e)}")
        raise


# --- Custom API Check Response ---
def check_api_response(response):
    if response.status_code == 401:
        logging.error("Invalid API key (401 Unauthorized). Stopping DAG.")
        raise AirflowFailException("API key is invalid.")
    if response.status_code != 200:
        logging.error(f"Unexpected API response: {response.status_code}")
        raise AirflowFailException(f"API returned status: {response.status_code}")
    return '"main"' in response.text


# --- DAG Definition ---
with DAG(
    dag_id='weather_etl_dag',
    default_args=default_args,
    description='ETL pipeline for weather data using OpenWeatherMap API and S3',
    schedule_interval='@hourly',
    catchup=False,
    tags=['weather', 'ETL', 'airflow']
) as dag:

    validate_task = PythonOperator(
        task_id='validate_airflow_vars',
        python_callable=validate_vars
    )

    check_api = HttpSensor(
        task_id='check_api_available',
        http_conn_id='openweathermap_api',
        endpoint='data/2.5/weather',
        request_params={
            'q': get_valid_city(),
            'appid': Variable.get("openweather_api_key", default_var="")
        },
        response_check=check_api_response,
        poke_interval=60,
        timeout=300
    )

    extract_task = PythonOperator(
        task_id='extract_weather',
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id='transform_weather',
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id='load_to_s3',
        python_callable=load
    )

    # DAG execution order
    validate_task >> check_api >> extract_task >> transform_task >> load_task
