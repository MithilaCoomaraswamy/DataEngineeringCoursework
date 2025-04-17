# Weather ETL - Test Suite

This repository contains a set of test cases for the Airflow DAG `weather_etl_dag`, which fetches weather data from the OpenWeatherMap API and uploads it to AWS S3.

---

## What's Covered

- Environment variable validation
- API key presence and correctness
- City name defaulting
- API key failure (401 Unauthorized)
- Invalid city (404 Not Found)
- API timeouts and retries
- Data transformation logic
- S3 upload success and failure

---

## Prerequisites

Before running the tests, ensure you have:

- Python 3.8+
- A working Airflow environment
- AWS credentials set up (for S3 uploads)
- The following Airflow Variables configured:
  - `openweather_api_key` – your API key from OpenWeatherMap
  - `city_name` – comma-separated list of cities (optional, defaults to `London`)


