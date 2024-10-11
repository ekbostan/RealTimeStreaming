from datetime import datetime, time
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import requests
import json
from typing import Optional, Any


default_args = {
    'owner': 'airscholar',
    'start_date': datetime.combine(datetime.now(), time.min) 
}

def stream_data() -> Optional[dict[str, Any]]:
    """
    Fetches user data from an API and returns it as a dictionary.

    Returns:
        Optional[dict[str, Any]]: User data if successful, None otherwise.
    """
    try:
        response = requests.get("https://randomuser.me/api/")
        response.raise_for_status()  
        data = response.json()  
        return data  

    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")  
    except requests.exceptions.ConnectionError as conn_err:
        logging.error(f"Connection error occurred: {conn_err}")  
    except requests.exceptions.Timeout as timeout_err:
        logging.error(f"Timeout error occurred: {timeout_err}")  
    except requests.exceptions.RequestException as req_err:
        logging.error(f"Request error occurred: {req_err}")  
    except json.JSONDecodeError as json_err:
        logging.error(f"JSON decode error: {json_err}")  
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}") 

    return None  



with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
