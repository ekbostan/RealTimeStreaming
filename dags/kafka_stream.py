from datetime import datetime, time
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import requests
import json
from typing import Optional, Any, Dict

default_args = {
    'owner': 'airscholar',
    'start_date': datetime.combine(datetime.now(), time.min) 
}

def get_data() -> Optional[dict[str, Any]]:
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

def format_data(res: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if res is None or 'results' not in res:  
        return None

    user_info = res['results'][0] if res['results'] else {}
    location = user_info.get('location', {}) 
    name = user_info.get('name', {})
    login = user_info.get('login', {})

    data = {
        "first_name": name.get('first', 'N/A'),
        'last_name': name.get('last', 'N/A'),
        'gender': user_info.get('gender', 'N/A'),
        'address': f"{location.get('street', {}).get('number', 'N/A')} " \
                   f"{location.get('street', {}).get('name', 'N/A')} " \
                   f"{location.get('city', 'N/A')} " \
                   f"{location.get('state', 'N/A')} " \
                   f"{location.get('country', 'N/A')}",
        'postcode': location.get('postcode', 'N/A'),
        'email': user_info.get('email', 'N/A'),
        'username': login.get('username', 'N/A'),
        'dob': user_info.get('dob', {}).get('date', 'N/A'),
        'registered_date': user_info.get('registered', {}).get('date', 'N/A'),
        'phone': user_info.get('phone', 'N/A'),
        'picture': user_info.get('picture', {}).get('medium', 'N/A')
    }

    return data
   
def stream_data() -> Optional[dict[str, Any]]:
    res = get_data()
    formatted_res = format_data(res)
    logging.info(formatted_res) 
    return formatted_res

# Define the Airflow DAG
# with DAG('user_automation',
#          default_args=default_args,
#          schedule_interval='@daily',
#          catchup=False) as dag:
    
#     streaming_task = PythonOperator(
#         task_id='stream_data_from_api',
#         python_callable=stream_data
#     )

stream_data()
