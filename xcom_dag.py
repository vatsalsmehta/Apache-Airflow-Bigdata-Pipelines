from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

import requests
import json

url = 'https://httpbin.org/post'



# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag=DAG('xcom_dag',
         start_date=datetime(2021, 1, 1),
         max_active_runs=2,
         schedule_interval=timedelta(minutes=30),
         default_args=default_args,
         catchup=False
         )


def api_data(ti):
    try:
        res = requests.post(url)
        ans=res.json()
        print("\n\nThe whole Response recieved form API is\n\n",ans)
        ti.xcom_push(key='send_origin_data', value=ans)
    except Exception as error:
        print(error)

def analyze_api_data(ti):
    try:
        whole_api_response =ti.xcom_pull(key='send_origin_data', task_ids='task_1')
        answer=whole_api_response['origin']
        print('\n\nThe origin of API request is', answer)
    except Exception as error:
        print(error)
    

task_1= PythonOperator(
        task_id = 'task_1',
        python_callable=api_data,
        dag=dag
    )

task_2 = PythonOperator(
        task_id = 'task_2',
        python_callable=analyze_api_data,
        dag=dag
    )

task_1 >> task_2