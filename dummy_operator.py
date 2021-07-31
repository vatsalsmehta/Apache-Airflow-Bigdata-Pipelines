from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator

default_args_man={'owner':'airflow','depends_on_past':False,'start_date':datetime(2021,7,7),'retries':0}

dag=DAG(dag_id='dummy_operator',default_args=default_args_man,catchup=False,schedule_interval='@once')


start=DummyOperator(task_id='start',dag=dag)
end=DummyOperator(task_id='end',dag=dag)

start>>end
