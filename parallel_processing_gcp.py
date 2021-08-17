#importing libraries
import csv
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.gcs_to_bq import \
    GoogleCloudStorageToBigQueryOperator
from google.cloud import bigquery
import pandas as pd
import calendar
import time
from google.cloud import storage
from pandas import DataFrame
from datetime import timedelta, datetime
from airflow import DAG
from airflow.contrib.operators.bigquery_get_data import BigQueryGetDataOperator
from airflow.operators import python_operator
from airflow.models import Variable
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
import googleapiclient.discovery
import numpy as np
from airflow.models import Variable
import pytz
from datetime import datetime as dt
import sys
import calendar
import warnings
import requests
from google.api_core.client_options import ClientOptions
import math


def_argsman={
    "owner":"vatron",
    "depends_on_past":False,
    "retry_delay":timedelta(minutes=1),
    "start_date":datetime(2021,7,20),
    "retries":1

}

dag=DAG(
    dag_id="parallel_processing_gcp.py",
    schedule_interval="@once",
    default_args=def_argsman,
    catchup=False
)

def get_started(**Kwargs):
    print("Starting parallel processing from next task")


def day1_query(**Kwargs):
    client=bigquery.Client()
    sql_query=''' '''
    result_day1_df=client.query(sql_query).to_dataframe()
    #print(result_day1_df.head())
    try:
            #link to your composer environment 
            path = '/home/airflow/gcs/data/xyz_preprocessing_datafiles/day1_eventfreq.csv'
            result_day1_df.to_csv(path,index=False)
            print("\n\nCSV File Saved\n\n")
    except Exception as e:
            print("!!!!!!!!!!!!!!!!!!!!Exception occured!!!!!!!!!!!!!!!!!!", e)
            raise Exception(e)


def day2_query(**Kwargs):
    client=bigquery.Client()
    sql_query='''Your day 2 sql query'''
    result_day2_df=client.query(sql_query).to_dataframe()
    #print(result_day2_df.head())
    try:
            path = '/home/airflow/gcs/data/xyz_preprocessing_datafiles/day2_eventfreq.csv'
            result_day2_df.to_csv(path,index=False)
            print("\n\nCSV File Saved\n\n")
    except Exception as e:
            print("!!!!!!!!!!!!!!!!!!!!Exception occured!!!!!!!!!!!!!!!!!!", e)
            raise Exception(e)


def day3_query(**Kwargs):
    client=bigquery.Client()
    sql_query='''your day 3 sql query'''
    result_day3_df=client.query(sql_query).to_dataframe()
    #print(result_day3_df.columns)
    try:
            path = '/home/airflow/gcs/data/xyz_preprocessing_datafiles/day3_eventfreq.csv'
            result_day3_df.to_csv(path,index=False)
            print("\n\nCSV File Saved\n\n")
    except Exception as e:
            print("!!!!!!!!!!!!!!!!!!!!Exception occured!!!!!!!!!!!!!!!!!!", e)
            raise Exception(e)

def day4_query(**Kwargs):
    client=bigquery.Client()
    sql_query=''' Your SQL Query Here'''
    result_day4_df=client.query(sql_query).to_dataframe()
    #print(result_day4_df.columns)
    #print(result_day4_df.info())
    try:
            path = '/home/airflow/gcs/data/xyz_preprocessing_datafiles/day4_eventfreq.csv'
            result_day4_df.to_csv(path,index=False)
            print("\n\nCSV File Saved\n\n")
    except Exception as e:
            print("!!!!!!!!!!!!!!!!!!!!Exception occured!!!!!!!!!!!!!!!!!!", e)
            raise Exception(e)

def day5_query(**Kwargs):
    client=bigquery.Client()
    sql_query='''Your Sql Query here'''
    result_day5_df=client.query(sql_query).to_dataframe()
    #print(result_day5_df.columns)
    try:
            path = '/home/airflow/gcs/data/xyz_preprocessing_datafiles/day5_eventfreq.csv'
            result_day5_df.to_csv(path,index=False)
            print("\n\nCSV File Saved\n\n")
    except Exception as e:
            print("!!!!!!!!!!!!!!!!!!!!Exception occured!!!!!!!!!!!!!!!!!!", e)
            raise Exception(e)

def day6_query(**Kwargs):
    client=bigquery.Client()
    sql_query='''Your Sql Query here'''
    result_day6_df=client.query(sql_query).to_dataframe()
    #print(result_day6_df.columns)
    try:
            path = '/home/airflow/gcs/data/xyz_preprocessing_datafiles/day6_eventfreq.csv'
            result_day6_df.to_csv(path,index=False)
            print("\n\nCSV File Saved\n\n")
    except Exception as e:
            print("!!!!!!!!!!!!!!!!!!!!Exception occured!!!!!!!!!!!!!!!!!!", e)
            raise Exception(e)

def day7_query(**Kwargs):
    client=bigquery.Client()
    sql_query='''Your Sql Query here'''
    result_day7_df=client.query(sql_query).to_dataframe()
    #print(result_day7_df.columns)
    try:
            path = '/home/airflow/gcs/data/xyz_preprocessing_datafiles/day7_eventfreq.csv'
            result_day7_df.to_csv(path,index=False)
            print("\n\nCSV File Saved\n\n")
    except Exception as e:
            print("!!!!!!!!!!!!!!!!!!!!Exception occured!!!!!!!!!!!!!!!!!!", e)
            raise Exception(e)


def ad_id_query(**Kwargs):
    client=bigquery.Client()
    sql_query='''Another Sql Query'''
    result_ad_id_df=client.query(sql_query).to_dataframe()
    try:
        path='/asu.csv'
        result_ad_id_df.to_csv(path,index=False)
    except Exception as error:
        print("Exception occured",error)
        raise Exception(error)

def fn1_evpreprocessing(**Kwargs):
    try:
        path='/home/airflow/gcs/data/xyz_preprocessing_datafiles/'
        dfs=[]
        for i in range(1,8):
            temp='day'+str(i)+'_eventfreq.csv'
            file_path=path+temp
            temp_df=pd.read_csv(file_path)
            dfs.append(temp_df)

        ad_file_path='/home/airflow/gcs/data/xyz_preprocessing_datafiles/ad_id_day1_day7_eventfreq.csv'
        ad=pd.read_csv(ad_file_path)
    except Exception as error:
        print("Exception occured",error)
        raise Exception(error)
    print("All Files Read Successfully")

    def preprocessing_evfreq(lst, ad):

        return lst

    evfreq=preprocessing_evfreq(dfs,ad)

def push_googlebq(**Kwargs):
    return GoogleCloudStorageToBigQueryOperator(
        task_id="push_googlebq",
        source_objects=['data/xyz_preprocessing_datafiles/ad_id_day1_day7.csv'],
        source_format='CSV',
        bucket='asia-east2-xyz-tatacliq-as-c3e3e551-bucket',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        destination_project_dataset_table='suger_cosmetics_branch.test_ppp',
        schema_fields=[
            {'name': 'string_field_0', 'type': 'STRING', 'mode': 'NULLABLE'},
        ]
    )

def xyz_usefulfeatures_query(**Kwargs):
    client=bigquery.Client()
    sql_query='''Select * from xyz'''
    result_xyz_usefulfeatures_df=client.query(sql_query).to_dataframe()
    print("Query executed")
    try:
            path = '/home/airflow/gcs/data/xyz_preprocessing_datafiles/xyz_usefulfeatures.csv'
            result_xyz_usefulfeatures_df.to_csv(path,index=False)
            print("\n\nCSV File Saved\n\n")
    except Exception as error:
            print("!!!!!!!!!!!!!!!!!!!!Exception occured!!!!!!!!!!!!!!!!!!", error)
            raise Exception(error)

def preprocessing_evdata(**Kwargs):
    try:
        usefulfeatures_path = '/home/airflow/gcs/data/xyz_preprocessing_datafiles/xyz_usefulfeatures.csv'
        city_tier_path='/home/airflow/gcs/data/xyz_preprocessing_datafiles/Copy of City_Tier.xlsx'

        evdata=pd.read_csv(usefulfeatures_path)
        city_tier=pd.read_excel(city_tier_path)

    except Exception as error:
        print("Exception occured",error)
        raise Exception(error)

    

with DAG(dag_id="parallel_processing_gcp", schedule_interval="@once", default_args=def_argsman, catchup=False) as dag:

    xyz_start = PythonOperator(task_id="xyz_start",python_callable=get_started,provide_context=True,)

    start_day1_query = PythonOperator(task_id="start_day1_query",python_callable=day1_query,provide_context=True,)
    start_day2_query = PythonOperator(task_id="start_day2_query",python_callable=day2_query,provide_context=True,)
    start_day3_query = PythonOperator(task_id="start_day3_query",python_callable=day3_query,provide_context=True,)
    start_day4_query = PythonOperator(task_id="start_day4_query",python_callable=day4_query,provide_context=True,)
    start_day5_query = PythonOperator(task_id="start_day5_query",python_callable=day5_query,provide_context=True,)
    start_day6_query = PythonOperator(task_id="start_day6_query",python_callable=day6_query,provide_context=True,)
    start_day7_query = PythonOperator(task_id="start_day7_query",python_callable=day7_query,provide_context=True,)
    start_ad_id_query=PythonOperator(task_id="start_ad_id_query",python_callable=ad_id_query,provide_context=True,)
    start_preprocessing_evpreprocessing=PythonOperator(task_id="start_preprocessing_evpreprocessing",python_callable=fn1_evpreprocessing,provide_context=True,)
    start_push_bigquery=push_googlebq()
    start_xyz_usefulfeatures=PythonOperator(task_id="start_xyz_usefulfeatures",python_callable=xyz_usefulfeatures_query,provide_context=True,)
    start_preprocessing_evdata=PythonOperator(task_id="start_preprocessing_evdata",python_callable=preprocessing_evdata,provide_context=True,)

xyz_start >> start_day1_query
xyz_start >> start_day2_query
xyz_start >> start_day3_query
xyz_start >> start_day4_query
xyz_start >> start_day5_query
xyz_start >> start_day6_query
xyz_start >> start_day7_query
xyz_start>>start_ad_id_query

start_day1_query>>start_preprocessing_evpreprocessing
start_day7_query>>start_preprocessing_evpreprocessing
start_day4_query>>start_preprocessing_evpreprocessing
start_day5_query>>start_preprocessing_evpreprocessing
start_day6_query>>start_preprocessing_evpreprocessing
start_day2_query>>start_preprocessing_evpreprocessing
start_day3_query>>start_preprocessing_evpreprocessing
start_ad_id_query>>start_preprocessing_evpreprocessing

start_preprocessing_evpreprocessing>>start_push_bigquery>>start_xyz_usefulfeatures>>start_preprocessing_evdata


    
