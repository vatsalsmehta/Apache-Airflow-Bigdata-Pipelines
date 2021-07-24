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
    "owner":"sokrati",
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
            path = '/home/airflow/gcs/data/mifo_preprocessing_datafiles/day1_eventfreq.csv'
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
            path = '/home/airflow/gcs/data/mifo_preprocessing_datafiles/day2_eventfreq.csv'
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
            path = '/home/airflow/gcs/data/mifo_preprocessing_datafiles/day3_eventfreq.csv'
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
            path = '/home/airflow/gcs/data/mifo_preprocessing_datafiles/day4_eventfreq.csv'
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
            path = '/home/airflow/gcs/data/mifo_preprocessing_datafiles/day5_eventfreq.csv'
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
            path = '/home/airflow/gcs/data/mifo_preprocessing_datafiles/day6_eventfreq.csv'
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
            path = '/home/airflow/gcs/data/mifo_preprocessing_datafiles/day7_eventfreq.csv'
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
        path='/home/airflow/gcs/data/mifo_preprocessing_datafiles/ad_id_day1_day7_eventfreq.csv'
        result_ad_id_df.to_csv(path,index=False)
    except Exception as error:
        print("Exception occured",error)
        raise Exception(error)

def fn1_evpreprocessing(**Kwargs):
    try:
        path='/home/airflow/gcs/data/mifo_preprocessing_datafiles/'
        dfs=[]
        for i in range(1,8):
            temp='day'+str(i)+'_eventfreq.csv'
            file_path=path+temp
            temp_df=pd.read_csv(file_path)
            dfs.append(temp_df)

        ad_file_path='/home/airflow/gcs/data/mifo_preprocessing_datafiles/ad_id_day1_day7_eventfreq.csv'
        ad=pd.read_csv(ad_file_path)
    except Exception as error:
        print("Exception occured",error)
        raise Exception(error)
    print("All Files Read Successfully")

    def preprocessing_evfreq(lst, ad):
        for df in dfs:
            df.fillna(0,inplace=True)
        # Storing the idetificable names of columns which are present in each 7 days data, with adding some text in prefix and postfix
        name = ['install','content_view','add_to_cart','login','complete_registration','purchase','add_to_wishlist','page_event','system_event']
    
        # Adding values of event frequency i.e D_0, D_1+D_0, D_2+D_1+D_0, and so on.
        for df in dfs:
            for i in range(1,8):
                for j in name:
                    df['event_name_'+j+'_D'+str(i)] = df['event_name_'+j+'_D'+str(i)] + df['event_name_'+j+'_D'+str(i-1)]
    
                
        # import ad id file
        # ad = pd.read_csv('sugar_purchased_ad_id_14June_20June.csv')
        lst = [(14,'fourteen'),(15,'fifteen'),(16,'sixteen'),(17,'seventeen'),(18,'eighteen'),(19,'nineteen'),(20,'twenty')]
    
        # appending dataframe of ad'ids and purchase column of each date in a list
        ad_df = []
        for i,j in lst:
            k = ad[['ad_'+str(i),j+'_jun']]
            k.dropna(inplace=True)
            k.rename(columns={'ad_'+str(i):'advertising_id'},inplace=True)
            ad_df.append(k)

        # creating target variable P13 in each 7 day data
        day = [(0,'fourteen_jun'),(1,'fifteen_jun'),(2,'sixteen_jun'),(3,'seventeen_jun'),(4,'eighteen_jun'),(5,'nineteen_jun'),(6,'twenty_jun')]
        merged_dfs = []
        for i,j in day:
            k = dfs[i].merge(ad_df[i],on='advertising_id',how='left')
            k.rename(columns={j:'P13'},inplace=True)
            k['P13'].fillna(0,inplace=True)
            k['P13'] = k['P13'].astype(int)
            merged_dfs.append(k)
    
        # creating separate copy of each 7 day data
        d14 = merged_dfs[0].copy(); d15 = merged_dfs[1].copy(); d16 = merged_dfs[2].copy(); 
        d17 = merged_dfs[3].copy(); d18 = merged_dfs[4].copy(); d19 = merged_dfs[5].copy(); 
        d20 = merged_dfs[6].copy()
    
        dtfs = merged_dfs.copy()
    
        # For ad_id with purchase event, 1st occurence was taken
        for i in range(0,6):
            x = set(ad_df[i].advertising_id)
            for j in range(i+1,7):
                y = set(dtfs[j].advertising_id)
                adid = pd.DataFrame(set(y-x))
                adid.rename(columns={0:'advertising_id'},inplace=True)
                dtfs[j] = dtfs[j].merge(adid,on='advertising_id',how='inner')
            
        # for ad_id no purchase events last occurence was taken
        for i in reversed(range(7)):
            x = set(dtfs[i].advertising_id)
            for j in reversed(range(i)):
                y = set(dtfs[j].advertising_id)
                adid = pd.DataFrame(set(y-x))
                adid.rename(columns={0:'advertising_id'},inplace=True)
                dtfs[j] = dtfs[j].merge(adid,on='advertising_id',how='inner')
    
        final_df = pd.concat(dtfs,ignore_index=True,sort=False)
        final_df.drop_duplicates(subset ="advertising_id", keep=False,inplace=True)
    #   print(final_df.shape)
    
        # taking final adIds to get their static features from BQ
        adid = pd.DataFrame(final_df.advertising_id)
        path='/home/airflow/gcs/data/mifo_preprocessing_datafiles/ad_id_day1_day7.csv'
        adid.to_csv(path,index=False)
        print("done")
        return final_df

    evfreq=preprocessing_evfreq(dfs,ad)

def push_googlebq(**Kwargs):
    return GoogleCloudStorageToBigQueryOperator(
        task_id="push_googlebq",
        source_objects=['data/mifo_preprocessing_datafiles/ad_id_day1_day7.csv'],
        source_format='CSV',
        bucket='asia-east2-mifo-tatacliq-as-c3e3e551-bucket',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        destination_project_dataset_table='suger_cosmetics_branch.test_ppp',
        schema_fields=[
            {'name': 'string_field_0', 'type': 'STRING', 'mode': 'NULLABLE'},
        ]
    )

def sugar_usefulfeatures_query(**Kwargs):
    client=bigquery.Client()
    sql_query='''select advertising_id, event_date_with_time,seconds_from_install_to_event,data_platform,city,internet_connection_type,data_channel,operator from(select distinct advertising_id,row_number() over (partition by advertising_id order by event_date_with_time desc) event_time_rank,event_date_with_time,seconds_from_install_to_event,user_data_platform as data_platform,user_data_geo_city_en as city,user_data_internet_connection_type as internet_connection_type,last_attributed_touch_data_channel as data_channel,user_data_carrier_name as operator from `tatacliq-data-analytics.suger_cosmetics_branch.sugar_events_data_14June_20June` where advertising_id in (select string_field_0 from `tatacliq-data-analytics.suger_cosmetics_branch.test_ppp` )) where event_time_rank=1;'''
    result_sugar_usefulfeatures_df=client.query(sql_query).to_dataframe()
    print("Query executed")
    try:
            path = '/home/airflow/gcs/data/mifo_preprocessing_datafiles/sugar_usefulfeatures.csv'
            result_sugar_usefulfeatures_df.to_csv(path,index=False)
            print("\n\nCSV File Saved\n\n")
    except Exception as error:
            print("!!!!!!!!!!!!!!!!!!!!Exception occured!!!!!!!!!!!!!!!!!!", error)
            raise Exception(error)

def preprocessing_evdata(**Kwargs):
    try:
        usefulfeatures_path = '/home/airflow/gcs/data/mifo_preprocessing_datafiles/sugar_usefulfeatures.csv'
        city_tier_path='/home/airflow/gcs/data/mifo_preprocessing_datafiles/Copy of City_Tier.xlsx'

        evdata=pd.read_csv(usefulfeatures_path)
        city_tier=pd.read_excel(city_tier_path)

    except Exception as error:
        print("Exception occured",error)
        raise Exception(error)

    

with DAG(dag_id="parallel_processing_gcp", schedule_interval="@once", default_args=def_argsman, catchup=False) as dag:

    mifo_start = PythonOperator(task_id="mifo_start",python_callable=get_started,provide_context=True,)

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
    start_sugar_usefulfeatures=PythonOperator(task_id="start_sugar_usefulfeatures",python_callable=sugar_usefulfeatures_query,provide_context=True,)
    start_preprocessing_evdata=PythonOperator(task_id="start_preprocessing_evdata",python_callable=preprocessing_evdata,provide_context=True,)

mifo_start >> start_day1_query
mifo_start >> start_day2_query
mifo_start >> start_day3_query
mifo_start >> start_day4_query
mifo_start >> start_day5_query
mifo_start >> start_day6_query
mifo_start >> start_day7_query
mifo_start>>start_ad_id_query

start_day1_query>>start_preprocessing_evpreprocessing
start_day7_query>>start_preprocessing_evpreprocessing
start_day4_query>>start_preprocessing_evpreprocessing
start_day5_query>>start_preprocessing_evpreprocessing
start_day6_query>>start_preprocessing_evpreprocessing
start_day2_query>>start_preprocessing_evpreprocessing
start_day3_query>>start_preprocessing_evpreprocessing
start_ad_id_query>>start_preprocessing_evpreprocessing

start_preprocessing_evpreprocessing>>start_push_bigquery>>start_sugar_usefulfeatures>>start_preprocessing_evdata


    
