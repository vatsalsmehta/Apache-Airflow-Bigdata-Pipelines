#step 1: import all libraries
import os
import sys
from airflow import DAG
from datetime import datetime, time,timedelta
from airflow.operators.python_operator import PythonOperator
import pandas as pd



#step 2: Defining default args
default_args={
    "owner":"airflow",
    "start_date":datetime(2021,6,17),
    "retries":1,
    "retry_delay":timedelta(minutes=2),
    "email":['vatsalsmehta@gmail.com'],
    "email_on_retry":False,
    "email_on_failure":True,
    "catchup":False
    }

# #step 3
# dag  = DAG(dag_id="parallel_task", schedule_interval="@once", default_args=default_args, catchup=False)

#step4 :Tasks:

def read_file(**kwargs): #Task 1
    file_path=os.path.join(os.getcwd(),"data_files/netflix_titles.csv")
    df=pd.read_csv(file_path)
    print("\n\nWe'll do preprocessing in this dataset \n\n") 
    kwargs['ti'].xcom_push(key="whole_dataframe",value=df)
    #This will push the dataframe and any task who needs it will download it

def delete_columns(**kwargs):
    #In this task we'll delete the columns and rows where we can't fill missing data with anything
    df=kwargs['ti'].xcom_pull(key="whole_dataframe")
    print("\n\nThere are missing values in many columns :\n\n",df.isnull().sum()) 

    #deleting these two columns as large amount of data is not available in this columns
    df.drop(['director','cast'],axis=1,inplace=True)
    kwargs['ti'].xcom_push(key="df_with_deleted_columns",value=df)

    return df

def add_column(**kwargs):
    #In this based on 1 column we'll add another column that is more easy to understand than the
    df=kwargs['ti'].xcom_pull(key="whole_dataframe")

    #Adding a new column 'target_ages' based on the content 'rating'.
    ratings_ages = {
    'TV-PG': 'Older Kids',
    'TV-MA': 'Adults',
    'TV-Y7-FV': 'Older Kids',
    'TV-Y7': 'Older Kids',
    'TV-14': 'Teens',
    'R': 'Adults',
    'TV-Y': 'Kids',
    'NR': 'Adults',
    'PG-13': 'Teens',
    'TV-G': 'Kids',
    'PG': 'Older Kids',
    'G': 'Kids',
    'UR': 'Adults',
    'NC-17': 'Adults'
    }
    df['target_ages'] = df['rating'].replace(ratings_ages)
    kwargs['ti'].xcom_push(key="added_targetages",value=df['target_ages'].to_list())

    return df

def complete_task(**kwargs):
    df=kwargs.get("ti").xcom_pull(key="df_with_deleted_columns")

    #now updating the changes we did in other columns
    df['target_ages'] =kwargs['ti'].xcom_pull(key="added_targetages")

    new_file_path=os.path.join(os.getcwd(),"data_files/new_dataset.csv")

    print("\n\n\n",df.columns)
    df.to_csv(new_file_path)
    

with DAG(dag_id="parallel_task", schedule_interval="@once", default_args=default_args, catchup=False) as dag:

    read_file = PythonOperator(task_id="read_file",python_callable=read_file,provide_context=True,)

    delete_columns = PythonOperator(task_id="delete_columns",python_callable=delete_columns,provide_context=True,)
    add_column = PythonOperator(task_id="add_column",python_callable=add_column,provide_context=True,)

    complete_task = PythonOperator(task_id="complete_task",python_callable=complete_task,provide_context=True,)


read_file >> delete_columns
read_file >> add_column

delete_columns>> complete_task
add_column >> complete_task
