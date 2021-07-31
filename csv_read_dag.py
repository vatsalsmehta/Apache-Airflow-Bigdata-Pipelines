try:
    import os
    import pandas as pd
    from datetime import datetime,timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    print("All Dag modules are imported Sucessfully")

except Exception as e:
    print("Error is {}".format(e))


#its recommended to give dag_id/dag name same as the file name

#you can also make a default_args dictionary before and then just call that dictionary variable here
#since our tasks are not that complex I am defining just one dag that I'll use for both task 
dag_both_task=DAG(

    dag_id="csv_read_dag",
    schedule_interval="@daily",
    default_args={'owner':'airflow','depends_on_past':False,'retry_delay':timedelta(minutes=5),'start_date':datetime(2021,7,7),'retries':1},
    catchup=False

)


#defining task 1 jaha data read karkwe print karayege
def first_function_execute():#task 1
    df=pd.read_csv('/home/vatsal.mehta/airflow/dags/data_files/titanic.csv')
    print("\n\nVatsal Testing\n\n")
    print(df.head())

    # df['Age'] = df['Age'].interpolate()
    # cols = ['Name', 'Ticket', 'Cabin']
    # df = df.drop(cols, axis=1)

#defining task 2 or the ending task
def second_function_execute():#task 2
    print("Everything Done")


start_task=PythonOperator(task_id="start_task",python_callable=first_function_execute,dag=dag_both_task)
end_task=PythonOperator(task_id="end_task",python_callable=second_function_execute,dag=dag_both_task)

start_task>>end_task
