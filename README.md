# Apache-Airflow-Bigdata-Pipelines 💻
This repo contains various DAGs for Apache Airflow pipelines in Python along with Google Bigquery as a Data Warehouse.<br><br> From Basics to Advance

## Dag - 1: Understanding the Steps in writing your Directed Acyclic Graph
[DummyOperator](https://github.com/vatsalsmehta/Apache-Airflow-Bigdata-Pipelines/blob/main/dummy_operator.py): This Code for Dag Contains what are the steps in writing a pipeline, from writing default args to writing the Tasks.

## Dag - 2: Perfoming Basic Functions inside a Task of Pipeline
[PythonOperator](https://github.com/vatsalsmehta/Apache-Airflow-Bigdata-Pipelines/blob/main/csv_read_dag.py): This Code implements reading a basic csv file and using PythonOperator for the same, one can perform various functions in the same

## Dag - 3: Using Xcom to send data between Tasks/Functions
[Xcom](https://github.com/vatsalsmehta/Apache-Airflow-Bigdata-Pipelines/blob/main/xcom_dag.py): Here we perform some preprocessing and after each task we send some or other data to the next Task.

## Dag-4 : Basic Parallel Processing
[Parallel Processing](https://github.com/vatsalsmehta/Apache-Airflow-Bigdata-Pipelines/blob/main/parallel_task.py): Here we have multiple task and we perfom the in parallel which results in more computation power but less computation time
