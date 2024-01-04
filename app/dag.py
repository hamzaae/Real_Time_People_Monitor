from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import json
from hive_script import save_to_hive
from pyspark_script import process_data

def rtdbpc_dag():
    # Data processing code goes here
    print("Data processing :" + str(datetime.now()))

# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'batch_dag',
    default_args=default_args,
    description='DAG for batch layer data processing',
    schedule_interval=timedelta(minutes=1),
)

# Define an Airflow task to execute the data processing and upload function
process_and_upload_task = PythonOperator(
    task_id='process_and_upload_data',
    python_callable=rtdbpc_dag,
    dag=dag,
)

# Set the task dependencies
process_and_upload_task

if __name__ == "__main__":
    dag.cli()

    # airflow dags trigger batch_dag
