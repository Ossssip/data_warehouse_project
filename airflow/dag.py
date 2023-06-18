import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import subprocess



def fetch_fresh_data():
    # Call the existing Python script
    #script_path = os.path.join(os.path.dirname(__file__), '../ergastdb_to_bigquery/upload_csv_to_bigquery.py')
    print('fetching data')
    #subprocess.call(['python', script_path])



def run_dbt():
    # This task will execute the dbt run command
    dbt_working_directory = os.path.join(os.path.dirname(__file__), '../')
    dbt_run_command = f"dbt run --project-dir {dbt_working_directory}"  # Modify this if needed
    
    return BashOperator(
        task_id='dbt_run',
        bash_command=dbt_run_command,
        dag=dag
 #       working_directory=dbt_working_directory
    )


default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('dbt_dag', default_args=default_args, schedule_interval='@weekly')

python_script_task = PythonOperator(
    task_id='fetch_fresh_data',
    python_callable=fetch_fresh_data,
    dag=dag
)

dbt_task = run_dbt()

python_script_task >> dbt_task