from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pendulum
import os

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.today('UTC').add(years=-1),
    'retries': 1,
}

data_path = os.getenv('DATA_PATH')

def print_data_path():
    print(f"DATA_PATH: {data_path}")

with DAG(
    'step2_load_target_db',
    default_args=DEFAULT_ARGS,
    description='DAG to load extracted data into target PostgreSQL database',
    schedule_interval='@daily',
    catchup=False
) as dag2:
    
    start = EmptyOperator(task_id='start')

    print_path = PythonOperator(
        task_id='print_data_path',
        python_callable=print_data_path
    )
    
    step2_load_target_db = BashOperator(
        task_id='load_target_db',
        bash_command=f"source {data_path}/meltano_dataloader/.venv/bin/activate && source {data_path}/set_env.sh && python {data_path}/src/meltano_script.py push"
    )

    run_final_query = BashOperator(
        task_id='run_final_query',
        bash_command=f"source {data_path}/meltano_dataloader/.venv/bin/activate && python {data_path}/src/final_query.py '{{{{ ds }}}}'"
    )

    end = EmptyOperator(task_id='end')
    
    start >> print_path >> step2_load_target_db >> run_final_query >> end