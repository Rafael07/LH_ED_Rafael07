from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

import os

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

root_dir = '/home/rafael/projects/indicium/LH_ED_Rafael07'

with DAG(
    'step2_load_target_db',
    default_args=DEFAULT_ARGS,
    description='DAG to load extracted data into target PostgreSQL database',
    schedule_interval='@daily',
    catchup=False
) as dag2:
    
    start = DummyOperator(task_id='start')
    
    step2_load_target_db = BashOperator(
        task_id='load_target_db',
        bash_command=f"source /home/rafael/projects/indicium/LH_ED_Rafael07/meltano_dataloader/.venv/bin/activate && python {root_dir}/src/meltano_script.py push"
    )

    end = DummyOperator(task_id='end')
    
    start >> step2_load_target_db >> end