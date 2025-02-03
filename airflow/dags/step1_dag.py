from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import pendulum
import os

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.today('UTC').add(days=-1),
    'retries': 1,
}

data_path = os.getenv('DATA_PATH')

def print_data_path():
    print(f"DATA_PATH: {data_path}")

with DAG(
    'step1_extract_to_files',
    default_args=DEFAULT_ARGS,
    description='DAG to extract data from CSV and PostgreSQL to system files',
    schedule='@daily',
    catchup=False
) as dag1:
    
    start = EmptyOperator(task_id='start')

    print_path = PythonOperator(
        task_id='print_data_path',
        python_callable=print_data_path
    )

    step1_extract_csv = BashOperator(
        task_id='extract_csv',
        bash_command=f"source /home/rafael/projects/indicium/LH_ED_Rafael07/meltano_dataloader/.venv/bin/activate && python /home/rafael/projects/indicium/LH_ED_Rafael07/src/meltano_script.py extract"
    )

    step1_extract_postgres = BashOperator(
        task_id='extract_postgres',
        bash_command=f"source /home/rafael/projects/indicium/LH_ED_Rafael07/meltano_dataloader/.venv/bin/activate && python /home/rafael/projects/indicium/LH_ED_Rafael07/src/meltano_script.py load"
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> print_path >> step1_extract_csv >> step1_extract_postgres >> end