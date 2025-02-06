from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
        bash_command=f"source {data_path}/meltano_dataloader/.venv/bin/activate && source {data_path}/airflow/set_air_env.sh && python {data_path}/src/meltano_script.py extract"
    )

    step1_extract_postgres = BashOperator(
        task_id='extract_postgres',
        bash_command=f"source {data_path}/meltano_dataloader/.venv/bin/activate && source {data_path}/airflow/set_air_env.sh && python {data_path}/src/meltano_script.py load"
    )

    trigger_step2 = TriggerDagRunOperator(
        task_id='trigger_step2',
        trigger_dag_id='step2_load_target_db',  # Nome exato da DAG a ser acionada
        wait_for_completion=False,  # Define se espera a DAG 2 terminar antes de concluir essa tarefa
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> print_path >> step1_extract_csv >> step1_extract_postgres >> trigger_step2 >> end