from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from helpers.extractors import extract_csv, extract_json, extract_api, extract_database
from helpers.transformers import transform_to_parquet
from helpers.loaders import load_to_sqlite, load_to_postgresql

def determine_extraction_path(**kwargs):
    source_type = kwargs['params'].get('source_type', 'csv')
    if source_type == 'csv':
        return 'extract_csv_task'
    elif source_type == 'json':
        return 'extract_json_task'
    elif source_type == 'api':
        return 'extract_api_task'
    elif source_type == 'database':
        return 'extract_database_task'
    else:
        return 'invalid_source_task'

def invalid_source():
    raise ValueError("Invalid source_type parameter. Choose from: csv, json, api, database.")

def stage_and_load(source_type):
    file_path = f"staging/{source_type}.parquet"
    table_name = "students" if source_type == "json" else source_type
    load_to_sqlite(file_path, db_name="combined_data", table_name=table_name)
    load_to_postgresql(file_path, table_name=table_name)

def stage_and_load(source_type):
    file_path = f"staging/{source_type}.parquet"
    load_to_sqlite(file_path, db_name="combined_data")
    load_to_postgresql(file_path)

def build_dag():
    with DAG(
        dag_id='etl_pipeline',
        default_args={
            'owner': 'airflow',
            'depends_on_past': False,
            'start_date': datetime(2025, 1, 1),
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=1),
        },
        schedule_interval=None,
        catchup=False,
        params={
            'source_type': 'csv'  # default value
        },
        description='DAG untuk extract dari berbagai sumber, staging, dan load ke database'
    ) as dag:

        start = EmptyOperator(task_id='start')

        choose_path = BranchPythonOperator(
            task_id='choose_extraction_path',
            python_callable=determine_extraction_path,
            provide_context=True
        )

        extract_csv_task = PythonOperator(
            task_id='extract_csv_task',
            python_callable=extract_csv
        )

        extract_json_task = PythonOperator(
            task_id='extract_json_task',
            python_callable=extract_json
        )

        extract_api_task = PythonOperator(
            task_id='extract_api_task',
            python_callable=extract_api
        )

        extract_database_task = PythonOperator(
            task_id='extract_database_task',
            python_callable=extract_database
        )

        invalid_source_task = PythonOperator(
            task_id='invalid_source_task',
            python_callable=invalid_source
        )

        stage_and_load_task = PythonOperator(
            task_id='stage_and_load_task',
            python_callable=stage_and_load,
            op_kwargs={'source_type': '{{ params.source_type }}'},
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
        )

        end = EmptyOperator(task_id='end')

        start >> choose_path >> [
            extract_csv_task,
            extract_json_task,
            extract_api_task,
            extract_database_task,
            invalid_source_task
        ]

        [
            extract_csv_task,
            extract_json_task,
            extract_api_task,
            extract_database_task
        ] >> stage_and_load_task >> end

    return dag


globals()['extract_transform_load_dag'] = build_dag()