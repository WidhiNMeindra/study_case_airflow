from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from helpers.extractors import extract_from_sqlite
from helpers.transformers import clean_and_standardize, aggregate_by_location
from helpers.loaders import load_cleaned_to_postgres

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='etl_sqlite_clean',
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    params={
        'table_name': 'penjualan_csv_raw'
    },
    description='Extract, Clean, Transform, and Aggregate data from SQLite to PostgreSQL'
) as dag:

    start = EmptyOperator(task_id='start')

    extract = PythonOperator(
        task_id='extract_from_sqlite',
        python_callable=extract_from_sqlite,
        op_kwargs={"table_name": "{{ params.table_name }}"}
    )

    clean = PythonOperator(
        task_id='clean_and_standardize',
        python_callable=clean_and_standardize,
        op_args=['{{ params.table_name }}']
    )

    aggregate = PythonOperator(
        task_id='aggregate_by_location',
        python_callable=aggregate_by_location,
        op_args=['{{ params.table_name }}']
    )

    load_clean = PythonOperator(
        task_id='load_cleaned_to_postgres',
        python_callable=load_cleaned_to_postgres,
        op_args=['{{ params.table_name }}', 'clean']
    )

    load_agg = PythonOperator(
        task_id='load_aggregated_to_postgres',
        python_callable=load_cleaned_to_postgres,
        op_args=['{{ params.table_name }}', 'agg_location']
    )

    end = EmptyOperator(task_id='end')

    start >> extract >> clean >> [aggregate, load_clean]
    aggregate >> load_agg >> end
    load_clean >> end