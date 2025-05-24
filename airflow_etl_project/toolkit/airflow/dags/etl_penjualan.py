from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from helpers.extractors import extract_penjualan_csv, extract_penjualan_json
from helpers.loaders import load_penjualan_csv, load_penjualan_json

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='etl_penjualan',
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    description='ETL penjualan sederhana dari CSV dan JSON ke PostgreSQL dan SQLite tanpa transformasi'
) as dag:

    start = EmptyOperator(task_id='start')

    extract_csv = PythonOperator(
        task_id='extract_penjualan_csv',
        python_callable=extract_penjualan_csv
    )

    extract_json = PythonOperator(
        task_id='extract_penjualan_json',
        python_callable=extract_penjualan_json
    )

    load_csv = PythonOperator(
        task_id='load_penjualan_csv',
        python_callable=load_penjualan_csv
    )

    load_json = PythonOperator(
        task_id='load_penjualan_json',
        python_callable=load_penjualan_json
    )

    end = EmptyOperator(task_id='end')

    start >> [extract_csv, extract_json]
    extract_csv >> load_csv
    extract_json >> load_json
    [load_csv, load_json] >> end