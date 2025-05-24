import pandas as pd
from sqlalchemy import create_engine
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine

def load_to_sqlite(file_path, db_name="combined_data"):
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    parquet_path = os.path.join(base_path, file_path)  # ensure absolute path

    df = pd.read_parquet(parquet_path)

    db_path = os.path.join(base_path, "data", f"{db_name}.db")
    engine = create_engine(f"sqlite:///{db_path}")
    table_name = os.path.splitext(os.path.basename(parquet_path))[0]

    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Loaded into SQLite DB: {db_path}, table: {table_name}")

def load_to_postgresql(file_path):
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    parquet_path = os.path.join(base_path, file_path)

    df = pd.read_parquet(parquet_path)
    table_name = os.path.splitext(os.path.basename(parquet_path))[0]

    # Konversi kolom dict/list ke string
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].astype(str)

    postgres_uri = "postgresql://neondb_owner:npg_LDrm98EHMpyn@ep-misty-smoke-a1yt8l9s-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require"
    engine = create_engine(postgres_uri)
    df.to_sql(table_name, engine, if_exists='replace', index=False)

    print(f"Loaded into PostgreSQL table: {table_name}")

def load_penjualan_csv():
    load_to_sqlite("staging/penjualan_csv_raw.parquet")
    load_to_postgresql("staging/penjualan_csv_raw.parquet")

def load_penjualan_json():
    load_to_sqlite("staging/penjualan_json_raw.parquet")
    load_to_postgresql("staging/penjualan_json_raw.parquet")

def load_cleaned_to_postgres(table_name, suffix):
    """Load processed data to PostgreSQL NeonDB"""
    try:
        dag_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        file_path = os.path.join(dag_dir, "staging", f"{table_name}_{suffix}.parquet")
        
        df = pd.read_parquet(file_path)
        
        # Menggunakan PostgresHook dengan connection yang sudah dibuat
        hook = PostgresHook(postgres_conn_id='neon_db')
        engine = hook.get_sqlalchemy_engine()
        target_table = f"{table_name}_{suffix}"
        
        # Load data dengan chunksize
        df.to_sql(
            target_table,
            engine,
            if_exists='replace',
            index=False,
            chunksize=1000, 
            method='multi'  
        )
        
        print(f"Successfully loaded data to {target_table} in NeonDB")
        return f"Data loaded to {target_table}"
    except Exception as e:
        raise Exception(f"Loading to PostgreSQL NeonDB failed: {str(e)}")