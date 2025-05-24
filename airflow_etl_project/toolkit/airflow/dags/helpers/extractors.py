import pandas as pd
import json
import os
import requests
from sqlalchemy import create_engine

def extract_csv():
    base_path = os.path.dirname(__file__) 
    csv_path = os.path.join(base_path, "..", "dummy_data", "Students_Grading_Dataset.csv")
    staging_path = os.path.join(base_path, "..", "staging", "csv.parquet")

    csv_path = os.path.abspath(csv_path)
    staging_path = os.path.abspath(staging_path)

    print(f"CSV path: {csv_path}")
    print(f"Staging path: {staging_path}")

    os.makedirs(os.path.dirname(staging_path), exist_ok=True)
    df = pd.read_csv(csv_path)
    df.to_parquet(staging_path, index=False)

    print(f"CSV extracted from {csv_path} and saved to {staging_path}")

def extract_json():
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    json_path = os.path.join(base_path, "dummy_data", "Students_Grading_Dataset.json")
    staging_path = os.path.join(base_path, "staging", "json.parquet")

    os.makedirs(os.path.dirname(staging_path), exist_ok=True)
    with open(json_path) as f:
        data = json.load(f)
    df = pd.DataFrame(data)
    df.to_parquet(staging_path, index=False)
    print(f"JSON extracted from {json_path} and saved to {staging_path}")

def extract_api():
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    staging_path = os.path.join(base_path, "staging", "api.parquet")

    os.makedirs(os.path.dirname(staging_path), exist_ok=True)

    url = "https://jsonplaceholder.typicode.com/users"
    response = requests.get(url)
    response.raise_for_status()

    data = response.json()

    #Flatten nested JSON
    df = pd.json_normalize(
        data,
        sep="."
    )

    df.to_parquet(staging_path, index=False)
    print(f"API data extracted from {url} and saved to {staging_path}")

def extract_database():
    data = {
        "emp_id": [1, 2, 3],
        "emp_name": ["Anna", "Brian", "Clara"],
        "salary": [70000, 80000, 75000]
    }
    df = pd.DataFrame(data)
    os.makedirs("staging", exist_ok=True)
    df.to_parquet("staging/database.parquet", index=False)
    print("Simulated DB data extracted and saved to staging/database.parquet")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DUMMY_DIR = os.path.join(BASE_DIR, "dummy_data")
STAGING_DIR = os.path.join(BASE_DIR, "staging")
os.makedirs(STAGING_DIR, exist_ok=True)

def extract_penjualan_csv():
    df = pd.read_csv(os.path.join(DUMMY_DIR, "penjualan.csv"), on_bad_lines='skip')
    df.to_parquet(os.path.join(STAGING_DIR, "penjualan_csv_raw.parquet"), index=False)
    print("Penjualan CSV extracted and saved to staging/penjualan_csv_raw.parquet")

def extract_penjualan_json():
    with open(os.path.join(DUMMY_DIR, "penjualan.json")) as f:
        data = json.load(f)
    df = pd.DataFrame(data)
    df.to_parquet(os.path.join(STAGING_DIR, "penjualan_json_raw.parquet"), index=False)
    print("Penjualan JSON extracted and saved to staging/penjualan_json_raw.parquet")


def extract_from_sqlite(table_name="penjualan_csv_raw", db_name="combined_data.db"):
    """Extract data from SQLite database"""
    dag_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.path.join(dag_dir, "data", db_name)
    
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"SQLite database not found at: {db_path}")
    
    try:
        engine = create_engine(f"sqlite:///{db_path}")
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, engine)
        
        staging_dir = os.path.join(dag_dir, "staging")
        os.makedirs(staging_dir, exist_ok=True)
        
        output_path = os.path.join(staging_dir, f"{table_name}.parquet")
        df.to_parquet(output_path, index=False)
        
        return output_path
    except Exception as e:
        raise Exception(f"Extraction failed: {str(e)}")