import pandas as pd
import os
import json

def transform_to_parquet(source_type):
    os.makedirs("staging", exist_ok=True)
    if source_type == 'csv':
        df = pd.read_csv("dummy_data/data.csv")
    elif source_type == 'json':
        with open("dummy_data/data.json") as f:
            data = json.load(f)
        df = pd.DataFrame(data)
    elif source_type == 'api':
        with open("dummy_data/api_response.json") as f:
            response = json.load(f)
        df = pd.DataFrame(response['data'])
    elif source_type == 'database':
        data = {
            "emp_id": [1, 2, 3],
            "emp_name": ["Anna", "Brian", "Clara"],
            "salary": [70000, 80000, 75000]
        }
        df = pd.DataFrame(data)
    else:
        raise ValueError("Unsupported source type for transformation")

    df.to_parquet(f"staging/{source_type}.parquet", index=False)
    print(f"Data from {source_type} transformed to Parquet and saved to staging/{source_type}.parquet")

def clean_and_standardize(table_name):
    """Clean and standardize the extracted data"""
    try:
        dag_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        input_path = os.path.join(dag_dir, "staging", f"{table_name}.parquet")
        output_path = os.path.join(dag_dir, "staging", f"{table_name}_clean.parquet")
        
        df = pd.read_parquet(input_path)
        
        # Data cleaning
        df.drop_duplicates(inplace=True)
        df.dropna(inplace=True)
        
        if 'transaction_datetime' in df.columns:
            df['transaction_datetime'] = pd.to_datetime(df['transaction_datetime'], errors='coerce')
            df = df.dropna(subset=['transaction_datetime'])
            df['transaction_datetime'] = df['transaction_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        df.to_parquet(output_path, index=False)
        return output_path
    except Exception as e:
        raise Exception(f"Data cleaning failed: {str(e)}")

def aggregate_by_location(table_name):
    """Aggregate data by location"""
    try:
        dag_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        input_path = os.path.join(dag_dir, "staging", f"{table_name}_clean.parquet")
        output_path = os.path.join(dag_dir, "staging", f"{table_name}_agg_location.parquet")
        
        df = pd.read_parquet(input_path)
        
        if not all(col in df.columns for col in ['location', 'total']):
            raise ValueError("DataFrame missing required columns ('location' and/or 'total')")
        
        agg = df.groupby("location")["total"].sum().reset_index()
        agg.to_parquet(output_path, index=False)
        return output_path
    except Exception as e:
        raise Exception(f"Aggregation failed: {str(e)}")