# 🌀 Airflow ETL Pipeline

Project ini adalah implementasi pipeline ETL (Extract-Transform-Load) menggunakan **Apache Airflow**, yang mengelola proses ekstraksi data dari berbagai sumber (CSV, JSON, API), menyimpannya dalam staging area (Parquet), dan kemudian memuatnya ke dalam database SQLite dan PostgreSQL (Neon DB).

---

## Struktur Folder

```
airflow_etl_project/
├── airflow/
│   ├── dags/                 # DAG utama (etl_pipeline.py)
│   ├── plugins/              # Plugin Airflow (jika ada)
│   ├── data/                 # Output database SQLite
│   ├── logs/                 # Log Airflow
│   ├── dummy_data/           # File dummy (CSV, JSON, API)
│   ├── staging/              # File hasil extract (Parquet)
│   ├── .env                  # Konfigurasi environment
│   └── docker-compose.yaml   # Konfigurasi layanan
```

---

## Fitur Pipeline

### Extract
- CSV: `dummy_data/data.csv`
- JSON: `dummy_data/data.json`
- API: `https://jsonplaceholder.typicode.com/users`
- (opsional) Database: dapat dikembangkan ke sumber eksternal

### Transform
- Semua data diubah ke format **Parquet** dan disimpan di `staging/`
- JSON & API otomatis di-*flatten* dengan `json_normalize`

### Load
- Data dimuat ke:
  - **SQLite** (`data/combined_data.db`)
  - **PostgreSQL** (NeonDB)