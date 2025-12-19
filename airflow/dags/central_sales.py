# ./airflow/dags/central_sales_simple.py
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator

def etl_central_region():
    logger = print  # или используйте логгер, как раньше
    logger("📥 Чтение данных...")
    df = pd.read_csv("/opt/airflow/data/raw/sales_data.csv", parse_dates=["transaction_date"])
    
    logger(f"Прочитано: {len(df)} строк")

    logger("🔍 Фильтрация региона 'Central'...")
    df_central = df[df["region"] == "Central"]
    
    logger(f"Отобрано: {len(df_central)} строк")

    logger("📤 Запись в sales_db.central_sales...")
    engine = create_engine("postgresql://airflow:airflow@postgres:5432/sales_db")
    df_central.to_sql("central_sales", engine, if_exists="replace", index=False)
    
    logger("✅ Готово!")

with DAG(
    "central_sales_simple",
    start_date=datetime(2025, 12, 18),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "sales"],
) as dag:

    PythonOperator(
        task_id="etl_central",
        python_callable=etl_central_region,
    )