# dags/ingest_raw_sales.py
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import ast

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

@task
def load_csv_to_db():
    # Путь к файлу (внутри контейнера)
    csv_path = "/opt/airflow/data/raw/sales.csv"
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    
    # Читаем CSV без предварительного парсинга
    df = pd.read_csv(csv_path, low_memory=False)
    
    # Парсим ТОЛЬКО prices как JSON
    if 'prices' in df.columns:
        df['prices'] = df['prices'].apply(
            lambda x: ast.literal_eval(x) 
            if pd.notna(x) and isinstance(x, str) and (x.startswith('[') or x.startswith('{'))
            else []
        )
    
    # Обрабатываем остальные "списковые" поля как строки → разделяем по запятой
    list_cols = ['categories', 'imageURLs', 'sourceURLs', 'keys', 'asins']
    for col in list_cols:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: [item.strip() for item in x.split(',')] if isinstance(x, str) else []
            )
    
    # Преобразуем даты
    date_cols = ['dateAdded', 'dateUpdated']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Подключаемся к БД
    hook = PostgresHook(postgres_conn_id='sales_db_conn')
    engine = hook.get_sqlalchemy_engine()
    
    # Загружаем в sales_raw (заменяем)
    df.to_sql('sales_raw', engine, if_exists='replace', index=False)
    print(f"✅ Загружено {len(df)} строк в sales_raw")

with DAG(
    'ingest_raw_sales',
    default_args=default_args,
    description='Load raw sales.csv → sales_raw',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['etl', 'sales'],
) as dag:
    load_csv_to_db()