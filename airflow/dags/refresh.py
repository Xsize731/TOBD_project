# dags/refresh_dashboard_data.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'refresh_dashboard_data',
    default_args=default_args,
    description='Trigger Dash dashboard refresh',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['dashboard', 'dash'],
) as dag:

    # Вариант 1: HTTP-запрос в Dash (если у вас есть /reload)
    trigger_dash = SimpleHttpOperator(
        task_id='trigger_dash_reload',
        method='POST',
        http_conn_id='dash_dashboard_conn',  # ← создайте в Airflow UI
        endpoint='/reload',
        headers={"Content-Type": "application/json"},
        data='{"source": "airflow"}',
        response_check=lambda response: response.status_code == 200,
    )

    # Вариант 2 (если нет /reload): просто логируем
    @task
    def log_dashboard_ready():
        print("✅ Аналитика обновлена. Откройте дашборд: http://localhost:8050")

    # Зависимости
    log_dashboard_ready() >> trigger_dash