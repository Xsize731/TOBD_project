from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_message():
    print("✅ Airflow работает корректно!")
    print("Это тестовый DAG для проверки установки.")
    return "Успешно выполнено"

with DAG(
    'test_installation',
    default_args=default_args,
    description='Тестовый DAG для проверки установки Airflow',
    schedule_interval='@once',
    catchup=False,
    tags=['test'],
) as dag:
    
    start = DummyOperator(task_id='start')
    
    test_task = PythonOperator(
        task_id='test_task',
        python_callable=print_message,
    )
    
    end = DummyOperator(task_id='end')
    
    start >> test_task >> end