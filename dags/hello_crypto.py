from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("Hello Crypto World!")
    print("Current time:", datetime.now())

with DAG(
    dag_id='hello_crypto',
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual trigger only
    catchup=False
) as dag:
    
    task1 = PythonOperator(
        task_id='say_hello',
        python_callable=print_hello
    )