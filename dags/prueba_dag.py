from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta

START_DATE = days_ago(1)

data = {
    "columna1": [None, "", "00000", "12345", "11111", "abcdef", "222", "33333", "test"],
    "columna2": ["123", None, "1111", "456", "", "0000", "repeated", "789", "9999"],
}


def crear_dataframe():
    try:
        import pandas as pd

        df = pd.DataFrame(data)
        print(df)
    except Exception as e:
        print(f"Ocurrió un error: {e}")
        raise


default_args = {
    "owner": "daniel.cristancho",
    "depends_on_past": False,
    "start_date": START_DATE,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "max_active_runs": 1,
}

dag = DAG(
    "prueba_librerias",
    default_args=default_args,
    schedule_interval="0 10,12,13 * * *",
    start_date=START_DATE,
)

tarea1 = PythonOperator(
    task_id="tarea1",
    python_callable=crear_dataframe,
    dag=dag,
)

tarea1
