from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys

from src.social_media.user_info import User_info

START_DATE = days_ago(1)


def user_info():
    try:
        pf = User_info()
        pf.run()
        print("****************** Success process to extract data to users")
    except Exception as e:
        print(
            f"****************** Unexpected error: {e}",
            sys.exc_info(),
        )
        raise


default_args = {
    "owner": "daniel.cristancho",
    "depends_on_past": False,
    "start_date": START_DATE,
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
    "max_active_runs": 1,
}

dag = DAG(
    "tab_social_media",
    default_args=default_args,
    schedule_interval="0 12 * * *",
    start_date=START_DATE,
)

task_user_info = PythonOperator(
    task_id="user_info",
    python_callable=user_info,
    dag=dag,
)

task_user_info
