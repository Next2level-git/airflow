from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys

from src.social_media.user_info import User_info
from src.social_media.instagram_posts import Instagram_Publications
from src.social_media.evolution_stats_instagram import Daily_Instagram

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


def daily_instagram_stats():
    try:
        pf = Daily_Instagram()
        pf.run()
        print("****************** Success process to update instagram post")
    except Exception as e:
        print(
            f"****************** Unexpected error: {e}",
            sys.exc_info(),
        )
        raise


def instagram_publications():
    try:
        pf = Instagram_Publications()
        pf.run()
        print("****************** Success process to update instagram post")
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

task_daily_instagram_statss = PythonOperator(
    task_id="daily_instagram_stats",
    python_callable=daily_instagram_stats,
    dag=dag,
)

task_instagram_publications = PythonOperator(
    task_id="instagram_publications",
    python_callable=instagram_publications,
    dag=dag,
)

task_user_info >> task_daily_instagram_statss >> task_instagram_publications
