from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import timedelta
import sys

from src.social_media.evolution_stats_instagram import Daily_Instagram


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


default_args = {
    "owner": "daniel.cristancho",
    "depends_on_past": False,
    "start_date": None,
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
    "max_active_runs": 1,
}

dag = DAG(
    "tab_refresh_stats",
    default_args=default_args,
    schedule_interval="5 * * * *",
    start_date=None,
)

task_daily_instagram_statss = PythonOperator(
    task_id="daily_instagram_stats",
    python_callable=daily_instagram_stats,
    dag=dag,
)

task_daily_instagram_statss
