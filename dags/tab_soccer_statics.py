import pandas as pd
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys

from src.soccer_statics.extract_data import Request_Data
from src.soccer_statics.dim_teams import Dim_Teams


START_DATE = days_ago(1)


def extract_data(**kwargs):
    try:
        pf = Request_Data()
        data_extract = pf.request_return()
        kwargs['ti'].xcom_push(key="data_extract", value=data_extract.to_json())
        print("****************** Success process extract data from api scores")
    except:
        print(
            "****************** Unexpected error: ",
            sys.exc_info(),
        )
        raise

def dim_teams(**kwargs):
    try:
        ti = kwargs["ti"]
        data_extract = ti.xcom_pull(task_ids='extract_data', key='data_extract')
        data_extract = pd.read_json(data_extract)
        data_teams = data_extract[['teams']]
        pf = Dim_Teams(data_teams=data_teams)
        pf.run()
        print("****************** Success process update basics datamart finance")
    except:
        print(
            "****************** Unexpected error update basics datamart finance: ",
            sys.exc_info(),
        )
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
    "tab_soccer_statics",
    default_args=default_args,
    schedule_interval="0 12 * * *",
    start_date=START_DATE,
)

extract_data = PythonOperator(
    task_id="extract_data",
    python_callable=extract_data,
    dag=dag,
)

dim_teams = PythonOperator(
    task_id="dim_teams",
    python_callable=dim_teams,
    dag=dag,
)

extract_data >> dim_teams
