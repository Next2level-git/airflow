import pandas as pd
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys

from src.soccer_statics.extract_data import Request_Data
from src.soccer_statics.dim_teams import Dim_Teams
from src.soccer_statics.dim_league import Dim_League
from src.soccer_statics.fact_matches import Fact_Match
from src.soccer_statics.fact_matches_statics import Fact_Match_Statics

START_DATE = days_ago(1)


def extract_data(**kwargs):
    try:
        pf = Request_Data()
        data_extract = pf.request_return()
        kwargs["ti"].xcom_push(key="data_extract", value=data_extract.to_json())
        print("****************** Success process extract data from api scores")
    except Exception as e:
        print(
            f"****************** Unexpected error: {e}",
            sys.exc_info(),
        )
        raise


def dim_teams(**kwargs):
    try:
        ti = kwargs["ti"]
        data_extract = ti.xcom_pull(task_ids="extract_data", key="data_extract")
        data_extract = pd.read_json(data_extract)
        data_teams = data_extract[["teams"]]
        pf = Dim_Teams(data_teams=data_teams)
        pf.run()
        print("****************** Success process update dim teams")
    except Exception as e:
        print(
            f"****************** Unexpected error: {e}",
            sys.exc_info(),
        )
        raise


def dim_league(**kwargs):
    try:
        ti = kwargs["ti"]
        data_extract = ti.xcom_pull(task_ids="extract_data", key="data_extract")
        data_extract = pd.read_json(data_extract)
        data_league = data_extract[["league"]]
        pf = Dim_League(data_league=data_league)
        pf.run()
        print("****************** Success process update dim league")
    except Exception as e:
        print(
            f"****************** Unexpected error: {e}",
            sys.exc_info(),
        )
        raise


def fact_matches(**kwargs):
    try:
        ti = kwargs["ti"]
        data_extract = ti.xcom_pull(task_ids="extract_data", key="data_extract")
        data_extract = pd.read_json(data_extract)
        pf = Fact_Match(data_extract=data_extract)
        pf.run()
        print("****************** Success process update fact matches")
    except Exception as e:
        print(
            f"****************** Unexpected error: {e}",
            sys.exc_info(),
        )
        raise


def fact_matches_statistics():
    try:
        pf = Fact_Match_Statics()
        pf.run()
        print("****************** Success process update fact matches statistics")
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

dim_league = PythonOperator(
    task_id="dim_league",
    python_callable=dim_league,
    dag=dag,
)

fact_matches = PythonOperator(
    task_id="fact_matches",
    python_callable=fact_matches,
    dag=dag,
)

fact_matches_statistics = PythonOperator(
    task_id="fact_matches_statistics",
    python_callable=fact_matches_statistics,
    dag=dag,
)

extract_data >> [dim_teams, dim_league] >> fact_matches >> fact_matches_statistics
