import requests
import random
import json
from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='trigger_dags_w_conf',
    start_date=datetime(2026, 3, 31),
    schedule_interval='@daily',
    default_args=default_args,
    tags=['base'],
    max_active_runs=1,
) as dag:
    trigger_dagrun_europe = TriggerDagRunOperator(
        task_id='trigger_dagrun_europe',
        trigger_dag_id='minio_streetview_dag',
        conf={"continent": "europe", "api_n": 1000},
    )

    trigger_dagrun_sa = TriggerDagRunOperator(
        task_id='trigger_dagrun_sa',
        trigger_dag_id='minio_streetview_dag',
        conf={"continent": "south_america", "api_n": 1000},
    )

    trigger_dagrun_na = TriggerDagRunOperator(
        task_id='trigger_dagrun_na',
        trigger_dag_id='minio_streetview_dag',
        conf={"continent": "north_america", "api_n": 1000},
    )

    trigger_dagrun_eurasia = TriggerDagRunOperator(
        task_id='trigger_dagrun_eurasia',
        trigger_dag_id='minio_streetview_dag',
        conf={"continent": "eurasia", "api_n": 1000},
    )

    trigger_dagrun_asia = TriggerDagRunOperator(
        task_id='trigger_dagrun_asia',
        trigger_dag_id='minio_streetview_dag',
        conf={"continent": "asia", "api_n": 1000},
    )

[trigger_dagrun_europe, trigger_dagrun_sa, trigger_dagrun_na, trigger_dagrun_eurasia, trigger_dagrun_asia]