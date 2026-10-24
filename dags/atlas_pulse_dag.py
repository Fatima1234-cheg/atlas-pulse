from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Fatima',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='atlas_pulse_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    description='Pipeline complet : Ingestion -> Spark -> dbt'
) as dag:

    task_ingest = BashOperator(
        task_id='ingestion_api',
        bash_command='cd /opt/airflow/project && python -m ingestion.fetch_weather',
    )

    # AJOUT DE SUDO ICI
    task_spark = BashOperator(
        task_id='spark_bronze_to_silver',
        bash_command='sudo docker exec atlas-pulse-spark python -m spark_jobs.bronze_to_silver',
    )

    task_dbt = BashOperator(
        task_id='dbt_run_gold',
        bash_command='cd /opt/airflow/project/atlas_pulse_dbt && python -c "from dbt.cli.main import cli; cli()" run',
    )

    task_ingest >> task_spark >> task_dbt