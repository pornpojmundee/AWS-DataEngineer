"""
Airflow DAG orchestrating the fintech data lakehouse pipeline:

    generate/ingest raw data -> Spark transform -> data quality checks -> load to Redshift

Run locally with the Astronomer/Airflow docker-compose setup, or deploy to
Amazon MWAA by uploading this file to the DAGs folder.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fintech_lakehouse_pipeline",
    description="Batch ETL: raw -> curated Iceberg -> data quality -> Redshift",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fintech", "lakehouse", "etl"],
) as dag:

    ingest = BashOperator(
        task_id="generate_raw_data",
        bash_command=(
            "python /opt/airflow/data_generator/generate_data.py "
            "--output /opt/airflow/data/raw --customers 5000 --transactions 200000"
        ),
    )

    transform = BashOperator(
        task_id="spark_transform_to_curated",
        bash_command=(
            "spark-submit /opt/airflow/pipelines/spark_jobs/etl_transactions.py "
            "--input /opt/airflow/data/raw --output /opt/airflow/data/curated"
        ),
    )

    def run_quality_checks(**_):
        # In production this loads the curated tables via Spark/Athena and
        # calls pipelines.spark_jobs.data_quality.run_all_checks(...)
        print("running data quality checks against curated tables")

    quality_check = PythonOperator(
        task_id="data_quality_checks",
        python_callable=run_quality_checks,
    )

    load_redshift = BashOperator(
        task_id="load_to_redshift",
        bash_command="echo 'COPY curated tables into Redshift Serverless (placeholder)'",
    )

    ingest >> transform >> quality_check >> load_redshift
