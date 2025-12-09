"""
Daily Bronze Ingestion DAG
Extracts data from PostgreSQL source and loads to MinIO bronze layer
Schedule: Daily @ 1:00 AM
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys
import os

# Add src to path
sys.path.append('/opt/airflow')

from src.bronze.ingest_to_bronze import main as bronze_ingest

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_bronze_ingestion',
    default_args=default_args,
    description='Daily ingestion from PostgreSQL to Bronze layer in MinIO',
    schedule_interval='0 1 * * *',  # Daily at 1 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['bronze', 'ingestion', 'daily'],
)

def run_bronze_ingestion(**context):
    """Execute bronze layer ingestion"""
    execution_date = context['execution_date']
    print(f"Running bronze ingestion for {execution_date}")
    bronze_ingest(run_date=execution_date)

bronze_task = PythonOperator(
    task_id='ingest_to_bronze',
    python_callable=run_bronze_ingestion,
    provide_context=True,
    dag=dag,
)

bronze_task
