"""
Daily Silver Transformation DAG
Reads from Bronze layer, applies transformations, writes to Silver layer
Schedule: Daily @ 3:00 AM (after bronze ingestion)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow')

from src.silver.transform_to_silver import main as silver_transform

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_silver_transformation',
    default_args=default_args,
    description='Daily transformation from Bronze to Silver layer',
    schedule_interval='0 3 * * *',  # Daily at 3 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['silver', 'transformation', 'daily'],
)

# Wait for bronze ingestion to complete
wait_for_bronze = ExternalTaskSensor(
    task_id='wait_for_bronze_ingestion',
    external_dag_id='daily_bronze_ingestion',
    external_task_id='ingest_to_bronze',
    allowed_states=['success'],
    failed_states=['failed', 'skipped'],
    mode='poke',
    timeout=7200,  # 2 hours
    poke_interval=300,  # 5 minutes
    dag=dag,
)

def run_silver_transformation(**context):
    """Execute silver layer transformation"""
    execution_date = context['execution_date']
    print(f"Running silver transformation for {execution_date}")
    silver_transform(run_date=execution_date)

silver_task = PythonOperator(
    task_id='transform_to_silver',
    python_callable=run_silver_transformation,
    provide_context=True,
    dag=dag,
)

wait_for_bronze >> silver_task
