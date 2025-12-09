"""
Daily Gold Aggregation DAG
Reads from Silver layer, creates aggregations, loads to PostgreSQL gold database
Schedule: Daily @ 5:00 AM (after silver transformation)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow')

from src.gold.aggregate_to_gold import main as gold_aggregate

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_gold_aggregation',
    default_args=default_args,
    description='Daily aggregation from Silver to Gold layer',
    schedule_interval='0 5 * * *',  # Daily at 5 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['gold', 'aggregation', 'daily'],
)

# Wait for silver transformation to complete
wait_for_silver = ExternalTaskSensor(
    task_id='wait_for_silver_transformation',
    external_dag_id='daily_silver_transformation',
    external_task_id='transform_to_silver',
    allowed_states=['success'],
    failed_states=['failed', 'skipped'],
    mode='poke',
    timeout=7200,  # 2 hours
    poke_interval=300,  # 5 minutes
    dag=dag,
)

def run_gold_aggregation(**context):
    """Execute gold layer aggregation"""
    execution_date = context['execution_date']
    print(f"Running gold aggregation for {execution_date}")
    gold_aggregate(run_date=execution_date)

gold_task = PythonOperator(
    task_id='aggregate_to_gold',
    python_callable=run_gold_aggregation,
    provide_context=True,
    dag=dag,
)

wait_for_silver >> gold_task
