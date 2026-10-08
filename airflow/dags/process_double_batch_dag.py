from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import logging
from validate_data.common_tasks import get_records_from_conf, check_records

log = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}
DOUBLE_CONCURRENT_FILES = 10

@task_group(group_id="process_single_double_record")
def process_single_double_record(record: dict):
    trigger_validate = TriggerDagRunOperator(
        task_id='trigger_validate_double',
        trigger_dag_id='validate_double_file_dag',
        conf={'record': record, 'stage': 'validate'},
        wait_for_completion=True, poke_interval=60,
        allowed_states=['success'], failed_states=['failed'],
        deferrable=True,
        pool='double_orchestration_pool',   # 🔥 НОВОЕ
        pool_slots=1,
    )
    trigger_upload = TriggerDagRunOperator(
        task_id='trigger_upload_double',
        trigger_dag_id='upload_double_data_dag',
        conf={'record': record, 'stage': 'upload'},
        wait_for_completion=True, poke_interval=60,
        allowed_states=['success'], failed_states=['failed'],
        deferrable=True,
        pool='double_orchestration_pool',   # 🔥
        pool_slots=1,
    )
    trigger_flag = TriggerDagRunOperator(
        task_id='trigger_update_flag_double',
        trigger_dag_id='update_double_record_flag_dag',
        conf={'record': record, 'stage': 'update_flag'},
        wait_for_completion=True, poke_interval=60,
        allowed_states=['success'], failed_states=['failed'],
        deferrable=True,
        pool='double_orchestration_pool',   # 🔥
        pool_slots=1,
    )
    trigger_validate >> trigger_upload >> trigger_flag


with DAG(
    dag_id='process_double_batch_dag',
    default_args=default_args,
    description='Пакетная обработка записей типа Double',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['cloudberry', 'Double', 'batch-processor'],
    max_active_runs=1,
    concurrency=DOUBLE_CONCURRENT_FILES + 3,
) as dag:

    records = get_records_from_conf()
    has_records = check_records(records, record_type="записей Double")
    
    processing_chains = process_single_double_record.expand(record=records)
    
    has_records >> processing_chains