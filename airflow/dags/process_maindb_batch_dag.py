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

MAINDB_CONCURRENT_FILES = 1

@task_group(group_id="process_single_maindb_record")
def process_single_maindb_record(record: dict):
    """
    Цепочка для одной записи MainDb:
    validate → upload → update_flag
    """
    trigger_validate = TriggerDagRunOperator(
        task_id='trigger_validate_maindb',
        trigger_dag_id='validate_maindb_file_dag',
        conf={'record': record, 'stage': 'validate'},
        wait_for_completion=True,
        poke_interval=60,
        pool='maindb_orchestration_pool', 
        pool_slots=1,
        allowed_states=['success'],
        failed_states=['failed'],
    )
    
    trigger_upload = TriggerDagRunOperator(
        task_id='trigger_upload_maindb',
        trigger_dag_id='upload_maindb_data_dag',
        conf={'record': record, 'stage': 'upload'},
        wait_for_completion=True,
        poke_interval=60,
        pool='maindb_orchestration_pool',  # 🔑 Ограничение параллелизма
        pool_slots=1,
        allowed_states=['success'],
        failed_states=['failed'],
    )
    
    trigger_flag = TriggerDagRunOperator(
        task_id='trigger_update_flag_maindb',
        trigger_dag_id='update_maindb_record_flag_dag',
        conf={'record': record, 'stage': 'update_flag'},
        wait_for_completion=True,
        poke_interval=60,
        pool='maindb_orchestration_pool',  # 🔑 Ограничение параллелизма
        pool_slots=1,
        allowed_states=['success'],
        failed_states=['failed'],
    )
    
    trigger_validate >> trigger_upload >> trigger_flag


with DAG(
    dag_id='process_maindb_batch_dag',
    default_args=default_args,
    description='Пакетная обработка записей типа MainDb: валидация → загрузка → флаг',
    max_active_runs=1,
    concurrency=MAINDB_CONCURRENT_FILES + 3,
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['cloudberry', 'maindb', 'batch-processor'],
) as dag:

    # Получаем записи
    records = get_records_from_conf()
    
    # Логирование/проверка (опционально)
    has_records = check_records(records, record_type="записей MainDb")
    
    # Динамическое создание цепочек
    # Если records == [], expand() автоматически создаст 0 задач и будет SKIPPED
    processing_chains = process_single_maindb_record.expand(record=records)
    
    # Простая цепочка без ShortCircuitOperator
    records >> has_records >> processing_chains