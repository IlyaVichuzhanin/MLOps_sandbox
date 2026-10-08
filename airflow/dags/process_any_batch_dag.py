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

# 🔥 Константа — должна совпадать с размером any_orchestration_pool в Admin → Pools
ANY_CONCURRENT_FILES = 5


@task_group(group_id="process_single_any_record")
def process_single_any_record(record: dict):
    """
    Цепочка для одного Any-файла.
    Все 3 триггера держат ОДИН слот any_orchestration_pool последовательно,
    поэтому в полёте всегда не больше ANY_CONCURRENT_FILES файлов.
    """
    trigger_validate = TriggerDagRunOperator(
        task_id='trigger_validate_any',
        trigger_dag_id='validate_any_file_dag',
        conf={'record': record, 'stage': 'validate'},
        wait_for_completion=True,
        deferrable=True,
        poke_interval=60,
        pool='any_orchestration_pool',   # 🔥 ОТДЕЛЬНЫЙ пул для оркестрации
        pool_slots=1,
        allowed_states=['success'],
        failed_states=['failed'],
    )

    trigger_upload = TriggerDagRunOperator(
        task_id='trigger_upload_any',
        trigger_dag_id='upload_any_data_dag',
        conf={'record': record, 'stage': 'upload'},
        wait_for_completion=True,
        deferrable=True,
        poke_interval=60,
        pool='any_orchestration_pool',   # 🔥 тот же пул
        pool_slots=1,
        allowed_states=['success'],
        failed_states=['failed'],
    )

    trigger_flag = TriggerDagRunOperator(
        task_id='trigger_update_flag_any',
        trigger_dag_id='update_any_record_flag_dag',
        conf={'record': record, 'stage': 'update_flag'},
        wait_for_completion=True,
        deferrable=True,
        poke_interval=60,
        pool='any_orchestration_pool',   # 🔥 и тут
        pool_slots=1,
        allowed_states=['success'],
        failed_states=['failed'],
    )

    trigger_validate >> trigger_upload >> trigger_flag


with DAG(
    dag_id='process_any_batch_dag',
    default_args=default_args,
    description=f'Пакетная обработка Any: до {ANY_CONCURRENT_FILES} файлов параллельно',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['cloudberry', 'any', 'batch-processor'],
    max_active_runs=1,   # 🔥 один батч за раз — параллелизм внутри через пул
    concurrency=ANY_CONCURRENT_FILES + 2,  # mapped instances + служебные
) as dag:

    records = get_records_from_conf()
    has_records = check_records(records, record_type="записей Any")

    processing_chains = process_single_any_record.expand(record=records)

    has_records >> processing_chains