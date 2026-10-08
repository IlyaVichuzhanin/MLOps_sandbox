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
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=15),
    'execution_timeout': timedelta(hours=6),
    'sla': timedelta(hours=4),
}

FH_CONCURRENT_FILES = 10

@task_group(group_id="process_single_fh_record")
def process_single_fh_record(record: dict):
    trigger_validate = TriggerDagRunOperator(
        task_id='trigger_validate_fh',
        trigger_dag_id='validate_fh_file_dag',
        conf={'record': record, 'stage': 'validate'},
        wait_for_completion=True, poke_interval=60,
        allowed_states=['success'], failed_states=['failed'],
        deferrable=True,
        pool='fh_orchestration_pool',   # 🔥 БЫЛО: нет пула
        pool_slots=1,
        retries=3, retry_delay=timedelta(minutes=5),
    )
    trigger_upload = TriggerDagRunOperator(
        task_id='trigger_upload_fh',
        trigger_dag_id='upload_fh_data_dag',
        conf={'record': record, 'stage': 'upload'},
        wait_for_completion=True, poke_interval=60,
        allowed_states=['success'], failed_states=['failed'],
        deferrable=True,
        pool='fh_orchestration_pool',   # 🔥
        pool_slots=1,
        retries=3, retry_delay=timedelta(minutes=10),
    )
    trigger_flag = TriggerDagRunOperator(
        task_id='trigger_update_flag_fh',
        trigger_dag_id='update_fh_record_flag_dag',
        conf={'record': record, 'stage': 'update_flag'},
        wait_for_completion=True, poke_interval=30,
        allowed_states=['success'], failed_states=['failed'],
        deferrable=True,
        pool='fh_orchestration_pool',   # 🔥 БЫЛО: fh_processing_pool (неправильно!)
        pool_slots=1,
        retries=2, retry_delay=timedelta(minutes=3),
    )
    
    # 🔥 НОВОЕ: Финальная задача, которая возвращает результат через XCom
    @task(task_id='fh_record_success')
    def fh_record_success(rec: dict) -> dict:
        """Возвращает статус успешной обработки записи"""
        return {
            'status': 'success',
            'data_file_id': rec.get('data_file_id'),
            'hdfs_full_path': rec.get('hdfs_full_path'),
            'completed_at': datetime.now().isoformat(),
        }
    
    result = fh_record_success(record)
    
    trigger_validate >> trigger_upload >> trigger_flag >> result
    
    return result  # 🔥 Возвращаем XComArg от финальной задачи


@task(task_id='log_fh_batch_start')
def log_fh_batch_start(records: list) -> dict:
    """Логирует начало обработки батча .fh файлов"""
    log.info(f"🚀 Начинаем обработку {len(records)} .fh файлов")
    for idx, record in enumerate(records):
        log.info(f"   [{idx+1}/{len(records)}] file_id={record.get('data_file_id')}, "
                 f"path={record.get('hdfs_full_path')}")
    
    return {
        'total_records': len(records),
        'start_time': datetime.now().isoformat(),
    }


@task(task_id='log_fh_batch_completion')
def log_fh_batch_completion(processing_results: list, batch_info: dict) -> dict:
    """Логирует завершение обработки батча"""
    log.info(f"✅ Завершена обработка {batch_info['total_records']} .fh файлов")
    log.info(f"   Начало: {batch_info['start_time']}")
    log.info(f"   Конец: {datetime.now().isoformat()}")
    
    # 🔥 ИСПРАВЛЕНО: безопасная обработка результатов
    successful = 0
    if processing_results:
        for r in processing_results:
            if isinstance(r, dict):
                if r.get('status') == 'success':
                    successful += 1
            elif hasattr(r, 'get'):
                # XComArg fallback - если дошли сюда, значит задача выполнена
                successful += 1
        # Если processing_results - это список XComArg'ов, считаем все успешными
        # (иначе DAG упал бы раньше)
        if successful == 0 and len(processing_results) > 0:
            successful = len(processing_results)
    else:
        successful = batch_info['total_records']
    
    log.info(f"   Успешно: {successful}/{batch_info['total_records']}")
    
    return {
        'total_processed': len(processing_results) if processing_results else batch_info['total_records'],
        'successful': successful,
        'completion_time': datetime.now().isoformat(),
    }


with DAG(
    dag_id='process_fh_batch_dag',
    default_args=default_args,
    description='Пакетная обработка записей типа FH с защитой от zombie jobs',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['cloudberry', 'FH', 'Double', 'batch-processor', 'binary-format', 'zombie-protection'],
    dagrun_timeout=timedelta(hours=12),
    max_active_runs=1,
    concurrency=10,
    doc_md="""
    ## Пакетная обработка .fh файлов
    
    ### Защита от zombie jobs:
    - Используется пул `fh_processing_pool` (3 слота)
    - Увеличенные timeouts (до 6 часов)
    - Retry с экспоненциальной задержкой
    - Деферабельные операторы для экономии ресурсов
    
    ### Поток выполнения:
    1. Проверка наличия записей
    2. Для каждой записи: validate → upload → update_flag → return_success
    3. Логирование результатов
    """,
) as dag:

    records = get_records_from_conf()
    has_records = check_records(records, record_type="записей FH")
    
    # Логируем начало батча
    batch_info = log_fh_batch_start(records)
    
    # Динамическое создание цепочек с ограничением параллелизма через pool
    processing_chains = process_single_fh_record.expand(record=records)
    
    # Логируем завершение после всех цепочек
    completion_log = log_fh_batch_completion(
        processing_results=processing_chains,
        batch_info=batch_info
    )
    
    # Граф зависимостей
    has_records >> batch_info >> processing_chains >> completion_log