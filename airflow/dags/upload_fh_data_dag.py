from datetime import datetime, timedelta
from airflow.decorators import task, dag
import logging
import os
from validate_data.common_tasks import get_record_info_from_conf
from functions.upload_functions import (
    stream_fh_to_staging,
    merge_fh_data_values_to_target_by_file,
)

from functions.common_functions import (
    clean_staging_layer_by_file,
    clean_duplicate_staging_rows_by_file
)

log = logging.getLogger(__name__)
STG_TABLE = 'stg_fh_double_object_data_values_hist'
TARGET_TABLE = 'hist_double_data'  # 🔥 Имя таблицы в ClickHouse и Iceberg

MAX_FILE_SIZE_MB = 2000
MEMORY_WARNING_THRESHOLD_MB = 8000
UPLOAD_CHUNK_SIZE = 10000


@dag(
    dag_id='upload_fh_data_dag',
    max_active_runs=15,
    concurrency=5,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'retry_exponential_backoff': True,
        'max_retry_delay': timedelta(minutes=30),
        'execution_timeout': timedelta(hours=4),
        'sla': timedelta(hours=2),
    },
    description='Загрузка .fh файла через изолированный staging сразу в CH и S3',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['hdfs', 'fh', 'binary-format', 'data-upload', 'double', 'staging-isolated', 'zombie-protection'],
)
def upload_fh_data_dag():

    # =========================================================================
    # 🔥 ЭТАП 0: Извлечение main_db_name из конфига
    # =========================================================================
    @task(task_id='get_main_db_name')
    def get_main_db_name(record_info: dict) -> str:
        """Имя исходной БД из конфига запуска (оркестратор читает из data_catalogue)."""
        return str((record_info or {}).get('main_db_name'))

    # @task(task_id='monitor_system_resources',
    #       pool='fh_processing_pool', pool_slots=1, retries=3, retry_delay=timedelta(minutes=5))
    # def monitor_system_resources(record_info: dict) -> dict:
    #     import psutil
    #     memory = psutil.virtual_memory()
    #     memory_used_mb = memory.used / (1024 * 1024)
    #     cpu_percent = psutil.cpu_percent(interval=1)
    #     log.info(f"💾 Память: {memory_used_mb:.0f} MB ({memory.percent:.1f}%), CPU: {cpu_percent:.1f}%")
    #     return {
    #         'memory_used_mb': memory_used_mb,
    #         'memory_percent': memory.percent,
    #         'cpu_percent': cpu_percent,
    #         'timestamp': datetime.now().isoformat(),
    #     }

    @task(task_id='upload_fh_to_staging',
          pool='fh_processing_pool', pool_slots=1, retries=3, retry_delay=timedelta(minutes=5))
    def upload_fh_to_staging(record_info: dict, main_db_name: str) -> dict:   #system_resources: dict,
        hdfs_path = record_info['hdfs_path']
        data_file_id = record_info['data_file_id']
        data_source_id = record_info['data_source_id']
        main_db_id = record_info['main_db_id']

        log.info(f"🚀 Начало загрузки: {hdfs_path} (file_id={data_file_id}, db={main_db_name})")

        from validate_data.hdfs_utils import hdfs_tempfile
        with hdfs_tempfile(hdfs_path=hdfs_path) as local_path:
            file_size_bytes = os.path.getsize(local_path)
            file_size_mb = file_size_bytes / (1024 * 1024)
            log.info(f"📦 Размер файла: {file_size_mb:.2f} MB")

            if file_size_mb > MAX_FILE_SIZE_MB:
                log.warning(f"⚠️ Файл больше {MAX_FILE_SIZE_MB} MB!")

            import psutil
            available_memory_mb = psutil.virtual_memory().available / (1024 * 1024)
            if available_memory_mb < MEMORY_WARNING_THRESHOLD_MB:
                log.warning(f"⚠️ Мало доступной памяти: {available_memory_mb:.0f} MB")

            log.info(f"📥 Stream в staging таблицу {STG_TABLE}")
            inserted = stream_fh_to_staging(
                local_path=local_path,
                data_file_id=data_file_id,
                data_source_id=data_source_id,
                main_db_id=main_db_id,
                stg_table_name=STG_TABLE,
                data_type_id='05229302-46d6-f9af-261c-6e0ea91996a2',  # 🔥 UUID для Double данных
            )

            # memory_after = psutil.virtual_memory()
            # memory_delta = memory_after.used / (1024 * 1024) - system_resources['memory_used_mb']
            # log.info(f"✅ Загружено записей: {inserted}, delta памяти: {memory_delta:.0f} MB")

        return {
            'status': 'success',
            'values_loaded': inserted,
            'data_file_id': data_file_id,
            'data_source_id': data_source_id,
            'main_db_id': main_db_id,
            'main_db_name': main_db_name,  
            'file_size_mb': file_size_mb,
            # 'memory_delta_mb': memory_delta,
        }

    @task(task_id='clean_duplicate_staging_rows', retries=2, retry_delay=timedelta(minutes=3))
    def clean_staging_duplicates(staging_result: dict) -> dict:
        data_file_id = staging_result['data_file_id']
        deleted = clean_duplicate_staging_rows_by_file(STG_TABLE, data_file_id)
        log.info(f"✅ Удалено дубликатов: {deleted}")
        return {**staging_result, 'duplicates_removed': deleted}

    @task(task_id='upload_fh_data_values_to_target', retries=2, retry_delay=timedelta(minutes=5),
          execution_timeout=timedelta(hours=3))
    def upload_fh_data_values_to_target(staging_result: dict) -> dict:
        """🔥 Streaming merge из staging СРАЗУ в ClickHouse и S3 (Iceberg)"""
        data_file_id = staging_result['data_file_id']
        main_db_name = staging_result.get('main_db_name')  
        log.info(f"📤 Merge staging → CH & S3 для file_id={data_file_id}, db={main_db_name}")

        result = merge_fh_data_values_to_target_by_file(
            stg_table_name=STG_TABLE,
            target_table_name=TARGET_TABLE,
            data_file_id=data_file_id,
            main_db_name=main_db_name,      
            is_hist_data=True,
            ch_batch_size=2_500_000,
            s3_batch_size=50_000_000,       
            read_chunk_size=500_000,
        )

        log.info(f"✅ Перенесено записей: {result['records_processed']:,}, ошибок: {result['errors']}")
        return {**staging_result, 'records_transferred': result['records_processed']}

    @task(task_id='clean_staging_layer', retries=2, retry_delay=timedelta(minutes=2))
    def clean_staging_layer_task(transfer_result: dict) -> dict:
        data_file_id = transfer_result['data_file_id']
        deleted = clean_staging_layer_by_file(STG_TABLE, data_file_id)
        log.info(f"🎉 Обработка файла завершена: file_id={data_file_id}, "
                 f"loaded={transfer_result.get('values_loaded')}, "
                 f"dedup={transfer_result.get('duplicates_removed')}, "
                 f"transferred={transfer_result.get('records_transferred')}, "
                 f"staging_cleaned={deleted}")
        return {**transfer_result, 'staging_cleaned': deleted}

    # =========================================================================
    # Граф зависимостей
    # =========================================================================
    record_info = get_record_info_from_conf()
    main_db_name = get_main_db_name(record_info)          
    # system_resources = monitor_system_resources(record_info)
    
    staging = upload_fh_to_staging(
        record_info,
        # system_resources,
        main_db_name=main_db_name                          
    )
    dedup = clean_staging_duplicates(staging)
    transfer = upload_fh_data_values_to_target(dedup)
    clean_staging_layer_task(transfer)


upload_fh_data_dag()