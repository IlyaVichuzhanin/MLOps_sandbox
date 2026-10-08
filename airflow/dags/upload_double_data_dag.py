from datetime import datetime, timedelta
from airflow.decorators import task, dag
import logging
from functions.common_functions import (
    clean_staging_layer_by_file,          # 🔥 by-file очистка
    clean_duplicate_staging_rows_by_file, # 🔥 by-file dedup
)

from functions.upload_functions import (
    extract_object_data_values_to_staging,
    merge_common_object_data_values,
)
from upload_data_configs.any_double_source_to_staging_upload_configs import (
    double_source_to_staging_upload_config,
)

from validate_data.check_system_type_data import check_table_exist
from validate_data.hdfs_utils import hdfs_tempfile
from validate_data.common_tasks import (
    get_record_info_from_conf,
)

log = logging.getLogger(__name__)
DWH_CONN_ID = 'cloudberry_test_dwh'
STG_TABLE = 'stg_double_object_data_values_hist'


@dag(
    dag_id='upload_double_data_dag',
    max_active_runs=15,      # 🔥 Параллелизм по data_file_id между запусками
    concurrency=10,         # 🔥 Поднято для лучшей параллельности
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=1)
    },
    description='Загрузка данных из Double файла в DWH (с изоляцией по file_id)',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['hdfs', 'sqlite', 'data-analysis', 'parallel'],
    params={
        'hdfs_path': '/user/hadoop/data/sample.db',
        'sql_query': 'SELECT * FROM sqlite_master WHERE type="table"',
        'limit': 100
    }
)
def upload_double_data_dag():

    # =========================================================================
    # 🔥 ЭТАП 1: Извлечение main_db_name из конфига
    # =========================================================================
    @task(task_id='get_main_db_name')
    def get_main_db_name(record_info: dict) -> str:
        """Имя исходной БД из конфига запуска (оркестратор читает из data_catalogue)."""
        return str((record_info or {}).get('main_db_name'))

    @task(task_id='upload_double_data_to_staging',
          pool='double_processing_pool',
          pool_slots=1,
          retries=3,
          retry_delay=timedelta(minutes=5))
    def upload_double_data_to_staging(record_info: dict):
        sqlite_path = record_info['hdfs_path']
        data_file_id = record_info['data_file_id']
        data_source_id = record_info['data_source_id']
        main_db_id = record_info['main_db_id']
        
        with hdfs_tempfile(hdfs_path=sqlite_path) as local_sqlite_path:
            if check_table_exist(double_source_to_staging_upload_config['source_table_name'], local_sqlite_path):
                logging.info(f"✅ Uploading data from {double_source_to_staging_upload_config['source_table_name']} table!")
                extract_object_data_values_to_staging(
                    sqlite_path=local_sqlite_path,
                    data_file_id=data_file_id,
                    data_source_id=data_source_id,
                    entity_config=double_source_to_staging_upload_config,
                    main_db_id=main_db_id
                )
                # 🔥 ИЗОЛЯЦИЯ: dedup только своего файла (не трогаем параллельные запуски)
                clean_duplicate_staging_rows_by_file(STG_TABLE, data_file_id)
        
        # 🔥 Возвращаем data_file_id (строку) для корректной работы XComArg
        return data_file_id

    @task(task_id='upload_double_object_data_values',
          pool='double_processing_pool',
          pool_slots=1,
          retries=3,
          retry_delay=timedelta(minutes=5),
          execution_timeout=timedelta(hours=2))
    def upload_double_data_values(data_file_id: str, main_db_name: str):
        logging.info(f"✅ Uploading double object data value records "
                     f"from {STG_TABLE} to hist_double_data (file={data_file_id}, db={main_db_name})")
        return merge_common_object_data_values(
            stg_table_name=STG_TABLE,
            target_table_name='hist_double_data',
            data_type_id='05229302-46d6-f9af-261c-6e0ea91996a2',
            data_type='Float64',
            is_hist_data=True,
            data_file_id=data_file_id,
            main_db_name=main_db_name,          # ✅ ПРОБРАСЫВАЕМ
            # 🔥 Новые параметры батчинга (вместо старого batch_size)
            ch_batch_size=5_000_000,      # Строк за insert_df в CH
            s3_batch_size=100_000_000,    # ~256 МБ на Parquet-файл
            read_chunk_size=500_000,      # yield_per при чтении из staging
        )

    @task(task_id='clean_staging_layer', retries=2, retry_delay=timedelta(minutes=1))
    def clean_data_from_staging_tables(data_file_id: str):
        # 🔥 ИЗОЛЯЦИЯ: удаляем ТОЛЬКО свой файл, не трогая параллельные запуски
        deleted = clean_staging_layer_by_file(STG_TABLE, data_file_id)
        log.info(f"🧹 Staging очищен для file_id={data_file_id}, удалено={deleted}")
        return deleted

    # =========================================================================
    # Граф зависимостей
    # =========================================================================
    record_info = get_record_info_from_conf()
    data_file_id = upload_double_data_to_staging(record_info)
    main_db_name = get_main_db_name(record_info)          # ✅ ИЗ КОНФИГА
    
    upload_double_data_result = upload_double_data_values(
        data_file_id=data_file_id,
        main_db_name=main_db_name                          # ✅ ПЕРЕДАЁМ
    )
    clean_staging_layer_result = clean_data_from_staging_tables(data_file_id=data_file_id)

    # 1. Базовая цепочка
    record_info >> data_file_id
    
    # 2. Staging → Common values (data_file_id и XComArg-источник, и upstream)
    data_file_id >> upload_double_data_result
    
    # 3. Финальная очистка (только после завершения merge)
    upload_double_data_result >> clean_staging_layer_result

    return clean_staging_layer_result


upload_double_data_dag()