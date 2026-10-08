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
    merge_creyt_data,
    merge_lcard_data,
    merge_bode_data,
    merge_sample_data,
    merge_spectrum_data,
    merge_siemens_sample_data,
    merge_diagnostic_array_data
)
from upload_data_configs.any_double_source_to_staging_upload_configs import (
    any_source_to_staging_upload_config,
)
from upload_data_configs.common_object_data_values_upload_configs import (
    hist_common_object_data_values_upload_config,
)
from validate_data.check_system_type_data import check_table_exist
from validate_data.hdfs_utils import hdfs_tempfile
from validate_data.common_tasks import get_record_info_from_conf

log = logging.getLogger(__name__)
STG_TABLE = 'stg_any_object_data_values_hist'

# 🔥 ВАЖНО: должно совпадать с размером пула any_processing_pool в Airflow (Admin → Pools).
# Если pool_slots больше размера пула — задача НИКОГДА не запустится!
ANY_POOL_TOTAL_SLOTS = 10
CREYT_POOL_SLOTS = 2


@dag(
    dag_id='upload_any_data_dag',
    # 🔥 СНИЖЕНО: должно совпадать с размером any_orchestration_pool (= 3)
    max_active_runs=5,     # 🔥 было 3 → 5 (совпадает с any_orchestration_pool)
    concurrency=10, # внутри одного запуска ~7 задач в пике
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=2),
        'retry_exponential_backoff': True,
    },
    description='Загрузка Any файла: лёгкие типы параллельно, Creyt — последним',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['hdfs', 'sqlite', 'data-analysis', 'parallel'],
)
def upload_any_data_dag():

    # =========================================================================
    # 🔥 ЭТАП 0: Извлечение main_db_name из конфига
    # =========================================================================
    @task(task_id='get_main_db_name')
    def get_main_db_name(record_info: dict) -> str:
        """Имя исходной БД из конфига запуска (оркестратор читает из data_catalogue)."""
        return str((record_info or {}).get('main_db_name') or 'Kriogen')

    @task(task_id='upload_any_data_to_staging',
          pool='any_processing_pool', pool_slots=1, retries=3, retry_delay=timedelta(minutes=5))
    def upload_any_data_to_staging(record_info: dict):
        sqlite_path = record_info['hdfs_path']
        data_file_id = record_info['data_file_id']
        data_source_id = record_info['data_source_id']
        main_db_id = record_info['main_db_id']

        with hdfs_tempfile(hdfs_path=sqlite_path) as local_sqlite_path:
            if check_table_exist(any_source_to_staging_upload_config['source_table_name'], local_sqlite_path):
                logging.info(f"✅ Uploading data from {any_source_to_staging_upload_config['source_table_name']}!")
                extract_object_data_values_to_staging(
                    sqlite_path=local_sqlite_path,
                    data_file_id=data_file_id,
                    data_source_id=data_source_id,
                    entity_config=any_source_to_staging_upload_config,
                    main_db_id=main_db_id
                )
                # 🔥 ИЗОЛЯЦИЯ: dedup только своего файла
                clean_duplicate_staging_rows_by_file(STG_TABLE, data_file_id)

        return data_file_id

    @task(task_id='get_object_data_values_entities')
    def get_any_object_data_values_upload_info(data_file_id: str, main_db_name: str) -> list[dict]:
        configs = [dict(c) for c in hist_common_object_data_values_upload_config]
        for c in configs:
            c['data_file_id'] = data_file_id
            c['main_db_name'] = main_db_name     # ✅ ПРОБРАСЫВАЕМ
        return configs

    @task(task_id='upload_any_object_data_values',
          pool='any_processing_pool', pool_slots=1, retries=3,
          retry_delay=timedelta(minutes=5), execution_timeout=timedelta(hours=3))
    def upload_common_object_data_values(**config):
        logging.info(f"✅ Uploading {config['stg_table_name']} → {config['target_table_name']} "
                     f"(file={config.get('data_file_id')}, db={config.get('main_db_name')})")
        return merge_common_object_data_values(
            stg_table_name=config['stg_table_name'],
            target_table_name=config['target_table_name'],
            data_type_id=config['data_type_id'],
            data_type=config.get('data_type'),
            is_hist_data=True,
            data_file_id=config.get('data_file_id'),
            main_db_name=config.get('main_db_name', 'Kriogen'), 
            ch_batch_size=5_000_000,
            s3_batch_size=100_000_000,
            read_chunk_size=500_000,
        )

    # =========================================================================
    # ЭТАП 2: CREYT — стартует ПОСЛЕ всех остальных, монополизирует весь пул
    # =========================================================================
    @task(task_id='upload_creyt_hist_data',
        pool='any_processing_pool',
        pool_slots=CREYT_POOL_SLOTS,   # 🔥 было 6 → стало 2 (3 параллельных data_file_id)
        priority_weight=10,
        retries=3,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=3))
    def upload_creyt_hist_data(data_file_id: str, main_db_name: str):
        logging.info(f"✅ Uploading Creyt data (file={data_file_id}, db={main_db_name})")
        return merge_creyt_data(
            stg_table_name=STG_TABLE,
            target_table_name='hist_creyt_timeseries_data',
            data_type_id='1b134c95-43d8-2793-ba0f-b6d0ba22c624',
            data_file_id=data_file_id,
            main_db_name=main_db_name,        # ✅
            is_hist_data=True,
            ch_batch_size=2_500_000, 
            s3_batch_size=50_000_000, 
            read_chunk_size=500,
        )

    # =========================================================================
    # 🔒 BLOB-ЗАДАЧИ (ЭТАП 1)
    # =========================================================================
    @task(task_id='upload_lcard_data',
        pool='any_processing_pool', pool_slots=1, retries=3,
        retry_delay=timedelta(minutes=5), execution_timeout=timedelta(hours=3))
    def upload_lcard_hist_data(data_file_id: str, main_db_name: str):
        logging.info(f"✅ Uploading LCard data (file={data_file_id}, db={main_db_name})")
        return merge_lcard_data(
            stg_table_name=STG_TABLE,
            target_table_name='hist_lcard_timeseries_data',
            data_type_id='548dc301-340b-4257-17a3-1f8b350af42a',
            data_file_id=data_file_id,
            main_db_name=main_db_name,        # ✅
            is_hist_data=True,
            ch_batch_size=5_000_000,
            s3_batch_size=50_000_000,    # ~125 МБ на Parquet
            read_chunk_size=100,
        )

    @task(task_id='upload_bode_data',
        pool='any_processing_pool', pool_slots=1, retries=3,
        retry_delay=timedelta(minutes=5), execution_timeout=timedelta(hours=3))
    def upload_bode_hist_data(data_file_id: str, main_db_name: str):
        logging.info(f"✅ Uploading BODE data (file={data_file_id}, db={main_db_name})")
        return merge_bode_data(
            stg_table_name=STG_TABLE,
            target_table_name='hist_bode_data',
            data_type_id='98015170-fd7e-23a2-8d27-c9506dc968d2',
            data_file_id=data_file_id,
            main_db_name=main_db_name,        # ✅
            is_hist_data=True,
            ch_batch_size=2_500_000,
            s3_batch_size=50_000_000,    # ~125 МБ на Parquet
            read_chunk_size=100,
        )

    @task(task_id='upload_sample_data',
        pool='any_processing_pool', pool_slots=1, retries=3,
        retry_delay=timedelta(minutes=5), execution_timeout=timedelta(hours=3))
    def upload_sample_hist_data(data_file_id: str, main_db_name: str):
        logging.info(f"✅ Uploading Sample data (file={data_file_id}, db={main_db_name})")
        return merge_sample_data(
            stg_table_name=STG_TABLE,
            target_table_name='hist_sample_timeseries_data',
            data_type_id='1e2ce5de-f03e-22b1-034c-8ee2e1bdef62',
            data_file_id=data_file_id,
            main_db_name=main_db_name,        # ✅
            is_hist_data=True,
            ch_batch_size=2_500_000,
            s3_batch_size=50_000_000,    # ~125 МБ на Parquet
            read_chunk_size=100,
        )

    @task(task_id='upload_siemens_sample_data',
        pool='any_processing_pool', pool_slots=1, retries=3, retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=3))
    def upload_siemens_sample_hist_data(data_file_id: str, main_db_name: str):
        logging.info(f"✅ Uploading Siemens Sample data (file={data_file_id}, db={main_db_name})")
        return merge_siemens_sample_data(
            stg_table_name=STG_TABLE,
            target_table_name='hist_siemens_sample_timeseries_data',
            data_type_id='80bdd702-d6f9-e90e-e3fa-1b9866940654',
            data_file_id=data_file_id,
            main_db_name=main_db_name,        # ✅
            is_hist_data=True,
            ch_batch_size=2_500_000,
            s3_batch_size=50_000_000,    # ~125 МБ на Parquet
            read_chunk_size=100,
        )

    @task(task_id='upload_diagnostic_array_nonhist_data',
      pool='any_processing_pool', pool_slots=1, retries=2,
      retry_delay=timedelta(minutes=2), execution_timeout=timedelta(hours=3))
    def upload_diagnostic_array_data(data_file_id: str, main_db_name: str):
        logging.info(f"✅ Uploading Diagnostic Array data (file={data_file_id}, db={main_db_name})")
        return merge_diagnostic_array_data(
            stg_table_name='stg_main_object_data_values_nonhist',
            target_table_name='nonhist_diagnostic_data',
            data_type_id='5550953e-6500-4d3f-f5a0-00a7f1034c0f',
            data_file_id=data_file_id,
            main_db_name=main_db_name,        # ✅
            is_hist_data=False,
            ch_batch_size=2_000_000,
            s3_batch_size=30_000_000,    # строки тяжелее (много string-колонок)
            read_chunk_size=100,
        )

    # @task(task_id='upload_spectrum_data',
    #       pool='any_processing_pool', pool_slots=1, retries=3, retry_delay=timedelta(minutes=5),
    #       execution_timeout=timedelta(minutes=60))
    # def upload_spectrum_hist_data(data_file_id: str, main_db_name: str):
    #     return merge_spectrum_data(
    #         stg_table_name=STG_TABLE, target_table_name='hist_spectrum_data',
    #         data_type_id='195542f6-0ed3-6f80-91ca-296219cf3f8b',
    #         data_file_id=data_file_id, main_db_name=main_db_name, is_hist_data=True,
    #         ch_batch_size=2_500_000, s3_batch_size=50_000_000, read_chunk_size=100,
    #     )

    @task(task_id='clean_staging_layer', retries=2, retry_delay=timedelta(minutes=2))
    def clean_data_from_staging_tables(data_file_id: str):
        deleted = clean_staging_layer_by_file(STG_TABLE, data_file_id)
        log.info(f"🧹 Staging очищен для file_id={data_file_id}, удалено={deleted}")
        return deleted

    # =========================================================================
    # Граф зависимостей: ЭТАП 1 (параллельно) → ЭТАП 2 (Creyt) → ЭТАП 3 (очистка)
    # =========================================================================
    record_info = get_record_info_from_conf()
    data_file_id = upload_any_data_to_staging(record_info)
    main_db_name = get_main_db_name(record_info)          # ✅ ИЗ КОНФИГА

    entities = get_any_object_data_values_upload_info(data_file_id, main_db_name)  # ✅ два аргумента
    common_result = upload_common_object_data_values.expand_kwargs(entities)

    creyt = upload_creyt_hist_data(data_file_id=data_file_id, main_db_name=main_db_name)

    lcard = upload_lcard_hist_data(data_file_id=data_file_id, main_db_name=main_db_name)
    bode = upload_bode_hist_data(data_file_id=data_file_id, main_db_name=main_db_name)
    sample = upload_sample_hist_data(data_file_id=data_file_id, main_db_name=main_db_name)
    siemens = upload_siemens_sample_hist_data(data_file_id=data_file_id, main_db_name=main_db_name)
    # spectrum = upload_spectrum_hist_data(data_file_id=data_file_id, main_db_name=main_db_name)
    diagnostic = upload_diagnostic_array_data(data_file_id=data_file_id, main_db_name=main_db_name)
    clean_result = clean_data_from_staging_tables(data_file_id)

    # --- Базовая цепочка ---
    record_info >> data_file_id

    # --- ЭТАП 1: все «лёгкие» типы параллельно ---
    data_file_id >> entities
    entities >> common_result

    # Результаты Этапа 1 (сюда же добавляйте BLOB-задачи при включении)
    stage1_results = [common_result]
    data_file_id >> lcard
    data_file_id >> bode
    data_file_id >> sample
    data_file_id >> siemens
    data_file_id >> diagnostic
    # data_file_id >> spectrum
    stage1_results += [lcard, bode, sample, siemens, diagnostic]

    # --- ЭТАП 2: Creyt СТРОГО после всех остальных типов ---
    stage1_results >> creyt

    # --- ЭТАП 3: очистка staging после всего ---
    creyt >> clean_result

    return clean_result


upload_any_data_dag()