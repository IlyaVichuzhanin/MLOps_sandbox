from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.exceptions import AirflowException
import logging
from functions.upload_functions import (
    extract_from_sqlite_to_staging,
    extract_object_data_values_data_to_staging,
    extract_diag_data_to_staging,
    merge_staging_to_hub, 
    merge_staging_to_satellite, 
    merge_staging_to_link,
    merge_common_object_data_values,
    merge_creyt_data,
    merge_lcard_data,
    merge_sample_data,
    merge_siemens_sample_data,
    merge_spectrum_data, 
    merge_bode_data,
    merge_diagnostic_array_data,
)

from validate_data.common_tasks import get_record_info_from_conf

from functions.common_functions import (
    clean_staging_layer_by_file,          # 🔥 by-file очистка
    clean_duplicate_staging_rows_by_file, # 🔥 by-file dedup
)

from upload_data_configs.source_to_staging_upload_configs import (
    main_db_source_to_staging_info, 
    diagnostic_data_source_to_staging_info, 
    object_data_value_records_source_to_staging_info, 
    staging_tables_list,
)

from upload_data_configs.staging_to_hub_upload_configs import staging_to_hubs_upload_config
from upload_data_configs.staging_to_satellite_upload_configs import staging_to_satellite_upload_config
from upload_data_configs.staging_to_link_upload_configs import staging_to_link_upload_config
from upload_data_configs.common_object_data_values_upload_configs import nonhist_common_object_data_values_upload_config
from validate_data.check_system_type_data import check_table_exist
from validate_data.hdfs_utils import hdfs_tempfile

log = logging.getLogger(__name__)
DWH_CONN_ID = 'cloudberry_test_dwh'

# 🔥 ВАЖНО: должно совпадать с размером пула maindb_processing_pool в Airflow (Admin → Pools).
MAINDB_POOL_TOTAL_SLOTS = 6

tables_to_check_duplicates = [
    'stg_pou_user_defined_items',
    'stg_pou_user_items_tree', 
    'stg_diagnostic_alarms', 
    'stg_io_modbus_tcp_register_configs',
    'stg_io_opc_da_client_item_configs',
    'stg_main_object_data_values_nonhist',
    'stg_tik_expert_slices',
    'stg_object_properties',
    'stg_io_creyt_controller_states',
    'stg_io_creyt_channel_configs',
    'stg_io_creyt_channel_states',
    'stg_io_creyt_configs',
    'stg_io_creyt_im_oper_times',
    'stg_io_modbus_tcp_register_bit_decompression_configs',
    'stg_io_device_configs',
    'stg_io_modbus_tcp_configs',
    'stg_measure_convert',
    'stg_model_templates',
    'stg_io_opc_ua_client_transform_item_configs',
    'stg_io_l_card_configs',
    'stg_io_opc_ua_client_item_configs',
]



@dag(
    dag_id='upload_maindb_data_dag',
    max_active_runs=5,      # 🔥 Параллелизм по data_file_id между запусками
    concurrency=10,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=1),
    },
    description='Загрузка данных из MainDb файла: лёгкие типы параллельно, Creyt — последним',
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
def read_sqlite_file_dag():

    @task(task_id='upload_main_db_data_to_staging',
          pool='maindb_processing_pool', pool_slots=1, retries=2, retry_delay=timedelta(minutes=2))
    def upload_main_db_data_to_staging(record_info: dict):
        sqlite_path = record_info['hdfs_path']
        data_file_id = record_info['data_file_id']
        data_source_id = record_info['data_source_id']
        main_db_id = record_info['main_db_id']
        
        with hdfs_tempfile(hdfs_path=sqlite_path) as local_sqlite_path:
            for entity in main_db_source_to_staging_info:
                if check_table_exist(entity['source_table_name'], local_sqlite_path):
                    logging.info(f"✅ Uploading data from {entity['source_table_name']} table!")
                    extract_from_sqlite_to_staging(
                        local_sqlite_path, entity,
                        data_file_id=data_file_id,
                        data_source_id=data_source_id,
                        main_db_id=main_db_id
                    )
            
            for diag_entity in diagnostic_data_source_to_staging_info:
                if check_table_exist(diag_entity['source_table_name'], local_sqlite_path):
                    logging.info(f"✅ Uploading data from {diag_entity['source_table_name']} table!")
                    extract_diag_data_to_staging(
                        local_sqlite_path, diag_entity,
                        data_file_id=data_file_id,
                        data_source_id=data_source_id,
                        main_db_id=main_db_id
                    )
            
            for non_hist_entity in object_data_value_records_source_to_staging_info:
                if check_table_exist(non_hist_entity['source_table_name'], local_sqlite_path):
                    logging.info(f"✅ Uploading data from {non_hist_entity['source_table_name']} table!")
                    extract_object_data_values_data_to_staging(
                        local_sqlite_path, non_hist_entity,
                        data_file_id=data_file_id,
                        data_source_id=data_source_id,
                        main_db_id=main_db_id
                    )
        
        # 🔥 ИЗОЛЯЦИЯ: dedup только своего файла (не трогаем параллельные запуски)
        for table in tables_to_check_duplicates:
            clean_duplicate_staging_rows_by_file(table, data_file_id)
        
        # 🔥 Возвращаем data_file_id (строку) для корректной работы XComArg
        return data_file_id

    @task(task_id='get_main_db_name')
    def get_main_db_name(record_info: dict) -> str:
        """Имя исходной БД из конфига запуска (оркестратор читает из data_catalogue)."""
        return str((record_info or {}).get('main_db_name'))
    
    @task(task_id='get_hub_entities')
    def get_hub_upload_info() -> list[dict]:
        return staging_to_hubs_upload_config

    @task(task_id='upload_staging_to_habs',
          pool='maindb_processing_pool', pool_slots=1)
    def upload_staging_data_to_hub(**config):
        logging.info(f"✅ Uploading staging data from {config['stg_table_name']} to {config['hub_table_name']} hub!")
        return merge_staging_to_hub(
            config['hub_table_name'],
            config['sk_hub_column_name'], 
            config['stg_table_name'],
            config['stg_hash_sk_column_name'],
            config['business_id_column_name']
        )
    
    @task(task_id='get_satellite_entities')
    def get_satellite_upload_info() -> list[dict]:
        return staging_to_satellite_upload_config

    @task(task_id='upload_staging_to_satellite',
          pool='maindb_processing_pool', pool_slots=1)
    def upload_staging_data_to_satellite(**config):
        logging.info(f"✅ Uploading staging data from {config['stg_table_name']} to {config['sat_table_name']} satellite!")
        return merge_staging_to_satellite(
            config['sat_table_name'],
            config['hab_sk_column_name'],
            config['sat_column_names'],
            config['stg_table_name'],
            config['stg_column_names'],
            config['stg_hash_sk_column_name'],
            config['stg_hash_sat_diff_column_name']
        )
    
    @task(task_id='get_reference_data_links')
    def get_links_upload_info() -> list[dict]:
        return staging_to_link_upload_config
    
    @task(task_id='upload_reference_links_tables',
          pool='maindb_processing_pool', pool_slots=1)
    def upload_hub_reference_links_tables(**config):
        logging.info(f"✅ Uploading reference data links from {config['stg_table_name']} to {config['link_table_name']}!")
        return merge_staging_to_link(
            config['stg_table_name'],
            config['link_table_name'],
            config['link_sk_column_name'],
            config['link_first_hub_sk_column_name'],
            config['link_second_hub_sk_column_name'],
            config['stg_link_sk_column_name'],
            config['stg_first_hub_sk_column_name'],
            config['stg_second_hub_sk_column_name']
        )
        
    @task(task_id='get_object_data_values_entities')
    def get_common_object_data_values_upload_info(data_file_id: str, main_db_name: str) -> list[dict]:
        configs = [dict(c) for c in nonhist_common_object_data_values_upload_config]
        for c in configs:
            c['data_file_id'] = data_file_id
            c['main_db_name'] = main_db_name
        return configs
    
    @task(task_id='upload_common_object_data_values',
          pool='maindb_processing_pool', pool_slots=1, retries=2,
          retry_delay=timedelta(minutes=2), execution_timeout=timedelta(minutes=60))
    def upload_common_object_data_values(**config):
        logging.info(f"✅ Uploading common object data values "
                     f"from {config['stg_table_name']} to {config['target_table_name']} "
                     f"(file={config.get('data_file_id')}, db={config.get('main_db_name')})")
        return merge_common_object_data_values(
            stg_table_name=config['stg_table_name'],
            target_table_name=config['target_table_name'],
            data_type_id=config['data_type_id'],
            data_type=config.get('data_type'),
            is_hist_data=False,
            data_file_id=config.get('data_file_id'),
            main_db_name=config.get('main_db_name'),  # ✅
            ch_batch_size=5_000_000,
            s3_batch_size=100_000_000,
            read_chunk_size=500_000,
        )

    # =========================================================================
    # 🔥 ЭТАП 1: Лёгкие BLOB-задачи (параллельно)
    # =========================================================================
    @task(task_id='upload_lcard_data', pool='maindb_processing_pool', pool_slots=1,
          retries=2, retry_delay=timedelta(minutes=2), execution_timeout=timedelta(hours=2))
    def upload_lcard_data(data_file_id: str, main_db_name: str):
        logging.info(f"✅ Uploading LCard data (file={data_file_id}, db={main_db_name})")
        return merge_lcard_data(
            stg_table_name='stg_main_object_data_values_nonhist',
            target_table_name='nonhist_lcard_timeseries_data',
            data_type_id='548dc301-340b-4257-17a3-1f8b350af42a',
            data_file_id=data_file_id,
            main_db_name=main_db_name,        # ✅
            is_hist_data=False,
            ch_batch_size=2_500_000,
            s3_batch_size=50_000_000,
            read_chunk_size=100,
        )

    @task(task_id='upload_bode_data', pool='maindb_processing_pool', pool_slots=1,
          retries=2, retry_delay=timedelta(minutes=2), execution_timeout=timedelta(hours=2))
    def upload_bode_data(data_file_id: str, main_db_name: str):
        logging.info(f"✅ Uploading BODE data (file={data_file_id}, db={main_db_name})")
        return merge_bode_data(
            stg_table_name='stg_main_object_data_values_nonhist',
            target_table_name='nonhist_bode_data',
            data_type_id='98015170-fd7e-23a2-8d27-c9506dc968d2',
            data_file_id=data_file_id,
            main_db_name=main_db_name,        # ✅
            is_hist_data=False,
            ch_batch_size=2_500_000,
            s3_batch_size=50_000_000,
            read_chunk_size=100,
        )
        
    @task(task_id='upload_sample_data', pool='maindb_processing_pool', pool_slots=1,
          retries=2, retry_delay=timedelta(minutes=2), execution_timeout=timedelta(hours=2))
    def upload_sample_data(data_file_id: str, main_db_name: str):
        logging.info(f"✅ Uploading Sample data (file={data_file_id}, db={main_db_name})")
        return merge_sample_data(
            stg_table_name='stg_main_object_data_values_nonhist',
            target_table_name='nonhist_sample_timeseries_data',
            data_type_id='1e2ce5de-f03e-22b1-034c-8ee2e1bdef62',
            data_file_id=data_file_id,
            main_db_name=main_db_name,        # ✅
            is_hist_data=False,
            ch_batch_size=2_500_000,
            s3_batch_size=50_000_000,
            read_chunk_size=100,
        )
        
    @task(task_id='upload_siemens_sample_data', pool='maindb_processing_pool', pool_slots=1,
          retries=2, retry_delay=timedelta(minutes=2), execution_timeout=timedelta(hours=2))
    def upload_siemens_sample_data(data_file_id: str, main_db_name: str):
        logging.info(f"✅ Uploading Siemens Sample data (file={data_file_id}, db={main_db_name})")
        return merge_siemens_sample_data(
            stg_table_name='stg_main_object_data_values_nonhist',
            target_table_name='nonhist_siemens_sample_timeseries_data',
            data_type_id='80bdd702-d6f9-e90e-e3fa-1b9866940654',
            data_file_id=data_file_id,
            main_db_name=main_db_name,        # ✅
            is_hist_data=False,
            ch_batch_size=2_500_000,
            s3_batch_size=50_000_000,
            read_chunk_size=100,
        )
        
    @task(task_id='upload_diagnostic_array_nonhist_data', pool='maindb_processing_pool',
          pool_slots=1, retries=2, retry_delay=timedelta(minutes=2), execution_timeout=timedelta(hours=2))
    def upload_diagnostic_array_data(data_file_id: str, main_db_name: str):
        logging.info(f"✅ Uploading Diagnostic Array data (file={data_file_id}, db={main_db_name})")
        return merge_diagnostic_array_data(
            stg_table_name='stg_main_object_data_values_nonhist',
            target_table_name='nonhist_diagnostic_data',
            data_type_id='5550953e-6500-4d3f-f5a0-00a7f1034c0f',
            data_file_id=data_file_id,
            main_db_name=main_db_name,        # ✅
            is_hist_data=False,
            ch_batch_size=2_500_000,
            s3_batch_size=50_000_000,
            read_chunk_size=100,
        )

    # =========================================================================
    # 🔥 ЭТАП 2: CREYT — стартует ПОСЛЕ всех остальных, монополизирует весь пул
    # =========================================================================
    @task(task_id='upload_creyt_data', pool='maindb_processing_pool',
          pool_slots=MAINDB_POOL_TOTAL_SLOTS, priority_weight=10,
          retries=2, retry_delay=timedelta(minutes=5), execution_timeout=timedelta(hours=3))
    def upload_creyt_data(data_file_id: str, main_db_name: str):
        logging.info(f"✅ Uploading Creyt data (file={data_file_id}, db={main_db_name}) - monopolizing pool")
        return merge_creyt_data(
            stg_table_name='stg_main_object_data_values_nonhist',
            target_table_name='nonhist_creyt_timeseries_data',
            data_type_id='1b134c95-43d8-2793-ba0f-b6d0ba22c624',
            data_file_id=data_file_id,
            main_db_name=main_db_name,        # ✅
            is_hist_data=False,
            ch_batch_size=2_500_000,
            s3_batch_size=50_000_000,
            read_chunk_size=100,
        )

    @task(task_id='clean_staging_layer', retries=2, retry_delay=timedelta(minutes=1))
    def clean_data_from_staging_tables(data_file_id: str):
        # 🔥 ИЗОЛЯЦИЯ: удаляем ТОЛЬКО свой файл
        total_deleted = 0
        for entity in staging_tables_list:
            deleted = clean_staging_layer_by_file(entity, data_file_id)
            total_deleted += deleted
        log.info(f"🧹 Staging очищен для file_id={data_file_id}, удалено={total_deleted}")
        return total_deleted

    # =========================================================================
    # Граф зависимостей: ЭТАП 1 (параллельно) → ЭТАП 2 (Creyt) → ЭТАП 3 (очистка)
    # =========================================================================
    record_info = get_record_info_from_conf()
    
    # 🔥 data_file_id теперь XComArg (строка)
    data_file_id = upload_main_db_data_to_staging(record_info=record_info)
    main_db_name = get_main_db_name(record_info)
    
    # --- ЭТАП 1: Лёгкие задачи параллельно ---
    hub_upload_info_list = get_hub_upload_info()
    staging_to_hub_upload_result = upload_staging_data_to_hub.expand_kwargs(hub_upload_info_list)
    
    satellite_upload_info_list = get_satellite_upload_info()
    staging_to_satellite_upload_result = upload_staging_data_to_satellite.expand_kwargs(satellite_upload_info_list)
    
    link_upload_info_list = get_links_upload_info()
    link_upload_result = upload_hub_reference_links_tables.expand_kwargs(link_upload_info_list)
    
    non_hist_upload_info_list = get_common_object_data_values_upload_info(data_file_id, main_db_name)
    non_hist_data_upload_result = upload_common_object_data_values.expand_kwargs(non_hist_upload_info_list)
    
    # BLOB-задачи Этапа 1 (параллельно)
    lcard_data_upload_result      = upload_lcard_data(data_file_id=data_file_id, main_db_name=main_db_name)
    bode_data_upload_result       = upload_bode_data(data_file_id=data_file_id, main_db_name=main_db_name)
    sample_data_upload_result     = upload_sample_data(data_file_id=data_file_id, main_db_name=main_db_name)
    siemens_sample_data_upload_result = upload_siemens_sample_data(data_file_id=data_file_id, main_db_name=main_db_name)
    diagnostic_array_data_upload_result = upload_diagnostic_array_data(data_file_id=data_file_id, main_db_name=main_db_name)

    # --- ЭТАП 2: Creyt ---
    creyt_data_upload_result = upload_creyt_data(data_file_id=data_file_id, main_db_name=main_db_name)
    
    # --- ЭТАП 3: Очистка ---
    clean_staging_layer_result = clean_data_from_staging_tables(data_file_id)
    
    # 1. Базовая цепочка
    record_info >> data_file_id

    # 2. FAN-OUT: все лёгкие типы стартуют параллельно от data_file_id
    data_file_id >> [
        hub_upload_info_list,
        satellite_upload_info_list,
        link_upload_info_list,
        non_hist_upload_info_list,
    ]
    
    data_file_id >> [
        lcard_data_upload_result,
        bode_data_upload_result,
        sample_data_upload_result,
        siemens_sample_data_upload_result,
        diagnostic_array_data_upload_result,
    ]

    # 3. Развёртка mapped tasks
    hub_upload_info_list >> staging_to_hub_upload_result
    satellite_upload_info_list >> staging_to_satellite_upload_result
    link_upload_info_list >> link_upload_result
    non_hist_upload_info_list >> non_hist_data_upload_result

    # 4. Результаты Этапа 1 (все лёгкие задачи)
    stage1_results = [
        staging_to_hub_upload_result, 
        staging_to_satellite_upload_result, 
        link_upload_result,
        non_hist_data_upload_result,
        lcard_data_upload_result,
        bode_data_upload_result,
        sample_data_upload_result,
        siemens_sample_data_upload_result,
        diagnostic_array_data_upload_result,
    ]

    # 5. 🔥 Creyt СТРОГО после всех остальных типов
    stage1_results >> creyt_data_upload_result

    # 6. Финальная очистка staging
    creyt_data_upload_result >> clean_staging_layer_result

    return clean_staging_layer_result


read_sqlite_file_dag()