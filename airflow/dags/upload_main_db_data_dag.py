from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.exceptions import AirflowException
from airflow.decorators import task, dag
from airflow.operators.python import get_current_context
import sqlite3
import tempfile
import os
import json
import logging
from contextlib import contextmanager
from typing import Dict, Any
from upload_main_db_data.upload_main_db_data import extract_from_sqlite_to_staging, merge_staging_to_hub, clean_duplecate_staging_rows
from upload_main_db_data.source_to_staging_tables import source_to_staging_info
from upload_main_db_data.staging_to_hub_tables import staging_to_hubs_info
from upload_main_db_data.staging_to_satellite_tables import staging_to_satellite_info

log = logging.getLogger(__name__)
DWH_CONN_ID = 'cloudberry_test_dwh'
HUB_ENTITIES_INFO = staging_to_hubs_info
SATELLITE_ENTITIES_INFO = staging_to_satellite_info



@contextmanager
def hdfs_tempfile(hdfs_path: str):
    hook = WebHDFSHook(webhdfs_conn_id='hdfs_default')
    client = hook.get_conn()
    
    file_status = client.status(hdfs_path, strict=False)
    if file_status is None:
        raise FileNotFoundError(f"Файл не найден в HDFS: {hdfs_path}")
    
    if file_status.get('type') != 'FILE':
        raise ValueError(f"Путь {hdfs_path} не является файлом (тип: {file_status.get('type')})")
    
    with tempfile.NamedTemporaryFile(mode='wb', suffix='.sqlite', delete=False) as tmp_file:
        temp_path = tmp_file.name
        
        try:
            with client.read(hdfs_path, chunk_size=65536) as reader:
                for chunk in reader:
                    tmp_file.write(chunk)
            tmp_file.flush()
            os.fsync(tmp_file.fileno())
            yield temp_path
        finally:
            if os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except OSError as e:
                    print(f"⚠ Не удалось удалить временный файл {temp_path}: {e}")

@dag(
    dag_id='upload_main_db_data_dag',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=1),
    },
    description='Загрузка данных из MainDb файла в DWH',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['hdfs', 'sqlite', 'data-analysis'],
    params={
        'hdfs_path': '/user/hadoop/data/sample.db',  # Путь по умолчанию
        'sql_query': 'SELECT * FROM sqlite_master WHERE type="table"',  # Запрос по умолчанию
        'limit': 100  # Лимит строк по умолчанию
    }
)




def read_sqlite_file_dag():
    @task(task_id='get_hdfs_path_from_conf')
    def get_hdfs_path_from_conf(**context) -> str:
        dag_run = context.get('dag_run')
        conf = dag_run.conf if dag_run else None
        
        hdfs_path = None
        if conf and 'record' in conf:
            hdfs_path = conf['record'].get('hdfs_full_path')
        if not hdfs_path:
            hdfs_path = context['params'].get('hdfs_path')
        
        if not hdfs_path:
            raise AirflowException(
                "Не указан путь к файлу SQLite в HDFS. Передайте через --conf или используйте параметр hdfs_path в UI."
            )
        
        if not hdfs_path.startswith('/'):
            raise AirflowException(f"Некорректный путь HDFS: {hdfs_path}")
        
        print(f"✓ Используется файл HDFS: {hdfs_path}")
        return hdfs_path

    @task(task_id='upload_main_db_data_to_staging')
    def upload_main_db_data_to_staging(sqlite_path: str):
        with hdfs_tempfile(hdfs_path=sqlite_path) as local_sqlite_path:
            for entity in source_to_staging_info:
                logging.info(f"✅ Uploading data from {source_to_staging_info[entity]['source_table_name']} table!")
                extract_from_sqlite_to_staging(
                    sqlite_path=local_sqlite_path, 
                    source_table_name=source_to_staging_info[entity]['source_table_name'], 
                    hash_gener_columns=source_to_staging_info[entity]['hash_gener_columns'],
                    source_column_names=source_to_staging_info[entity]['source_column_names'],
                    staging_table_name=source_to_staging_info[entity]['staging_table_name'],
                    column_types=source_to_staging_info[entity]['column_types']
                    )
                
        tables_to_check_duplicates=['stg_pou_user_defined_items','stg_pou_user_items_tree']
        for table in tables_to_check_duplicates:
            clean_duplecate_staging_rows(table)
        return True
    
    @task(task_id='get_hub_entities')
    def get_hub_entities() -> list[dict]:
        return HUB_ENTITIES_INFO

    @task(task_id='upload_staging_to_habs')
    def upload_staging_data_to_hub(**config):
        logging.info(f"✅ Uploading staging data from {config['stg_table_name']} table to {config['hub_table_name']} hub table!")
        return merge_staging_to_hub(
            hub_table_name=config['hub_table_name'],
            sk_hub_column_name=config['sk_hub_column_name'], 
            stg_table_name=config['stg_table_name']
    )

    file_path = get_hdfs_path_from_conf()
    staging_upload_result = upload_main_db_data_to_staging(sqlite_path=file_path)
    hub_entitis_list = get_hub_entities()

    staging_to_hub_upload_result = upload_staging_data_to_hub.expand_kwargs(hub_entitis_list)
    file_path>>staging_upload_result>>hub_entitis_list>>staging_to_hub_upload_result

    # hub_entitis_list>>staging_to_hub_upload_result

    return staging_to_hub_upload_result

    # Инициализация DAG
read_sqlite_file_dag()

