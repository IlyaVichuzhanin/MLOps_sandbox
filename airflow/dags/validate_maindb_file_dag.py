from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
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
from check_system_type_data.system_type_data_info import main_db_system_type_data_info
from check_system_type_data.check_system_type_data import check_system_type_data_tables, check_table_exist
from check_system_type_data.main_db_tables_list import main_db_tables_list

log = logging.getLogger(__name__)



@contextmanager
def hdfs_tempfile(hdfs_path: str):
    """
    Контекстный менеджер для безопасного скачивания файла из HDFS во временное хранилище.
    Автоматически удаляет временный файл после использования.
    """
    hook = WebHDFSHook(webhdfs_conn_id='hdfs_default')
    client = hook.get_conn()
    
    # Проверяем существование файла
    file_status = client.status(hdfs_path, strict=False)
    if file_status is None:
        raise FileNotFoundError(f"Файл не найден в HDFS: {hdfs_path}")
    
    if file_status.get('type') != 'FILE':
        raise ValueError(f"Путь {hdfs_path} не является файлом (тип: {file_status.get('type')})")
    
    # Создаём временный файл с расширением .sqlite
    with tempfile.NamedTemporaryFile(mode='wb', suffix='.sqlite', delete=False) as tmp_file:
        temp_path = tmp_file.name
        
        try:
            # Читаем бинарные данные из HDFS (chunked reading для больших файлов)
            with client.read(hdfs_path, chunk_size=65536) as reader:
                # ✅ ПРАВИЛЬНО: итерируемся по генератору
                for chunk in reader:
                    tmp_file.write(chunk)
            
            tmp_file.flush()
            os.fsync(tmp_file.fileno())
            
            yield temp_path
            
        finally:
            # Гарантированная очистка
            if os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except OSError as e:
                    print(f"⚠ Не удалось удалить временный файл {temp_path}: {e}")



@dag(
    dag_id='validate_maindb_file_dag',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=1),
    },
    description='Проверка файла MainDb перед загрузкой данных в DWH для обеспечения целостности данных',
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
        
        # Приоритет: сначала из conf, потом из params
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
    
    @task(task_id='validate_sqlite_file')
    def validate_sqlite_file(hdfs_path: str) -> Dict[str, Any]:
        """
        Валидация целостности SQLite файла в HDFS.
        """
        from hdfs.util import HdfsError
        
        try:
            hook = WebHDFSHook(webhdfs_conn_id='hdfs_default')
            client = hook.get_conn()
            
            # Проверяем существование файла
            file_status = client.status(hdfs_path, strict=True)
            file_size = file_status.get('length', 0)
            
            if file_size == 0:
                raise ValueError(f"Файл {hdfs_path} пустой (размер 0 байт)")
            
            # Читаем заголовок для проверки магического числа SQLite
            with client.read(hdfs_path, length=100) as reader:
                header = reader.read(100)
            
            is_valid_header = header.startswith(b'SQLite format 3\x00')
            
            # Полная валидация через sqlite3
            is_valid_sqlite = False
            error_detail = None
            
            with hdfs_tempfile(hdfs_path) as local_path:
                try:
                    conn = sqlite3.connect(local_path)
                    cursor = conn.cursor()
                    
                    # Проверка целостности
                    cursor.execute("PRAGMA quick_check")
                    integrity_result = cursor.fetchone()
                    
                    if integrity_result and integrity_result[0] == 'ok':
                        is_valid_sqlite = True
                    else:
                        error_detail = f"PRAGMA quick_check failed: {integrity_result}"
                    
                    conn.close()
                except sqlite3.DatabaseError as e:
                    error_detail = f"SQLite error: {str(e)}"
                except Exception as e:
                    error_detail = f"Unexpected error: {str(e)}"
                
                missing_tables = []   
                if is_valid_sqlite:
                    for table_name in main_db_tables_list:
                        if check_table_exist(table_name, local_path):
                            continue
                        else:
                            log.info(f"{table_name} doesn't exist in sqlite file!")
                            missing_tables.append(table_name)
                            
            
            result = {
                'hdfs_path': hdfs_path,
                'file_size_bytes': file_size,
                'file_size_mb': round(file_size / 1024**2, 2),
                'header_valid': is_valid_header,
                'sqlite_valid': is_valid_sqlite,
                'error_detail': error_detail,
                'status': 'valid' if (is_valid_header and is_valid_sqlite) else 'invalid',
                'missing_tables': missing_tables
            }
            
            if is_valid_header and is_valid_sqlite:
                print(f"✓ Файл валиден: {hdfs_path} ({file_size / 1024**2:.2f} MB)")
            else:
                print(f"✗ Файл повреждён или не является SQLite: {hdfs_path}")
                print(f"  Header valid: {is_valid_header}")
                print(f"  SQLite valid: {is_valid_sqlite}")
                if error_detail:
                    print(f"  Детали: {error_detail}")
                raise AirflowException(f"Валидация файла не пройдена: {hdfs_path}")
            
            return result
            
        except Exception as e:
            raise AirflowException(f"Ошибка валидации файла {hdfs_path}: {str(e)}")
    
    
    @task(task_id='validate_system_type_data')
    def validate_system_type_data(hdfs_path: str) -> bool:
        with hdfs_tempfile(hdfs_path=hdfs_path) as local_sqlite_path:
            for system_type_data_set in main_db_system_type_data_info:
                data_matches=check_system_type_data_tables(sqlite_path=local_sqlite_path, 
                                            main_db_table_name=main_db_system_type_data_info[system_type_data_set]['main_db_adress']['table_name'],
                                            main_db_column_name=main_db_system_type_data_info[system_type_data_set]['main_db_adress']['column_name'],
                                            dwh_table_name=main_db_system_type_data_info[system_type_data_set]['dwh_adress']['table_name'],
                                            dwh_column_name=main_db_system_type_data_info[system_type_data_set]['dwh_adress']['column_name'])
                if data_matches:
                    print(f"✓ Проверка пройдена, все элементы справочных данных {system_type_data_set} из SQLite найдены в DWH.")
                else:
                    raise AirflowException(f"❌ Проверка НЕ пройдена: в DWH отсутствуют элементы справочных данных {system_type_data_set}!/n" 
                                        f"Необходимо актуализировать справочные данные {system_type_data_set}!")
        return True
    
    # === Определение последовательности задач ===
    file_path = get_hdfs_path_from_conf()
    file_validation = validate_sqlite_file(hdfs_path=file_path)
    system_type_data_validation = validate_system_type_data(hdfs_path=file_path)
    
    # Зависимости
    file_path >> file_validation >> system_type_data_validation


# Инициализация DAG
dag = read_sqlite_file_dag()



