from datetime import datetime, timedelta
from airflow.decorators import task, dag
import logging
from validate_data.system_type_data_info import main_db_system_type_data_info
from validate_data.tables_list import main_db_tables_list
from validate_data.common_tasks import validate_sqlite_file, validate_system_type_data
from validate_data.common_tasks import (
    get_record_info_from_conf,
    
)

log = logging.getLogger(__name__)


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
    # === Определение последовательности задач ===
    record_info = get_record_info_from_conf()
    file_validation = validate_sqlite_file(hdfs_path=record_info['hdfs_path'], tables_list=main_db_tables_list)
    system_type_data_validation = validate_system_type_data(hdfs_path=record_info['hdfs_path'], source_system_type_data_info = main_db_system_type_data_info)
    
    # Зависимости
    record_info >> file_validation >> system_type_data_validation


# Инициализация DAG
dag = read_sqlite_file_dag()



