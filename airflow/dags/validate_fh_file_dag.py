from datetime import datetime, timedelta
from airflow.decorators import task, dag
import logging
from validate_data.system_type_data_info import fh_system_type_data_info
from validate_data.common_tasks import validate_fh_file
from validate_data.common_tasks import (
    get_record_info_from_conf,
)

log = logging.getLogger(__name__)


@dag(
    dag_id='validate_fh_file_dag',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=1),
    },
    description='Проверка .fh файла (бинарный формат) перед загрузкой данных в DWH для обеспечения целостности данных',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['hdfs', 'fh', 'binary-format', 'data-analysis'],
    params={
        'hdfs_path': '/user/hadoop/data/sample.fh',  # Путь по умолчанию
    }
)
def validate_fh_file_dag():
    """
    DAG для валидации .fh файлов (бинарный формат исторических данных).
    
    Проверки:
    1. Целостность заголовка (магическое слово, CRC32, структура)
    2. Корректность метаданных (Id, MainDbId, MainDbName, DataType)
    3. Валидация системных типов данных
    """
    
    # === Получение информации о записи из конфигурации ===
    record_info = get_record_info_from_conf()
    
    file_validation = validate_fh_file(hdfs_path=record_info['hdfs_path'])
    
    # === Зависимости ===
    record_info >> file_validation


# Инициализация DAG
dag = validate_fh_file_dag()