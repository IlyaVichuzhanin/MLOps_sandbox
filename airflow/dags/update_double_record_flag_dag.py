# dags/update_data_catalogue_flag_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
import logging

log = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=30)
}

@dag(
    dag_id='update_double_record_flag_dag',
    default_args=default_args,
    description='Обновление флага is_uploaded_to_dwh=true в data_catalogue после успешной загрузки',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['cloudberry', 'data-catalogue', 'flag-update'],
)
def update_double_record_flag():
    
    @task(task_id='extract_record_identifiers')
    def extract_record_identifiers(**context) -> dict:
        """
        Извлекает идентификаторы записи из conf для обновления флага.
        Ожидает: {'record': {'data_source_id': ..., 'main_db_id': ..., 'hdfs_full_path': ...}}
        """
        dag_run = context.get('dag_run')
        conf = dag_run.conf if dag_run else {}
        
        record = conf.get('record', {})
        if not record:
            raise AirflowException("❌ Не передана запись (record) в конфигурации DAG")
        
        required_fields = ['data_source_id', 'main_db_id', 'hdfs_full_path']
        missing = [f for f in required_fields if f not in record]
        if missing:
            raise AirflowException(f"❌ В записи отсутствуют обязательные поля: {missing}")
        
        log.info(f"📦 Подготовка к обновлению флага для записи: "
                f"data_source_id={record['data_source_id']}, "
                f"main_db_id={record['main_db_id']}")
        
        return {
            'data_source_id': record['data_source_id'],
            'main_db_id': record['main_db_id'],
            'hdfs_full_path': record['hdfs_full_path'],
            'updated_at': datetime.now().isoformat()
        }
    
    @task(task_id='update_catalogue_flag')
    def update_catalogue_flag(record_info: dict):
        """
        Обновляет is_uploaded_to_dwh = true и фиксирует время загрузки.
        Использует UPSERT-логику для идемпотентности.
        """
        hook = PostgresHook(postgres_conn_id='cloudberry_test_dwh')
        
        # Проверяем текущее состояние перед обновлением
        check_sql = """
            SELECT is_uploaded_to_dwh, upload_date_time 
            FROM public.data_catalogue 
            WHERE data_source_id = %s AND main_db_id = %s
        """
        check_result = hook.get_first(check_sql, parameters=(
            record_info['data_source_id'],
            record_info['main_db_id']
        ))
        
        if not check_result:
            raise AirflowException(
                f"❌ Запись не найдена в data_catalogue: "
                f"data_source_id={record_info['data_source_id']}, "
                f"main_db_id={record_info['main_db_id']}"
            )
        
        if check_result[0] is True:
            log.warning(f"⚠️ Флаг уже установлен для записи {record_info['main_db_id']}, пропускаем обновление")
            return {'status': 'already_updated', 'record_id': record_info['main_db_id']}
        
        # Основное обновление с защитой от конкурентных изменений
        update_sql = """
            UPDATE public.data_catalogue 
            SET 
                is_uploaded_to_dwh = true,
                dwh_upload_dttm = NOW()
            WHERE 
                data_source_id = %s 
                AND main_db_id = %s 
                AND is_uploaded_to_dwh = false  -- защита от race condition
            RETURNING main_db_id, hdfs_full_path
        """
        
        updated = hook.get_first(update_sql, parameters=(
            record_info['data_source_id'],
            record_info['main_db_id']
        ))
        
        if not updated:
            # Возможно, запись уже была обновлена другим процессом
            log.warning(f"⚠️ Запись {record_info['main_db_id']} могла быть обновлена параллельным процессом")
            return {'status': 'concurrent_update', 'record_id': record_info['main_db_id']}
        
        log.info(f"✅ Успешно обновлён флаг для записи: {updated[0]}, путь: {updated[1]}")
        
        return {
            'status': 'success',
            'record_id': updated[0],
            'hdfs_path': updated[1],
            'updated_at': record_info['updated_at']
        }
    
    @task(task_id='log_update_result')
    def log_update_result(update_result: dict):
        """Логирует результат обновления для аудита."""
        status = update_result.get('status')
        record_id = update_result.get('record_id')
        
        if status == 'success':
            log.info(f"🎯 ЗАВЕРШЕНО: Запись {record_id} отмечена как загруженная в DWH")
        elif status == 'already_updated':
            log.info(f"ℹ️  ПРОПУЩЕНО: Запись {record_id} уже была отмечена как загруженная")
        elif status == 'concurrent_update':
            log.warning(f"⚠️  КОНКУРЕНТНОЕ ОБНОВЛЕНИЕ: Запись {record_id} обработана другим процессом")
        else:
            log.warning(f"❓ НЕИЗВЕСТНЫЙ СТАТУС: {status} для записи {record_id}")
        
        return update_result
    
    # === Построение цепочки задач ===
    record_info = extract_record_identifiers()
    update_result = update_catalogue_flag(record_info)
    log_result = log_update_result(update_result)
    
    record_info >> update_result >> log_result
    
    return log_result

# Инициализация DAG
update_double_record_flag_result = update_double_record_flag()