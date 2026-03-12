from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@task_group(group_id="process_main_db_record")
def create_uploading_main_db_data_chain(record: dict):
    """
    Группа задач для обработки одной записи MainDb из data_catalogue:
    1. Сначала загружаются справочные данные
    2. Затем — основные данные
    """
    # Запуск DAG для справочных данных
    trigger_reference_data_check = TriggerDagRunOperator(
        task_id='trigger_reference_data_check',
        trigger_dag_id='check_main_db_reference_data_dag',
        conf={
            'record': record,
            'stage': 'check_main_db_reference_data',
            'source_dag_run_id': '{{ dag_run.run_id }}'
        },
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=False,
        execution_date=None,  # Используем None для автоматической генерации execution_date
        execution_timeout=timedelta(hours=1), # Max time the *triggering* task will run
        allowed_states=['success'],
        failed_states=['failed'],
    )
    
    # Запуск DAG для  данных
    trigger_data_upload = TriggerDagRunOperator(
        task_id='trigger_data_upload',
        trigger_dag_id='upload_main_db_data_dag',
        conf={
            'record': record,
            'stage': 'upload_main_db_data',
            'source_dag_run_id': '{{ dag_run.run_id }}'
        },
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=False,
        execution_date=None,
        execution_timeout=timedelta(hours=1), 
        allowed_states=['success'],
        failed_states=['failed'],
    )
    
    # Устанавливаем зависимость: сначала справочные, потом основные данные
    trigger_reference_data_check >> trigger_data_upload

with DAG(
    dag_id='upload_new_main_db_data',
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['maindb', 'data-upload'],
) as dag:

    @task
    def get_main_db_records(**context):
        main_db_records = context['dag_run'].conf.get('main_db_records', [])
        print(f"Получено записей для обработки: {len(main_db_records)}")
        return main_db_records

    @task.short_circuit
    def check_records(records):
        if not records:
            print("✅ Отсутствуют новые данные MainDb для добавления в DWH")
            return False  # Прерывает выполнение последующих задач
        return True

    records = get_main_db_records()
    check = check_records(records)

    uploading_data_chain = create_uploading_main_db_data_chain.expand(record=records)

    # Установка зависимостей
    records >> check >> uploading_data_chain