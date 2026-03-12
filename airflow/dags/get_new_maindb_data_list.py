from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.param import Param


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=5)
}
with DAG(
    dag_id='get_new_maindb_data_list_from_data_catalogue',
    default_args=default_args,
    description='Сканирование таблицы data_catalogue, получение новых MainDb данных для добавления в DWH',
    schedule_interval=None,  # Запуск только вручную
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['cloudberry', 'MainDb_data', 'DWH'],
) as dag:
    @task
    def get_new_main_db_data_list():
        hook=PostgresHook(postgres_conn_id = 'cloudberry_test_dwh')
        rows = hook.get_records("""       
            SELECT 
                data_source_id, 
                main_db_id,
                main_db_name,
                last_update,
                upload_date_time,
                created_dttm,
                hdfs_full_path,
                data_type,
                data_format,
                is_uploaded_to_dwh
            FROM
                public.data_catalogue as data_catalogue
            WHERE
                data_catalogue.is_uploaded_to_dwh = false AND data_catalogue.data_type = 'MainDb'
                
            ORDER BY
                CASE data_type
                    WHEN 'MainDb' THEN 1
                    WHEN 'Double' THEN 2
                    WHEN 'Any' THEN 3
                    ELSE 4
                END
            """)
        main_db_records=[]
        for row in rows:
            record = {
                'data_source_id': row[0], 
                'main_db_id': row[1],
                'main_db_name': row[2],
                'last_update': row[3].isoformat() if row[3] else None,
                'upload_date_time': row[4].isoformat() if row[4] else None,
                'created_dttm': row[5].isoformat() if row[5] else None,
                'hdfs_full_path': row[6],
                'data_type': row[7],
                'data_format': row[8],
                'is_uploaded_to_dwh': row[9]
            }
            main_db_records.append(record)
        return main_db_records
    
    main_db_records = get_new_main_db_data_list()

    trigger_upload_main_db_data=TriggerDagRunOperator(
        task_id='trigger_process_new_main_db_data',
        trigger_dag_id='upload_new_main_db_data',
        conf={'main_db_records': main_db_records},
        wait_for_completion=False,
        reset_dag_run=True
    )

    main_db_records >> trigger_upload_main_db_data
