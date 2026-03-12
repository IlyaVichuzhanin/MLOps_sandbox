"""
DAG для добавления 3 записей в таблицу h_diagnostic_defect_types Cloudberry
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
with DAG(
    dag_id='cloudberry_insert_3_records',
    default_args=default_args,
    description='Добавление 3 записей в h_diagnostic_defect_types',
    schedule_interval=None,  # Запуск только вручную
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['cloudberry', 'simple-insert'],
) as dag:

    insert_records = PostgresOperator(
        task_id='insert_three_records',
        postgres_conn_id='cloudberry_test_dwh',
        sql="""
            INSERT INTO public.h_diagnostic_defect_types 
                (h_diagnostic_defect_type, data_catalogue)
            VALUES
                ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '11111111-1111-1111-1111-111111111111'),
                ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12', '22222222-2222-2222-2222-222222222222'),
                ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a13', '33333333-3333-3333-3333-333333333333')
            ON CONFLICT (h_diagnostic_defect_type) DO NOTHING;
        """,
    )