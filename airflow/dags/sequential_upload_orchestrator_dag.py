from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import logging

log = logging.getLogger(__name__)

BATCH_SIZE = 100  # записей на один запуск process_*_batch_dag

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='sequential_upload_orchestrator_dag',
    default_args=default_args,
    max_active_runs=1,
    description='Оркестратор: Any → FH → (Double ∥ MainDb). СТРОГО последовательно',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['cloudberry', 'orchestrator', 'dwh-upload', 'sequential'],
) as dag:

    @task(task_id='get_pending_records')
    def get_pending_records() -> dict[str, list[dict]]:
        hook = PostgresHook(postgres_conn_id='cloudberry_test_dwh')
        rows = hook.get_records("""
            SELECT
                data_source_id, data_file_id, main_db_id, main_db_name, last_update,
                upload_date_time, created_dttm, hdfs_full_path,
                data_type, data_format, is_uploaded_to_dwh
            FROM public.data_catalogue
            WHERE is_uploaded_to_dwh = false 
              AND data_type IN ('MainDb', 'Double', 'Any')
            ORDER BY created_dttm ASC
        """)
        grouped = {'MainDb': [], 'Double': [], 'Any': [], 'FH': []}
        for row in rows:
            record = {
                'data_source_id': str(row[0]),
                'data_file_id': str(row[1]),
                'main_db_id': str(row[2]),
                'main_db_name': row[3] if row[3] is not None else '',
                'last_update': row[4].isoformat() if row[4] else None,
                'upload_date_time': row[5].isoformat() if row[5] else None,
                'created_dttm': row[6].isoformat() if row[6] else None,
                'hdfs_full_path': str(row[7]) if row[7] else '',
                'data_type': str(row[8]),
                'data_format': str(row[9]),
                'is_uploaded_to_dwh': bool(row[10])
            }
            if record['data_format'] == 'fh':
                grouped['FH'].append(record)
            elif record['data_type'] in grouped:
                grouped[record['data_type']].append(record)
        log.info(f"📦 Найдено: MainDb={len(grouped['MainDb'])}, Double={len(grouped['Double'])}, "
                 f"Any={len(grouped['Any'])}, FH={len(grouped['FH'])}")
        return grouped

    @task(task_id='make_batch_confs')
    def make_batch_confs(grouped: dict, data_type: str) -> list[dict]:
        """Режет записи типа на чанки по BATCH_SIZE → список conf для триггеров."""
        records = grouped.get(data_type, [])
        confs = [
            {'records': records[i:i + BATCH_SIZE], 'record_type': data_type}
            for i in range(0, len(records), BATCH_SIZE)
        ]
        log.info(f"🧩 {data_type}: {len(records)} записей → {len(confs)} батчей по {BATCH_SIZE}")
        return confs

    @task(task_id='log_completion', trigger_rule='none_failed_min_one_success')
    def log_completion(*args):
        log.info("🎉 Оркестратор завершил ВСЕ этапы последовательно")
        return "Sequential orchestration completed"

    # ═══════════════════════════════════════════════════════════════
    #  Подготовка conf для каждого этапа
    # ═══════════════════════════════════════════════════════════════
    grouped = get_pending_records()

    any_confs    = make_batch_confs.override(task_id='make_any_confs')(grouped, 'Any')
    fh_confs     = make_batch_confs.override(task_id='make_fh_confs')(grouped, 'FH')
    double_confs = make_batch_confs.override(task_id='make_double_confs')(grouped, 'Double')
    maindb_confs = make_batch_confs.override(task_id='make_maindb_confs')(grouped, 'MainDb')

    # ═══════════════════════════════════════════════════════════════
    #  ТРИГГЕРЫ: mapped по чанкам, ЖДУТ завершения каждого запуска.
    #  Этап N+1 стартует ТОЛЬКО когда ВСЕ mapped-инстансы этапа N success.
    # ═══════════════════════════════════════════════════════════════
    trigger_any = TriggerDagRunOperator.partial(
        task_id='trigger_any_batches',
        trigger_dag_id='process_any_batch_dag',
        wait_for_completion=True,          # 🔥 ЖДЁМ завершения каждого батча
        deferrable=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed'],
        reset_dag_run=False,
    ).expand(conf=any_confs)

    trigger_fh = TriggerDagRunOperator.partial(
        task_id='trigger_fh_batches',
        trigger_dag_id='process_fh_batch_dag',
        wait_for_completion=True,
        deferrable=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed'],
        reset_dag_run=False,
        trigger_rule='none_failed_min_one_success',  # пустой этап Any не ломает цепочку
    ).expand(conf=fh_confs)

    trigger_double = TriggerDagRunOperator.partial(
        task_id='trigger_double_batches',
        trigger_dag_id='process_double_batch_dag',
        wait_for_completion=True,
        deferrable=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed'],
        reset_dag_run=False,
        trigger_rule='none_failed_min_one_success',
    ).expand(conf=double_confs)

    trigger_maindb = TriggerDagRunOperator.partial(
        task_id='trigger_maindb_batches',
        trigger_dag_id='process_maindb_batch_dag',
        wait_for_completion=True,
        deferrable=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed'],
        reset_dag_run=False,
        trigger_rule='none_failed_min_one_success',
    ).expand(conf=maindb_confs)

    completion = log_completion()

    # ═══════════════════════════════════════════════════════════════
    #  🔥 ГРАФ: СТРОГО ПОСЛЕДОВАТЕЛЬНО
    #  ЭТАП 1: Any  →  ЭТАП 2: FH  →  ЭТАП 3: Double ∥ MainDb
    # ═══════════════════════════════════════════════════════════════
    grouped >> [any_confs, fh_confs, double_confs, maindb_confs]

    any_confs >> trigger_any

    # FH только после завершения ВСЕХ Any-батчей
    [trigger_any, fh_confs] >> trigger_fh

    # Double ∥ MainDb только после завершения ВСЕХ FH-батчей
    [trigger_fh, double_confs] >> trigger_double
    [trigger_fh, maindb_confs] >> trigger_maindb

    [trigger_double, trigger_maindb] >> completion