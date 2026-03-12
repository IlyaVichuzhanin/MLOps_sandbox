from pathlib import Path
from datetime import datetime, timedelta, timezone
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
from contextlib import contextmanager
from typing import Dict, Any, Optional, List
from sqlalchemy import Column, Integer, String, ForeignKey, create_engine, DateTime, func, Index, Boolean, select, text
from sqlalchemy.orm import declarative_base, relationship, Session, declared_attr, sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from uuid import uuid4, UUID
import logging
from helper.helper import parse_uuid
import pandas as pd
import hashlib
from typing import Union, Optional
import numpy as np
from sqlalchemy import (
    Column, Integer, String, ForeignKey, create_engine, 
    DateTime, func, Index, Boolean, select, text, Float, LargeBinary  # ← добавлен LargeBinary
)

Base=declarative_base()
DWH_CONN_ID = 'cloudberry_test_dwh'
log = logging.getLogger(__name__)
# DATA_SOURCE_ID = get_data_source_id



class DatabaseIdent(Base):
    __tablename__ = 'DatabaseIdent'
    id = Column('ID', String(36), primary_key=True)  # UUID как TEXT
    main_db_name = Column('MainDbName', String(100))
    main_db_id = Column('MainDbId', String(36))
    data_type = Column('DataType', Integer)


def get_dwh_engine(conn_id: str = 'cloudberry_test_dwh') -> Engine:
    try:
        connection = PostgresHook.get_connection(conn_id)
        db_url = (
            f"postgresql+psycopg2://{connection.login}:{connection.password}"
            f"@{connection.host}:{connection.port}/{connection.schema}"
        )
        
        engine = create_engine(
            db_url,
            echo=False,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10
        )
        return engine
        
    except Exception as e:
        raise AirflowException(f"Ошибка создания Engine для подключения '{conn_id}': {str(e)}")


def get_data_source_id(engine: Engine)-> UUID:
    uid: UUID
    try:
        with Session(engine) as session:
            stmt = select(DatabaseIdent)
            row = session.execute(stmt.limit(1)).scalars().one_or_none()

            if row is None:
                raise AirflowException("Таблица DatabaseIdent пуста или не содержит записей")
            uid = parse_uuid(row.id)
            if uid is None:
                log.error(f"❌ Невалидный UUID в строке: {row}")
                raise AirflowException(f"Не удалось распарсить ID: {row.id}")
                    
    except Exception as e:
        raise AirflowException(f"Ошибка при подключении к БД: {str(e)}")
    return uid


def md5_hex_to_uuid(hex_string: str) -> UUID:
    if not hex_string or len(hex_string) != 32:
        return uuid4(UUID.Nil, hex_string or 'empty')
    uuid_str = f"{hex_string[:8]}-{hex_string[8:12]}-{hex_string[12:16]}-{hex_string[16:20]}-{hex_string[20:]}"
    return UUID(uuid_str)


# hash_sat_diff: хеш от бизнес-данных + источник (для детекта изменений в Satellite)
def calc_hash_sat_diff(row: pd.Series, data_source_id: UUID) -> UUID:
    values_str = '|'.join(str(val) for val in row) 
    raw_string = f"{values_str}|{str(data_source_id)}"
    md5_hex = hashlib.md5(raw_string.encode('utf-8')).hexdigest()
    return md5_hex_to_uuid(md5_hex)

# hash_sk: хеш от бизнес-ключа + источник (уникальный ключ Hub)
def calc_hash_sk(values: pd.Series) -> UUID:
    raw_string = '|'.join(str(values))
    md5_hex = hashlib.md5(raw_string.encode('utf-8')).hexdigest()
    return md5_hex_to_uuid(md5_hex)



def normalize_value(val, target_type: str) -> Union[str, int, float, UUID, datetime, None]:
    if pd.isna(val) or val is None or (isinstance(val, str) and val.strip() == ''):
        return None
    try:
        if target_type == 'uuid':
            if isinstance(val, bytes):
                if len(val) == 16:
                    return UUID(bytes=val)
                val = val.decode('utf-8', errors='replace').strip()
            if isinstance(val, str):
                return UUID(val.strip())
            if isinstance(val, UUID):
                return val
            return UUID(str(val))
            
        elif target_type == 'timestamp':
            if isinstance(val, (datetime, pd.Timestamp)):
                return val if pd.notna(val) else None
            if isinstance(val, str):
                val = val.strip()
                if not val:
                    return None
                return pd.to_datetime(val, errors='coerce')
            if isinstance(val, (int, float)):
                return pd.to_datetime(val, unit='s', errors='coerce') if val else None
            return pd.to_datetime(str(val), errors='coerce')
            
        elif target_type == 'integer':
            if isinstance(val, (int, np.integer)):
                return int(val)
            if isinstance(val, (float, np.floating)):
                return int(val) if not np.isnan(val) else None  # type: ignore
            if isinstance(val, str):
                return int(float(val.strip()))  # "20.0" → 20
            return int(float(str(val)))
            
        elif target_type == 'float':
            if isinstance(val, (int, float, np.number)):
                return float(val) if not pd.isna(val) else None
            if isinstance(val, str):
                return float(val.strip()) if val.strip() else None
            return float(str(val))
            
        elif target_type == 'boolean':
            if isinstance(val, bool):
                return val
            if isinstance(val, (int, np.integer)):
                return bool(val)
            if isinstance(val, str):
                v = val.strip().lower()
                if v in ('1', 'true', 'yes', 'on'):
                    return True
                if v in ('0', 'false', 'no', 'off', ''):
                    return False
            return bool(val)
            
        elif target_type == 'string':
            if isinstance(val, bytes):
                return val.decode('utf-8', errors='replace').strip().replace('\x00', '')
            return str(val).strip().replace('\x00', '')
         
        elif target_type == 'bytea':
            # Возвращаем байты как есть, или конвертируем строку в байты
            if isinstance(val, bytes):
                return val
            if isinstance(val, str):
                val = val.strip()
                if not val:
                    return None
                # Пытаемся декодировать base64 (частый формат хранения бинарных данных в тексте)
                try:
                    import base64
                    return base64.b64decode(val)
                except Exception:
                    # Если не base64 — кодируем как UTF-8 байты
                    return val.encode('utf-8', errors='replace')
            if pd.isna(val) or val is None:
                return None
            # Fallback: конвертируем в строку и затем в байты
            return str(val).encode('utf-8', errors='replace')
    except Exception:
        return None


def extract_from_sqlite_to_staging(
    sqlite_path: str, 
    source_table_name: str, 
    hash_gener_columns: list[str],
    source_column_names: list[str], 
    staging_table_name: str,
    column_types: dict[str, str] | None = None  
) -> bool:
    """
    column_types: словарь { 'column_name': 'uuid' | 'timestamp' | 'integer' | 'float' | 'string' | 'boolean' }
    Все ключи должны быть в нижнем регистре!
    """
    db_path = sqlite_path if sqlite_path.startswith("sqlite:///") else f"sqlite:///{sqlite_path}"
    sqlite_engine = create_engine(db_path, echo=False)
    dwh_engine = get_dwh_engine(DWH_CONN_ID)
    hash_gener_columns = [col.lower() for col in hash_gener_columns]
    source_column_names = [col.lower() for col in source_column_names]
    
    if column_types is None:
        column_types = {}
    
    try:
        data_source_id: UUID = get_data_source_id(sqlite_engine)
        logging.info(f"Using data_source_id: {data_source_id}")
        
        query = f"SELECT {', '.join(source_column_names).lower()} FROM {source_table_name.lower()}"
        df = pd.read_sql(query, sqlite_engine)
        df.columns = df.columns.str.lower()

        if df.empty:
            logging.info("No data to process in SQLite")
            return True

        df['data_source_id'] = data_source_id
        
        # 🔥 УНИВЕРСАЛЬНАЯ ОБРАБОТКА: каждая колонка → свой тип
        for col in df.columns:
            if col in ['data_source_id', 'hash_sk', 'hash_sat_diff', 'load_dttm']:
                continue  # служебные колонки обрабатываются отдельно
            
            target_type = column_types.get(col, 'string')  # по умолчанию строка
            df[col] = df[col].apply(lambda x: normalize_value(x, target_type))
        
        cols_for_hash = hash_gener_columns + ['data_source_id']

        df['hash_sk'] = df[cols_for_hash].apply(
            lambda row: calc_hash_sk(row),  # row уже содержит нужные значения
            axis=1
        )
        df['hash_sat_diff'] = df.apply(
            lambda row: calc_hash_sat_diff(row.drop(['hash_sk', 'hash_sat_diff', 'load_dttm'], errors='ignore'), data_source_id), 
            axis=1
        )
        df['load_dttm'] = datetime.now(timezone.utc)

        # 7. Подготовка dtype для to_sql
        dtype_mapping = {
            'hash_sk': PG_UUID(as_uuid=True),
            'hash_sat_diff': PG_UUID(as_uuid=True),
            'data_source_id': PG_UUID(as_uuid=True),
            'load_dttm': DateTime(timezone=True),
        }
        # В dtype_mapping внутри extract_from_sqlite_to_staging:
        for col, ttype in column_types.items():
            if col in df.columns:
                if ttype == 'uuid':
                    dtype_mapping[col] = PG_UUID(as_uuid=True)
                elif ttype == 'timestamp':
                    dtype_mapping[col] = DateTime(timezone=False)
                elif ttype == 'integer':
                    dtype_mapping[col] = Integer
                elif ttype == 'float':
                    dtype_mapping[col] = Float 
                elif ttype == 'boolean':
                    dtype_mapping[col] = Boolean
                elif ttype == 'bytea':  # ← новый тип
                    dtype_mapping[col] = LargeBinary
                # 'string' не требует явного dtype

        rows_loaded = df.to_sql(
            name=staging_table_name, 
            con=dwh_engine, 
            if_exists='append', 
            index=False,
            method='multi',
            chunksize=200,
            dtype=dtype_mapping if hasattr(dwh_engine.dialect, 'name') and dwh_engine.dialect.name == 'postgresql' else None
        )
        
        logging.info(f"✅ Loaded {len(df)} rows to {staging_table_name}")
        return True
        
    except SQLAlchemyError as e:
        logging.error(f"Database error in extract_from_sqlite_to_staging: {e}")
        raise AirflowException(f"DB error: {str(e)}")
    except Exception as e:
        logging.error(f"Unexpected error in extract_from_sqlite_to_staging: {e}", exc_info=True)
        raise
    finally:
        sqlite_engine.dispose()
        dwh_engine.dispose()
        logging.debug("Engines disposed")


def clean_duplecate_staging_rows(stg_table_name:str)->bool:
    dwh_engine = get_dwh_engine(DWH_CONN_ID)
    sql_query = f"""
    DELETE FROM {stg_table_name} a
    USING {stg_table_name} b
    WHERE a.ctid < b.ctid
        AND a.* IS NOT DISTINCT FROM b.*
    """
    try:
        with dwh_engine.begin() as conn:
            result = conn.execute(sql_query)
            deleted_count = result.rowcount
            logging.info(
                f"Удалено дубликатов из таблицы {stg_table_name}: {deleted_count}"
            )
            return True
    except Exception as e:
        logging.error(f"Ошибка при удалении дубликатов из {stg_table_name}: {str(e)}")
        return False
    return True


def merge_staging_to_hub(hub_table_name:str, sk_hub_column_name:str, stg_table_name:str):
    dwh_engine = get_dwh_engine(DWH_CONN_ID)
    try:
        with dwh_engine.begin() as conn:

            insert_hub_sql = text(f"""
                INSERT INTO {hub_table_name} (
                    {sk_hub_column_name},
                    load_dttm,
                    data_source_id
                )
                SELECT 
                    stg.hash_sk,
                    stg.load_dttm,
                    stg.data_source_id
                FROM {stg_table_name} stg
                ON CONFLICT ({sk_hub_column_name}) DO NOTHING
            
            """)
            result = conn.execute(insert_hub_sql)
            hub_inserted = result.rowcount
            logging.info(f"Hub: inserted {hub_inserted} new keys")
            
            return {
                "hub_inserted": hub_inserted,
                "status": "success"
            }
            
    except SQLAlchemyError as e:
        logging.error(f"SQLAlchemy error during DV load: {e}")
        raise e
    except Exception as e:
        logging.error(f"Unexpected error during DV load: {e}")
        raise e

            

def merge_staging_to_satellite(
    sat_table_name:str, 
    hab_sk_column_name:str, 
    sat_column_names:list[str], 
    stg_table_name:str, 
    stg_column_names:list[str]):

    dwh_engine = get_dwh_engine(DWH_CONN_ID)
    satellite_columns = ', '.join(sat_column_names).lower()
    staging_columns = 'stg.'+', stg.'.join(stg_column_names).lower()
    try:
        with dwh_engine.begin() as conn:

            # ==================== SATELLITE: ЗАКРЫТИЕ ====================
            # Закрываем старые активные записи ТОЛЬКО если в staging есть ИЗМЕНЕНИЯ
            # IS DISTINCT FROM корректно обрабатывает NULL (NULL ≠ NULL → TRUE при изменении)
            close_old_sat_sql = text(f"""
                UPDATE {sat_table_name} sat
                SET 
                    valid_to_dttm = stg.load_dttm,
                    active_flag = false                 
                FROM {stg_table_name} stg
                WHERE sat.{hab_sk_column_name} = stg.hash_sk
                  AND sat.active_flag = true
                  AND sat.hash_sat_diff IS DISTINCT FROM stg.hash_sat_diff
            """)
            result = conn.execute(close_old_sat_sql)
            sat_closed = result.rowcount
            logging.info(f"Satellite: closed {sat_closed} old records")
            
            # ==================== SATELLITE: ВСТАВКА ====================
            # Вставляем новые записи ТОЛЬКО если нет активной записи 
            # с идентичным hash_sat_diff (избегаем дубликатов при отсутствии изменений)
            insert_sat_sql = text(f"""
                INSERT INTO {sat_table_name} (
                    {hab_sk_column_name},
                    load_dttm,
                    valid_from_dttm,
                    valid_to_dttm,
                    active_flag,
                    data_source_id,
                    hash_sat_diff,
                    {satellite_columns}              
                )
                SELECT 
                    stg.hash_sk,
                    stg.load_dttm,
                    stg.load_dttm,
                    NULL,              -- valid_to_dttm = NULL для активной записи
                    true,              -- active_flag
                    stg.data_source_id,
                    stg.hash_sat_diff,
                    {staging_columns}
                FROM {stg_table_name} stg
                LEFT JOIN {sat_table_name} sat
                    ON stg.hash_sk = sat.{hab_sk_column_name}
                    AND sat.load_dttm = (SELECT MAX(load_dttm) FROM {sat_table_name} WHERE {hab_sk_column_name} = stg.hash_sk)
                WHERE sat.hash_sat_diff IS NULL
                    OR stg.hash_sat_diff <> sat.hash_sat_diff
            """)
            result = conn.execute(insert_sat_sql)
            sat_inserted = result.rowcount
            logging.info(f"Satellite: inserted {sat_inserted} new records")
            
            # # Очистка staging
            # conn.execute(text("TRUNCATE TABLE stg_measure_groups;"))
            # logging.info("Staging table truncated")
            
            return {
                "sat_closed": sat_closed,
                "sat_inserted": sat_inserted,
                "status": "success"
            }
            
    except SQLAlchemyError as e:
        logging.error(f"SQLAlchemy error during DV load: {e}")
        raise e
    except Exception as e:
        logging.error(f"Unexpected error during DV load: {e}")
        raise e







    
    
        



    
