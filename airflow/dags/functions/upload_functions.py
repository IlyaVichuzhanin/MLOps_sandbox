from pathlib import Path
from datetime import datetime, timezone, date
import os
import uuid, tempfile, gc, numpy as np
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
from typing import Dict, Any, Optional, List, Union, Tuple
from sqlalchemy import Column, Integer, String, ForeignKey, create_engine, DateTime, func, Index, Boolean, select, text, Float, LargeBinary
from sqlalchemy.orm import declarative_base, relationship, Session, declared_attr, sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
import logging
import json
import pandas as pd
import tempfile
import time
import gc
from dataclasses import dataclass
import uuid
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.config import Config
from collections import defaultdict
from pathlib import Path
import tempfile
import pyarrow as pa
import pyarrow.parquet as pq
from collections import defaultdict
from pathlib import Path
from datetime import datetime, timezone, timedelta
import json
import uuid
import tempfile
import struct
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.client import Config
from sqlalchemy import text
from airflow.models import Connection
import psycopg2
import psycopg2.extras
from validate_data.hdfs_utils import hdfs_tempfile
from airflow.hooks.base import BaseHook
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import PrimitiveType
from pyiceberg.io.pyarrow import PyArrowFileIO
from airflow.hooks.base import BaseHook

from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.types import (
    StringType, IntegerType, LongType, DoubleType, BooleanType,
    TimestampType, ListType, PrimitiveType
)

from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.manifest import write_manifest, ManifestEntryStatus



from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from psycopg2.extras import execute_values
from psycopg2.errors import DeadlockDetected
import time


from functions.common_functions import (
    calc_hash_sk, calc_hub_hash_for_column,
    FH_MAGIC_HEX, FH_FROM_DATE_OFFSET, fh_ts_to_datetime,
    FH_MIN_HEADER_LENGTH, FH_HEADER_DATA_OFFSET, FH_EXTRA_MAGIC,
    FH_ROW_ID_LENGTH, FH_QUALITY_LENGTH, FH_FIELD_LENGTH,
    fh_guid_from_bytes_le
)

DATATYPE_ID = '05229302-46d6-f9af-261c-6e0ea91996a2'




from functions.common_functions import (
    get_dwh_engine, 
    normalize_value,
    calc_hash_sk, 
    calc_hash_sat_diff,
    calc_link_hash,
    calc_hub_hash_for_column,  
    read_entity_config,
    deserialize_spectrum_blob,
    deserialize_value,
    deserialize_creyt_blob,
    deserialize_lcard_blob,
    deserialize_sample_data_blob,
    deserialize_siemens_sample_blob,
    deserialize_bode_blob,
    decode_creyt_timeseries,
    decode_sample_timeseries,
    decode_siemens_sample_timeseries,
    ensure_uuid_string,
    pg_insert_values,
    create_clickhouse_client,
    prepare_record_for_clickhouse,
    mark_records_as_exported,
    insert_to_clickhouse,
    get_data_source_name,
    decode_lcard_timeseries,
    deserialize_diagnostic_array_blob,
    load_array_data_to_datamart,
    parse_fh_file,
    FhParsedValue,
    merge_fh_data_optimized,
    mark_records_as_exported_via_temp_table,
    decode_creyt_timeseries_numpy,
    _flush_common_partition_to_s3,
    _commit_and_update_db_common,
    _get_type_for_iceberg,
    _init_or_load_table_metadata,
    _build_partition_path,
    _convert_value_for_pyarrow,
    _write_manifest_file,
    _write_manifest_list,
    _commit_snapshot,
    _parse_timestamp,
    
    
    
    
    
    
    
    )





# ==============================================================================
# Константы
# ==============================================================================

Base = declarative_base()
DWH_CONN_ID = 'cloudberry_test_dwh'
cloudberry_engine = get_dwh_engine(DWH_CONN_ID)
log = logging.getLogger(__name__)
TARGET_FILE_SIZE_MB = int(os.getenv("PARQUET_TARGET_SIZE_MB", "256"))
TARGET_FILE_SIZE_BYTES = TARGET_FILE_SIZE_MB * 1024 * 1024
ZSTD_COMPRESSION_LEVEL = int(os.getenv("ZSTD_LEVEL", "3"))



def extract_from_sqlite_to_staging(
    sqlite_path: str, 
    entity_config: Dict[str, Any],
    data_file_id: str,
    data_source_id: str,
    main_db_id: str 
) -> bool:
    # Парсим конфигурацию
    if 'source_column_descriptions' in entity_config:
        config = read_entity_config(entity_config)
    else:
        config = entity_config.copy()
        config['hash_hub_gener_columns'] = [c.lower() for c in config.get('hash_hub_gener_columns', [])]
        config['source_column_names'] = [c.lower() for c in config.get('source_column_names', [])]
        config['column_types'] = {k.lower(): v for k, v in config.get('column_types', {}).items()}
    
    # Извлекаем параметры из конфигурации
    source_table_name = config['source_table_name']
    staging_table_name = config['staging_table_name']
    hash_hub_gener_columns = config['hash_hub_gener_columns']
    source_column_names = config['source_column_names']
    column_types = config.get('column_types', {})
    link_columns_dict = config.get('link_columns', {})
    link_columns = []
    
    for col_dict in link_columns_dict:
        link_columns.append(col_dict.get('column_name', ''))
    
    db_path = sqlite_path if sqlite_path.startswith("sqlite:///") else f"sqlite:///{sqlite_path}"
    sqlite_engine = create_engine(db_path, echo=False)
    dwh_engine = get_dwh_engine(DWH_CONN_ID)
    
    try:
        log.info(f"Extract data from sqlite started")
        
        # ИСПРАВЛЕНИЕ: Экранируем все колонки
        quoted_cols = [f'"{col}"' for col in source_column_names]
        query = f"SELECT {', '.join(quoted_cols)} FROM \"{source_table_name}\""
        df = pd.read_sql(query, sqlite_engine)
        
        log.info(f"Data transform for staging starts")
        df.columns = df.columns.str.lower()

        if df.empty:
            log.info(f"⚠️ No data to process in SQLite for entity '{source_table_name}' - returning early")
            return True

        df['data_file_id'] = data_file_id
        df['data_source_id'] = data_source_id
        df['main_db_id'] = main_db_id

        # ==========================================
        # 1. НОРМАЛИЗАЦИЯ ЗНАЧЕНИЙ (ВЕКТОРИЗАЦИЯ)
        # ==========================================
        for col in df.columns:
            if col in ['main_db_id', 'data_source_id', 'data_file_id', 'hash_sk', 'hash_sat_diff', 'load_dttm']:
                continue
            
            target_type = column_types.get(col, 'string')
            
            # Векторные методы вместо apply (в 100+ раз быстрее)
            if target_type == 'string':
                # ==========================================
                # 🔥 ИСПРАВЛЕНИЕ: NULL/NaN → пустая строка (вместо наоборот)
                # ==========================================
                # 1. Сначала заполняем все None/NaN пустой строкой ДО astype(str)
                #    (иначе astype(str) превратит NaN в строку 'nan', а None в 'None')
                df[col] = df[col].fillna('')
                
                # 2. Конвертируем в строку и убираем пробелы
                df[col] = df[col].astype(str).str.strip()
                
                # 3. Заменяем строковые представления "пустоты" на реальную пустую строку
                df[col] = df[col].replace({
                    'nan': '', 
                    'None': '', 
                    'null': '', 
                    'NaT': '',
                    '<NA>': ''
                })
                
            elif target_type == 'integer':
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            elif target_type == 'float':
                df[col] = pd.to_numeric(df[col], errors='coerce')
            elif target_type == 'timestamp':
                # Надёжный парсинг timestamp со смешанными форматами
                if df[col].dtype == 'object':
                    df[col] = df[col].replace({
                        'nan': None, 'None': None, 'NaT': None, '': None, 'null': None
                    })
                    df[col] = df[col].astype(str).str.replace(
                        r'(\.\d{6})\d+', r'\1', regex=True
                    )
                    df[col] = df[col].replace({'nan': None, 'None': None})
                
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce', format='mixed')
                except (ValueError, TypeError):
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                
                if df[col].dt.tz is None:
                    df[col] = df[col].dt.tz_localize('UTC')
                
                fallback_date = pd.Timestamp('1970-01-01', tz='UTC')
                nat_count = df[col].isna().sum()
                if nat_count > 0:
                    log.warning(f"⚠️ Column '{col}': {nat_count} values replaced with fallback date")
                df[col] = df[col].fillna(fallback_date)
                
            elif target_type == 'boolean':
                df[col] = df[col].astype(str).str.lower().isin(['true', '1', 't', 'yes'])
            else:
                # Fallback для сложных типов (uuid, json, bytea)
                df[col] = df[col].map(lambda x: normalize_value(x, target_type))

        # ==========================================
        # 2. РАСЧЕТ hash_sk (ОПТИМИЗАЦИЯ)
        # ==========================================
        cols_for_hash = hash_hub_gener_columns + ['main_db_id']
        df['hash_sk'] = [calc_hash_sk(row) for row in df[cols_for_hash].to_dict('records')]
        
        # ==========================================
        # 3. РАСЧЕТ hash_sat_diff (ОПТИМИЗАЦИЯ)
        # ==========================================
        cols_to_drop = ['hash_sk', 'hash_sat_diff', 'load_dttm', 'data_source_id', 'data_file_id', 'main_db_id'] + link_columns
        if link_columns:
            for column in link_columns:
                target_col = f"l_{str(column).lower()}_sk"
                cols_to_drop.append(target_col)
        
        # Конвертируем в список словарей и фильтруем ключи
        all_records = df.to_dict('records')
        cols_to_drop_set = set(cols_to_drop)  # set для O(1) проверки
        
        # Создаём отфильтрованные словари для hash_sat_diff
        filtered_records = [
            {k: v for k, v in row.items() if k not in cols_to_drop_set}
            for row in all_records
        ]
        
        df['hash_sat_diff'] = [
            calc_hash_sat_diff(filtered_row, data_file_id) 
            for filtered_row in filtered_records
        ]

        df['load_dttm'] = datetime.now(timezone.utc)

        # ==========================================
        # 4. ГЕНЕРАЦИЯ ЛИНК-КОЛОНОК (ОПТИМИЗАЦИЯ)
        # ==========================================
        # 🔥 ВАЖНО: Пересоздаём all_records, т.к. в df добавились новые колонки
        all_records = df.to_dict('records')
        
        if link_columns_dict:
            for link_config in link_columns_dict:
                link_col = str(link_config.get('column_name', ''))
                is_system_type_data = str(link_config.get('is_system_type_data', 'False')).lower().strip() in ('true', '1', 'yes', 'on')
                prefix = str(link_config.get('prefix_entity_name', ''))
                if not link_col:
                    continue
                
                target_col = f"l_hub_{str(link_col).lower()}_sk"
                df[target_col] = [
                    calc_link_hash(row, link_col, is_system_type_data, prefix, main_db_id) 
                    for row in all_records
                ]
                
                link_hub_sk = f"{link_col.lower()}_sk"
                df[link_hub_sk] = [
                    calc_hub_hash_for_column(row, link_col, is_system_type_data, prefix, main_db_id) 
                    for row in all_records
                ]
        
        # ==========================================
        # 5. ЗАГРУЗКА В POSTGRESQL (ОПТИМИЗАЦИЯ)
        # ==========================================
        log.info(f"Data insertion starts")
        
        rows_loaded = df.to_sql(
            name=staging_table_name, 
            con=dwh_engine, 
            if_exists='append', 
            index=False,
            method=pg_insert_values,
            chunksize=5_000_000
        )
        
        log.info(f"Data insertion ended. Rows loaded: {rows_loaded}")
        return True
        
    except SQLAlchemyError as e:
        raise AirflowException(f"DB error: {str(e)}")
    except Exception as e:
        raise
    finally:
        sqlite_engine.dispose()
        dwh_engine.dispose()
        log.debug("🔌 Database engines disposed")

import os
import gc
import logging

log = logging.getLogger(__name__)

# Пытаемся импортировать psutil для мониторинга ресурсов
try:
    import psutil
    _HAS_PSUTIL = True
except ImportError:
    _HAS_PSUTIL = False
    log.warning("⚠️ Библиотека 'psutil' не найдена. Детальный мониторинг RAM/CPU процесса будет недоступен. Установите: pip install psutil")

def _log_process_resources(iteration: int, buffered_points: int):
    """Логирует потребление ресурсов текущим процессом Python (Airflow Worker)."""
    try:
        if _HAS_PSUTIL:
            process = psutil.Process(os.getpid())
            mem_info = process.memory_info()
            mem_mb = mem_info.rss / (1024 * 1024)
            mem_gb = mem_mb / 1024
            # interval=0.1 дает мгновенную оценку загрузки CPU процессом
            cpu_pct = process.cpu_percent(interval=0.1)
            log.info(
                f"📊 [Iter {iteration}] Ресурсы процесса: "
                f"CPU={cpu_pct:.1f}%, RAM={mem_gb:.2f} GB ({mem_mb:.0f} MB), "
                f"Точек в буфере: {buffered_points:,}"
            )
        else:
            log.info(f"📊 [Iter {iteration}] Ресурсы процесса: psutil не установлен. Точек в буфере: {buffered_points:,}")
    except Exception as e:
        log.warning(f"⚠️ Не удалось получить метрики процесса: {e}")

def extract_object_data_values_data_to_staging(
    sqlite_path: str, 
    entity_config: Dict[str, Any],
    data_file_id: str,
    data_source_id: str,
    main_db_id: str, 
) -> bool:
    
    # Парсим конфигурацию
    if 'source_column_descriptions' in entity_config:
        config = read_entity_config(entity_config)
    else:
        config = entity_config.copy()
        config['source_column_names'] = [c.lower() for c in config.get('source_column_names', [])]
        config['column_types'] = {k.lower(): v for k, v in config.get('column_types', {}).items()}

    source_table_name = config['source_table_name']
    staging_table_name = config['staging_table_name']
    source_column_names = config['source_column_names']
    hash_hub_gener_columns = config['hash_hub_gener_columns']
    column_types = config.get('column_types', {})
    
    db_path = sqlite_path if sqlite_path.startswith("sqlite:///") else f"sqlite:///{sqlite_path}"
    sqlite_engine = create_engine(db_path, echo=False)
    dwh_engine = get_dwh_engine(DWH_CONN_ID)
    
    try:
        log.info(f"Extract data from sqlite started")
        
        # ИСПРАВЛЕНИЕ: Экранируем все колонки
        quoted_cols = [f'"{col}"' for col in source_column_names]
        query = f"SELECT {', '.join(quoted_cols)} FROM \"{source_table_name}\""
        df = pd.read_sql(query, sqlite_engine)
        
        log.info(f"Data transform for staging starts")
        df.columns = df.columns.str.lower()

        if df.empty:
            log.info(f"⚠️ No data to process in SQLite for entity '{source_table_name}' - returning early")
            return True

        df['data_file_id'] = data_file_id
        df['data_source_id'] = data_source_id
        df['main_db_id'] = main_db_id
        df['load_dttm'] = datetime.now(timezone.utc)
        
        # ==========================================
        # 1. РАСЧЕТ hash_sk (ОПТИМИЗАЦИЯ)
        # ==========================================
        cols_for_hash = hash_hub_gener_columns + ['main_db_id']
        df['hash_sk'] = [calc_hash_sk(row) for row in df[cols_for_hash].to_dict('records')]
        
        # ==========================================
        # 2. НОРМАЛИЗАЦИЯ ЗНАЧЕНИЙ (ВЕКТОРИЗАЦИЯ)
        # ==========================================
        for col in df.columns:
            if col in ['main_db_id', 'data_file_id', 'data_source_id', 'hash_sk', 'load_dttm', 
                       'h_object_property_sk', 'h_data_type_sk']:
                continue
            
            target_type = column_types.get(col, 'string')
            
            # 🔥 ИСПРАВЛЕНИЕ: NULL/NaN → пустая строка для string типа
            if target_type == 'string':
                # 1. Сначала заполняем все None/NaN пустой строкой ДО astype(str)
                df[col] = df[col].fillna('')
                # 2. Конвертируем в строку и убираем пробелы
                df[col] = df[col].astype(str).str.strip()
                # 3. Заменяем строковые представления "пустоты" на реальную пустую строку
                df[col] = df[col].replace({
                    'nan': '', 
                    'None': '', 
                    'null': '', 
                    'NaT': '',
                    '<NA>': ''
                })
            elif target_type == 'integer':
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            elif target_type == 'float':
                df[col] = pd.to_numeric(df[col], errors='coerce')
            elif target_type == 'timestamp':
                # Надёжный парсинг timestamp со смешанными форматами
                if df[col].dtype == 'object':
                    # Очищаем "мусорные" значения
                    df[col] = df[col].replace({
                        'nan': None, 'None': None, 'NaT': None, '': None, 'null': None
                    })
                    
                    # Обрезаем наносекунды (7+ знаков) до микросекунд
                    df[col] = df[col].astype(str).str.replace(
                        r'(\.\d{6})\d+', r'\1', regex=True
                    )
                    
                    # Повторная очистка после astype(str)
                    df[col] = df[col].replace({'nan': None, 'None': None})
                
                # Парсим с format='mixed' (поддержка смешанных форматов)
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce', format='mixed')
                except (ValueError, TypeError):
                    # Fallback для старых версий pandas
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                
                # Устанавливаем UTC timezone
                if df[col].dt.tz is None:
                    df[col] = df[col].dt.tz_localize('UTC')
                
                # Заменяем оставшиеся NaT на fallback (гарантия NOT NULL)
                fallback_date = pd.Timestamp('1970-01-01', tz='UTC')
                nat_count = df[col].isna().sum()
                if nat_count > 0:
                    log.warning(f"⚠️ Column '{col}': {nat_count} values replaced with fallback date")
                df[col] = df[col].fillna(fallback_date)
                
            elif target_type == 'boolean':
                df[col] = df[col].astype(str).str.lower().isin(['true', '1', 't', 'yes'])
            else:
                # Fallback для сложных типов (uuid, json, bytea)
                df[col] = df[col].map(lambda x: normalize_value(x, target_type))

        # ==========================================
        # 3. РАСЧЕТ ХЕШЕЙ ДЛЯ HUB (ОПТИМИЗАЦИЯ)
        # ==========================================
        # Конвертируем в список словарей ОДИН раз для обоих хешей
        records = df.to_dict('records')
        
        # Обратите внимание: здесь используется 'propertyid' вместо 'id'
        df['h_object_property_sk'] = [
            calc_hub_hash_for_column(row, 'propertyid', False, '', main_db_id) 
            for row in records
        ]
        df['h_data_type_sk'] = [
            calc_hub_hash_for_column(row, 'datatypeid', True, 'h_data_types', main_db_id) 
            for row in records
        ]
        
        # ==========================================
        # 4. ЗАГРУЗКА В POSTGRESQL (ОПТИМИЗАЦИЯ)
        # ==========================================
        log.info(f"Data insertion starts")
        
        rows_loaded = df.to_sql(
            name=staging_table_name, 
            con=dwh_engine, 
            if_exists='append', 
            index=False,
            method=pg_insert_values,
            chunksize=5_000_000
        )
        
        log.info(f"Data insertion ended. Rows loaded: {rows_loaded}")
        return True
        
    except SQLAlchemyError as e:
        raise AirflowException(f"DB error: {str(e)}")
    except Exception as e:
        raise
    finally:
        sqlite_engine.dispose()
        dwh_engine.dispose()
        log.debug("🔌 Database engines disposed")




def extract_object_data_values_to_staging_2(
    sqlite_path: str, 
    entity_config: Dict[str, Any],
    data_file_id: str,
    data_source_id: str,
    main_db_id: str
) -> bool:
    
    # Парсим конфигурацию
    if 'source_column_descriptions' in entity_config:
        config = read_entity_config(entity_config)
    else:
        config = entity_config.copy()
        config['source_column_names'] = [c.lower() for c in config.get('source_column_names', [])]
        config['column_types'] = {k.lower(): v for k, v in config.get('column_types', {}).items()}

    source_table_name = config['source_table_name']
    staging_table_name = config['staging_table_name']
    source_column_names = config['source_column_names']
    hash_hub_gener_columns = config['hash_hub_gener_columns']
    column_types = config.get('column_types', {})
    
    db_path = sqlite_path if sqlite_path.startswith("sqlite:///") else f"sqlite:///{sqlite_path}"
    sqlite_engine = create_engine(db_path, echo=False)
    dwh_engine = get_dwh_engine(DWH_CONN_ID)
    
    try:
        log.info(f"Extract data from sqlite started")
        
        quoted_cols = [f'"{col}"' for col in source_column_names]
        query = f"SELECT {', '.join(quoted_cols)} FROM \"{source_table_name}\""
        df = pd.read_sql(query, sqlite_engine)
        
        log.info(f"Data transform for staging starts")
        df.columns = df.columns.str.lower()

        if df.empty:
            log.info(f"⚠️ No data to process in SQLite for entity '{source_table_name}'")
            return True

        df['data_file_id'] = data_file_id
        df['data_source_id'] = data_source_id
        df['load_dttm'] = datetime.now(timezone.utc)
        
        # ==========================================
        # 1. РАСЧЕТ hash_sk
        # ==========================================
        cols_for_hash = hash_hub_gener_columns + ['data_file_id']
        df['hash_sk'] = [calc_hash_sk(row) for row in df[cols_for_hash].to_dict('records')]
        
        # ==========================================
        # 2. НОРМАЛИЗАЦИЯ ЗНАЧЕНИЙ (БЫСТРАЯ ВЕРСИЯ)
        # ==========================================
        for col in df.columns:
            if col in ['data_file_id', 'data_source_id', 'hash_sk', 'load_dttm', 
                       'h_object_property_sk', 'h_data_type_sk']:
                continue
            
            target_type = column_types.get(col, 'string')
            
            if target_type == 'string':
                df[col] = df[col].astype(str).str.strip().replace({'nan': None, 'None': None, '': None})
            elif target_type == 'integer':
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            elif target_type == 'float':
                df[col] = pd.to_numeric(df[col], errors='coerce')
            elif target_type == 'timestamp':
                # ==========================================
                # НАДЁЖНЫЙ ПАРСИНГ TIMESTAMP (БЕЗ ДИАГНОСТИКИ)
                # ==========================================
                if df[col].dtype == 'object':
                    # 1. Очищаем "мусорные" значения
                    df[col] = df[col].replace({
                        'nan': None, 'None': None, 'NaT': None, '': None, 'null': None
                    })
                    
                    # 2. Обрезаем наносекунды (7+ знаков) до микросекунд
                    df[col] = df[col].astype(str).str.replace(
                        r'(\.\d{6})\d+', r'\1', regex=True
                    )
                    
                    # 3. Повторная очистка после astype(str)
                    df[col] = df[col].replace({'nan': None, 'None': None})
                
                # 4. Парсим с format='mixed' (поддержка смешанных форматов)
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce', format='mixed')
                except (ValueError, TypeError):
                    # Fallback для старых версий pandas
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                
                # 5. Устанавливаем UTC timezone
                if df[col].dt.tz is None:
                    df[col] = df[col].dt.tz_localize('UTC')
                
                # 6. Заменяем оставшиеся NaT на fallback (гарантия NOT NULL)
                fallback_date = pd.Timestamp('1970-01-01', tz='UTC')
                nat_count = df[col].isna().sum()
                if nat_count > 0:
                    log.warning(f"⚠️ Column '{col}': {nat_count} values replaced with fallback date")
                df[col] = df[col].fillna(fallback_date)
                
            elif target_type == 'boolean':
                df[col] = df[col].astype(str).str.lower().isin(['true', '1', 't', 'yes'])
            else:
                df[col] = df[col].map(lambda x: normalize_value(x, target_type))

        # ==========================================
        # 3. РАСЧЕТ ХЕШЕЙ ДЛЯ HUB
        # ==========================================
        records = df.to_dict('records')
        
        df['h_object_property_sk'] = [
            calc_hub_hash_for_column(row, 'id', False, '', main_db_id) 
            for row in records
        ]
        df['h_data_type_sk'] = [
            calc_hub_hash_for_column(row, 'datatypeid', True, 'h_data_types', main_db_id) 
            for row in records
        ]
        
        # ==========================================
        # 4. ЗАГРУЗКА В POSTGRESQL
        # ==========================================
        log.info(f"Data insertion starts")
        
        rows_loaded = df.to_sql(
            name=staging_table_name, 
            con=dwh_engine, 
            if_exists='append', 
            index=False,
            method=pg_insert_values,
            chunksize=500_000
        )
        
        log.info(f"Data insertion ended. Rows loaded: {rows_loaded}")
        return True
        
    except SQLAlchemyError as e:
        raise AirflowException(f"DB error: {str(e)}")
    except Exception as e:
        raise
    finally:
        sqlite_engine.dispose()
        dwh_engine.dispose()
        log.debug("🔌 Database engines disposed")


from psycopg2.extras import execute_values
        
def extract_object_data_values_to_staging(
    sqlite_path: str, 
    entity_config: Dict[str, Any],
    data_file_id: str,
    data_source_id: str,
    main_db_id: str
) -> bool:
    
    # Парсим конфигурацию
    if 'source_column_descriptions' in entity_config:
        config = read_entity_config(entity_config)
    else:
        config = entity_config.copy()
        config['source_column_names'] = [c.lower() for c in config.get('source_column_names', [])]
        config['column_types'] = {k.lower(): v for k, v in config.get('column_types', {}).items()}

    source_table_name = config['source_table_name']
    staging_table_name = config['staging_table_name']
    source_column_names = config['source_column_names']
    hash_hub_gener_columns = config['hash_hub_gener_columns']
    column_types = config.get('column_types', {})
    
    db_path = sqlite_path if sqlite_path.startswith("sqlite:///") else f"sqlite:///{sqlite_path}"
    sqlite_engine = create_engine(db_path, echo=False)
    dwh_engine = get_dwh_engine(DWH_CONN_ID)
    
    try:
        log.info(f"Extract data from sqlite started")
        
        quoted_cols = [f'"{col}"' for col in source_column_names]
        query = f"SELECT {', '.join(quoted_cols)} FROM \"{source_table_name}\""
        df = pd.read_sql(query, sqlite_engine)
        
        log.info(f"Data transform for staging starts")
        df.columns = df.columns.str.lower()

        if df.empty:
            log.info(f"⚠️ No data to process in SQLite for entity '{source_table_name}'")
            return True

        df['data_file_id'] = data_file_id
        df['data_source_id'] = data_source_id
        df['main_db_id'] = main_db_id
        # 🔥 Сохраняем время загрузки как есть, без принудительного UTC
        df['load_dttm'] = datetime.now()
        
        # ==========================================
        # 1. РАСЧЕТ hash_sk
        # ==========================================
        cols_for_hash = hash_hub_gener_columns + ['main_db_id']
        df['hash_sk'] = [calc_hash_sk(row) for row in df[cols_for_hash].to_dict('records')]
        
        # ==========================================
        # 2. НОРМАЛИЗАЦИЯ ЗНАЧЕНИЙ (БЫСТРАЯ ВЕРСИЯ)
        # ==========================================
        for col in df.columns:
            if col in ['main_db_id', 'data_file_id', 'data_source_id', 'hash_sk', 'load_dttm', 
                       'h_object_property_sk', 'h_data_type_sk']:
                continue
            
            target_type = column_types.get(col, 'string')
            
            if target_type == 'string':
                df[col] = df[col].astype(str).str.strip().replace({'nan': None, 'None': None, '': None})
            elif target_type == 'integer':
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            elif target_type == 'float':
                df[col] = pd.to_numeric(df[col], errors='coerce')
            elif target_type == 'timestamp':
                # ==========================================
                # 🔥 НАДЁЖНЫЙ ПАРСИНГ TIMESTAMP (БЕЗ КОНВЕРТАЦИИ В UTC)
                # ==========================================
                if df[col].dtype == 'object':
                    # 1. Очищаем "мусорные" значения
                    df[col] = df[col].replace({
                        'nan': None, 'None': None, 'NaT': None, '': None, 'null': None
                    })
                    
                    # 2. Обрезаем наносекунды (7+ знаков) до микросекунд
                    df[col] = df[col].astype(str).str.replace(
                        r'(\.\d{6})\d+', r'\1', regex=True
                    )
                    
                    # 3. Повторная очистка после astype(str)
                    df[col] = df[col].replace({'nan': None, 'None': None})
                
                # 4. Парсим с format='mixed', явно указывая utc=False
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce', format='mixed', utc=False)
                except (ValueError, TypeError):
                    # Fallback для старых версий pandas
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                
                # 5. Заменяем оставшиеся NaT на fallback (БЕЗ таймзоны)
                fallback_date = pd.Timestamp('1970-01-01')
                nat_count = df[col].isna().sum()
                if nat_count > 0:
                    log.warning(f"⚠️ Column '{col}': {nat_count} values replaced with fallback date")
                df[col] = df[col].fillna(fallback_date)
                
            elif target_type == 'boolean':
                df[col] = df[col].astype(str).str.lower().isin(['true', '1', 't', 'yes'])
            else:
                df[col] = df[col].map(lambda x: normalize_value(x, target_type))

        # ==========================================
        # 3. РАСЧЕТ ХЕШЕЙ ДЛЯ HUB
        # ==========================================
        records = df.to_dict('records')
        
        df['h_object_property_sk'] = [
            calc_hub_hash_for_column(row, 'id', False, '', main_db_id) 
            for row in records
        ]
        df['h_data_type_sk'] = [
            calc_hub_hash_for_column(row, 'datatypeid', True, 'h_data_types', main_db_id) 
            for row in records
        ]
        
        # ==========================================
        # 4. ЗАГРУЗКА В POSTGRESQL
        # ==========================================
        log.info(f"Data insertion starts")
        
        rows_loaded = df.to_sql(
            name=staging_table_name, 
            con=dwh_engine, 
            if_exists='append', 
            index=False,
            method=pg_insert_values,
            chunksize=500_000
        )
        
        log.info(f"Data insertion ended. Rows loaded: {rows_loaded}")
        return True
        
    except SQLAlchemyError as e:
        raise AirflowException(f"DB error: {str(e)}")
    except Exception as e:
        raise
    finally:
        sqlite_engine.dispose()
        dwh_engine.dispose()
        log.debug("🔌 Database engines disposed")


from psycopg2.extras import execute_values



import gc
import uuid
import pandas as pd
import boto3
import clickhouse_connect
from botocore.config import Config
from datetime import datetime, timedelta, timezone, date
from typing import Dict, Tuple, List, Any
from sqlalchemy import text
import pyarrow as pa
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.io.pyarrow import PyArrowFileIO
from airflow.hooks.base import BaseHook



def merge_common_object_data_values(
    stg_table_name: str,
    target_table_name: str,
    data_type_id: str,
    data_type: str,
    main_db_name: str,
    is_hist_data: bool = None,  # 🔥 Теперь опциональный - автоопределяется
    data_file_id: str = None,
    ch_batch_size: int = 5_000_000,
    s3_batch_size: int = 50_000_000,
    read_chunk_size: int = 500_000,
) -> dict:
    """
    Двухэтапный MERGE с автоматическим определением типа данных.
    """
    import gc, uuid, boto3, pandas as pd, numpy as np, clickhouse_connect
    from botocore.config import Config
    from datetime import datetime, timezone, date, timedelta
    from typing import Dict, Tuple, Any, List
    from sqlalchemy import text
    from airflow.hooks.base import BaseHook
    import pyarrow as pa
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import DayTransform, IdentityTransform
    from pyiceberg.io.pyarrow import PyArrowFileIO
    
    # 🔥 АВТООПРЕДЕЛЕНИЕ is_hist_data на основе имени staging таблицы
    if is_hist_data is None:
        is_hist_data = '_hist' in stg_table_name.lower()
    
    dwh_engine = get_dwh_engine('cloudberry_test_dwh')
    timestamp_column_name = 'timestamp' if is_hist_data else 'date'
    
    conn_config = BaseHook.get_connection('clickhouse_conn')
    ch_client = clickhouse_connect.get_client(
        host=conn_config.host, port=8123, database=conn_config.schema or 'default',
        username=conn_config.login or 'default', password=conn_config.password or '',
        compress=True, query_limit=0,
    )
    
    s3_conn = BaseHook.get_connection('minio_conn')
    s3_endpoint = s3_conn.extra_dejson.get("host", "http://minio:9000")
    if not s3_endpoint.startswith("http"): s3_endpoint = f"http://{s3_endpoint}"
    bucket = s3_conn.extra_dejson.get("bucket", "iceberg-warehouse")
    region = s3_conn.extra_dejson.get("region", "us-east-1")
    
    s3_client = boto3.client("s3", endpoint_url=s3_endpoint, aws_access_key_id=s3_conn.login,
                             aws_secret_access_key=s3_conn.password, region_name=region,
                             config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))
    file_io = PyArrowFileIO(properties={"s3.endpoint": s3_endpoint, "s3.access-key-id": s3_conn.login,
                                        "s3.secret-access-key": s3_conn.password, "s3.region": region, "s3.path-style-access": "true"})
    
    hist_path = 'history-data' if is_hist_data else 'non-history-data'
    table_path = f"{main_db_name}/{hist_path}/{target_table_name}"
    metadata_prefix, data_prefix = f"{table_path}/metadata", f"{table_path}/data"
    
    iceberg_type, pa_type = _get_type_for_iceberg(data_type)
    value_is_nullable = False
    
    table_metadata = _init_or_load_table_metadata(s3_client, bucket, table_path, metadata_prefix, iceberg_type, pa_type, True)
    
    pa_schema = pa.schema([
        pa.field("data_record_sk", pa.string(), nullable=False),
        pa.field("h_object_property_sk", pa.string(), nullable=True),
        pa.field("data_file_id", pa.string(), nullable=False),
        pa.field("data_source_id", pa.string(), nullable=False),
        pa.field("main_db_id", pa.string(), nullable=True),
        pa.field("load_dttm", pa.string(), nullable=True),
        pa.field("value", pa_type, nullable=value_is_nullable),
        pa.field("timestamp", pa.timestamp("us"), nullable=True),
    ])
    
    def _get_field_id(metadata_dict: dict, field_names: list, schema_obj: pa.Schema) -> int:
        def find_field_id(obj, target_names):
            if isinstance(obj, dict):
                if obj.get("name") in target_names and "id" in obj: return obj.get("id")
                for v in obj.values():
                    res = find_field_id(v, target_names)
                    if res is not None: return res
            elif isinstance(obj, list):
                for item in obj:
                    res = find_field_id(item, target_names)
                    if res is not None: return res
            return None
        found_id = find_field_id(metadata_dict, field_names)
        if found_id is not None: return found_id
        for i, field in enumerate(schema_obj):
            if field.name in field_names: return i + 1
        raise ValueError(f"Field {field_names} not found")

    ts_field_id = _get_field_id(table_metadata, ["timestamp", "date", "base_timestamp"], pa_schema)
    src_field_id = _get_field_id(table_metadata, ["data_source_id", "source_id"], pa_schema)
    
    partition_spec = PartitionSpec(
        PartitionField(source_id=ts_field_id, field_id=1000, transform=DayTransform(), name="dt_day"),
        PartitionField(source_id=src_field_id, field_id=1001, transform=IdentityTransform(), name="source_id"),
        spec_id=0
    )
    
    file_filter = ""
    sql_params = {"data_type_id": data_type_id}
    if data_file_id:
        file_filter = "AND stg.data_file_id = :data_file_id"
        sql_params["data_file_id"] = data_file_id
    
    # 🔥 Используем правильно определённую колонку timestamp_column_name
    sql_fetch = text(f"""
        SELECT stg.hash_sk, stg.h_object_property_sk, stg.load_dttm, 
               stg.data_file_id, stg.data_source_id, stg.main_db_id, stg.value, 
               stg.{timestamp_column_name} AS timestamp
        FROM public.{stg_table_name} stg
        WHERE stg.h_data_type_sk = :data_type_id 
          AND stg.hash_sk IS NOT NULL 
          AND stg.value IS NOT NULL
          {file_filter}
        ORDER BY stg.hash_sk
    """)
    
    log.info(f"📥 ЭТАП 1: Чтение всех данных (file={data_file_id}, is_hist={is_hist_data}) из {stg_table_name}...")
    
    all_records: List[dict] = []
    all_day_keys: List[date] = []
    error_count = 0
    
    def _normalize_ts(ts_val: Any) -> datetime:
        if ts_val is None or (isinstance(ts_val, float) and pd.isna(ts_val)):
            return datetime(1970, 1, 1, tzinfo=timezone.utc)
        if hasattr(ts_val, 'total_seconds'):
            return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=ts_val.total_seconds())
        if isinstance(ts_val, datetime):
            return ts_val if ts_val.tzinfo else ts_val.replace(tzinfo=timezone.utc)
        try:
            ts_str = str(ts_val).replace(" ", "T")
            if "." not in ts_str and "T" in ts_str:
                ts_str += ".000000"
            dt = datetime.fromisoformat(ts_str)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return datetime(1970, 1, 1, tzinfo=timezone.utc)

    def _cast_value(val, data_type_str, is_nullable):
        t = data_type_str.lower()
        if val is None or (isinstance(val, float) and pd.isna(val)):
            if is_nullable: return None
            if 'interval' in t: return ""
            if 'datetime' in t or 'timestamp' in t or t == 'date': return datetime(1970, 1, 1, timezone.utc)
            if 'array' in t: return []
            if 'bool' in t: return False
            if 'float' in t or 'double' in t or 'decimal' in t: return 0.0
            if 'int' in t: return 0
            return ""
        if 'interval' in t: return str(val)
        if 'datetime' in t or 'timestamp' in t or t == 'date': 
            return datetime(1970, 1, 1, tzinfo=timezone.utc)
        if 'array' in t:
            if isinstance(val, (list, tuple, np.ndarray)): return list(val)
            if isinstance(val, str): return []
            return [val]
        if 'bool' in t:
            if isinstance(val, str): return val.lower() in ('1', 'true', 't', 'yes')
            return bool(val)
        if 'int' in t:
            try: return int(val)
            except (ValueError, TypeError): return 0 if not is_nullable else None
        if 'float' in t or 'double' in t or 'decimal' in t:
            try: return float(val)
            except (ValueError, TypeError): return 0.0 if not is_nullable else None
        if isinstance(val, (bytes, bytearray)): return val.decode('utf-8', errors='replace')
        if isinstance(val, timedelta): return str(val)
        if isinstance(val, (datetime, date)): return val.isoformat()
        return str(val)

    with dwh_engine.begin() as conn:
        result = conn.execution_options(stream_results=True, yield_per=read_chunk_size).execute(sql_fetch, sql_params)
        for row in result:
            hash_sk, h_obj_prop_sk, load_dttm, data_file_id_row, data_src_id, main_db_id, bytea_val, timestamp = row
            try:
                deserialized_val = deserialize_value(bytea_val, data_type_id)
            except Exception:
                error_count += 1
                continue
            
            norm_ts = _normalize_ts(timestamp)
            norm_ld = _normalize_ts(load_dttm)
            
            all_records.append({
                'data_record_sk': ensure_uuid_string(hash_sk),
                'h_object_property_sk': ensure_uuid_string(h_obj_prop_sk),
                'data_file_id': ensure_uuid_string(data_file_id_row),
                'data_source_id': ensure_uuid_string(data_src_id),
                'main_db_id': ensure_uuid_string(main_db_id) if main_db_id else None,
                'load_dttm': norm_ld,
                'value': _cast_value(deserialized_val, data_type, value_is_nullable),
                'timestamp': norm_ts,
            })
            all_day_keys.append(norm_ts.date())
    
    total_records = len(all_records)
    log.info(f"✅ ЭТАП 1 ЗАВЕРШЕН: {total_records:,} записей в памяти (ошибок: {error_count})")
    
    if total_records == 0:
        ch_client.close()
        return {'stg_table_name': stg_table_name, 'target_table_name': target_table_name,
                'records_processed': 0, 'errors': error_count, 'manifests_created': 0}
    
    # ========================================================================
    # ЭТАП 2: Раскидывание в ClickHouse и S3
    # ========================================================================
    log.info(f"📤 ЭТАП 2: Раскидывание {total_records:,} записей "
             f"(CH batch={ch_batch_size:,}, S3 batch={s3_batch_size:,})...")
    
    base_seq = table_metadata.get("last-sequence-number", 0) + 1
    base_snap_id = abs(hash(f"{table_path}_{datetime.now(timezone.utc).isoformat()}_{uuid.uuid4().hex}"))
    
    all_manifests = []
    total_s3_rows = 0
    
    def _flush_ch_batch(records: List[dict]):
        if not records: return
        df = pd.DataFrame(records)
        for col in ['data_record_sk', 'h_object_property_sk', 'data_file_id', 'data_source_id', 'main_db_id']:
            if col in df.columns: df[col] = df[col].astype(str)
        
        # 🔥 ИСПРАВЛЕНИЕ: Конвертируем timestamp в UTC, но load_dttm оставляем как есть
        if 'timestamp' in df.columns: 
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        
        # 🔥 НЕ конвертируем load_dttm в UTC - ClickHouse сам хранит в UTC
        if 'load_dttm' in df.columns:
            # Просто убеждаемся что это datetime, но не меняем timezone
            df['load_dttm'] = pd.to_datetime(df['load_dttm'])
            # Если есть timezone - конвертируем в UTC
            if df['load_dttm'].dt.tz is not None:
                df['load_dttm'] = df['load_dttm'].dt.tz_convert('UTC').dt.tz_localize(None)
        
        cols_order = ['data_record_sk', 'h_object_property_sk', 'data_file_id', 'data_source_id', 'main_db_id',
                    'timestamp', 'value', 'load_dttm']
        for c in cols_order:
            if c not in df.columns: df[c] = None
        ch_client.insert_df(target_table_name, df[cols_order])
    
    def _flush_s3_batch(s3_partition_buffers: Dict[Tuple[str, date], Dict[str, list]]) -> List:
        manifests = []
        for (fid, day), buf in list(s3_partition_buffers.items()):
            if not buf['data_record_sk']: continue
            m = _flush_common_partition_to_s3(
                day, buf, s3_client, file_io, bucket, data_prefix, metadata_prefix,
                partition_spec, True, iceberg_type, value_is_nullable, pa_schema,
                table_metadata, fid, base_snap_id, base_seq
            )
            if m: manifests.append(m)
        gc.collect()
        return manifests
    
    ch_buf: List[dict] = []
    s3_partition_buffers: Dict[Tuple[str, date], Dict[str, list]] = {}
    s3_buffered_rows = 0
    
    for rec, day_key in zip(all_records, all_day_keys):
        ch_buf.append(rec)
        if len(ch_buf) >= ch_batch_size:
            _flush_ch_batch(ch_buf)
            log.info(f"  ✅ ClickHouse: {len(ch_buf):,} строк записано")
            ch_buf = []
        
        key = (rec['data_file_id'], day_key)
        if key not in s3_partition_buffers:
            s3_partition_buffers[key] = {k: [] for k in rec.keys()}
        buf = s3_partition_buffers[key]
        for k, v in rec.items():
            buf[k].append(v.isoformat() if (k == 'load_dttm' and isinstance(v, datetime)) else v)
        s3_buffered_rows += 1
        
        if s3_buffered_rows >= s3_batch_size:
            manifests = _flush_s3_batch(s3_partition_buffers)
            all_manifests.extend(manifests)
            total_s3_rows += s3_buffered_rows
            log.info(f"  ✅ S3: {s3_buffered_rows:,} строк → {len(manifests)} файлов")
            s3_partition_buffers = {}
            s3_buffered_rows = 0
    
    if ch_buf:
        _flush_ch_batch(ch_buf)
        log.info(f"  ✅ ClickHouse: финальный батч {len(ch_buf):,} строк")
    if s3_partition_buffers:
        manifests = _flush_s3_batch(s3_partition_buffers)
        all_manifests.extend(manifests)
        total_s3_rows += s3_buffered_rows
        log.info(f"  ✅ S3: финальный батч {s3_buffered_rows:,} строк → {len(manifests)} файлов")
    
    if all_manifests:
        log.info(f"📝 Commit snapshot: {len(all_manifests)} манифестов, {total_s3_rows:,} строк...")
        ml = _write_manifest_list(s3_client, file_io, bucket, metadata_prefix,
                                  all_manifests, table_metadata, partition_spec, base_snap_id, base_seq)
        _commit_snapshot(s3_client, bucket, table_path, metadata_prefix, table_metadata,
                         ml, all_manifests, total_s3_rows, base_snap_id, base_seq)
    
    ch_client.close()
    
    return {
        'stg_table_name': stg_table_name,
        'target_table_name': target_table_name,
        'records_processed': total_records,
        'errors': error_count,
        'manifests_created': len(all_manifests),
        's3_rows': total_s3_rows,
    }


def _insert_batch_with_retry(
    dbapi_conn,
    sql_template: str,
    values_list: list,
    batch_size: int = 50_000,
    max_retries: int = 3
):
    """
    Вставка батча с retry при deadlock.
    """
    for attempt in range(max_retries):
        try:
            cursor = dbapi_conn.cursor()
            execute_values(
                cursor, 
                sql_template, 
                values_list, 
                page_size=batch_size
            )
            cursor.close()
            return  # Успех
            
        except OperationalError as e:
            if isinstance(e.orig, DeadlockDetected) and attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                log.warning(
                    f"⚠️ Deadlock detected, retry {attempt + 1}/{max_retries} "
                    f"after {wait_time}s"
                )
                time.sleep(wait_time)
            else:
                raise





def merge_creyt_data(
    stg_table_name: str,
    target_table_name: str,
    main_db_name: str,
    is_hist_data: bool = None,
    data_file_id: str = None,
    data_type_id: str = "1b134c95-43d8-2793-ba0f-b6d0ba22c624",
    # 🔥 ИСПРАВЛЕННЫЕ ПАРАМЕТРЫ (было 50M — OOM)
    ch_batch_size: int = 2_000_000,    # строк за insert_df в CH
    s3_batch_size: int = 10_000_000,   # 🔥 СНИЖЕНО: было 50M → OOM. 10M ≈ 5 ГБ RAM в буфере
    read_chunk_size: int = 10,         # BLOB'ов за раз (не 100, иначе память)
) -> dict:
    """
    🔥 STREAMING merge Creyt (без загрузки всех точек в память).
    Читает BLOB'ы по read_chunk_size, декодирует и СРАЗУ флешит в CH и S3.
    Память ограничена O(ch_batch_size + s3_batch_size) ~ 5-8 ГБ.
    """
    import gc, uuid, boto3, pandas as pd, numpy as np, clickhouse_connect
    from botocore.config import Config
    from datetime import datetime, timezone, date, timedelta
    from typing import Dict, Tuple, Any, List
    from sqlalchemy import text
    from airflow.hooks.base import BaseHook
    import pyarrow as pa
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import DayTransform, IdentityTransform
    from pyiceberg.types import DoubleType
    from pyiceberg.io.pyarrow import PyArrowFileIO

    if data_file_id is not None and type(data_file_id).__name__ in ('XComArg', 'PlainXComArg'):
        raise TypeError("❌ Передан XComArg вместо строки!")

    if is_hist_data is None:
        is_hist_data = '_hist' in stg_table_name.lower()

    dwh_engine = get_dwh_engine('cloudberry_test_dwh')
    timestamp_column_name = 'timestamp' if is_hist_data else 'date'

    conn_config = BaseHook.get_connection('clickhouse_conn')
    ch_client = clickhouse_connect.get_client(
        host=conn_config.host, port=8123,
        database=conn_config.schema or 'default',
        username=conn_config.login or 'default',
        password=conn_config.password or '',
        compress=True, query_limit=0,
    )

    s3_conn = BaseHook.get_connection('minio_conn')
    s3_endpoint = s3_conn.extra_dejson.get("host", "http://minio:9000")
    if not s3_endpoint.startswith("http"):
        s3_endpoint = f"http://{s3_endpoint}"
    bucket = s3_conn.extra_dejson.get("bucket", "iceberg-warehouse")
    region = s3_conn.extra_dejson.get("region", "us-east-1")

    s3_client = boto3.client(
        "s3", endpoint_url=s3_endpoint,
        aws_access_key_id=s3_conn.login, aws_secret_access_key=s3_conn.password,
        region_name=region, config=Config(signature_version="s3v4", s3={"addressing_style": "path"})
    )
    file_io = PyArrowFileIO(properties={
        "s3.endpoint": s3_endpoint, "s3.access-key-id": s3_conn.login,
        "s3.secret-access-key": s3_conn.password, "s3.region": region,
        "s3.path-style-access": "true"
    })

    hist_path = 'history-data' if is_hist_data else 'non-history-data'
    table_path = f"{main_db_name}/{hist_path}/{target_table_name}"
    metadata_prefix = f"{table_path}/metadata"
    data_prefix = f"{table_path}/data"

    iceberg_type = DoubleType()
    pa_type = pa.float64()
    value_is_nullable = False

    table_metadata = _init_or_load_table_metadata(
        s3_client, bucket, table_path, metadata_prefix,
        iceberg_type, pa_type, partition_by_source=True
    )

    partition_spec = PartitionSpec(
        PartitionField(source_id=7, field_id=1000, transform=DayTransform(), name="dt_day"),
        PartitionField(source_id=3, field_id=1001, transform=IdentityTransform(), name="source_id"),
        spec_id=0
    )

    pa_schema = pa.schema([
        pa.field("data_record_sk", pa.string(), nullable=False),
        pa.field("h_object_property_sk", pa.string(), nullable=True),
        pa.field("data_file_id", pa.string(), nullable=False),
        pa.field("data_source_id", pa.string(), nullable=False),
        pa.field("main_db_id", pa.string(), nullable=False),
        pa.field("load_dttm", pa.string(), nullable=True),
        pa.field("value", pa.float64(), nullable=False),
        pa.field("timestamp", pa.timestamp("us"), nullable=True),
    ])

    def _normalize_ts(ts_val: Any) -> datetime:
        if ts_val is None or (isinstance(ts_val, float) and pd.isna(ts_val)):
            return datetime(1970, 1, 1, tzinfo=timezone.utc)
        if hasattr(ts_val, 'total_seconds'):
            return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=ts_val.total_seconds())
        if isinstance(ts_val, datetime):
            return ts_val if ts_val.tzinfo else ts_val.replace(tzinfo=timezone.utc)
        try:
            ts_str = str(ts_val).replace(" ", "T")
            if "." not in ts_str and "T" in ts_str:
                ts_str += ".000000"
            dt = datetime.fromisoformat(ts_str)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return datetime(1970, 1, 1, tzinfo=timezone.utc)

    # SQL с фильтром по data_file_id для изоляции
    file_filter = ""
    sql_params = {"data_type_id": data_type_id}
    if data_file_id:
        file_filter = "AND stg.data_file_id = :data_file_id"
        sql_params["data_file_id"] = data_file_id

    sql_fetch = text(f"""
        SELECT stg.hash_sk, stg.h_object_property_sk, stg.load_dttm,
               stg.data_file_id, stg.data_source_id, stg.main_db_id,
               stg.value, stg.{timestamp_column_name} AS base_timestamp
        FROM public.{stg_table_name} stg
        WHERE stg.h_data_type_sk = :data_type_id
          AND stg.hash_sk IS NOT NULL
          AND stg.value IS NOT NULL
          {file_filter}
        ORDER BY stg.hash_sk
    """)

    log.info(f"📥 STREAMING Creyt (file={data_file_id}): "
             f"ch_batch={ch_batch_size:,}, s3_batch={s3_batch_size:,}, read_chunk={read_chunk_size}")

    # 🔥 Фиксируем snapshot ОДИН раз на весь прогон (как раньше)
    base_seq = table_metadata.get("last-sequence-number", 0) + 1
    base_snap_id = abs(hash(f"{table_path}_{datetime.now(timezone.utc).isoformat()}_{uuid.uuid4().hex}"))
    all_manifests = []
    total_s3_rows = 0
    total_source_records = 0
    total_points = 0
    error_count = 0

    # 🔥 Буферы (ограничены s3_batch_size + ch_batch_size)
    ch_buf: List[dict] = []
    s3_partition_buffers: Dict[Tuple[str, date], Dict[str, list]] = {}
    s3_buffered_rows = 0

    def _flush_ch():
        if not ch_buf: return
        df = pd.DataFrame(ch_buf)
        for col in ['data_record_sk', 'h_object_property_sk', 'data_file_id', 'data_source_id', 'main_db_id']:
            if col in df.columns: df[col] = df[col].astype(str)
        if 'timestamp' in df.columns: df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        if 'load_dttm' in df.columns: df['load_dttm'] = pd.to_datetime(df['load_dttm'], utc=True)
        if 'value' in df.columns: df['value'] = df['value'].astype('float64')
        cols_order = ['data_record_sk', 'h_object_property_sk', 'data_file_id', 'data_source_id',
                      'main_db_id', 'timestamp', 'value', 'load_dttm']
        for c in cols_order:
            if c not in df.columns: df[c] = None
        ch_client.insert_df(target_table_name, df[cols_order])
        log.info(f"  ✅ ClickHouse: {len(ch_buf):,} точек")

    def _flush_s3():
        nonlocal s3_partition_buffers, s3_buffered_rows, all_manifests, total_s3_rows
        if not s3_partition_buffers: return
        for (fid, day), buf in list(s3_partition_buffers.items()):
            if not buf['data_record_sk']: continue
            m = _flush_common_partition_to_s3(
                day, buf, s3_client, file_io, bucket, data_prefix, metadata_prefix,
                partition_spec, True, iceberg_type, value_is_nullable, pa_schema,
                table_metadata, fid, base_snap_id, base_seq
            )
            if m: all_manifests.append(m)
        total_s3_rows += s3_buffered_rows
        log.info(f"  ✅ S3: {s3_buffered_rows:,} точек → {len([m for m in all_manifests])} файлов всего")
        s3_partition_buffers = {}
        s3_buffered_rows = 0
        gc.collect()
        try:
            pa.default_memory_pool().release_unused()
        except Exception:
            pass

    try:
        with dwh_engine.begin() as conn:
            result = conn.execution_options(stream_results=True, yield_per=read_chunk_size).execute(
                sql_fetch, sql_params
            )

            # 🔥 STREAMING: обрабатываем каждую строку по очереди, НЕ копим в all_points
            for row in result:
                hash_sk, h_obj_prop_sk, load_dttm, df_id, data_src_id, main_db_id, bytea_val, base_timestamp = row
                total_source_records += 1

                try:
                    blob_bytes = bytes(bytea_val) if hasattr(bytea_val, 'tobytes') else bytea_val
                    creyt = deserialize_creyt_blob(blob_bytes)
                    ts_arr, val_arr = decode_creyt_timeseries_numpy(
                        raw_values=creyt.raw_values,
                        calibration_factor_a=creyt.calibration_factor_a,
                        calibration_factor_b=creyt.calibration_factor_b,
                        min_raw=creyt.scale_min_raw, max_raw=creyt.scale_max_raw,
                        min_eu=creyt.scale_min_eu, max_eu=creyt.scale_max_eu,
                        sample_rate=creyt.sample_rate,
                        first_timestamp=base_timestamp
                    )
                except Exception as e:
                    error_count += 1
                    log.warning(f"⚠️ Failed decode hash_sk={hash_sk}: {e}")
                    continue

                if len(ts_arr) == 0:
                    continue

                rec_sk_str = ensure_uuid_string(hash_sk)
                h_obj_prop_str = ensure_uuid_string(h_obj_prop_sk)
                file_id_str = ensure_uuid_string(df_id)
                src_id_str = ensure_uuid_string(data_src_id)
                main_id_str = ensure_uuid_string(main_db_id)
                norm_load_dttm = _normalize_ts(load_dttm)

                ts_series = pd.to_datetime(ts_arr, utc=True)
                ts_list = ts_series.to_pydatetime()
                val_list = val_arr.tolist()

                # 🔥 СРАЗУ раскладываем точки в CH и S3 буферы (без all_points)
                for ts, val in zip(ts_list, val_list):
                    day_key = ts.date()
                    float_val = float(val) if val is not None else 0.0

                    rec = {
                        'data_record_sk': rec_sk_str,
                        'h_object_property_sk': h_obj_prop_str,
                        'data_file_id': file_id_str,
                        'data_source_id': src_id_str,
                        'main_db_id': main_id_str,
                        'load_dttm': norm_load_dttm,
                        'value': float_val,
                        'timestamp': ts,
                    }

                    # CH буфер
                    ch_buf.append(rec)
                    if len(ch_buf) >= ch_batch_size:
                        _flush_ch()
                        ch_buf.clear()

                    # S3 буфер
                    key = (file_id_str, day_key)
                    if key not in s3_partition_buffers:
                        s3_partition_buffers[key] = {k: [] for k in rec.keys()}
                    buf = s3_partition_buffers[key]
                    for k, v in rec.items():
                        buf[k].append(v.isoformat() if (k == 'load_dttm' and isinstance(v, datetime)) else v)
                    s3_buffered_rows += 1
                    total_points += 1

                    if s3_buffered_rows >= s3_batch_size:
                        _flush_s3()

                # 🔥 Освобождаем память после каждого BLOB'а
                del blob_bytes, creyt, ts_arr, val_arr, ts_list, val_list, ts_series
                if total_source_records % 100 == 0:
                    gc.collect()

                if total_source_records % 500 == 0:
                    log.info(f"📊 Creyt progress: {total_points:,} точек из {total_source_records} BLOB'ов")

        # Финальные флеши
        if ch_buf:
            _flush_ch()
        if s3_partition_buffers:
            _flush_s3()

        # ОДИН commit snapshot на весь прогон
        # 🔥 КОММИТ С МЕЖПРОЦЕССНОЙ БЛОКИРОВКОЙ (безопасно при параллельных data_file_id)
        if all_manifests:
            import hashlib
            # Стабильный ключ блокировки (одинаковый на всех воркерах, в отличие от hash())
            lock_key = int(hashlib.md5(table_path.encode()).hexdigest()[:15], 16)

            with dwh_engine.begin() as lock_conn:
                # 🔒 Ждём, пока другие запуски не закоммитят эту же таблицу
                lock_conn.execute(text("SELECT pg_advisory_xact_lock(:k)"), {"k": lock_key})
                log.info(f"🔒 Advisory lock получен для {table_path}")

                # 🔥 ВНУТРИ блокировки перечитываем АКТУАЛЬНУЮ версию метаданных,
                # чтобы не затереть коммиты, ушедшие пока мы писали Parquet
                fresh_metadata = _init_or_load_table_metadata(
                    s3_client, bucket, table_path, metadata_prefix,
                    iceberg_type, pa_type, True
                )
                fresh_seq = fresh_metadata.get("last-sequence-number", 0) + 1

                ml = _write_manifest_list(
                    s3_client, file_io, bucket, metadata_prefix,
                    all_manifests, fresh_metadata, partition_spec, base_snap_id, fresh_seq
                )
                _commit_snapshot(
                    s3_client, bucket, table_path, metadata_prefix, fresh_metadata,
                    ml, all_manifests, total_s3_rows, base_snap_id, fresh_seq
                )
                log.info(f"📝 Snapshot закоммичен (seq={fresh_seq}), lock освобождён")

    except Exception as e:
        log.error(f"❌ Creyt streaming failed: {e}", exc_info=True)
        raise
    finally:
        ch_client.close()

    log.info(f"✅ Creyt completed: {total_points:,} точек из {total_source_records} BLOB'ов, errors={error_count}")

    return {
        'data_file_id': data_file_id,
        'stg_table_name': stg_table_name,
        'target_table_name': target_table_name,
        'records_processed': total_source_records,
        'points_inserted': total_points,
        'errors': error_count,
        'manifests_created': len(all_manifests),
        's3_rows': total_s3_rows,
    }




def _insert_with_retry(conn, sql_insert, params_list: list, batch_number: int, max_retries: int = 3):
    """
    Вставка батча с retry при deadlock.
    Вынесена в отдельную функцию для переиспользования.
    """
    for attempt in range(max_retries):
        try:
            conn.execute(sql_insert, params_list)
            return  # Успех
        except OperationalError as e:
            if isinstance(e.orig, DeadlockDetected) and attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                log.warning(
                    f"⚠️ Deadlock detected in batch {batch_number}, "
                    f"retry {attempt + 1}/{max_retries} after {wait_time}s"
                )
                time.sleep(wait_time)
            else:
                raise
            
            
def merge_lcard_data(
    stg_table_name: str,
    target_table_name: str,  # Имя таблицы в ClickHouse и Iceberg
    main_db_name: str,
    is_hist_data: bool = None,     # 🔥 Автоопределяется по имени таблицы
    data_file_id: str = None,      # 🔥 Опциональный для изоляции
    data_type_id: str = "548dc301-340b-4257-17a3-1f8b350af42a",
    # =====================================================================
    # 🔥 ПАРАМЕТРЫ БАТЧИНГА (вынесены в сигнатуру)
    # =====================================================================
    ch_batch_size: int = 5_000_000,    # Строк за один insert_df в CH
    s3_batch_size: int = 50_000_000,   # Строк на один Parquet-файл (~125 МБ)
    read_chunk_size: int = 100,        # BLOB'ов за раз (yield_per)
) -> dict:
    """
    🔥 STREAMING merge LCard (без загрузки всех точек в память).
    Читает BLOB'ы по read_chunk_size, декодирует на лету и СРАЗУ флешит в CH и S3.
    Память ограничена O(ch_batch_size + s3_batch_size) ~ 5-8 ГБ.
    Использует column-wise extend для ускорения в 3-5 раз.
    """
    import gc, uuid, boto3, pandas as pd, numpy as np, clickhouse_connect
    from botocore.config import Config
    from datetime import datetime, timezone, date, timedelta
    from typing import Dict, Tuple, Any, List
    from sqlalchemy import text
    from airflow.hooks.base import BaseHook
    import pyarrow as pa
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import DayTransform, IdentityTransform
    from pyiceberg.types import DoubleType
    from pyiceberg.io.pyarrow import PyArrowFileIO

    if data_file_id is not None and type(data_file_id).__name__ in ('XComArg', 'PlainXComArg'):
        raise TypeError("❌ Передан XComArg вместо строки!")

    # 🔥 АВТООПРЕДЕЛЕНИЕ is_hist_data
    if is_hist_data is None:
        is_hist_data = '_hist' in stg_table_name.lower()

    dwh_engine = get_dwh_engine('cloudberry_test_dwh')
    timestamp_column_name = 'timestamp' if is_hist_data else 'date'

    # =====================================================================
    # 1. ClickHouse Client Setup
    # =====================================================================
    conn_config = BaseHook.get_connection('clickhouse_conn')
    ch_client = clickhouse_connect.get_client(
        host=conn_config.host, port=8123,
        database=conn_config.schema or 'default',
        username=conn_config.login or 'default',
        password=conn_config.password or '',
        compress=True, query_limit=0,
    )

    # =====================================================================
    # 2. S3 / Iceberg Setup
    # =====================================================================
    s3_conn = BaseHook.get_connection('minio_conn')
    s3_endpoint = s3_conn.extra_dejson.get("host", "http://minio:9000")
    if not s3_endpoint.startswith("http"):
        s3_endpoint = f"http://{s3_endpoint}"
    bucket = s3_conn.extra_dejson.get("bucket", "iceberg-warehouse")
    region = s3_conn.extra_dejson.get("region", "us-east-1")

    s3_client = boto3.client(
        "s3", endpoint_url=s3_endpoint,
        aws_access_key_id=s3_conn.login, aws_secret_access_key=s3_conn.password,
        region_name=region, config=Config(signature_version="s3v4", s3={"addressing_style": "path"})
    )
    file_io = PyArrowFileIO(properties={
        "s3.endpoint": s3_endpoint, "s3.access-key-id": s3_conn.login,
        "s3.secret-access-key": s3_conn.password, "s3.region": region,
        "s3.path-style-access": "true"
    })

    hist_path = 'history-data' if is_hist_data else 'non-history-data'
    table_path = f"{main_db_name}/{hist_path}/{target_table_name}"
    metadata_prefix = f"{table_path}/metadata"
    data_prefix = f"{table_path}/data"

    iceberg_type = DoubleType()
    pa_type = pa.float64()
    value_is_nullable = False

    table_metadata = _init_or_load_table_metadata(
        s3_client, bucket, table_path, metadata_prefix,
        iceberg_type, pa_type, partition_by_source=True
    )

    partition_spec = PartitionSpec(
        PartitionField(source_id=7, field_id=1000, transform=DayTransform(), name="dt_day"),
        PartitionField(source_id=3, field_id=1001, transform=IdentityTransform(), name="source_id"),
        spec_id=0
    )

    pa_schema = pa.schema([
        pa.field("data_record_sk", pa.string(), nullable=False),
        pa.field("h_object_property_sk", pa.string(), nullable=True),
        pa.field("data_file_id", pa.string(), nullable=False),
        pa.field("data_source_id", pa.string(), nullable=False),
        pa.field("main_db_id", pa.string(), nullable=False),
        pa.field("load_dttm", pa.string(), nullable=True),
        pa.field("value", pa.float64(), nullable=False),
        pa.field("timestamp", pa.timestamp("us"), nullable=True),
    ])

    def _normalize_ts(ts_val: Any) -> datetime:
        if ts_val is None or (isinstance(ts_val, float) and pd.isna(ts_val)):
            return datetime(1970, 1, 1, tzinfo=timezone.utc)
        if hasattr(ts_val, 'total_seconds'):
            return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=ts_val.total_seconds())
        if isinstance(ts_val, datetime):
            return ts_val if ts_val.tzinfo else ts_val.replace(tzinfo=timezone.utc)
        try:
            ts_str = str(ts_val).replace(" ", "T")
            if "." not in ts_str and "T" in ts_str:
                ts_str += ".000000"
            dt = datetime.fromisoformat(ts_str)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return datetime(1970, 1, 1, tzinfo=timezone.utc)

    # =====================================================================
    # 3. SQL Query (с фильтром по data_file_id для изоляции)
    # =====================================================================
    file_filter = ""
    sql_params = {"data_type_id": data_type_id}
    if data_file_id:
        file_filter = "AND stg.data_file_id = :data_file_id"
        sql_params["data_file_id"] = data_file_id

    sql_fetch = text(f"""
        SELECT
            stg.hash_sk, stg.h_object_property_sk, stg.load_dttm,
            stg.data_file_id, stg.data_source_id, stg.main_db_id,
            stg.value, stg.{timestamp_column_name} AS base_timestamp
        FROM public.{stg_table_name} stg
        WHERE stg.h_data_type_sk = :data_type_id
          AND stg.hash_sk IS NOT NULL
          AND stg.value IS NOT NULL
          {file_filter}
        ORDER BY stg.hash_sk
    """)

    log.info(f"📥 STREAMING LCard (file={data_file_id}): "
             f"ch_batch={ch_batch_size:,}, s3_batch={s3_batch_size:,}, read_chunk={read_chunk_size}")

    # 🔥 Фиксируем snapshot ОДИН раз на весь прогон
    base_seq = table_metadata.get("last-sequence-number", 0) + 1
    base_snap_id = abs(hash(f"{table_path}_{datetime.now(timezone.utc).isoformat()}_{uuid.uuid4().hex}"))
    all_manifests = []
    total_s3_rows = 0
    total_source_records = 0
    total_points = 0
    error_count = 0

    # 🔥 Column-wise буферы (dict-of-lists) вместо list-of-dicts
    ch_buf = {
        'data_record_sk': [], 'h_object_property_sk': [], 'data_file_id': [],
        'data_source_id': [], 'main_db_id': [], 'load_dttm': [], 'value': [], 'timestamp': []
    }
    s3_partition_buffers: Dict[Tuple[str, date], Dict[str, list]] = {}
    s3_buffered_rows = 0

    def _flush_ch():
        if not ch_buf['value']: return
        df = pd.DataFrame(ch_buf)
        for col in ['data_record_sk', 'h_object_property_sk', 'data_file_id', 'data_source_id', 'main_db_id']:
            if col in df.columns: df[col] = df[col].astype(str)
        if 'timestamp' in df.columns: df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        if 'load_dttm' in df.columns: df['load_dttm'] = pd.to_datetime(df['load_dttm'], utc=True)
        if 'value' in df.columns: df['value'] = df['value'].astype('float64')
        cols_order = ['data_record_sk', 'h_object_property_sk', 'data_file_id', 'data_source_id',
                      'main_db_id', 'timestamp', 'value', 'load_dttm']
        for c in cols_order:
            if c not in df.columns: df[c] = None
        ch_client.insert_df(target_table_name, df[cols_order])
        log.info(f"  ✅ ClickHouse: {len(ch_buf['value']):,} точек")
        # Очищаем буфер
        for k in ch_buf:
            ch_buf[k] = []

    def _flush_s3():
        nonlocal s3_partition_buffers, s3_buffered_rows, all_manifests, total_s3_rows
        if not s3_partition_buffers: return
        for (fid, day), buf in list(s3_partition_buffers.items()):
            if not buf['data_record_sk']: continue
            m = _flush_common_partition_to_s3(
                day, buf, s3_client, file_io, bucket, data_prefix, metadata_prefix,
                partition_spec, True, iceberg_type, value_is_nullable, pa_schema,
                table_metadata, fid, base_snap_id, base_seq
            )
            if m: all_manifests.append(m)
        total_s3_rows += s3_buffered_rows
        log.info(f"  ✅ S3: {s3_buffered_rows:,} точек → {len(all_manifests)} файлов всего")
        s3_partition_buffers = {}
        s3_buffered_rows = 0
        gc.collect()
        try:
            pa.default_memory_pool().release_unused()
        except Exception:
            pass

    # =====================================================================
    # 4. Main Streaming Loop
    # =====================================================================
    try:
        with dwh_engine.begin() as conn:
            result = conn.execution_options(stream_results=True, yield_per=read_chunk_size).execute(
                sql_fetch, sql_params
            )

            for row in result:
                hash_sk, h_obj_prop_sk, load_dttm, df_id, data_src_id, main_db_id, bytea_val, base_timestamp = row
                total_source_records += 1

                try:
                    # 1. Десериализация BLOB
                    blob_bytes = bytes(bytea_val) if hasattr(bytea_val, 'tobytes') else bytea_val
                    lcard = deserialize_lcard_blob(blob_bytes)

                    # 2. Векторное декодирование (NumPy)
                    if isinstance(lcard.raw_values, (bytes, bytearray)):
                        raw_arr = np.frombuffer(lcard.raw_values, dtype=np.int16).astype(np.float64)
                    else:
                        raw_arr = np.asarray(lcard.raw_values, dtype=np.float64)

                    n_points = len(raw_arr)
                    if n_points == 0:
                        continue

                    # Применение калибровки (scale) или шага (step)
                    is_scale = getattr(lcard, 'is_scale', False)
                    if is_scale and (lcard.scale_max_raw - lcard.scale_min_raw) != 0:
                        val_arr = (raw_arr - lcard.scale_min_raw) / (lcard.scale_max_raw - lcard.scale_min_raw) * \
                                  (lcard.scale_max_eu - lcard.scale_min_eu) + lcard.scale_min_eu
                    else:
                        val_arr = raw_arr * getattr(lcard, 'step', 1.0)

                except Exception as e:
                    error_count += 1
                    log.warning(f"⚠️ Failed to deserialize/decode LCard hash_sk={hash_sk}: {e}")
                    continue

                # 3. Подготовка общих полей (один раз на BLOB)
                rec_sk_str = ensure_uuid_string(hash_sk)
                h_obj_prop_str = ensure_uuid_string(h_obj_prop_sk)
                file_id_str = ensure_uuid_string(df_id)
                src_id_str = ensure_uuid_string(data_src_id)
                main_id_str = ensure_uuid_string(main_db_id) if main_db_id else ""
                norm_load_dttm = _normalize_ts(load_dttm)
                norm_base_ts = _normalize_ts(base_timestamp)

                # 4. Генерация временных меток
                sample_rate = float(getattr(lcard, 'sample_rate', 0) or 0)
                if sample_rate > 0:
                    base_pd_ts = pd.Timestamp(norm_base_ts)
                    if base_pd_ts.tzinfo is None:
                        base_pd_ts = base_pd_ts.tz_localize('UTC')
                    else:
                        base_pd_ts = base_pd_ts.tz_convert('UTC')
                    ts_deltas = pd.to_timedelta(np.arange(n_points) / sample_rate, unit='s')
                    ts_series = base_pd_ts + ts_deltas
                    ts_list = ts_series.to_pydatetime()
                else:
                    ts_list = [norm_base_ts] * n_points

                val_list = val_arr.tolist()

                # 🔥 5. COLUMN-WISE EXTEND — ускорение в 3-5 раз vs pototchechnogo append
                # CH буфер
                ch_buf['data_record_sk'].extend([rec_sk_str] * n_points)
                ch_buf['h_object_property_sk'].extend([h_obj_prop_str] * n_points)
                ch_buf['data_file_id'].extend([file_id_str] * n_points)
                ch_buf['data_source_id'].extend([src_id_str] * n_points)
                ch_buf['main_db_id'].extend([main_id_str] * n_points)
                ch_buf['load_dttm'].extend([norm_load_dttm] * n_points)
                ch_buf['timestamp'].extend(ts_list)
                ch_buf['value'].extend(val_list)

                if len(ch_buf['value']) >= ch_batch_size:
                    _flush_ch()

                # S3 буфер (группируем по первому дню — основное большинство точек в одном дне)
                # Для длинных BLOB'ов точки могут пересекать день — распределяем корректно
                day_keys = [ts.date() for ts in ts_list]
                unique_days = set(day_keys)

                for day in unique_days:
                    # Индексы точек этого дня
                    day_indices = [i for i, dk in enumerate(day_keys) if dk == day]
                    n_day = len(day_indices)

                    key = (file_id_str, day)
                    if key not in s3_partition_buffers:
                        s3_partition_buffers[key] = {
                            'data_record_sk': [], 'h_object_property_sk': [], 'data_file_id': [],
                            'data_source_id': [], 'main_db_id': [], 'load_dttm': [], 'timestamp': [], 'value': []
                        }
                    buf = s3_partition_buffers[key]
                    buf['data_record_sk'].extend([rec_sk_str] * n_day)
                    buf['h_object_property_sk'].extend([h_obj_prop_str] * n_day)
                    buf['data_file_id'].extend([file_id_str] * n_day)
                    buf['data_source_id'].extend([src_id_str] * n_day)
                    buf['main_db_id'].extend([main_id_str] * n_day)
                    buf['load_dttm'].extend([norm_load_dttm.isoformat()] * n_day)
                    buf['timestamp'].extend([ts_list[i] for i in day_indices])
                    buf['value'].extend([val_list[i] for i in day_indices])
                    s3_buffered_rows += n_day

                total_points += n_points

                if s3_buffered_rows >= s3_batch_size:
                    _flush_s3()

                # 🔥 Освобождаем память после каждого BLOB'а
                del blob_bytes, lcard, raw_arr, val_arr, ts_list, val_list
                if total_source_records % 100 == 0:
                    gc.collect()

                if total_source_records % 500 == 0:
                    log.info(f"📊 LCard progress: {total_points:,} точек из {total_source_records} BLOB'ов")

        # Финальные флеши
        if ch_buf['value']:
            _flush_ch()
        if s3_partition_buffers:
            _flush_s3()

        # ОДИН commit snapshot на весь прогон
        if all_manifests:
            log.info(f"📝 Commit snapshot: {len(all_manifests)} манифестов, {total_s3_rows:,} точек")
            ml = _write_manifest_list(
                s3_client, file_io, bucket, metadata_prefix,
                all_manifests, table_metadata, partition_spec, base_snap_id, base_seq
            )
            _commit_snapshot(
                s3_client, bucket, table_path, metadata_prefix, table_metadata,
                ml, all_manifests, total_s3_rows, base_snap_id, base_seq
            )

    except Exception as e:
        log.error(f"❌ LCard streaming failed: {e}", exc_info=True)
        raise
    finally:
        ch_client.close()

    log.info(f"✅ LCard completed: {total_points:,} точек из {total_source_records} BLOB'ов, errors={error_count}")

    return {
        'data_file_id': data_file_id,
        'stg_table_name': stg_table_name,
        'target_table_name': target_table_name,
        'records_processed': total_source_records,
        'points_inserted': total_points,
        'errors': error_count,
        'manifests_created': len(all_manifests),
        's3_rows': total_s3_rows,
    }




def merge_sample_data(
    stg_table_name: str,
    target_table_name: str,  # Имя таблицы в ClickHouse и Iceberg
    main_db_name: str,
    is_hist_data: bool = None,     # 🔥 Автоопределяется по имени таблицы
    data_file_id: str = None,      # 🔥 Опциональный для изоляции
    data_type_id: str = "969420b8-fd94-4476-b06e-ed7408d42a61",
    # =====================================================================
    # 🔥 ПАРАМЕТРЫ БАТЧИНГА (вынесены в сигнатуру)
    # =====================================================================
    ch_batch_size: int = 2_500_000,    # Строк за один insert_df в CH
    s3_batch_size: int = 50_000_000,   # Строк на один Parquet-файл (~125 МБ)
    read_chunk_size: int = 100,        # BLOB'ов за раз (yield_per)
) -> dict:
    """
    🔥 STREAMING merge SampleData (без загрузки всех точек в память).
    Читает BLOB'ы по read_chunk_size, декодирует на лету и СРАЗУ флешит в CH и S3.
    Память ограничена O(ch_batch_size + s3_batch_size) ~ 5-8 ГБ.
    Использует column-wise extend для ускорения в 3-5 раз.
    """
    import gc, uuid, boto3, pandas as pd, numpy as np, clickhouse_connect
    from botocore.config import Config
    from datetime import datetime, timezone, date, timedelta
    from typing import Dict, Tuple, Any, List
    from sqlalchemy import text
    from airflow.hooks.base import BaseHook
    import pyarrow as pa
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import DayTransform, IdentityTransform
    from pyiceberg.types import DoubleType
    from pyiceberg.io.pyarrow import PyArrowFileIO

    if data_file_id is not None and type(data_file_id).__name__ in ('XComArg', 'PlainXComArg'):
        raise TypeError("❌ Передан XComArg вместо строки!")

    # 🔥 АВТООПРЕДЕЛЕНИЕ is_hist_data
    if is_hist_data is None:
        is_hist_data = '_hist' in stg_table_name.lower()

    dwh_engine = get_dwh_engine('cloudberry_test_dwh')
    timestamp_column_name = 'timestamp' if is_hist_data else 'date'

    # =====================================================================
    # 1. ClickHouse Client Setup
    # =====================================================================
    conn_config = BaseHook.get_connection('clickhouse_conn')
    ch_client = clickhouse_connect.get_client(
        host=conn_config.host, port=8123,
        database=conn_config.schema or 'default',
        username=conn_config.login or 'default',
        password=conn_config.password or '',
        compress=True, query_limit=0,
    )

    # =====================================================================
    # 2. S3 / Iceberg Setup
    # =====================================================================
    s3_conn = BaseHook.get_connection('minio_conn')
    s3_endpoint = s3_conn.extra_dejson.get("host", "http://minio:9000")
    if not s3_endpoint.startswith("http"):
        s3_endpoint = f"http://{s3_endpoint}"
    bucket = s3_conn.extra_dejson.get("bucket", "iceberg-warehouse")
    region = s3_conn.extra_dejson.get("region", "us-east-1")

    s3_client = boto3.client(
        "s3", endpoint_url=s3_endpoint,
        aws_access_key_id=s3_conn.login, aws_secret_access_key=s3_conn.password,
        region_name=region, config=Config(signature_version="s3v4", s3={"addressing_style": "path"})
    )
    file_io = PyArrowFileIO(properties={
        "s3.endpoint": s3_endpoint, "s3.access-key-id": s3_conn.login,
        "s3.secret-access-key": s3_conn.password, "s3.region": region,
        "s3.path-style-access": "true"
    })

    hist_path = 'history-data' if is_hist_data else 'non-history-data'
    table_path = f"{main_db_name}/{hist_path}/{target_table_name}"
    metadata_prefix = f"{table_path}/metadata"
    data_prefix = f"{table_path}/data"

    iceberg_type = DoubleType()
    pa_type = pa.float64()
    value_is_nullable = False

    table_metadata = _init_or_load_table_metadata(
        s3_client, bucket, table_path, metadata_prefix,
        iceberg_type, pa_type, partition_by_source=True
    )

    partition_spec = PartitionSpec(
        PartitionField(source_id=7, field_id=1000, transform=DayTransform(), name="dt_day"),
        PartitionField(source_id=3, field_id=1001, transform=IdentityTransform(), name="source_id"),
        spec_id=0
    )

    pa_schema = pa.schema([
        pa.field("data_record_sk", pa.string(), nullable=False),
        pa.field("h_object_property_sk", pa.string(), nullable=True),
        pa.field("data_file_id", pa.string(), nullable=False),
        pa.field("data_source_id", pa.string(), nullable=False),
        pa.field("main_db_id", pa.string(), nullable=True),
        pa.field("load_dttm", pa.string(), nullable=True),
        pa.field("value", pa.float64(), nullable=False),
        pa.field("timestamp", pa.timestamp("us"), nullable=True),
    ])

    def _normalize_ts(ts_val: Any) -> datetime:
        if ts_val is None or (isinstance(ts_val, float) and pd.isna(ts_val)):
            return datetime(1970, 1, 1, tzinfo=timezone.utc)
        if hasattr(ts_val, 'total_seconds'):
            return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=ts_val.total_seconds())
        if isinstance(ts_val, datetime):
            return ts_val if ts_val.tzinfo else ts_val.replace(tzinfo=timezone.utc)
        try:
            ts_str = str(ts_val).replace(" ", "T")
            if "." not in ts_str and "T" in ts_str:
                ts_str += ".000000"
            dt = datetime.fromisoformat(ts_str)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return datetime(1970, 1, 1, tzinfo=timezone.utc)

    # =====================================================================
    # 3. SQL Query (с фильтром по data_file_id для изоляции)
    # =====================================================================
    file_filter = ""
    sql_params = {"data_type_id": data_type_id}
    if data_file_id:
        file_filter = "AND stg.data_file_id = :data_file_id"
        sql_params["data_file_id"] = data_file_id

    sql_fetch = text(f"""
        SELECT
            stg.hash_sk, stg.h_object_property_sk, stg.load_dttm,
            stg.data_file_id, stg.data_source_id, stg.main_db_id,
            stg.value, stg.{timestamp_column_name} AS base_timestamp
        FROM public.{stg_table_name} stg
        WHERE stg.h_data_type_sk = :data_type_id
          AND stg.hash_sk IS NOT NULL
          AND stg.value IS NOT NULL
          {file_filter}
        ORDER BY stg.hash_sk
    """)

    log.info(f"📥 STREAMING SampleData (file={data_file_id}): "
             f"ch_batch={ch_batch_size:,}, s3_batch={s3_batch_size:,}, read_chunk={read_chunk_size}")

    # 🔥 Фиксируем snapshot ОДИН раз на весь прогон
    base_seq = table_metadata.get("last-sequence-number", 0) + 1
    base_snap_id = abs(hash(f"{table_path}_{datetime.now(timezone.utc).isoformat()}_{uuid.uuid4().hex}"))
    all_manifests = []
    total_s3_rows = 0
    total_source_records = 0
    total_points = 0
    error_count = 0

    # 🔥 Column-wise буферы (dict-of-lists)
    ch_buf = {
        'data_record_sk': [], 'h_object_property_sk': [], 'data_file_id': [],
        'data_source_id': [], 'main_db_id': [], 'load_dttm': [], 'value': [], 'timestamp': []
    }
    s3_partition_buffers: Dict[Tuple[str, date], Dict[str, list]] = {}
    s3_buffered_rows = 0

    def _flush_ch():
        if not ch_buf['value']: return
        df = pd.DataFrame(ch_buf)
        for col in ['data_record_sk', 'h_object_property_sk', 'data_file_id', 'data_source_id', 'main_db_id']:
            if col in df.columns: df[col] = df[col].astype(str)
        if 'timestamp' in df.columns: df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        if 'load_dttm' in df.columns: df['load_dttm'] = pd.to_datetime(df['load_dttm'], utc=True)
        if 'value' in df.columns: df['value'] = df['value'].astype('float64')
        cols_order = ['data_record_sk', 'h_object_property_sk', 'data_file_id', 'data_source_id',
                      'main_db_id', 'timestamp', 'value', 'load_dttm']
        for c in cols_order:
            if c not in df.columns: df[c] = None
        ch_client.insert_df(target_table_name, df[cols_order])
        log.info(f"  ✅ ClickHouse: {len(ch_buf['value']):,} точек")
        for k in ch_buf:
            ch_buf[k] = []

    def _flush_s3():
        nonlocal s3_partition_buffers, s3_buffered_rows, all_manifests, total_s3_rows
        if not s3_partition_buffers: return
        for (fid, day), buf in list(s3_partition_buffers.items()):
            if not buf['data_record_sk']: continue
            m = _flush_common_partition_to_s3(
                day, buf, s3_client, file_io, bucket, data_prefix, metadata_prefix,
                partition_spec, True, iceberg_type, value_is_nullable, pa_schema,
                table_metadata, fid, base_snap_id, base_seq
            )
            if m: all_manifests.append(m)
        total_s3_rows += s3_buffered_rows
        log.info(f"  ✅ S3: {s3_buffered_rows:,} точек → {len(all_manifests)} файлов всего")
        s3_partition_buffers = {}
        s3_buffered_rows = 0
        gc.collect()
        try:
            pa.default_memory_pool().release_unused()
        except Exception:
            pass

    # =====================================================================
    # 4. Main Streaming Loop
    # =====================================================================
    try:
        with dwh_engine.begin() as conn:
            result = conn.execution_options(stream_results=True, yield_per=read_chunk_size).execute(
                sql_fetch, sql_params
            )

            for row in result:
                hash_sk, h_obj_prop_sk, load_dttm, df_id, data_src_id, main_db_id, bytea_val, base_timestamp = row
                total_source_records += 1

                try:
                    # 1. Десериализация BLOB
                    blob_bytes = bytes(bytea_val) if hasattr(bytea_val, 'tobytes') else bytea_val
                    sample = deserialize_sample_data_blob(blob_bytes)

                    # 2. Векторное декодирование (NumPy)
                    raw_values = sample.get('raw_values')
                    if isinstance(raw_values, (bytes, bytearray)):
                        # Автоопределение dtype: если размер кратен 8 → float64, иначе float32
                        if len(raw_values) % 8 == 0:
                            val_arr = np.frombuffer(raw_values, dtype=np.float64)
                        else:
                            val_arr = np.frombuffer(raw_values, dtype=np.float32).astype(np.float64)
                    elif isinstance(raw_values, np.ndarray):
                        val_arr = raw_values.astype(np.float64)
                    else:
                        val_arr = np.asarray(raw_values, dtype=np.float64)

                    n_points = len(val_arr)
                    if n_points == 0:
                        continue

                    sample_rate = float(sample.get('sample_rate', 0))

                except Exception as e:
                    error_count += 1
                    log.warning(f"⚠️ Failed to deserialize/decode SampleData hash_sk={hash_sk}: {e}")
                    continue

                # 3. Подготовка общих полей (один раз на BLOB)
                rec_sk_str = ensure_uuid_string(hash_sk)
                h_obj_prop_str = ensure_uuid_string(h_obj_prop_sk)
                file_id_str = ensure_uuid_string(df_id)
                src_id_str = ensure_uuid_string(data_src_id)
                main_id_str = ensure_uuid_string(main_db_id) if main_db_id else ""
                norm_load_dttm = _normalize_ts(load_dttm)
                norm_base_ts = _normalize_ts(base_timestamp)

                # 4. Генерация временных меток
                if sample_rate > 0:
                    base_pd_ts = pd.Timestamp(norm_base_ts)
                    if base_pd_ts.tzinfo is None:
                        base_pd_ts = base_pd_ts.tz_localize('UTC')
                    else:
                        base_pd_ts = base_pd_ts.tz_convert('UTC')
                    ts_deltas = pd.to_timedelta(np.arange(n_points) / sample_rate, unit='s')
                    ts_series = base_pd_ts + ts_deltas
                    ts_list = ts_series.to_pydatetime()
                else:
                    ts_list = [norm_base_ts] * n_points

                val_list = val_arr.tolist()

                # 🔥 5. COLUMN-WISE EXTEND — ускорение в 3-5 раз vs pototchechnogo append
                # CH буфер
                ch_buf['data_record_sk'].extend([rec_sk_str] * n_points)
                ch_buf['h_object_property_sk'].extend([h_obj_prop_str] * n_points)
                ch_buf['data_file_id'].extend([file_id_str] * n_points)
                ch_buf['data_source_id'].extend([src_id_str] * n_points)
                ch_buf['main_db_id'].extend([main_id_str] * n_points)
                ch_buf['load_dttm'].extend([norm_load_dttm] * n_points)
                ch_buf['timestamp'].extend(ts_list)
                ch_buf['value'].extend(val_list)

                if len(ch_buf['value']) >= ch_batch_size:
                    _flush_ch()

                # S3 буфер (группируем по уникальным дням)
                day_keys = [ts.date() for ts in ts_list]
                unique_days = set(day_keys)

                for day in unique_days:
                    day_indices = [i for i, dk in enumerate(day_keys) if dk == day]
                    n_day = len(day_indices)

                    key = (file_id_str, day)
                    if key not in s3_partition_buffers:
                        s3_partition_buffers[key] = {
                            'data_record_sk': [], 'h_object_property_sk': [], 'data_file_id': [],
                            'data_source_id': [], 'main_db_id': [], 'load_dttm': [], 'timestamp': [], 'value': []
                        }
                    buf = s3_partition_buffers[key]
                    buf['data_record_sk'].extend([rec_sk_str] * n_day)
                    buf['h_object_property_sk'].extend([h_obj_prop_str] * n_day)
                    buf['data_file_id'].extend([file_id_str] * n_day)
                    buf['data_source_id'].extend([src_id_str] * n_day)
                    buf['main_db_id'].extend([main_id_str] * n_day)
                    buf['load_dttm'].extend([norm_load_dttm.isoformat()] * n_day)
                    buf['timestamp'].extend([ts_list[i] for i in day_indices])
                    buf['value'].extend([val_list[i] for i in day_indices])
                    s3_buffered_rows += n_day

                total_points += n_points

                if s3_buffered_rows >= s3_batch_size:
                    _flush_s3()

                # 🔥 Освобождаем память после каждого BLOB'а
                del blob_bytes, sample, raw_values, val_arr, ts_list, val_list
                if total_source_records % 100 == 0:
                    gc.collect()

                if total_source_records % 500 == 0:
                    log.info(f"📊 SampleData progress: {total_points:,} точек из {total_source_records} BLOB'ов")

        # Финальные флеши
        if ch_buf['value']:
            _flush_ch()
        if s3_partition_buffers:
            _flush_s3()

        # ОДИН commit snapshot на весь прогон
        if all_manifests:
            log.info(f"📝 Commit snapshot: {len(all_manifests)} манифестов, {total_s3_rows:,} точек")
            ml = _write_manifest_list(
                s3_client, file_io, bucket, metadata_prefix,
                all_manifests, table_metadata, partition_spec, base_snap_id, base_seq
            )
            _commit_snapshot(
                s3_client, bucket, table_path, metadata_prefix, table_metadata,
                ml, all_manifests, total_s3_rows, base_snap_id, base_seq
            )

    except Exception as e:
        log.error(f"❌ SampleData streaming failed: {e}", exc_info=True)
        raise
    finally:
        ch_client.close()

    log.info(f"✅ SampleData completed: {total_points:,} точек из {total_source_records} BLOB'ов, errors={error_count}")

    return {
        'data_file_id': data_file_id,
        'stg_table_name': stg_table_name,
        'target_table_name': target_table_name,
        'records_processed': total_source_records,
        'points_inserted': total_points,
        'errors': error_count,
        'manifests_created': len(all_manifests),
        's3_rows': total_s3_rows,
    }


 
    
def merge_siemens_sample_data(
    stg_table_name: str,
    target_table_name: str,  # Имя таблицы в ClickHouse и Iceberg
    main_db_name: str,
    is_hist_data: bool = None,     # 🔥 Автоопределяется по имени таблицы
    data_file_id: str = None,      # 🔥 Опциональный для изоляции
    data_type_id: str = '80bdd702-d6f9-e90e-e3fa-1b9866940654',
    # =====================================================================
    # 🔥 ПАРАМЕТРЫ БАТЧИНГА (вынесены в сигнатуру)
    # =====================================================================
    ch_batch_size: int = 2_500_000,    # Строк за один insert_df в CH
    s3_batch_size: int = 50_000_000,   # Строк на один Parquet-файл (~125 МБ)
    read_chunk_size: int = 100,        # BLOB'ов за раз (yield_per)
) -> dict:
    """
    🔥 STREAMING merge Siemens SampleData (без загрузки всех точек в память).
    Читает BLOB'ы по read_chunk_size, декодирует на лету и СРАЗУ флешит в CH и S3.
    Память ограничена O(ch_batch_size + s3_batch_size) ~ 5-8 ГБ.
    Использует column-wise extend для ускорения в 3-5 раз.
    """
    import gc, uuid, boto3, pandas as pd, numpy as np, clickhouse_connect
    from botocore.config import Config
    from datetime import datetime, timezone, date, timedelta
    from typing import Dict, Tuple, Any, List
    from sqlalchemy import text
    from airflow.hooks.base import BaseHook
    import pyarrow as pa
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import DayTransform, IdentityTransform
    from pyiceberg.types import DoubleType
    from pyiceberg.io.pyarrow import PyArrowFileIO

    if data_file_id is not None and type(data_file_id).__name__ in ('XComArg', 'PlainXComArg'):
        raise TypeError("❌ Передан XComArg вместо строки!")

    # 🔥 АВТООПРЕДЕЛЕНИЕ is_hist_data
    if is_hist_data is None:
        is_hist_data = '_hist' in stg_table_name.lower()

    dwh_engine = get_dwh_engine('cloudberry_test_dwh')
    timestamp_column_name = 'timestamp' if is_hist_data else 'date'

    # =====================================================================
    # 1. ClickHouse Client Setup
    # =====================================================================
    conn_config = BaseHook.get_connection('clickhouse_conn')
    ch_client = clickhouse_connect.get_client(
        host=conn_config.host, port=8123,
        database=conn_config.schema or 'default',
        username=conn_config.login or 'default',
        password=conn_config.password or '',
        compress=True, query_limit=0,
    )

    # =====================================================================
    # 2. S3 / Iceberg Setup
    # =====================================================================
    s3_conn = BaseHook.get_connection('minio_conn')
    s3_endpoint = s3_conn.extra_dejson.get("host", "http://minio:9000")
    if not s3_endpoint.startswith("http"):
        s3_endpoint = f"http://{s3_endpoint}"
    bucket = s3_conn.extra_dejson.get("bucket", "iceberg-warehouse")
    region = s3_conn.extra_dejson.get("region", "us-east-1")

    s3_client = boto3.client(
        "s3", endpoint_url=s3_endpoint,
        aws_access_key_id=s3_conn.login, aws_secret_access_key=s3_conn.password,
        region_name=region, config=Config(signature_version="s3v4", s3={"addressing_style": "path"})
    )
    file_io = PyArrowFileIO(properties={
        "s3.endpoint": s3_endpoint, "s3.access-key-id": s3_conn.login,
        "s3.secret-access-key": s3_conn.password, "s3.region": region,
        "s3.path-style-access": "true"
    })

    hist_path = 'history-data' if is_hist_data else 'non-history-data'
    table_path = f"{main_db_name}/{hist_path}/{target_table_name}"
    metadata_prefix = f"{table_path}/metadata"
    data_prefix = f"{table_path}/data"

    iceberg_type = DoubleType()
    pa_type = pa.float64()
    value_is_nullable = False

    table_metadata = _init_or_load_table_metadata(
        s3_client, bucket, table_path, metadata_prefix,
        iceberg_type, pa_type, partition_by_source=True
    )

    partition_spec = PartitionSpec(
        PartitionField(source_id=7, field_id=1000, transform=DayTransform(), name="dt_day"),
        PartitionField(source_id=3, field_id=1001, transform=IdentityTransform(), name="source_id"),
        spec_id=0
    )

    pa_schema = pa.schema([
        pa.field("data_record_sk", pa.string(), nullable=False),
        pa.field("h_object_property_sk", pa.string(), nullable=True),
        pa.field("data_file_id", pa.string(), nullable=False),
        pa.field("data_source_id", pa.string(), nullable=False),
        pa.field("main_db_id", pa.string(), nullable=True),
        pa.field("load_dttm", pa.string(), nullable=True),
        pa.field("value", pa.float64(), nullable=False),
        pa.field("timestamp", pa.timestamp("us"), nullable=True),
    ])

    def _normalize_ts(ts_val: Any) -> datetime:
        if ts_val is None or (isinstance(ts_val, float) and pd.isna(ts_val)):
            return datetime(1970, 1, 1, tzinfo=timezone.utc)
        if hasattr(ts_val, 'total_seconds'):
            return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=ts_val.total_seconds())
        if isinstance(ts_val, datetime):
            return ts_val if ts_val.tzinfo else ts_val.replace(tzinfo=timezone.utc)
        try:
            ts_str = str(ts_val).replace(" ", "T")
            if "." not in ts_str and "T" in ts_str:
                ts_str += ".000000"
            dt = datetime.fromisoformat(ts_str)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return datetime(1970, 1, 1, tzinfo=timezone.utc)

    # =====================================================================
    # 3. SQL Query (с фильтром по data_file_id для изоляции)
    # =====================================================================
    file_filter = ""
    sql_params = {"data_type_id": data_type_id}
    if data_file_id:
        file_filter = "AND stg.data_file_id = :data_file_id"
        sql_params["data_file_id"] = data_file_id

    sql_fetch = text(f"""
        SELECT
            stg.hash_sk, stg.h_object_property_sk, stg.load_dttm,
            stg.data_file_id, stg.data_source_id, stg.main_db_id,
            stg.value, stg.{timestamp_column_name} AS base_timestamp
        FROM public.{stg_table_name} stg
        WHERE stg.h_data_type_sk = :data_type_id
          AND stg.hash_sk IS NOT NULL
          AND stg.value IS NOT NULL
          {file_filter}
        ORDER BY stg.hash_sk
    """)

    log.info(f"📥 STREAMING Siemens SampleData (file={data_file_id}): "
             f"ch_batch={ch_batch_size:,}, s3_batch={s3_batch_size:,}, read_chunk={read_chunk_size}")

    # 🔥 Фиксируем snapshot ОДИН раз на весь прогон
    base_seq = table_metadata.get("last-sequence-number", 0) + 1
    base_snap_id = abs(hash(f"{table_path}_{datetime.now(timezone.utc).isoformat()}_{uuid.uuid4().hex}"))
    all_manifests = []
    total_s3_rows = 0
    total_source_records = 0
    total_points = 0
    error_count = 0

    # 🔥 Column-wise буферы (dict-of-lists)
    ch_buf = {
        'data_record_sk': [], 'h_object_property_sk': [], 'data_file_id': [],
        'data_source_id': [], 'main_db_id': [], 'load_dttm': [], 'value': [], 'timestamp': []
    }
    s3_partition_buffers: Dict[Tuple[str, date], Dict[str, list]] = {}
    s3_buffered_rows = 0

    def _flush_ch():
        if not ch_buf['value']: return
        df = pd.DataFrame(ch_buf)
        for col in ['data_record_sk', 'h_object_property_sk', 'data_file_id', 'data_source_id', 'main_db_id']:
            if col in df.columns: df[col] = df[col].astype(str)
        if 'timestamp' in df.columns: df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        if 'load_dttm' in df.columns: df['load_dttm'] = pd.to_datetime(df['load_dttm'], utc=True)
        if 'value' in df.columns: df['value'] = df['value'].astype('float64')
        cols_order = ['data_record_sk', 'h_object_property_sk', 'data_file_id', 'data_source_id',
                      'main_db_id', 'timestamp', 'value', 'load_dttm']
        for c in cols_order:
            if c not in df.columns: df[c] = None
        ch_client.insert_df(target_table_name, df[cols_order])
        log.info(f"  ✅ ClickHouse: {len(ch_buf['value']):,} точек")
        for k in ch_buf:
            ch_buf[k] = []

    def _flush_s3():
        nonlocal s3_partition_buffers, s3_buffered_rows, all_manifests, total_s3_rows
        if not s3_partition_buffers: return
        for (fid, day), buf in list(s3_partition_buffers.items()):
            if not buf['data_record_sk']: continue
            m = _flush_common_partition_to_s3(
                day, buf, s3_client, file_io, bucket, data_prefix, metadata_prefix,
                partition_spec, True, iceberg_type, value_is_nullable, pa_schema,
                table_metadata, fid, base_snap_id, base_seq
            )
            if m: all_manifests.append(m)
        total_s3_rows += s3_buffered_rows
        log.info(f"  ✅ S3: {s3_buffered_rows:,} точек → {len(all_manifests)} файлов всего")
        s3_partition_buffers = {}
        s3_buffered_rows = 0
        gc.collect()
        try:
            pa.default_memory_pool().release_unused()
        except Exception:
            pass

    # =====================================================================
    # 4. Main Streaming Loop
    # =====================================================================
    try:
        with dwh_engine.begin() as conn:
            result = conn.execution_options(stream_results=True, yield_per=read_chunk_size).execute(
                sql_fetch, sql_params
            )

            for row in result:
                hash_sk, h_obj_prop_sk, load_dttm, df_id, data_src_id, main_db_id, bytea_val, base_timestamp = row
                total_source_records += 1

                try:
                    # 1. Десериализация BLOB
                    if isinstance(bytea_val, memoryview):
                        blob_bytes = bytea_val.tobytes()
                    elif isinstance(bytea_val, (bytes, bytearray)):
                        blob_bytes = bytes(bytea_val)
                    else:
                        blob_bytes = bytes(bytea_val)

                    sample = deserialize_siemens_sample_blob(blob_bytes)

                    # 2. Векторное декодирование (NumPy)
                    raw_values = sample.get('raw_values')
                    if isinstance(raw_values, (bytes, bytearray)):
                        # Автоопределение dtype: если размер кратен 8 → float64, иначе float32
                        if len(raw_values) % 8 == 0:
                            val_arr = np.frombuffer(raw_values, dtype=np.float64)
                        else:
                            val_arr = np.frombuffer(raw_values, dtype=np.float32).astype(np.float64)
                    elif isinstance(raw_values, np.ndarray):
                        val_arr = raw_values.astype(np.float64)
                    else:
                        val_arr = np.asarray(raw_values, dtype=np.float64)

                    n_points = len(val_arr)
                    if n_points == 0:
                        continue

                    sample_rate = float(sample.get('sample_rate', 0))

                except Exception as e:
                    error_count += 1
                    log.warning(f"⚠️ Failed to deserialize/decode Siemens SampleData hash_sk={hash_sk}: {e}")
                    continue

                # 3. Подготовка общих полей (один раз на BLOB)
                rec_sk_str = ensure_uuid_string(hash_sk)
                h_obj_prop_str = ensure_uuid_string(h_obj_prop_sk)
                file_id_str = ensure_uuid_string(df_id)
                src_id_str = ensure_uuid_string(data_src_id)
                main_id_str = ensure_uuid_string(main_db_id) if main_db_id else ""
                norm_load_dttm = _normalize_ts(load_dttm)
                norm_base_ts = _normalize_ts(base_timestamp)

                # 4. Генерация временных меток
                if sample_rate > 0:
                    base_pd_ts = pd.Timestamp(norm_base_ts)
                    if base_pd_ts.tzinfo is None:
                        base_pd_ts = base_pd_ts.tz_localize('UTC')
                    else:
                        base_pd_ts = base_pd_ts.tz_convert('UTC')
                    ts_deltas = pd.to_timedelta(np.arange(n_points) / sample_rate, unit='s')
                    ts_series = base_pd_ts + ts_deltas
                    ts_list = ts_series.to_pydatetime()
                else:
                    ts_list = [norm_base_ts] * n_points

                val_list = val_arr.tolist()

                # 🔥 5. COLUMN-WISE EXTEND — ускорение в 3-5 раз vs pototchechnogo append
                # CH буфер
                ch_buf['data_record_sk'].extend([rec_sk_str] * n_points)
                ch_buf['h_object_property_sk'].extend([h_obj_prop_str] * n_points)
                ch_buf['data_file_id'].extend([file_id_str] * n_points)
                ch_buf['data_source_id'].extend([src_id_str] * n_points)
                ch_buf['main_db_id'].extend([main_id_str] * n_points)
                ch_buf['load_dttm'].extend([norm_load_dttm] * n_points)
                ch_buf['timestamp'].extend(ts_list)
                ch_buf['value'].extend(val_list)

                if len(ch_buf['value']) >= ch_batch_size:
                    _flush_ch()

                # S3 буфер (группируем по уникальным дням)
                day_keys = [ts.date() for ts in ts_list]
                unique_days = set(day_keys)

                for day in unique_days:
                    day_indices = [i for i, dk in enumerate(day_keys) if dk == day]
                    n_day = len(day_indices)

                    key = (file_id_str, day)
                    if key not in s3_partition_buffers:
                        s3_partition_buffers[key] = {
                            'data_record_sk': [], 'h_object_property_sk': [], 'data_file_id': [],
                            'data_source_id': [], 'main_db_id': [], 'load_dttm': [], 'timestamp': [], 'value': []
                        }
                    buf = s3_partition_buffers[key]
                    buf['data_record_sk'].extend([rec_sk_str] * n_day)
                    buf['h_object_property_sk'].extend([h_obj_prop_str] * n_day)
                    buf['data_file_id'].extend([file_id_str] * n_day)
                    buf['data_source_id'].extend([src_id_str] * n_day)
                    buf['main_db_id'].extend([main_id_str] * n_day)
                    buf['load_dttm'].extend([norm_load_dttm.isoformat()] * n_day)
                    buf['timestamp'].extend([ts_list[i] for i in day_indices])
                    buf['value'].extend([val_list[i] for i in day_indices])
                    s3_buffered_rows += n_day

                total_points += n_points

                if s3_buffered_rows >= s3_batch_size:
                    _flush_s3()

                # 🔥 Освобождаем память после каждого BLOB'а
                del blob_bytes, sample, raw_values, val_arr, ts_list, val_list
                if total_source_records % 100 == 0:
                    gc.collect()

                if total_source_records % 500 == 0:
                    log.info(f"📊 Siemens SampleData progress: {total_points:,} точек из {total_source_records} BLOB'ов")

        # Финальные флеши
        if ch_buf['value']:
            _flush_ch()
        if s3_partition_buffers:
            _flush_s3()

        # ОДИН commit snapshot на весь прогон
        if all_manifests:
            log.info(f"📝 Commit snapshot: {len(all_manifests)} манифестов, {total_s3_rows:,} точек")
            ml = _write_manifest_list(
                s3_client, file_io, bucket, metadata_prefix,
                all_manifests, table_metadata, partition_spec, base_snap_id, base_seq
            )
            _commit_snapshot(
                s3_client, bucket, table_path, metadata_prefix, table_metadata,
                ml, all_manifests, total_s3_rows, base_snap_id, base_seq
            )

    except Exception as e:
        log.error(f"❌ Siemens SampleData streaming failed: {e}", exc_info=True)
        raise
    finally:
        ch_client.close()

    log.info(f"✅ Siemens SampleData completed: {total_points:,} точек из {total_source_records} BLOB'ов, errors={error_count}")

    return {
        'data_file_id': data_file_id,
        'stg_table_name': stg_table_name,
        'target_table_name': target_table_name,
        'records_processed': total_source_records,
        'points_inserted': total_points,
        'errors': error_count,
        'manifests_created': len(all_manifests),
        's3_rows': total_s3_rows,
    }  




def merge_spectrum_data(
    stg_table_name: str,
    target_table_name: str,  # Имя таблицы в ClickHouse и Iceberg
    is_hist_data: bool,
    data_file_id: str,       # 🔥 ОБЯЗАТЕЛЬНЫЙ ПАРАМЕТР ДЛЯ ИЗОЛЯЦИИ
    data_type_id: str = '195542f6-0ed3-6f80-91ca-296219cf3f8b',
    batch_size: int = 2000,  # Оптимально для BLOB данных
) -> dict:
    """
    🔥 Streaming merge из staging слоя СРАЗУ в ClickHouse и S3 (Iceberg).
    Минует промежуточное хранение в DWH (Cloudberry).
    Десериализует BLOB Spectrum и сразу отправляет в целевые хранилища.
    """
    import gc
    import uuid
    import boto3
    import pandas as pd
    import numpy as np
    import clickhouse_connect
    from botocore.config import Config
    from datetime import datetime, timezone, date, timedelta
    from typing import Dict, Tuple, Any
    from sqlalchemy import text
    from airflow.hooks.base import BaseHook
    import pyarrow as pa
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import DayTransform, IdentityTransform
    from pyiceberg.types import DoubleType, ListType, StringType
    from pyiceberg.io.pyarrow import PyArrowFileIO
    
    # Защита от нерезолвленного XComArg
    if type(data_file_id).__name__ in ('XComArg', 'PlainXComArg'):
        raise TypeError(
            f"❌ ОШИБКА КОНФИГУРАЦИИ DAG: В функцию передан нерезолвленный XComArg вместо строки! "
            f"Проверьте файл upload_maindb_data_dag.py."
        )
    
    dwh_engine = get_dwh_engine('cloudberry_test_dwh')
    timestamp_column_name = 'timestamp' if is_hist_data else 'date'
    
    # =====================================================================
    # 1. ClickHouse Client Setup
    # =====================================================================
    conn_config = BaseHook.get_connection('clickhouse_conn')
    ch_client = clickhouse_connect.get_client(
        host=conn_config.host, port=8123,
        database=conn_config.schema or 'default',
        username=conn_config.login or 'default',
        password=conn_config.password or '',
        compress=True, query_limit=0,
    )
    
    # =====================================================================
    # 2. S3 / Iceberg Setup
    # =====================================================================
    s3_conn = BaseHook.get_connection('minio_conn')
    s3_endpoint = s3_conn.extra_dejson.get("host", "http://minio:9000")
    if not s3_endpoint.startswith("http"): 
        s3_endpoint = f"http://{s3_endpoint}"
    bucket = s3_conn.extra_dejson.get("bucket", "iceberg-warehouse")
    region = s3_conn.extra_dejson.get("region", "us-east-1")
    
    s3_client = boto3.client(
        "s3", endpoint_url=s3_endpoint, 
        aws_access_key_id=s3_conn.login, aws_secret_access_key=s3_conn.password, 
        region_name=region, config=Config(signature_version="s3v4", s3={"addressing_style": "path"})
    )
    file_io = PyArrowFileIO(properties={
        "s3.endpoint": s3_endpoint, "s3.access-key-id": s3_conn.login,
        "s3.secret-access-key": s3_conn.password, "s3.region": region,
        "s3.path-style-access": "true"
    })
    
    hist_path = 'history-data' if is_hist_data else 'non-history-data'
    table_path = f"{main_db_name}/{hist_path}/{target_table_name}"
    metadata_prefix = f"{table_path}/metadata"
    data_prefix = f"{table_path}/data"
    
    table_metadata = _init_or_load_table_metadata(
        s3_client, bucket, table_path, metadata_prefix,
        ListType(1, DoubleType(), True), pa.list_(pa.float64()), partition_by_source=True
    )
    
    # 🔥 Схема для Spectrum: raw_values - это массив (List<Float64>)
    pa_schema = pa.schema([
        pa.field("data_record_sk", pa.string(), nullable=False),
        pa.field("h_object_property_sk", pa.string(), nullable=True),
        pa.field("data_file_id", pa.string(), nullable=False),
        pa.field("data_source_id", pa.string(), nullable=False),
        pa.field("main_db_id", pa.string(), nullable=True),
        pa.field("load_dttm", pa.string(), nullable=True),
        pa.field("raw_values", pa.list_(pa.float64()), nullable=True),
        pa.field("multiplier", pa.float64(), nullable=True),
        pa.field("h_spectrum_type_sk", pa.string(), nullable=True),
        pa.field("timestamp", pa.timestamp("us"), nullable=True), # Поле 10
    ])
    
    # 🔥 ВАЖНО: source_id в PartitionField соответствует номеру поля в схеме (1-based)
    partition_spec = PartitionSpec(
        PartitionField(source_id=10, field_id=1000, transform=DayTransform(), name="dt_day"), # timestamp
        PartitionField(source_id=4, field_id=1001, transform=IdentityTransform(), name="source_id"), # data_source_id
        spec_id=0
    )
    
    # =====================================================================
    # 3. SQL Query (Streaming from Staging)
    # =====================================================================
    sql_fetch = text(f"""
        SELECT 
            stg.hash_sk, stg.h_object_property_sk, stg.load_dttm, 
            stg.data_file_id, stg.data_source_id, stg.main_db_id,
            stg.value, stg.{timestamp_column_name} AS base_timestamp
        FROM public.{stg_table_name} stg
        WHERE stg.h_data_type_sk = :data_type_id
          AND stg.data_file_id = :data_file_id  -- 🔥 ИЗОЛЯЦИЯ
          AND stg.hash_sk IS NOT NULL
          AND stg.value IS NOT NULL
        ORDER BY stg.hash_sk
    """)
    
    total_processed = 0
    error_count = 0
    
    partition_buffers: Dict[Tuple[str, date], Dict[str, list]] = {}
    current_buffered_rows = 0
    # 🔥 Для записей с массивами (спектрами) лимит должен быть меньше, чем для скаляров
    TARGET_ROWS_PER_FLUSH = 5_000_000 
    
    def _normalize_ts(ts_val: Any) -> datetime:
        if ts_val is None or (isinstance(ts_val, float) and pd.isna(ts_val)):
            return datetime(1970, 1, 1, tzinfo=timezone.utc)
        if hasattr(ts_val, 'total_seconds'):
            return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=ts_val.total_seconds())
        if isinstance(ts_val, datetime):
            return ts_val if ts_val.tzinfo else ts_val.replace(tzinfo=timezone.utc)
        try:
            ts_str = str(ts_val).replace(" ", "T")
            if "." not in ts_str and "T" in ts_str: 
                ts_str += ".000000"
            dt = datetime.fromisoformat(ts_str)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return datetime(1970, 1, 1, tzinfo=timezone.utc)

    def _flush_ch(ch_records):
        if not ch_records: 
            return
        df = pd.DataFrame(ch_records)
        for col in ['data_record_sk', 'h_object_property_sk', 'data_file_id', 'data_source_id', 'main_db_id', 'h_spectrum_type_sk']:
            if col in df.columns:
                df[col] = df[col].astype(str)
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        df['load_dttm'] = pd.to_datetime(df['load_dttm'], utc=True)
        
        cols_order = ['data_record_sk', 'h_object_property_sk', 'data_file_id', 'data_source_id', 'main_db_id',
                      'raw_values', 'multiplier', 'h_spectrum_type_sk', 'timestamp', 'load_dttm']
        for c in cols_order:
            if c not in df.columns:
                df[c] = None
        df = df[cols_order]
        ch_client.insert_df(target_table_name, df)

    def _flush_iceberg():
        nonlocal table_metadata
        if not partition_buffers: 
            return
        pending_manifests = []
        current_batch_sequence_number = table_metadata.get("last-sequence-number", 0) + 1
        current_batch_snapshot_id = abs(hash(f"{table_path}_{datetime.now(timezone.utc).isoformat()}_{uuid.uuid4().hex}"))
        
        total_recs_in_batch = 0
        for (file_id_str, day_date), buffer in list(partition_buffers.items()):
            if not buffer['data_record_sk']: 
                continue
            manifest = _flush_common_partition_to_s3(
                day_date, buffer, s3_client, file_io, bucket, data_prefix, metadata_prefix,
                partition_spec, True, ListType(1, DoubleType(), True), True, pa_schema, table_metadata, file_id_str,
                snapshot_id=current_batch_snapshot_id, sequence_number=current_batch_sequence_number
            )
            if manifest:
                pending_manifests.append(manifest)
                total_recs_in_batch += len(buffer['data_record_sk'])
                
        if pending_manifests:
            manifest_list_path = _write_manifest_list(
                s3_client, file_io, bucket, metadata_prefix, pending_manifests, table_metadata,
                partition_spec, snapshot_id=current_batch_snapshot_id, sequence_number=current_batch_sequence_number
            )
            _commit_snapshot(
                s3_client, bucket, table_path, metadata_prefix, table_metadata,
                manifest_list_path, pending_manifests, total_recs_in_batch,
                snapshot_id=current_batch_snapshot_id, sequence_number=current_batch_sequence_number
            )
            table_metadata = _init_or_load_table_metadata(
                s3_client, bucket, table_path, metadata_prefix, ListType(1, DoubleType(), True), pa.list_(pa.float64()), True
            )
            
        gc.collect()
        try:
            pa.default_memory_pool().release_unused()
        except Exception:
            pass

    # =====================================================================
    # 4. Main Streaming Loop
    # =====================================================================
    try:
        with dwh_engine.begin() as conn:
            result = conn.execution_options(stream_results=True, yield_per=batch_size).execute(
                sql_fetch, {"data_type_id": data_type_id, "data_file_id": data_file_id}
            )
            
            ch_records = []
            
            for partition in result.partitions():
                for row in partition:
                    hash_sk, h_obj_prop_sk, load_dttm, df_id, data_src_id, main_db_id, bytea_val, base_timestamp = row
                    
                    try:
                        # 1. Десериализация BLOB
                        if isinstance(bytea_val, memoryview):
                            blob_bytes = bytea_val.tobytes()
                        elif isinstance(bytea_val, (bytes, bytearray)):
                            blob_bytes = bytes(bytea_val)
                        else:
                            blob_bytes = bytes(bytea_val)
                            
                        spectrum = deserialize_spectrum_blob(blob_bytes)
                        del blob_bytes
                        
                        # 2. Извлечение и конвертация массива raw_values
                        raw_vals = spectrum.get('raw_values')
                        if isinstance(raw_vals, (bytes, bytearray)):
                            if len(raw_vals) % 8 == 0:
                                arr = np.frombuffer(raw_vals, dtype=np.float64).tolist()
                            else:
                                arr = np.frombuffer(raw_vals, dtype=np.float32).astype(np.float64).tolist()
                        elif isinstance(raw_vals, np.ndarray):
                            arr = raw_vals.astype(np.float64).tolist()
                        else:
                            arr = [float(x) for x in raw_vals] if raw_vals else []
                            
                        multiplier = float(spectrum.get('multiplier')) if spectrum.get('multiplier') is not None else None
                        h_spec_type = ensure_uuid_string(spectrum.get('h_spectrum_type_sk')) if spectrum.get('h_spectrum_type_sk') else None
                        
                    except Exception as e:
                        error_count += 1
                        log.warning(f"⚠️ Failed to deserialize Spectrum hash_sk={hash_sk}: {e}")
                        continue
                        
                    rec_sk_str = ensure_uuid_string(hash_sk)
                    h_obj_prop_str = ensure_uuid_string(h_obj_prop_sk)
                    file_id_str = ensure_uuid_string(df_id)
                    src_id_str = ensure_uuid_string(data_src_id)
                    main_db_str = ensure_uuid_string(main_db_id) if main_db_id else None
                    norm_load_dttm = _normalize_ts(load_dttm)
                    norm_timestamp = _normalize_ts(base_timestamp)
                    day_key = norm_timestamp.date()
                    
                    # 3. Распределение по буферам
                    ch_records.append({
                        'data_record_sk': rec_sk_str,
                        'h_object_property_sk': h_obj_prop_str,
                        'data_file_id': file_id_str,
                        'data_source_id': src_id_str,
                        'main_db_id': main_db_str,
                        'load_dttm': norm_load_dttm,
                        'raw_values': arr,
                        'multiplier': multiplier,
                        'h_spectrum_type_sk': h_spec_type,
                        'timestamp': norm_timestamp,
                    })
                    
                    key = (file_id_str, day_key)
                    if key not in partition_buffers:
                        partition_buffers[key] = {
                            'data_record_sk': [], 'h_object_property_sk': [], 'data_file_id': [],
                            'data_source_id': [], 'main_db_id': [], 'load_dttm': [], 'timestamp': [],
                            'raw_values': [], 'multiplier': [], 'h_spectrum_type_sk': []
                        }
                    buf = partition_buffers[key]
                    buf['data_record_sk'].append(rec_sk_str)
                    buf['h_object_property_sk'].append(h_obj_prop_str)
                    buf['data_file_id'].append(file_id_str)
                    buf['data_source_id'].append(src_id_str)
                    buf['main_db_id'].append(main_db_str)
                    buf['load_dttm'].append(norm_load_dttm.isoformat())
                    buf['timestamp'].append(norm_timestamp)
                    buf['raw_values'].append(arr)
                    buf['multiplier'].append(multiplier)
                    buf['h_spectrum_type_sk'].append(h_spec_type)
                    
                    current_buffered_rows += 1
                    total_processed += 1
                    
                    # Flush при достижении лимита
                    if current_buffered_rows >= TARGET_ROWS_PER_FLUSH:
                        _flush_ch(ch_records)
                        ch_records = []
                        _flush_iceberg()
                        partition_buffers = {}
                        current_buffered_rows = 0
                        log.info(f"📊 Spectrum [{data_file_id[:8]}]: progress {total_processed} records")
                
                # End of partition - flush remaining CH records
                if ch_records:
                    _flush_ch(ch_records)
                    ch_records = []
            
            # Final Iceberg flush
            if current_buffered_rows > 0:
                if ch_records:
                    _flush_ch(ch_records)
                _flush_iceberg()
                
    except Exception as e:
        log.error(f"❌ Error during Spectrum streaming merge for {data_file_id}: {e}", exc_info=True)
        raise
    finally:
        ch_client.close()
        
    log.info(
        f"✅ Spectrum [{data_file_id[:8]}] completed: "
        f"{total_processed} records, errors {error_count}"
    )
    return {
        'data_file_id': data_file_id,
        'stg_table_name': stg_table_name,
        'target_table_name': target_table_name,
        'records_processed': total_processed,
        'errors': error_count
    }



def merge_bode_data(
    stg_table_name: str,
    target_table_name: str,
    main_db_name: str,
    is_hist_data: bool = None,      # 🔥 Автоопределяется по имени таблицы
    data_file_id: str = None,       # 🔥 Опциональный для изоляции
    data_type_id: str = "98015170-fd7e-23a2-8d27-c9506dc968d2",
    # =====================================================================
    # 🔥 ПАРАМЕТРЫ БАТЧИНГА (вынесены в сигнатуру)
    # =====================================================================
    ch_batch_size: int = 2_500_000,     # Строк за insert_df в CH
    s3_batch_size: int = 50_000_000,    # Строк на один Parquet-файл (~125 МБ)
    read_chunk_size: int = 100,         # BLOB'ов за раз (yield_per)
) -> dict:
    """
    🔥 STREAMING merge BODE (без загрузки всех записей в память).
    1 строка staging = 1 точка в target (magnitude, phase, turnover_frequency).
    Десериализует BLOB на лету, использует column-wise extend и ОДИН snapshot.
    """
    import gc, uuid, boto3, pandas as pd, numpy as np, clickhouse_connect
    from botocore.config import Config
    from datetime import datetime, timezone, date, timedelta
    from typing import Dict, Tuple, Any, List
    from sqlalchemy import text
    from airflow.hooks.base import BaseHook
    import pyarrow as pa
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import DayTransform, IdentityTransform
    from pyiceberg.types import DoubleType
    from pyiceberg.io.pyarrow import PyArrowFileIO

    if data_file_id is not None and type(data_file_id).__name__ in ('XComArg', 'PlainXComArg'):
        raise TypeError("❌ Передан XComArg вместо строки!")

    # 🔥 АВТООПРЕДЕЛЕНИЕ is_hist_data
    if is_hist_data is None:
        is_hist_data = '_hist' in stg_table_name.lower()

    dwh_engine = get_dwh_engine('cloudberry_test_dwh')
    timestamp_column_name = 'timestamp' if is_hist_data else 'date'

    # =====================================================================
    # 1. ClickHouse Client Setup
    # =====================================================================
    conn_config = BaseHook.get_connection('clickhouse_conn')
    ch_client = clickhouse_connect.get_client(
        host=conn_config.host, port=8123, database=conn_config.schema or 'default',
        username=conn_config.login or 'default', password=conn_config.password or '',
        compress=True, query_limit=0,
    )

    # =====================================================================
    # 2. S3 / Iceberg Setup
    # =====================================================================
    s3_conn = BaseHook.get_connection('minio_conn')
    s3_endpoint = s3_conn.extra_dejson.get("host", "http://minio:9000")
    if not s3_endpoint.startswith("http"):
        s3_endpoint = f"http://{s3_endpoint}"
    bucket = s3_conn.extra_dejson.get("bucket", "iceberg-warehouse")
    region = s3_conn.extra_dejson.get("region", "us-east-1")

    s3_client = boto3.client(
        "s3", endpoint_url=s3_endpoint,
        aws_access_key_id=s3_conn.login, aws_secret_access_key=s3_conn.password,
        region_name=region, config=Config(signature_version="s3v4", s3={"addressing_style": "path"})
    )
    file_io = PyArrowFileIO(properties={
        "s3.endpoint": s3_endpoint, "s3.access-key-id": s3_conn.login,
        "s3.secret-access-key": s3_conn.password, "s3.region": region,
        "s3.path-style-access": "true"
    })

    hist_path = 'history-data' if is_hist_data else 'non-history-data'
    table_path = f"{main_db_name}/{hist_path}/{target_table_name}"
    metadata_prefix = f"{table_path}/metadata"
    data_prefix = f"{table_path}/data"

    iceberg_type = DoubleType()
    pa_type = pa.float64()
    value_is_nullable = True  # magnitude/phase/freq могут быть NULL

    table_metadata = _init_or_load_table_metadata(
        s3_client, bucket, table_path, metadata_prefix,
        iceberg_type, pa_type, partition_by_source=True
    )

    pa_schema = pa.schema([
        pa.field("data_record_sk", pa.string(), nullable=False),
        pa.field("h_object_property_sk", pa.string(), nullable=True),
        pa.field("data_file_id", pa.string(), nullable=False),
        pa.field("data_source_id", pa.string(), nullable=False),
        pa.field("main_db_id", pa.string(), nullable=True),
        pa.field("load_dttm", pa.string(), nullable=True),
        pa.field("magnitude_value", pa.float64(), nullable=True),
        pa.field("phase_value", pa.float64(), nullable=True),
        pa.field("turnover_frequency_value", pa.float64(), nullable=True),
        pa.field("timestamp", pa.timestamp("us"), nullable=True),
    ])

    def _get_field_id(metadata_dict: dict, field_names: list, schema_obj: pa.Schema) -> int:
        def find_field_id(obj, target_names):
            if isinstance(obj, dict):
                if obj.get("name") in target_names and "id" in obj: return obj.get("id")
                for v in obj.values():
                    res = find_field_id(v, target_names)
                    if res is not None: return res
            elif isinstance(obj, list):
                for item in obj:
                    res = find_field_id(item, target_names)
                    if res is not None: return res
            return None
        found_id = find_field_id(metadata_dict, field_names)
        if found_id is not None: return found_id
        for i, field in enumerate(schema_obj):
            if field.name in field_names: return i + 1
        raise ValueError(f"Field {field_names} not found")

    ts_field_id = _get_field_id(table_metadata, ["timestamp", "date", "base_timestamp"], pa_schema)
    src_field_id = _get_field_id(table_metadata, ["data_source_id", "source_id"], pa_schema)

    partition_spec = PartitionSpec(
        PartitionField(source_id=ts_field_id, field_id=1000, transform=DayTransform(), name="dt_day"),
        PartitionField(source_id=src_field_id, field_id=1001, transform=IdentityTransform(), name="source_id"),
        spec_id=0
    )

    def _normalize_ts(ts_val: Any) -> datetime:
        if ts_val is None or (isinstance(ts_val, float) and pd.isna(ts_val)):
            return datetime(1970, 1, 1, tzinfo=timezone.utc)
        if hasattr(ts_val, 'total_seconds'):
            return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=ts_val.total_seconds())
        if isinstance(ts_val, datetime):
            return ts_val if ts_val.tzinfo else ts_val.replace(tzinfo=timezone.utc)
        try:
            ts_str = str(ts_val).replace(" ", "T")
            if "." not in ts_str and "T" in ts_str:
                ts_str += ".000000"
            dt = datetime.fromisoformat(ts_str)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return datetime(1970, 1, 1, tzinfo=timezone.utc)

    # =====================================================================
    # 3. SQL Query (с фильтром по data_file_id для изоляции)
    # =====================================================================
    file_filter = ""
    sql_params = {"data_type_id": data_type_id}
    if data_file_id:
        file_filter = "AND stg.data_file_id = :data_file_id"
        sql_params["data_file_id"] = data_file_id

    sql_fetch = text(f"""
        SELECT stg.hash_sk, stg.h_object_property_sk, stg.load_dttm,
               stg.data_file_id, stg.data_source_id, stg.main_db_id,
               stg.value, stg.{timestamp_column_name} AS base_timestamp
        FROM public.{stg_table_name} stg
        WHERE stg.h_data_type_sk = :data_type_id
          AND stg.hash_sk IS NOT NULL
          AND stg.value IS NOT NULL
          {file_filter}
        ORDER BY stg.hash_sk
    """)

    log.info(f"📥 STREAMING BODE (file={data_file_id}): "
             f"ch_batch={ch_batch_size:,}, s3_batch={s3_batch_size:,}, read_chunk={read_chunk_size}")

    # 🔥 Фиксируем snapshot ОДИН раз на весь прогон
    base_seq = table_metadata.get("last-sequence-number", 0) + 1
    base_snap_id = abs(hash(f"{table_path}_{datetime.now(timezone.utc).isoformat()}_{uuid.uuid4().hex}"))
    all_manifests = []
    total_s3_rows = 0
    total_source_records = 0
    total_points = 0
    error_count = 0

    # 🔥 Column-wise буферы (dict-of-lists)
    ch_buf = {
        'data_record_sk': [], 'h_object_property_sk': [], 'data_file_id': [],
        'data_source_id': [], 'main_db_id': [], 'load_dttm': [],
        'magnitude_value': [], 'phase_value': [], 'turnover_frequency_value': [], 'timestamp': []
    }
    s3_partition_buffers: Dict[Tuple[str, date], Dict[str, list]] = {}
    s3_buffered_rows = 0

    def _flush_ch():
        if not ch_buf['data_record_sk']: return
        df = pd.DataFrame(ch_buf)
        for col in ['data_record_sk', 'h_object_property_sk', 'data_file_id', 'data_source_id', 'main_db_id']:
            if col in df.columns: df[col] = df[col].astype(str)
        if 'timestamp' in df.columns: df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        if 'load_dttm' in df.columns: df['load_dttm'] = pd.to_datetime(df['load_dttm'], utc=True)
        for col in ['magnitude_value', 'phase_value', 'turnover_frequency_value']:
            if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
        cols_order = ['data_record_sk', 'h_object_property_sk', 'data_file_id', 'data_source_id', 'main_db_id',
                      'magnitude_value', 'phase_value', 'turnover_frequency_value', 'timestamp', 'load_dttm']
        for c in cols_order:
            if c not in df.columns: df[c] = None
        ch_client.insert_df(target_table_name, df[cols_order])
        log.info(f"  ✅ ClickHouse: {len(ch_buf['data_record_sk']):,} записей")
        for k in ch_buf:
            ch_buf[k] = []

    def _flush_s3():
        nonlocal s3_partition_buffers, s3_buffered_rows, all_manifests, total_s3_rows
        if not s3_partition_buffers: return
        for (fid, day), buf in list(s3_partition_buffers.items()):
            if not buf['data_record_sk']: continue
            m = _flush_common_partition_to_s3(
                day, buf, s3_client, file_io, bucket, data_prefix, metadata_prefix,
                partition_spec, True, iceberg_type, value_is_nullable, pa_schema,
                table_metadata, fid, base_snap_id, base_seq
            )
            if m: all_manifests.append(m)
        total_s3_rows += s3_buffered_rows
        log.info(f"  ✅ S3: {s3_buffered_rows:,} записей → {len(all_manifests)} файлов всего")
        s3_partition_buffers = {}
        s3_buffered_rows = 0
        gc.collect()
        try:
            pa.default_memory_pool().release_unused()
        except Exception:
            pass

    # =====================================================================
    # 4. Main Streaming Loop
    # =====================================================================
    try:
        with dwh_engine.begin() as conn:
            result = conn.execution_options(stream_results=True, yield_per=read_chunk_size).execute(
                sql_fetch, sql_params
            )

            for row in result:
                hash_sk, h_obj_prop_sk, load_dttm, df_id, data_src_id, main_db_id, bytea_val, base_timestamp = row
                total_source_records += 1

                try:
                    if not bytea_val:
                        continue
                    blob_bytes = bytes(bytea_val) if hasattr(bytea_val, 'tobytes') else bytea_val
                    bode = deserialize_bode_blob(blob_bytes)

                    mag = float(bode.get('magnitude')) if bode.get('magnitude') is not None else None
                    phase = float(bode.get('phase')) if bode.get('phase') is not None else None
                    freq = float(bode.get('turnover_frequency')) if bode.get('turnover_frequency') is not None else None
                except Exception as e:
                    error_count += 1
                    log.warning(f"⚠️ Failed to deserialize BODE hash_sk={hash_sk}: {e}")
                    continue

                # 🔥 BODE: 1 строка staging = 1 точка в target
                rec_sk_str = ensure_uuid_string(hash_sk)
                h_obj_prop_str = ensure_uuid_string(h_obj_prop_sk)
                file_id_str = ensure_uuid_string(df_id)
                src_id_str = ensure_uuid_string(data_src_id)
                main_id_str = ensure_uuid_string(main_db_id) if main_db_id else ""
                norm_ld = _normalize_ts(load_dttm)
                norm_ts = _normalize_ts(base_timestamp)
                day_key = norm_ts.date()

                # 🔥 CH буфер (column-wise append)
                ch_buf['data_record_sk'].append(rec_sk_str)
                ch_buf['h_object_property_sk'].append(h_obj_prop_str)
                ch_buf['data_file_id'].append(file_id_str)
                ch_buf['data_source_id'].append(src_id_str)
                ch_buf['main_db_id'].append(main_id_str)
                ch_buf['load_dttm'].append(norm_ld)
                ch_buf['magnitude_value'].append(mag)
                ch_buf['phase_value'].append(phase)
                ch_buf['turnover_frequency_value'].append(freq)
                ch_buf['timestamp'].append(norm_ts)

                if len(ch_buf['data_record_sk']) >= ch_batch_size:
                    _flush_ch()

                # 🔥 S3 буфер (по партициям file_id+day)
                key = (file_id_str, day_key)
                if key not in s3_partition_buffers:
                    s3_partition_buffers[key] = {k: [] for k in ch_buf.keys()}
                buf = s3_partition_buffers[key]
                buf['data_record_sk'].append(rec_sk_str)
                buf['h_object_property_sk'].append(h_obj_prop_str)
                buf['data_file_id'].append(file_id_str)
                buf['data_source_id'].append(src_id_str)
                buf['main_db_id'].append(main_id_str)
                buf['load_dttm'].append(norm_ld.isoformat() if isinstance(norm_ld, datetime) else str(norm_ld))
                buf['magnitude_value'].append(mag)
                buf['phase_value'].append(phase)
                buf['turnover_frequency_value'].append(freq)
                buf['timestamp'].append(norm_ts)
                s3_buffered_rows += 1
                total_points += 1

                if s3_buffered_rows >= s3_batch_size:
                    _flush_s3()

                # 🔥 Освобождаем память после каждого BLOB'а
                del blob_bytes, bode
                if total_source_records % 100 == 0:
                    gc.collect()

                if total_source_records % 500 == 0:
                    log.info(f"📊 BODE progress: {total_points:,} записей из {total_source_records} BLOB'ов")

        # Финальные флеши
        if ch_buf['data_record_sk']:
            _flush_ch()
        if s3_partition_buffers:
            _flush_s3()

        # ОДИН commit snapshot на весь прогон
        if all_manifests:
            log.info(f"📝 Commit snapshot: {len(all_manifests)} манифестов, {total_s3_rows:,} записей")
            ml = _write_manifest_list(
                s3_client, file_io, bucket, metadata_prefix,
                all_manifests, table_metadata, partition_spec, base_snap_id, base_seq
            )
            _commit_snapshot(
                s3_client, bucket, table_path, metadata_prefix, table_metadata,
                ml, all_manifests, total_s3_rows, base_snap_id, base_seq
            )

    except Exception as e:
        log.error(f"❌ BODE streaming failed: {e}", exc_info=True)
        raise
    finally:
        ch_client.close()

    log.info(f"✅ BODE completed: {total_points:,} записей из {total_source_records} BLOB'ов, errors={error_count}")

    return {
        'data_file_id': data_file_id,
        'stg_table_name': stg_table_name,
        'target_table_name': target_table_name,
        'records_processed': total_source_records,
        'points_inserted': total_points,
        'errors': error_count,
        'manifests_created': len(all_manifests),
        's3_rows': total_s3_rows,
    }


    

def merge_diagnostic_array_data(
    stg_table_name: str,
    target_table_name: str,
    main_db_name: str,
    is_hist_data: bool = None,     # 🔥 Автоопределяется по имени таблицы
    data_file_id: str = None,
    data_type_id: str = "5550953e-6500-4d3f-f5a0-00a7f1034c0f",
    # =====================================================================
    # 🔥 ПАРАМЕТРЫ БАТЧИНГА (вынесены в сигнатуру)
    # =====================================================================
    ch_batch_size: int = 2_000_000,    # Строк за insert_df в CH (строки тяжелее float64)
    s3_batch_size: int = 30_000_000,   # Строк на один Parquet-файл
    read_chunk_size: int = 100,        # BLOB'ов за раз (yield_per)
) -> dict:
    """
    🔥 STREAMING merge Diagnostic Array из staging СРАЗУ в ClickHouse и S3 (Iceberg).
    Десериализует BLOB в массив дефектов, раскладывает в column-wise буферы.
    Использует ОДИН snapshot на весь прогон и защищён от OOM.
    """
    import gc, uuid, boto3, pandas as pd, numpy as np, clickhouse_connect
    from botocore.config import Config
    from datetime import datetime, timezone, date, timedelta
    from typing import Dict, Tuple, Any, List
    from sqlalchemy import text
    from airflow.hooks.base import BaseHook
    import pyarrow as pa
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import DayTransform, IdentityTransform
    from pyiceberg.types import IntegerType
    from pyiceberg.io.pyarrow import PyArrowFileIO

    if data_file_id is not None and type(data_file_id).__name__ in ('XComArg', 'PlainXComArg'):
        raise TypeError("❌ Передан XComArg вместо строки!")

    # 🔥 АВТООПРЕДЕЛЕНИЕ is_hist_data
    if is_hist_data is None:
        is_hist_data = '_hist' in stg_table_name.lower()

    ZERO_UUID = '00000000-0000-0000-0000-000000000000'
    UUID_COLS = ['data_record_sk', 'h_object_property_sk', 'data_source_id', 'data_file_id',
                 'main_db_id', 'diagnostic_data_id', 'h_defect_state_sk']
    STR_COLS = ['tag_name', 'defect_name', 'defect_details', 'group_name', 'recommendations']

    dwh_engine = get_dwh_engine('cloudberry_test_dwh')
    timestamp_column_name = 'timestamp' if is_hist_data else 'date'

    # =====================================================================
    # 1. ClickHouse Client Setup
    # =====================================================================
    conn_config = BaseHook.get_connection('clickhouse_conn')
    ch_client = clickhouse_connect.get_client(
        host=conn_config.host, port=8123, database=conn_config.schema or 'default',
        username=conn_config.login or 'default', password=conn_config.password or '',
        compress=True, query_limit=0,
    )

    # =====================================================================
    # 2. S3 / Iceberg Setup
    # =====================================================================
    s3_conn = BaseHook.get_connection('minio_conn')
    s3_endpoint = s3_conn.extra_dejson.get("host", "http://minio:9000")
    if not s3_endpoint.startswith("http"):
        s3_endpoint = f"http://{s3_endpoint}"
    bucket = s3_conn.extra_dejson.get("bucket", "iceberg-warehouse")
    region = s3_conn.extra_dejson.get("region", "us-east-1")

    s3_client = boto3.client(
        "s3", endpoint_url=s3_endpoint,
        aws_access_key_id=s3_conn.login, aws_secret_access_key=s3_conn.password,
        region_name=region, config=Config(signature_version="s3v4", s3={"addressing_style": "path"})
    )
    file_io = PyArrowFileIO(properties={
        "s3.endpoint": s3_endpoint, "s3.access-key-id": s3_conn.login,
        "s3.secret-access-key": s3_conn.password, "s3.region": region,
        "s3.path-style-access": "true"
    })

    hist_path = 'history-data' if is_hist_data else 'non-history-data'
    table_path = f"{main_db_name}/{hist_path}/{target_table_name}"
    metadata_prefix = f"{table_path}/metadata"
    data_prefix = f"{table_path}/data"

    iceberg_type = IntegerType()
    pa_type = pa.int32()
    value_is_nullable = True  # priority может быть NULL

    table_metadata = _init_or_load_table_metadata(
        s3_client, bucket, table_path, metadata_prefix,
        iceberg_type, pa_type, partition_by_source=True
    )

    pa_schema = pa.schema([
        pa.field("data_record_sk", pa.string(), nullable=False),
        pa.field("h_object_property_sk", pa.string(), nullable=True),
        pa.field("data_file_id", pa.string(), nullable=False),
        pa.field("data_source_id", pa.string(), nullable=False),
        pa.field("main_db_id", pa.string(), nullable=True),
        pa.field("timestamp", pa.timestamp("us"), nullable=True),
        pa.field("diagnostic_data_id", pa.string(), nullable=True),
        pa.field("h_defect_state_sk", pa.string(), nullable=True),
        pa.field("priority", pa.int32(), nullable=True),
        pa.field("tag_name", pa.string(), nullable=True),
        pa.field("defect_name", pa.string(), nullable=True),
        pa.field("defect_details", pa.string(), nullable=True),
        pa.field("group_name", pa.string(), nullable=True),
        pa.field("recommendations", pa.string(), nullable=True),
        pa.field("load_dttm", pa.string(), nullable=True),
    ])

    def _get_field_id(metadata_dict: dict, field_names: list, schema_obj: pa.Schema) -> int:
        def find_field_id(obj, target_names):
            if isinstance(obj, dict):
                if obj.get("name") in target_names and "id" in obj: return obj.get("id")
                for v in obj.values():
                    res = find_field_id(v, target_names)
                    if res is not None: return res
            elif isinstance(obj, list):
                for item in obj:
                    res = find_field_id(item, target_names)
                    if res is not None: return res
            return None
        found_id = find_field_id(metadata_dict, field_names)
        if found_id is not None: return found_id
        for i, field in enumerate(schema_obj):
            if field.name in field_names: return i + 1
        raise ValueError(f"Field {field_names} not found")

    ts_field_id = _get_field_id(table_metadata, ["timestamp", "date", "base_timestamp"], pa_schema)
    src_field_id = _get_field_id(table_metadata, ["data_source_id", "source_id"], pa_schema)

    partition_spec = PartitionSpec(
        PartitionField(source_id=ts_field_id, field_id=1000, transform=DayTransform(), name="dt_day"),
        PartitionField(source_id=src_field_id, field_id=1001, transform=IdentityTransform(), name="source_id"),
        spec_id=0
    )

    def _normalize_ts(ts_val: Any) -> datetime:
        if ts_val is None or (isinstance(ts_val, float) and pd.isna(ts_val)):
            return datetime(1970, 1, 1, tzinfo=timezone.utc)
        if hasattr(ts_val, 'total_seconds'):
            return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=ts_val.total_seconds())
        if isinstance(ts_val, datetime):
            return ts_val if ts_val.tzinfo else ts_val.replace(tzinfo=timezone.utc)
        try:
            ts_str = str(ts_val).replace(" ", "T")
            if "." not in ts_str and "T" in ts_str:
                ts_str += ".000000"
            dt = datetime.fromisoformat(ts_str)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return datetime(1970, 1, 1, tzinfo=timezone.utc)

    def _get_diag_field(rec, key, default=None):
        if isinstance(rec, dict): return rec.get(key, default)
        return getattr(rec, key, default)

    # =====================================================================
    # 3. SQL Query (с фильтром по data_file_id для изоляции)
    # =====================================================================
    file_filter = ""
    sql_params = {"data_type_id": data_type_id}
    if data_file_id:
        file_filter = "AND stg.data_file_id = :data_file_id"
        sql_params["data_file_id"] = data_file_id

    sql_fetch = text(f"""
        SELECT stg.hash_sk, stg.h_object_property_sk, stg.load_dttm,
               stg.data_file_id, stg.data_source_id, stg.main_db_id,
               stg.value, stg.{timestamp_column_name} AS base_timestamp
        FROM public.{stg_table_name} stg
        WHERE stg.h_data_type_sk = :data_type_id
          AND stg.hash_sk IS NOT NULL
          AND stg.value IS NOT NULL
          {file_filter}
        ORDER BY stg.hash_sk
    """)

    log.info(f"📥 STREAMING DiagnosticArray (file={data_file_id}): "
             f"ch_batch={ch_batch_size:,}, s3_batch={s3_batch_size:,}, read_chunk={read_chunk_size}")

    # 🔥 Фиксируем snapshot ОДИН раз на весь прогон
    base_seq = table_metadata.get("last-sequence-number", 0) + 1
    base_snap_id = abs(hash(f"{table_path}_{datetime.now(timezone.utc).isoformat()}_{uuid.uuid4().hex}"))
    all_manifests = []
    total_s3_rows = 0
    total_source = 0
    total_processed = 0
    error_count = 0

    # 🔥 Column-wise буферы (dict-of-lists)
    ch_buf = {
        'data_record_sk': [], 'h_object_property_sk': [], 'data_file_id': [],
        'data_source_id': [], 'main_db_id': [], 'timestamp': [],
        'diagnostic_data_id': [], 'h_defect_state_sk': [], 'priority': [],
        'tag_name': [], 'defect_name': [], 'defect_details': [],
        'group_name': [], 'recommendations': [], 'load_dttm': []
    }
    s3_partition_buffers: Dict[Tuple[str, date], Dict[str, list]] = {}
    s3_buffered_rows = 0

    def _flush_ch():
        if not ch_buf['data_record_sk']: return
        df = pd.DataFrame(ch_buf)
        for col in UUID_COLS:
            if col in df.columns: df[col] = df[col].fillna(ZERO_UUID).astype(str)
        for col in STR_COLS:
            if col in df.columns: df[col] = df[col].fillna('').astype(str)
        if 'priority' in df.columns:
            df['priority'] = pd.to_numeric(df['priority'], errors='coerce').fillna(0).astype('int32')
        if 'timestamp' in df.columns: df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        if 'load_dttm' in df.columns: df['load_dttm'] = pd.to_datetime(df['load_dttm'], utc=True)
        cols_order = ['data_record_sk', 'h_object_property_sk', 'data_source_id', 'data_file_id', 'main_db_id',
                      'timestamp', 'diagnostic_data_id', 'h_defect_state_sk', 'priority',
                      'tag_name', 'defect_name', 'defect_details', 'group_name', 'recommendations', 'load_dttm']
        for c in cols_order:
            if c not in df.columns: df[c] = None
        ch_client.insert_df(target_table_name, df[cols_order])
        log.info(f"  ✅ ClickHouse: {len(ch_buf['data_record_sk']):,} записей")
        for k in ch_buf:
            ch_buf[k] = []

    def _flush_s3():
        nonlocal s3_partition_buffers, s3_buffered_rows, all_manifests, total_s3_rows
        if not s3_partition_buffers: return
        for (fid, day), buf in list(s3_partition_buffers.items()):
            if not buf['data_record_sk']: continue
            m = _flush_common_partition_to_s3(
                day, buf, s3_client, file_io, bucket, data_prefix, metadata_prefix,
                partition_spec, True, iceberg_type, value_is_nullable, pa_schema,
                table_metadata, fid, base_snap_id, base_seq
            )
            if m: all_manifests.append(m)
        total_s3_rows += s3_buffered_rows
        log.info(f"  ✅ S3: {s3_buffered_rows:,} записей → {len(all_manifests)} файлов всего")
        s3_partition_buffers = {}
        s3_buffered_rows = 0
        gc.collect()
        try:
            pa.default_memory_pool().release_unused()
        except Exception:
            pass

    # =====================================================================
    # 4. Main Streaming Loop
    # =====================================================================
    try:
        with dwh_engine.begin() as conn:
            result = conn.execution_options(stream_results=True, yield_per=read_chunk_size).execute(
                sql_fetch, sql_params
            )

            for row in result:
                hash_sk, h_obj_prop_sk, load_dttm, df_id, data_src_id, main_db_id, bytea_val, base_timestamp = row
                total_source += 1

                try:
                    if not bytea_val:
                        continue
                    blob_bytes = bytes(bytea_val) if hasattr(bytea_val, 'tobytes') else bytea_val
                    diagnostic_records = deserialize_diagnostic_array_blob(blob_bytes)
                    del blob_bytes
                    if not diagnostic_records:
                        continue
                except Exception as e:
                    error_count += 1
                    log.warning(f"⚠️ Failed to deserialize DiagnosticArray hash_sk={hash_sk}: {e}")
                    continue

                # 🔥 Общие поля для всех дефектов этого рекорда (один раз)
                rec_sk_str = ensure_uuid_string(hash_sk)
                h_obj_prop_str = ensure_uuid_string(h_obj_prop_sk)
                file_id_str = ensure_uuid_string(df_id)
                src_id_str = ensure_uuid_string(data_src_id)
                main_db_str = ensure_uuid_string(main_db_id) if main_db_id else ZERO_UUID
                norm_ld = _normalize_ts(load_dttm)
                norm_base_ts = _normalize_ts(base_timestamp)

                # 🔥 Раскладываем каждый дефект в column-wise буферы
                for diag_rec in diagnostic_records:
                    diag_ts_raw = _get_diag_field(diag_rec, 'timestamp')
                    row_ts = _normalize_ts(diag_ts_raw) if diag_ts_raw else norm_base_ts
                    day_key = row_ts.date()

                    try:
                        priority_val = int(_get_diag_field(diag_rec, 'priority', 0) or 0)
                    except (ValueError, TypeError):
                        priority_val = 0

                    diag_data_id = _get_diag_field(diag_rec, 'diagnostic_data_id')
                    defect_state = _get_diag_field(diag_rec, 'h_defect_state_sk')

                    # CH буфер (column-wise append)
                    ch_buf['data_record_sk'].append(rec_sk_str)
                    ch_buf['h_object_property_sk'].append(h_obj_prop_str)
                    ch_buf['data_file_id'].append(file_id_str)
                    ch_buf['data_source_id'].append(src_id_str)
                    ch_buf['main_db_id'].append(main_db_str)
                    ch_buf['timestamp'].append(row_ts)
                    ch_buf['diagnostic_data_id'].append(
                        ensure_uuid_string(diag_data_id) if diag_data_id else ZERO_UUID
                    )
                    ch_buf['h_defect_state_sk'].append(
                        ensure_uuid_string(defect_state) if defect_state else ZERO_UUID
                    )
                    ch_buf['priority'].append(priority_val)
                    ch_buf['tag_name'].append(str(_get_diag_field(diag_rec, 'tag_name', '') or ''))
                    ch_buf['defect_name'].append(str(_get_diag_field(diag_rec, 'defect_name', '') or ''))
                    ch_buf['defect_details'].append(str(_get_diag_field(diag_rec, 'defect_details', '') or ''))
                    ch_buf['group_name'].append(str(_get_diag_field(diag_rec, 'group_name', '') or ''))
                    ch_buf['recommendations'].append(str(_get_diag_field(diag_rec, 'recommendations', '') or ''))
                    ch_buf['load_dttm'].append(norm_ld)

                    if len(ch_buf['data_record_sk']) >= ch_batch_size:
                        _flush_ch()

                    # S3 буфер (по партициям file_id+day)
                    key = (file_id_str, day_key)
                    if key not in s3_partition_buffers:
                        s3_partition_buffers[key] = {k: [] for k in ch_buf.keys()}
                    buf = s3_partition_buffers[key]
                    buf['data_record_sk'].append(rec_sk_str)
                    buf['h_object_property_sk'].append(h_obj_prop_str)
                    buf['data_file_id'].append(file_id_str)
                    buf['data_source_id'].append(src_id_str)
                    buf['main_db_id'].append(main_db_str)
                    buf['timestamp'].append(row_ts)
                    buf['diagnostic_data_id'].append(
                        ensure_uuid_string(diag_data_id) if diag_data_id else ZERO_UUID
                    )
                    buf['h_defect_state_sk'].append(
                        ensure_uuid_string(defect_state) if defect_state else ZERO_UUID
                    )
                    buf['priority'].append(priority_val)
                    buf['tag_name'].append(str(_get_diag_field(diag_rec, 'tag_name', '') or ''))
                    buf['defect_name'].append(str(_get_diag_field(diag_rec, 'defect_name', '') or ''))
                    buf['defect_details'].append(str(_get_diag_field(diag_rec, 'defect_details', '') or ''))
                    buf['group_name'].append(str(_get_diag_field(diag_rec, 'group_name', '') or ''))
                    buf['recommendations'].append(str(_get_diag_field(diag_rec, 'recommendations', '') or ''))
                    buf['load_dttm'].append(norm_ld.isoformat() if isinstance(norm_ld, datetime) else str(norm_ld))
                    s3_buffered_rows += 1
                    total_processed += 1

                    if s3_buffered_rows >= s3_batch_size:
                        _flush_s3()

                # 🗑️ Освобождаем память после каждого BLOB'а
                del diagnostic_records
                if total_source % 100 == 0:
                    gc.collect()

                if total_source % 500 == 0:
                    log.info(f"📊 Diagnostic progress: {total_processed:,} записей из "
                             f"{total_source} BLOB'ов (file={data_file_id[:8]})")

        # Финальные флеши
        if ch_buf['data_record_sk']:
            _flush_ch()
        if s3_partition_buffers:
            _flush_s3()

        # ОДИН commit snapshot на весь прогон
        if all_manifests:
            log.info(f"📝 Commit snapshot: {len(all_manifests)} манифестов, {total_s3_rows:,} записей")
            ml = _write_manifest_list(
                s3_client, file_io, bucket, metadata_prefix,
                all_manifests, table_metadata, partition_spec, base_snap_id, base_seq
            )
            _commit_snapshot(
                s3_client, bucket, table_path, metadata_prefix, table_metadata,
                ml, all_manifests, total_s3_rows, base_snap_id, base_seq
            )

    except Exception as e:
        log.error(f"❌ DiagnosticArray streaming failed for {data_file_id}: {e}", exc_info=True)
        raise
    finally:
        ch_client.close()

    log.info(f"✅ DiagnosticArray [{data_file_id[:8] if data_file_id else '?'}] completed: "
             f"{total_processed:,} записей из {total_source} BLOB'ов, errors={error_count}")

    return {
        'data_file_id': data_file_id,
        'stg_table_name': stg_table_name,
        'target_table_name': target_table_name,
        'source_records': total_source,
        'records_processed': total_processed,
        'errors': error_count,
        'manifests_created': len(all_manifests),
        's3_rows': total_s3_rows,
    }

    
def extract_diag_data_to_staging(
    sqlite_path: str,
    entity_config: Dict[str, Any],
    data_file_id: str,
    data_source_id: str,
    main_db_id: str
) -> bool:
    # Парсим конфигурацию
    if 'source_column_descriptions' in entity_config:
        config = read_entity_config(entity_config)
    else:
        config = entity_config.copy()
        config['source_column_names'] = [c.lower() for c in config.get('source_column_names', [])]
        config['column_types'] = {k.lower(): v for k, v in config.get('column_types', {}).items()}

    # Извлекаем параметры из конфигурации
    source_table_name = config['source_table_name']
    staging_table_name = config['staging_table_name']
    source_column_names = config['source_column_names']
    column_types = config.get('column_types', {})
    link_columns_dict = config.get('link_columns', {})
    link_columns = []
    
    for col_dict in link_columns_dict:
        link_columns.append(col_dict.get('column_name', ''))
    
    db_path = sqlite_path if sqlite_path.startswith("sqlite:///") else f"sqlite:///{sqlite_path}"
    sqlite_engine = create_engine(db_path, echo=False)
    dwh_engine = get_dwh_engine(DWH_CONN_ID)
    
    try:
        log.info(f"Extract data from sqlite started")
        
        # ИСПРАВЛЕНИЕ: Экранируем все колонки
        quoted_cols = [f'"{col}"' for col in source_column_names]
        query = f"SELECT {', '.join(quoted_cols)} FROM \"{source_table_name}\""
        df = pd.read_sql(query, sqlite_engine)
        
        log.info(f"Data transform for staging starts")
        df.columns = df.columns.str.lower()

        if df.empty:
            log.info(f"⚠️ No data to process in SQLite for entity '{source_table_name}' - returning early")
            return True

        df['data_file_id'] = data_file_id
        df['data_source_id'] = data_source_id
        df['main_db_id'] =  main_db_id

        # ==========================================
        # 1. НОРМАЛИЗАЦИЯ ЗНАЧЕНИЙ (ВЕКТОРИЗАЦИЯ)
        # ==========================================
        for col in df.columns:
            if col in ['main_db_id','data_file_id', 'data_source_id', 'hash_sk', 'hash_sat_diff', 'load_dttm']:
                continue
            target_type = column_types.get(col, 'string')
            
            # 🔥 ИСПРАВЛЕНИЕ: NULL/NaN → пустая строка для string типа
            if target_type == 'string':
                # 1. Сначала заполняем все None/NaN пустой строкой ДО astype(str)
                df[col] = df[col].fillna('')
                # 2. Конвертируем в строку и убираем пробелы
                df[col] = df[col].astype(str).str.strip()
                # 3. Заменяем строковые представления "пустоты" на реальную пустую строку
                df[col] = df[col].replace({
                    'nan': '', 
                    'None': '', 
                    'null': '', 
                    'NaT': '',
                    '<NA>': ''
                })
            elif target_type == 'integer':
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            elif target_type == 'float':
                df[col] = pd.to_numeric(df[col], errors='coerce')
            elif target_type == 'timestamp':
                if df[col].dtype == 'object':
                    df[col] = df[col].replace({
                        'nan': None, 'None': None, 'NaT': None, '': None, 'null': None
                    })
                    df[col] = df[col].astype(str).str.replace(
                        r'(\.\d{6})\d+', r'\1', regex=True
                    )
                    df[col] = df[col].replace({'nan': None, 'None': None})
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce', format='mixed')
                except (ValueError, TypeError):
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                if df[col].dt.tz is None:
                    df[col] = df[col].dt.tz_localize('UTC')
                fallback_date = pd.Timestamp('1970-01-01', tz='UTC')
                nat_count = df[col].isna().sum()
                if nat_count > 0:
                    log.warning(f"⚠️ Column '{col}': {nat_count} values replaced with fallback date")
                df[col] = df[col].fillna(fallback_date)
            elif target_type == 'boolean':
                df[col] = df[col].astype(str).str.lower().isin(['true', '1', 't', 'yes'])
            else:
                df[col] = df[col].map(lambda x: normalize_value(x, target_type))

        # ==========================================
        # 2. РАСЧЕТ hash_sk (ОПТИМИЗАЦИЯ)
        # ==========================================
        # Конвертируем в список словарей ОДИН раз
        all_records = df.to_dict('records')
        
        # hash_diag_alarm_sk
        df['hash_diag_alarm_sk'] = [
            calc_hash_sk({k: row[k] for k in ['diagalarmid', 'date', 'alarmstate', 'main_db_id']})
            for row in all_records
        ]
        
        # hash_diag_data_sk
        df['hash_diag_data_sk'] = [
            calc_hash_sk({k: row[k] for k in ['diagid', 'date', 'main_db_id']})
            for row in all_records
        ]
        
        # ==========================================
        # 3. РАСЧЕТ hash_sat_diff (ОПТИМИЗАЦИЯ)
        # ==========================================
        # hash_sat_diag_alarm_diff
        df['hash_sat_diag_alarm_diff'] = [
            calc_hash_sat_diff(
                {k: row[k] for k in ['date', 'confirmed', 'comment', 'main_db_id']},
                data_file_id
            )
            for row in all_records
        ]
        
        # hash_sat_diag_data_diff
        df['hash_sat_diag_data_diff'] = [
            calc_hash_sat_diff(
                {k: row[k] for k in ['diagtagname', 'defectname', 'defectdetails', 'recommendation', 'priority', 'groupname', 'main_db_id']},
                data_file_id
            )
            for row in all_records
        ]
        
        df['load_dttm'] = datetime.now(timezone.utc)
        
        # ==========================================
        # 🔥 ИСПРАВЛЕНИЕ: ПЕРЕЗАПИСЫВАЕМ all_records
        # ==========================================
        all_records = df.to_dict('records')

        # ==========================================
        # 4. ГЕНЕРАЦИЯ ЛИНК-КОЛОНОК (ОПТИМИЗАЦИЯ)
        # ==========================================
        df['l_diag_alarm_propertyid_sk'] = [
            calc_link_hash(row, 'propertyid', False, '', main_db_id, 'hash_diag_alarm_sk')
            for row in all_records
        ]
        df['propertyid_sk'] = [
            calc_hub_hash_for_column(row, 'propertyid', False, '', main_db_id)
            for row in all_records
        ]
        
        df['l_diag_alarm_alarmstate_sk'] = [
            calc_link_hash(row, 'alarmstate', True, 'h_diagnostic_alarm_states', main_db_id, 'hash_diag_alarm_sk')
            for row in all_records
        ]
        df['alarmstate_sk'] = [
            calc_hub_hash_for_column(row, 'alarmstate', True, 'h_diagnostic_alarm_states', main_db_id)
            for row in all_records
        ]
        
        df['l_diag_alarm_diag_data_sk'] = [
            calc_link_hash(row, 'diagid', False, '', main_db_id, 'hash_diag_alarm_sk')
            for row in all_records
        ]
        df['diag_data_sk'] = [
            calc_hub_hash_for_column(row, 'diagid', False, '', main_db_id)
            for row in all_records
        ]
        
        df['l_diag_data_defectstate_sk'] = [
            calc_link_hash(row, 'defectstate', True, 'h_diagnostic_defect_states', main_db_id, 'hash_diag_data_sk')
            for row in all_records
        ]
        df['defectstate_sk'] = [
            calc_hub_hash_for_column(row, 'defectstate', True, 'h_diagnostic_defect_states', main_db_id)
            for row in all_records
        ]
        
        df['l_diag_data_defecttype_sk'] = [
            calc_link_hash(row, 'defecttype', True, 'h_diagnostic_defect_types', main_db_id, 'hash_diag_data_sk')
            for row in all_records
        ]
        df['defecttype_sk'] = [
            calc_hub_hash_for_column(row, 'defecttype', True, 'h_diagnostic_defect_types', main_db_id)
            for row in all_records
        ]
        
        # ==========================================
        # 5. ЗАГРУЗКА В POSTGRESQL (ОПТИМИЗАЦИЯ)
        # ==========================================
        log.info(f"Data insertion starts")
        
        rows_loaded = df.to_sql(
            name=staging_table_name,
            con=dwh_engine,
            if_exists='append',
            index=False,
            method=pg_insert_values,
            chunksize=5_000_000
        )
        
        log.info(f"Data insertion ended. Rows loaded: {rows_loaded}")
        return True
        
    except SQLAlchemyError as e:
        raise AirflowException(f"DB error: {str(e)}")
    except Exception as e:
        raise
    finally:
        sqlite_engine.dispose()
        dwh_engine.dispose()
        log.debug("🔌 Database engines disposed")
    
    





def merge_staging_to_hub(
    hub_table_name: str, 
    sk_hub_column_name: str, 
    stg_table_name: str, 
    stg_hash_column_name: str = 'hash_sk',
    business_id_column_name: str ='ID' or None):

    dwh_engine = get_dwh_engine(DWH_CONN_ID)
    if business_id_column_name:
       business_id_stg_source = f"stg.{business_id_column_name},"
       hub_business_id = "business_id," 
    else:
        business_id_stg_source=""
        hub_business_id = ""
    try:
        with dwh_engine.begin() as conn:
            insert_hub_sql = text(f"""
                INSERT INTO {hub_table_name} (
                    {sk_hub_column_name},
                    {hub_business_id}
                    load_dttm,
                    data_file_id,
                    data_source_id,
                    main_db_id
                )
                SELECT 
                    stg.{stg_hash_column_name},
                    {business_id_stg_source}
                    stg.load_dttm,
                    stg.data_file_id,
                    stg.data_source_id,
                    stg.main_db_id
                FROM {stg_table_name} stg
                ON CONFLICT ({sk_hub_column_name}) DO NOTHING
            """)

            result = conn.execute(insert_hub_sql)
            hub_inserted = result.rowcount

            return {
                "hub_inserted": hub_inserted,
                "status": "success"
            }
            
    except SQLAlchemyError as e:
        log.error(f"❌ SQLAlchemy error during hub merge: {e}", exc_info=True)
        raise e
    except Exception as e:
        log.error(f"❌ Unexpected error during hub merge: {e}", exc_info=True)
        raise e



def merge_staging_to_satellite(
    sat_table_name: str, 
    hab_sk_column_name: str, 
    sat_column_names: list[str], 
    stg_table_name: str, 
    stg_column_names: list[str],
    stg_hash_sk_column_name: str = 'hash_sk',
    stg_hash_sat_diff_column_name: str = 'hash_sat_diff'
):

    dwh_engine = get_dwh_engine(DWH_CONN_ID)
    
    # Формируем списки колонок для INSERT и SELECT
    base_sat_cols = [
        hab_sk_column_name,
        'load_dttm',
        'valid_from_dttm', 
        'valid_to_dttm',
        'active_flag',
        'data_file_id',
        'data_source_id',
        'main_db_id',
        'hash_sat_diff'
    ]
    all_sat_cols = base_sat_cols + [c.lower() for c in sat_column_names]
    satellite_columns = ',\n                    '.join(all_sat_cols)
    
    base_select_cols = [
        f'stg.{stg_hash_sk_column_name}',
        'stg.load_dttm',
        'stg.load_dttm',  # valid_from_dttm
        'NULL',           # valid_to_dttm
        'true',           # active_flag
        'stg.data_file_id',
        'stg.data_source_id',
        'stg.main_db_id',
        f'stg.{stg_hash_sat_diff_column_name}'
    ]
    all_select_cols = base_select_cols + [f'stg.{c.lower()}' for c in stg_column_names]
    staging_columns = ',\n                    '.join(all_select_cols)
    
    try:
        with dwh_engine.begin() as conn:
            # ==================== 1. ЗАКРЫТИЕ: хэш изменился ====================
            # Находим активные записи в Satellite, для которых в Staging есть пара 
            # с тем же hab_sk + data_file_id, но РАЗНЫМ hash_sat_diff
            close_old_sat_sql = text(f"""
                UPDATE public.{sat_table_name} AS sat
                SET 
                    valid_to_dttm = NOW(),
                    active_flag = false
                FROM public.{stg_table_name} AS stg
                WHERE sat.{hab_sk_column_name} = stg.{stg_hash_sk_column_name}
                  AND sat.data_file_id = stg.data_file_id
                  AND sat.main_db_id = stg.main_db_id
                  AND sat.active_flag = true
                  AND sat.hash_sat_diff IS DISTINCT FROM stg.{stg_hash_sat_diff_column_name};
            """)
            
            result_close = conn.execute(close_old_sat_sql)
            sat_closed = result_close.rowcount
            log.info(f"✅ Closed {sat_closed} satellite records (hash changed)")

            # ==================== 2. ВСТАВКА: новые или измененные ====================
            # Вставляем только те записи Staging, для которых НЕТ активной записи в Satellite 
            # с точно таким же хэшем. Если хэш совпадает -> запись существует, пропускаем.
            insert_sat_sql = text(f"""
                INSERT INTO public.{sat_table_name} (
                    {satellite_columns}
                )
                SELECT
                    {staging_columns}
                FROM public.{stg_table_name} stg
                LEFT JOIN public.{sat_table_name} sat
                    ON stg.{stg_hash_sk_column_name} = sat.{hab_sk_column_name}
                    AND stg.data_file_id = sat.data_file_id
                    AND stg.main_db_id = sat.main_db_id
                    AND sat.active_flag = true
                    AND stg.{stg_hash_sat_diff_column_name} = sat.hash_sat_diff
                WHERE sat.{hab_sk_column_name} IS NULL;
            """)
            
            result_insert = conn.execute(insert_sat_sql)
            sat_inserted = result_insert.rowcount
            log.info(f"✅ Inserted {sat_inserted} satellite records")
              
            return {
                "sat_closed": sat_closed,
                "sat_inserted": sat_inserted,
                "status": "success"
            }
            
    except SQLAlchemyError as e:
        log.error(f"❌ SQLAlchemy error during satellite merge: {e}", exc_info=True)
        raise e
    except Exception as e:
        log.error(f"❌ Unexpected error during satellite merge: {e}", exc_info=True)
        raise e



def merge_staging_to_link(
    stg_table_name: str,
    link_table_name: str,
    link_sk_column_name: str,
    link_first_hub_sk_column_name: str,
    link_second_hub_sk_column_name: str,
    stg_link_sk_column_name: str,
    stg_first_hub_sk_column_name: str,
    stg_second_hub_sk_column_name: str
) -> bool:
    dwh_engine = get_dwh_engine(DWH_CONN_ID)
    
    # ==================== 1. ЗАКРЫТИЕ (отдельное соединение) ====================
    # 🔥 Убрали точку с запятой в конце и вынесли в отдельный with-блок
    sql_close = text(f"""
        WITH missing_in_stg AS (
            SELECT
                link.{link_sk_column_name} as link_sk_val,
                link.data_file_id as ds_id
            FROM public.{link_table_name} link
            LEFT JOIN public.{stg_table_name} stg
                ON link.{link_sk_column_name} = stg.{stg_link_sk_column_name}
            WHERE stg.{stg_link_sk_column_name} IS NULL
        )
        UPDATE public.{link_table_name} AS link
        SET
            valid_to_dttm = NOW(),
            active_flag = false
        FROM missing_in_stg mis
        WHERE link.{link_sk_column_name} = mis.link_sk_val
          AND link.data_file_id = mis.ds_id
          AND link.active_flag = true
    """)
    
    try:
        with dwh_engine.begin() as conn_close:
            result_close = conn_close.execute(sql_close)
            closed_count = result_close.rowcount
            result_close.close()  # 🔥 Явно закрываем CursorResult
        log.info(f"✅ Closed {closed_count} links (business key removed from staging)")
    except SQLAlchemyError as e:
        log.error(f"❌ SQLAlchemy error during link close: {e}", exc_info=True)
        raise e
    except Exception as e:
        log.error(f"❌ Unexpected error during link close: {e}", exc_info=True)
        raise e

    # ==================== 2. ВСТАВКА (отдельное соединение) ====================
    # 🔥 DISTINCT ON + ORDER BY для защиты от дублей
    sql_insert = text(f"""
        INSERT INTO public.{link_table_name} (
            {link_sk_column_name},
            load_dttm,
            valid_from_dttm,
            valid_to_dttm,
            active_flag,
            data_file_id,
            data_source_id,
            main_db_id,
            {link_first_hub_sk_column_name},
            {link_second_hub_sk_column_name}
        )
        SELECT DISTINCT ON (stg.{stg_link_sk_column_name})
            stg.{stg_link_sk_column_name},
            stg.load_dttm,
            stg.load_dttm,
            NULL,
            true,
            stg.data_file_id,
            stg.data_source_id,
            stg.main_db_id,
            stg.{stg_first_hub_sk_column_name},
            stg.{stg_second_hub_sk_column_name}
        FROM public.{stg_table_name} stg
        LEFT JOIN public.{link_table_name} link
            ON stg.{stg_link_sk_column_name} = link.{link_sk_column_name}
        WHERE link.{link_sk_column_name} IS NULL
        ORDER BY stg.{stg_link_sk_column_name}, stg.load_dttm DESC
    """)
    
    try:
        with dwh_engine.begin() as conn_insert:
            result_insert = conn_insert.execute(sql_insert)
            inserted_count = result_insert.rowcount
            result_insert.close()  # 🔥 Явно закрываем CursorResult
        log.info(f"✅ Inserted {inserted_count} new links")
    except SQLAlchemyError as e:
        log.error(f"❌ SQLAlchemy error during link upload: {e}", exc_info=True)
        raise e
    except Exception as e:
        log.error(f"❌ Unexpected error during link upload: {e}", exc_info=True)
        raise e

    log.info(f"🎉 Link upload to '{link_table_name}' completed")
    return True





def load_common_object_data_values_to_datamart(
    dwh_table_name: str,
    data_mart_table_name: str,
    data_type: str,
    value_is_nullable: bool = False,
    value_default_on_null: Any = None,
    fetch_batch_size: int = 500000,  # ← Добавлен параметр
) -> bool:
    """
    Загружает неэкспортированные записи из Cloudberry в ClickHouse батчами.
    Избежает OOM при обработке больших объёмов данных.
    """
    import gc
    
    total_loaded = 0
    total_marked = 0
    iteration = 0
    
    # KEYSET PAGINATION для безопасного извлечения
    last_timestamp = None
    last_record_id = None
    
    log.info(f"🚀 Старт загрузки: {dwh_table_name} → {data_mart_table_name}")
    
    while True:
        iteration += 1
        
        # 1️⃣ Извлечение батча с keyset pagination
        if last_timestamp is None:
            extract_sql = text(f"""
                SELECT h.data_record_sk, h.h_object_property_sk, h.data_file_id, 
                       h.data_source_id, h.load_dttm, h.value, h.timestamp
                FROM public.{dwh_table_name} h
                WHERE h.exported_to_datamart = FALSE
                ORDER BY h.timestamp ASC, h.data_record_sk ASC
                LIMIT :limit
            """)
            params = {"limit": fetch_batch_size}
        else:
            extract_sql = text(f"""
                SELECT h.data_record_sk, h.h_object_property_sk, h.data_file_id, 
                       h.data_source_id, h.load_dttm, h.value, h.timestamp
                FROM public.{dwh_table_name} h
                WHERE h.exported_to_datamart = FALSE
                  AND (h.timestamp, h.data_record_sk) > (:last_ts, :last_id)
                ORDER BY h.timestamp ASC, h.data_record_sk ASC
                LIMIT :limit
            """)
            params = {"limit": fetch_batch_size, "last_ts": last_timestamp, "last_id": last_record_id}
        
        try:
            with cloudberry_engine.begin() as conn:
                result = conn.execute(extract_sql, params)
                rows = result.fetchall()
        except Exception as e:
            log.error(f"❌ Ошибка извлечения батча #{iteration}: {e}", exc_info=True)
            raise
        
        if not rows:
            log.info(f"✅ Все записи обработаны (итераций: {iteration})")
            break
        
        log.info(f"📥 Батч #{iteration}: извлечено {len(rows)} записей")
        
        # 2️⃣ Подготовка данных для ClickHouse
        records = [
            {
                'data_record_sk': ensure_uuid_string(row[0]),
                'h_object_property_sk': ensure_uuid_string(row[1]),
                'data_file_id': ensure_uuid_string(row[2]),
                'data_source_id': ensure_uuid_string(row[3]),
                'load_dttm': row[4],
                'value': row[5],
                'timestamp': row[6],
            }
            for row in rows
        ]
        
        exported_record_ids = [rec['data_record_sk'] for rec in records]
        
        prepared_records = [
            prepare_record_for_clickhouse(
                rec, data_type, value_is_nullable, value_default_on_null
            )
            for rec in records
        ]
        
        # 3️⃣ Загрузка в ClickHouse
        client = None
        try:
            client = create_clickhouse_client()
            success = insert_to_clickhouse(
                client, data_mart_table_name, prepared_records, data_type
            )
            if success:
                total_loaded += len(prepared_records)
                log.info(f"✅ Батч #{iteration}: загружено {len(prepared_records)} записей в ClickHouse")
        finally:
            if client:
                try:
                    client.disconnect()
                except:
                    pass
        
        # 4️⃣ Пометка экспортированных (батчами внутри функции)
        if exported_record_ids:
            try:
                updated = mark_records_as_exported(
                    cloudberry_engine,
                    dwh_table_name,
                    exported_record_ids,
                    batch_size=500000  # ← Явно указываем размер батча
                )
                total_marked += updated
            except Exception as e:
                log.error(f"⚠️ Не удалось обновить флаги для батча #{iteration}: {e}", exc_info=True)
        
        # 5️⃣ Обновление курсора для keyset pagination
        if rows:
            last_timestamp = rows[-1][6]  # timestamp
            last_record_id = ensure_uuid_string(rows[-1][0])  # data_record_sk
        
        # 6️⃣ Очистка памяти
        del rows, records, prepared_records, exported_record_ids
        gc.collect()
    
    log.info(f"🎉 Загрузка в '{data_mart_table_name}' завершена:")
    log.info(f"   • Всего загружено: {total_loaded}")
    log.info(f"   • Всего помечено: {total_marked}")
    
    return True

def load_creyt_time_series_to_datamart(
    dwh_table_name: str,
    clickhouse_table_name: str,
    clickhouse_batch_size: int = 50000,
    cloudberry_fetch_chunk: int = 100,
    clickhouse_conn_id: str = 'clickhouse_conn',
) -> bool:
    return load_array_data_to_datamart(
        dwh_table_name=dwh_table_name,
        clickhouse_table_name=clickhouse_table_name,
        deserialize_func=None,  # 🔥 НЕ десериализуем повторно — raw_values уже в таблице
        decode_timeseries_func=decode_creyt_timeseries,
        extract_fields=[
            'h.data_record_sk',
            'h.h_object_property_sk',
            'h.data_file_id',
            'h.data_source_id',
            'h.load_dttm',
            'h.raw_values',
            'h.sample_rate',
            'h.colibration_factor_a',
            'h.colibration_factor_b',
            'h.min_raw',
            'h.max_raw',
            'h.min_eu',
            'h.max_eu',
            'h.timestamp'
        ],
        timeseries_params_mapping={
            'raw_values': 'raw_values',
            'calibration_factor_a': 'colibration_factor_a',
            'calibration_factor_b': 'colibration_factor_b',
            'min_raw': 'min_raw',
            'max_raw': 'max_raw',
            'min_eu': 'min_eu',
            'max_eu': 'max_eu',
            'sample_rate': 'sample_rate',
            'first_timestamp': 'timestamp'
        },
        clickhouse_batch_size=clickhouse_batch_size,
        cloudberry_fetch_chunk=cloudberry_fetch_chunk,
        clickhouse_conn_id=clickhouse_conn_id,
    )
    
def load_lcard_time_series_to_datamart(
    dwh_table_name: str,
    clickhouse_table_name: str,
    clickhouse_batch_size: int = 50000,
    cloudberry_fetch_chunk: int = 100,
    clickhouse_conn_id: str = 'clickhouse_conn',
) -> bool:
    return load_array_data_to_datamart(
        dwh_table_name=dwh_table_name,
        clickhouse_table_name=clickhouse_table_name,
        deserialize_func=None,  # 🔥 НЕ десериализуем повторно — raw_values уже в таблице
        decode_timeseries_func=decode_lcard_timeseries,
        extract_fields=[
            'h.data_record_sk',
            'h.h_object_property_sk',
            'h.data_file_id',
            'h.data_source_id',
            'h.load_dttm',
            'h.raw_values',
            'h.sample_rate',
            'h.step',
            'h.is_scale',
            'h.min_raw',
            'h.max_raw',
            'h.min_eu',
            'h.max_eu',
            'h.timestamp'
        ],
        timeseries_params_mapping={
            'raw_values': 'raw_values',
            'step': 'step',
            'is_scale': 'is_scale',
            'min_raw': 'min_raw',
            'max_raw': 'max_raw',
            'min_eu': 'min_eu',
            'max_eu': 'max_eu',
            'sample_rate': 'sample_rate',
            'first_timestamp': 'timestamp'
        },
        clickhouse_batch_size=clickhouse_batch_size,
        cloudberry_fetch_chunk=cloudberry_fetch_chunk,
        clickhouse_conn_id=clickhouse_conn_id,
    )
    


def load_sample_time_series_to_datamart(
    dwh_table_name: str,
    clickhouse_table_name: str,
    clickhouse_batch_size: int = 50000,
    cloudberry_fetch_chunk: int = 100,
    clickhouse_conn_id: str = 'clickhouse_conn',
) -> bool:
    return load_array_data_to_datamart(
        dwh_table_name=dwh_table_name,
        clickhouse_table_name=clickhouse_table_name,
        deserialize_func=None,  # 🔥 НЕ десериализуем повторно — raw_values уже в таблице
        decode_timeseries_func=decode_sample_timeseries,
        extract_fields=[
            'h.data_record_sk',
            'h.h_object_property_sk',
            'h.data_file_id',
            'h.data_source_id',
            'h.load_dttm',
            'h.raw_values',
            'h.sample_rate',
            'h.timestamp'
        ],
        timeseries_params_mapping={
            'raw_values': 'raw_values',
            'sample_rate': 'sample_rate',
            'first_timestamp': 'timestamp'
        },
        clickhouse_batch_size=clickhouse_batch_size,
        cloudberry_fetch_chunk=cloudberry_fetch_chunk,
        clickhouse_conn_id=clickhouse_conn_id,
    )



def load_siemens_sample_time_series_to_datamart(
    dwh_table_name: str,
    clickhouse_table_name: str,
    clickhouse_batch_size: int = 50000,
    cloudberry_fetch_chunk: int = 100,
    clickhouse_conn_id: str = 'clickhouse_conn',
) -> bool:
    """
    Загрузка временных рядов Siemens Sample из Cloudberry в ClickHouse.
    
    Пайплайн:
    1. Извлечение raw_values (bytea) + sample_rate + timestamp из БД
    2. Декодирование через decode_siemens_sample_timeseries
    3. Вставка развёрнутых точек (timestamp, value) в ClickHouse
    
    Структура таблицы-источника идентична SampleData:
    - raw_values (bytea): массив double little-endian
    - sample_rate (integer): частота дискретизации
    - timestamp: базовая метка времени
    """
    return load_array_data_to_datamart(
        dwh_table_name=dwh_table_name,
        clickhouse_table_name=clickhouse_table_name,
        deserialize_func=None,  # 🔥 НЕ десериализуем повторно — raw_values уже в таблице
        decode_timeseries_func=decode_siemens_sample_timeseries,
        extract_fields=[
            'h.data_record_sk',
            'h.h_object_property_sk',
            'h.data_file_id',
            'h.data_source_id',
            'h.load_dttm',
            'h.raw_values',
            'h.sample_rate',
            'h.timestamp'
        ],
        timeseries_params_mapping={
            'raw_values': 'raw_values',
            'sample_rate': 'sample_rate',
            'first_timestamp': 'timestamp'
        },
        clickhouse_batch_size=clickhouse_batch_size,
        cloudberry_fetch_chunk=cloudberry_fetch_chunk,
        clickhouse_conn_id=clickhouse_conn_id,
    )
    
    
def load_diagnostic_data_to_datamart(
    dwh_table_name: str,
    clickhouse_table_name: str,
    clickhouse_batch_size: int = 50000,
    cloudberry_fetch_chunk: int = 100,
    clickhouse_conn_id: str = 'clickhouse_conn',
) -> bool:
    """
    🔥 ОПТИМИЗИРОВАННАЯ ВЕРСИЯ с clickhouse_connect.insert_df
    
    Загрузка диагностических данных (DiagnosticArray) из Cloudberry в ClickHouse.
    """
    import gc
    import numpy as np
    import pandas as pd
    import clickhouse_connect
    
    # ========================================================================
    # 1. МАППИНГ defect_state -> UUID
    # ========================================================================
    defect_state_uuid_map = {
        0: 'ca96189c-0685-9e77-22f3-f4a0c5b77c7c',
        1: 'de52b683-9997-c776-7e0d-332b90db8e7d',
        2: '96ef9f95-3e13-63de-5454-0b300019e116',
        3: '465afb20-695a-212f-c26d-81b25a71939e',
        4: '23991a7f-3993-52ea-f991-9e55fee651c2',
        5: '2427d0a4-37ec-1612-60a0-d92b28ce4888',
    }
    
    total_processed = 0
    total_source_records = 0
    total_diagnostic_records = 0
    total_empty_decodes = 0
    
    log.info(f"🚀 Старт загрузки диагностических данных: table={dwh_table_name}, chunk={cloudberry_fetch_chunk}")
    
    # =====================================================================
    # 🔥 Создаём clickhouse_connect клиент (HTTP, порт 8123)
    # =====================================================================
    conn_config = BaseHook.get_connection(clickhouse_conn_id)
    client = clickhouse_connect.get_client(
        host=conn_config.host,
        port=8123,
        database=conn_config.schema or 'default',
        username=conn_config.login or 'default',
        password=conn_config.password or '',
        compress=True,
        query_limit=0,
    )
    
    try:
        last_timestamp: Optional[datetime] = None
        last_record_id: Optional[str] = None
        iteration = 0
        
        while True:
            iteration += 1
            
            # 1️⃣ ИЗВЛЕЧЕНИЕ ЧАНКА
            if last_timestamp is None:
                extract_sql = text(f"""
                    SELECT h.data_record_sk, h.h_object_property_sk, h.data_file_id,
                           h.data_source_id, h.load_dttm, h.raw_values, h.timestamp
                    FROM public.{dwh_table_name} h
                    WHERE h.exported_to_datamart = FALSE
                    ORDER BY h.timestamp ASC, h.data_record_sk ASC
                    LIMIT :limit
                """)
                params = {"limit": cloudberry_fetch_chunk}
            else:
                extract_sql = text(f"""
                    SELECT h.data_record_sk, h.h_object_property_sk, h.data_file_id,
                           h.data_source_id, h.load_dttm, h.raw_values, h.timestamp
                    FROM public.{dwh_table_name} h
                    WHERE h.exported_to_datamart = FALSE
                    AND (h.timestamp, h.data_record_sk) > (:last_ts, :last_id)
                    ORDER BY h.timestamp ASC, h.data_record_sk ASC
                    LIMIT :limit
                """)
                params = {
                    "limit": cloudberry_fetch_chunk,
                    "last_ts": last_timestamp,
                    "last_id": last_record_id
                }
            
            try:
                with cloudberry_engine.begin() as conn:
                    result = conn.execute(extract_sql, params)
                    rows = result.fetchall()
            except Exception as e:
                log.error(f"❌ Ошибка при извлечении чанка #{iteration}: {e}", exc_info=True)
                raise
            
            if not rows:
                log.info(f"✅ Завершено: все записи обработаны (итераций: {iteration})")
                break
            
            log.info(f"📥 Чанк #{iteration}: получено {len(rows)} записей")
            total_source_records += len(rows)
            
            # 2️⃣ 🔥 СБОР ДАННЫХ В СЛОВАРЬ СПИСКОВ
            batch_cols = {
                'data_record_sk': [],
                'h_object_property_sk': [],
                'data_file_id': [],
                'data_source_id': [],
                'timestamp': [],
                'diagnostic_data_id': [],
                'h_defect_state_sk': [],
                'priority': [],
                'tag_name': [],
                'defect_name': [],
                'defect_details': [],
                'group_name': [],
                'recommendations': [],
                'load_dttm': [],
            }
            successfully_processed_ids: List[str] = []
            chunk_count = 0
            
            for row in rows:
                (data_record_sk, h_obj_prop_sk, data_file_id, data_src_id, 
                 load_dttm, raw_vals, timestamp) = row
                
                record_id = ensure_uuid_string(data_record_sk)
                
                if not raw_vals:
                    log.warning(f"⚠️ NULL/empty raw_values для {record_id}")
                    total_empty_decodes += 1
                    continue
                
                try:
                    if isinstance(raw_vals, (bytes, bytearray)):
                        blob_bytes = bytes(raw_vals)
                    elif isinstance(raw_vals, memoryview):
                        blob_bytes = bytes(raw_vals)
                    else:
                        blob_bytes = bytes(raw_vals)
                    
                    diagnostic_records = deserialize_diagnostic_array_blob(blob_bytes)
                    
                    if not diagnostic_records:
                        log.warning(f"⚠️ Пустой результат десериализации для {record_id}")
                        total_empty_decodes += 1
                        continue
                    
                    ts = timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=timezone.utc)
                    ld = load_dttm if load_dttm.tzinfo else load_dttm.replace(tzinfo=timezone.utc)
                    
                    # 🔥 Каждая диагностическая запись → отдельная строка
                    for diag_rec in diagnostic_records:
                        defect_state = diag_rec.get('defect_state', 0)
                        h_defect_state_sk = defect_state_uuid_map.get(defect_state, defect_state_uuid_map[0])
                        
                        batch_cols['data_record_sk'].append(record_id)
                        batch_cols['h_object_property_sk'].append(ensure_uuid_string(h_obj_prop_sk))
                        batch_cols['data_file_id'].append(ensure_uuid_string(data_file_id))
                        batch_cols['data_source_id'].append(ensure_uuid_string(data_src_id))
                        batch_cols['timestamp'].append(ts)
                        batch_cols['diagnostic_data_id'].append(diag_rec.get('id'))
                        batch_cols['h_defect_state_sk'].append(h_defect_state_sk)
                        batch_cols['priority'].append(diag_rec.get('priority'))
                        batch_cols['tag_name'].append(diag_rec.get('tag_name'))
                        batch_cols['defect_name'].append(diag_rec.get('defect_name'))
                        batch_cols['defect_details'].append(diag_rec.get('defect_details'))
                        batch_cols['group_name'].append(diag_rec.get('group_name'))
                        batch_cols['recommendations'].append(diag_rec.get('recommendations'))
                        batch_cols['load_dttm'].append(ld)
                    
                    successfully_processed_ids.append(record_id)
                    chunk_count += len(diagnostic_records)
                    total_diagnostic_records += len(diagnostic_records)
                    
                except Exception as e:
                    log.error(f"❌ Ошибка десериализации {record_id}: {e}", exc_info=True)
                    total_empty_decodes += 1
                    continue
            
            if rows:
                last_row = rows[-1]
                last_timestamp = last_row.timestamp
                last_record_id = ensure_uuid_string(last_row.data_record_sk)
            
            del rows
            
            if not successfully_processed_ids:
                log.warning(f"⚠️ Чанк #{iteration}: нет данных для вставки после обработки")
                continue
            
            # 3️⃣ 🔥 ФОРМИРОВАНИЕ DataFrame И ВСТАВКА ЧЕРЕЗ insert_df
            try:
                df = pd.DataFrame(batch_cols)
                
                # 🔥 Гарантируем правильные dtypes
                for col in ['data_record_sk', 'h_object_property_sk', 'data_file_id', 
                           'data_source_id', 'diagnostic_data_id', 'h_defect_state_sk']:
                    df[col] = df[col].astype(str)
                
                df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
                df['load_dttm'] = pd.to_datetime(df['load_dttm'], utc=True)
                
                # String columns
                for col in ['tag_name', 'defect_name', 'defect_details', 'group_name', 'recommendations']:
                    df[col] = df[col].fillna('').astype(str)
                
                # Priority как int
                df['priority'] = df['priority'].fillna(0).astype('int32')
                
                # Порядок колонок
                cols_order = [
                    'data_record_sk', 'h_object_property_sk', 'data_file_id', 'data_source_id',
                    'timestamp', 'diagnostic_data_id', 'h_defect_state_sk', 'priority',
                    'tag_name', 'defect_name', 'defect_details', 'group_name', 
                    'recommendations', 'load_dttm'
                ]
                df = df[cols_order]
                
                client.insert_df(clickhouse_table_name, df)
                
                total_processed += chunk_count
                log.info(f"✅ Вставлено {chunk_count} диагностических записей через insert_df из чанка #{iteration}")
                
                # 4️⃣ ПОМЕТКА ЭКСПОРТИРОВАННЫХ
                if successfully_processed_ids:
                    marked = mark_records_as_exported_via_temp_table(
                        cloudberry_engine,
                        dwh_table_name,
                        successfully_processed_ids,
                        insert_batch_size=50_000,
                        use_copy=True,
                        data_file_id=data_file_id,
                    )
                    log.info(f"✅ Помечено {marked} записей-источников как экспортированные")
                
            except Exception as e:
                log.error(f"❌ Ошибка при вставке/пометке чанка #{iteration}: {type(e).__name__}: {e}", exc_info=True)
                raise
            finally:
                del batch_cols, successfully_processed_ids, df
            
            log.info(f"📊 Прогресс: источник={total_source_records}, "
                    f"диагностических_записей={total_diagnostic_records}, "
                    f"вставлено={total_processed}, "
                    f"пропущено={total_empty_decodes}")
        
    finally:
        client.close()
    
    # ========================================================================
    # ФИНАЛЬНАЯ СТАТИСТИКА
    # ========================================================================
    log.info(f"🎉 Загрузка диагностических данных завершена:")
    log.info(f"   • Обработано записей-источников: {total_source_records}")
    log.info(f"   • Всего диагностических записей: {total_diagnostic_records}")
    log.info(f"   • Вставлено строк в ClickHouse: {total_processed}")
    log.info(f"   • Пропущено записей: {total_empty_decodes}")
    
    if total_source_records > 0 and total_diagnostic_records > 0:
        avg_diag = total_diagnostic_records / total_source_records
        log.info(f"   • Среднее диагностических записей на источник: {avg_diag:.1f}")
    
    return True


def load_bode_data_to_datamart(
    dwh_table_name: str,
    clickhouse_table_name: str,
    clickhouse_batch_size: int = 50000,
    cloudberry_fetch_chunk: int = 100,
    clickhouse_conn_id: str = 'clickhouse_conn',
) -> bool:
    """
    🔥 ОПТИМИЗИРОВАННАЯ ВЕРСИЯ с clickhouse_connect.insert_df
    
    Загрузка данных BODE из Cloudberry в ClickHouse.
    """
    import gc
    import numpy as np
    import pandas as pd
    import clickhouse_connect
    
    total_processed = 0
    total_source_records = 0
    total_empty_records = 0
    
    log.info(f"🚀 Старт загрузки BODE данных: table={dwh_table_name}, chunk={cloudberry_fetch_chunk}")
    
    # =====================================================================
    # 🔥 Создаём clickhouse_connect клиент (HTTP, порт 8123)
    # =====================================================================
    conn_config = BaseHook.get_connection(clickhouse_conn_id)
    client = clickhouse_connect.get_client(
        host=conn_config.host,
        port=8123,
        database=conn_config.schema or 'default',
        username=conn_config.login or 'default',
        password=conn_config.password or '',
        compress=True,
        query_limit=0,
    )
    
    try:
        last_timestamp: Optional[datetime] = None
        last_record_id: Optional[str] = None
        iteration = 0
        
        while True:
            iteration += 1
            
            # 1️⃣ ИЗВЛЕЧЕНИЕ ЧАНКА
            if last_timestamp is None:
                extract_sql = text(f"""
                    SELECT h.data_record_sk, h.h_object_property_sk, h.data_file_id,
                           h.data_source_id, h.load_dttm,
                           h.magnitude_value, h.phase_value, h.turnover_frequency_value,
                           h.timestamp
                    FROM public.{dwh_table_name} h
                    WHERE h.exported_to_datamart = FALSE
                    ORDER BY h.timestamp ASC, h.data_record_sk ASC
                    LIMIT :limit
                """)
                params = {"limit": cloudberry_fetch_chunk}
            else:
                extract_sql = text(f"""
                    SELECT h.data_record_sk, h.h_object_property_sk, h.data_file_id,
                           h.data_source_id, h.load_dttm,
                           h.magnitude_value, h.phase_value, h.turnover_frequency_value,
                           h.timestamp
                    FROM public.{dwh_table_name} h
                    WHERE h.exported_to_datamart = FALSE
                    AND (h.timestamp, h.data_record_sk) > (:last_ts, :last_id)
                    ORDER BY h.timestamp ASC, h.data_record_sk ASC
                    LIMIT :limit
                """)
                params = {
                    "limit": cloudberry_fetch_chunk,
                    "last_ts": last_timestamp,
                    "last_id": last_record_id
                }
            
            try:
                with cloudberry_engine.begin() as conn:
                    result = conn.execute(extract_sql, params)
                    rows = result.fetchall()
            except Exception as e:
                log.error(f"❌ Ошибка при извлечении чанка #{iteration}: {e}", exc_info=True)
                raise
            
            if not rows:
                log.info(f"✅ Завершено: все записи обработаны (итераций: {iteration})")
                break
            
            log.info(f"📥 Чанк #{iteration}: получено {len(rows)} записей")
            total_source_records += len(rows)
            
            # 2️⃣ 🔥 СБОР ДАННЫХ В СЛОВАРЬ СПИСКОВ
            batch_cols = {
                'data_record_sk': [],
                'h_object_property_sk': [],
                'data_file_id': [],
                'data_source_id': [],
                'timestamp': [],
                'magnitude_value': [],
                'phase_value': [],
                'turnover_frequency_value': [],
                'load_dttm': [],
            }
            successfully_processed_ids: List[str] = []
            
            for row in rows:
                (data_record_sk, h_obj_prop_sk, data_file_id, data_src_id, load_dttm,
                 magnitude_value, phase_value, turnover_frequency_value, timestamp) = row
                
                record_id = ensure_uuid_string(data_record_sk)
                
                if magnitude_value is None or phase_value is None or turnover_frequency_value is None:
                    log.warning(f"⚠️ NULL значения в записи {record_id}, пропускаем")
                    total_empty_records += 1
                    continue
                
                try:
                    ts = timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=timezone.utc)
                    ld = load_dttm if load_dttm.tzinfo else load_dttm.replace(tzinfo=timezone.utc)
                    
                    batch_cols['data_record_sk'].append(record_id)
                    batch_cols['h_object_property_sk'].append(ensure_uuid_string(h_obj_prop_sk))
                    batch_cols['data_file_id'].append(ensure_uuid_string(data_file_id))
                    batch_cols['data_source_id'].append(ensure_uuid_string(data_src_id))
                    batch_cols['timestamp'].append(ts)
                    batch_cols['magnitude_value'].append(float(magnitude_value))
                    batch_cols['phase_value'].append(float(phase_value))
                    batch_cols['turnover_frequency_value'].append(float(turnover_frequency_value))
                    batch_cols['load_dttm'].append(ld)
                    
                    successfully_processed_ids.append(record_id)
                    
                except Exception as e:
                    log.error(f"❌ Ошибка подготовки записи {record_id}: {e}", exc_info=True)
                    total_empty_records += 1
                    continue
            
            if rows:
                last_timestamp = rows[-1][8]
                last_record_id = ensure_uuid_string(rows[-1][0])
            
            del rows
            
            if not successfully_processed_ids:
                log.warning(f"⚠️ Чанк #{iteration}: нет данных для вставки после обработки")
                continue
            
            # 3️⃣ 🔥 ФОРМИРОВАНИЕ DataFrame И ВСТАВКА ЧЕРЕЗ insert_df
            try:
                df = pd.DataFrame(batch_cols)
                
                # 🔥 Гарантируем правильные dtypes
                for col in ['data_record_sk', 'h_object_property_sk', 
                           'data_file_id', 'data_source_id']:
                    df[col] = df[col].astype(str)
                
                df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
                df['load_dttm'] = pd.to_datetime(df['load_dttm'], utc=True)
                
                # Float columns
                for col in ['magnitude_value', 'phase_value', 'turnover_frequency_value']:
                    df[col] = df[col].astype('float64')
                
                # Порядок колонок
                cols_order = [
                    'data_record_sk', 'h_object_property_sk', 'data_file_id', 'data_source_id',
                    'timestamp', 'magnitude_value', 'phase_value', 'turnover_frequency_value',
                    'load_dttm'
                ]
                df = df[cols_order]
                
                client.insert_df(clickhouse_table_name, df)
                
                chunk_count = len(successfully_processed_ids)
                total_processed += chunk_count
                log.info(f"✅ Вставлено {chunk_count} записей BODE через insert_df из чанка #{iteration}")
                
                # 4️⃣ ПОМЕТКА ЭКСПОРТИРОВАННЫХ
                if successfully_processed_ids:
                    marked = mark_records_as_exported_via_temp_table(
                        cloudberry_engine,
                        dwh_table_name,
                        successfully_processed_ids,
                        insert_batch_size=50_000,
                        data_file_id=data_file_id,
                        use_copy=True,
                    )
                    log.info(f"✅ Помечено {marked} записей-источников как экспортированные")
                
            except Exception as e:
                log.error(f"❌ Ошибка при вставке/пометке чанка #{iteration}: {type(e).__name__}: {e}", exc_info=True)
                raise
            finally:
                del batch_cols, successfully_processed_ids, df
            
            log.info(f"📊 Прогресс: источник={total_source_records}, "
                    f"вставлено={total_processed}, "
                    f"пропущено={total_empty_records}")
        
    finally:
        client.close()
    
    # ========================================================================
    # ФИНАЛЬНАЯ СТАТИСТИКА
    # ========================================================================
    log.info(f"🎉 Загрузка BODE данных завершена:")
    log.info(f"   • Обработано записей-источников: {total_source_records}")
    log.info(f"   • Вставлено строк в ClickHouse: {total_processed}")
    log.info(f"   • Пропущено записей: {total_empty_records}")
    
    return True



from pathlib import Path
from datetime import datetime, timezone, date
import os
import logging
import json
import uuid
import tempfile
from typing import Any, Optional, Dict, List
from collections import defaultdict
from sqlalchemy import text
from airflow.models import Connection
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# PyIceberg imports
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.manifest import (
    DataFile, DataFileContent, ManifestEntry, ManifestEntryStatus,
    ManifestFile, FileFormat, write_manifest
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.types import (
    StringType, IntegerType, LongType, DoubleType, BooleanType,
    TimestampType, ListType, PrimitiveType
)
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.schema import Schema as IcebergSchema

log = logging.getLogger(__name__)




ESTIMATED_BYTES_PER_ROW_COMMON = 120


import ctypes
import gc
import pandas as pd
from datetime import datetime, timezone, date, timedelta
from typing import Dict, List, Any
from sqlalchemy import text
import pyarrow as pa

def _force_malloc_trim():
    try: ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception: pass

def _release_arrow_memory():
    try: pa.default_memory_pool().release_unused()
    except Exception: pass

def load_common_object_data_values_to_iceberg_for_file_id(
    dwh_table_name: str,
    data_mart_table_name: str,
    data_type: str,
    is_hist_data: bool,
    data_file_id: str,
    value_is_nullable: bool = False,
    fetch_batch_size: int = 100_000,
    target_file_size_mb: int = 256,
    iceberg_warehouse_path: str = "iceberg_warehouse",
    iceberg_namespace: str = "object_data_values",
    partition_by_source: bool = True,
) -> dict:
    """
    Загрузка common object data в Iceberg с КОЛОНОЧНЫМ хранением в памяти.
    """
    # 🔥 Снижаем лимит для безопасности: 20 млн строк в колоночном формате ~ 2-4 ГБ RAM
    TARGET_ROWS_PER_FLUSH = 70_000_000 
    
    s3_conn = BaseHook.get_connection('minio_conn')
    s3_endpoint = s3_conn.extra_dejson.get("host", "http://minio:9000")
    if not s3_endpoint.startswith("http"): 
        s3_endpoint = f"http://{s3_endpoint}"
    bucket = s3_conn.extra_dejson.get("bucket", "iceberg-warehouse")
    region = s3_conn.extra_dejson.get("region", "us-east-1")
    
    s3_client = boto3.client(
        "s3", endpoint_url=s3_endpoint, 
        aws_access_key_id=s3_conn.login,
        aws_secret_access_key=s3_conn.password, 
        region_name=region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"})
    )
    
    file_io = PyArrowFileIO(properties={
        "s3.endpoint": s3_endpoint, "s3.access-key-id": s3_conn.login,
        "s3.secret-access-key": s3_conn.password, "s3.region": region,
        "s3.path-style-access": "true"
    })
    
    hist_path = 'history-data' if is_hist_data else 'non-history-data'
    table_path = f"{iceberg_warehouse_path}/{iceberg_namespace}/{hist_path}/{data_mart_table_name}"
    metadata_prefix = f"{table_path}/metadata"
    data_prefix = f"{table_path}/data"
    table_location = f"s3://{bucket}/{table_path}"
    
    log.info(f"📍 Iceberg table: {table_location} | TARGET_ROWS={TARGET_ROWS_PER_FLUSH:,}")
    
    iceberg_type, pa_type = _get_type_for_iceberg(data_type)
    
    table_metadata = _init_or_load_table_metadata(
        s3_client, bucket, table_path, metadata_prefix,
        iceberg_type, pa_type, partition_by_source
    )
    
    partition_spec = PartitionSpec(
        # 🔥 ИСПРАВЛЕНИЕ: Сначала день, потом источник (соответствует структуре папок в S3)
        PartitionField(source_id=7, field_id=1000, transform=DayTransform(), name="dt_day"),
        PartitionField(source_id=3, field_id=1001, transform=IdentityTransform(), name="source_id"),
        spec_id=0
    )
    
    pa_schema = pa.schema([
        pa.field("data_record_sk", pa.string(), nullable=False),
        pa.field("h_object_property_sk", pa.string(), nullable=True),
        pa.field("data_file_id", pa.string(), nullable=False),
        pa.field("data_source_id", pa.string(), nullable=False),
        pa.field("load_dttm", pa.string(), nullable=True),
        pa.field("value", pa_type, nullable=value_is_nullable),
        pa.field("timestamp", pa.timestamp("us"), nullable=True),
    ])
    
    cloudberry_engine = get_dwh_engine('cloudberry_test_dwh')
    
    last_timestamp = None
    last_record_id = None
    
    total_loaded = 0
    total_marked = 0
    iteration = 0
    
    # 🔥 КОЛОНОЧНОЕ ХРАНЕНИЕ: Словарь словарей списков
    partition_buffers: Dict[date, Dict[str, list]] = {}
    pending_record_ids: List[str] = []
    pending_manifests: List[Any] = []
    
    current_buffered_rows = 0
    
    def _normalize_ts(ts_val: Any) -> datetime:
        if ts_val is None or (isinstance(ts_val, float) and pd.isna(ts_val)):
            return datetime(1970, 1, 1, tzinfo=timezone.utc)
        if hasattr(ts_val, 'total_seconds'):
            return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=ts_val.total_seconds())
        if isinstance(ts_val, datetime):
            return ts_val if ts_val.tzinfo else ts_val.replace(tzinfo=timezone.utc)
        try:
            ts_str = str(ts_val).replace(" ", "T")
            if "." not in ts_str and "T" in ts_str:
                ts_str += ".000000"
            dt = datetime.fromisoformat(ts_str)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return datetime(1970, 1, 1, tzinfo=timezone.utc)

    while True:
        iteration += 1
        
        if last_timestamp is None:
            extract_sql = text(f"""
                SELECT h.data_record_sk, h.h_object_property_sk, h.data_file_id,
                       h.data_source_id, h.load_dttm, h.value, h.timestamp
                FROM public.{dwh_table_name} h
                WHERE h.data_file_id = :file_id AND h.exported_to_s3_storage = FALSE
                ORDER BY h.timestamp ASC, h.data_record_sk ASC LIMIT :limit
            """)
            params = {"file_id": data_file_id, "limit": fetch_batch_size}
        else:
            extract_sql = text(f"""
                SELECT h.data_record_sk, h.h_object_property_sk, h.data_file_id,
                       h.data_source_id, h.load_dttm, h.value, h.timestamp
                FROM public.{dwh_table_name} h
                WHERE h.data_file_id = :file_id AND h.exported_to_s3_storage = FALSE
                  AND (h.timestamp, h.data_record_sk) > (:last_ts, :last_id)
                ORDER BY h.timestamp ASC, h.data_record_sk ASC LIMIT :limit
            """)
            params = {"file_id": data_file_id, "limit": fetch_batch_size, "last_ts": last_timestamp, "last_id": last_record_id}
        
        try:
            with cloudberry_engine.begin() as conn:
                rows = conn.execute(extract_sql, params).fetchall()
        except Exception as e:
            log.error(f"❌ Ошибка извлечения батча #{iteration}: {e}", exc_info=True)
            raise
        
        if not rows:
            log.info(f"✅ data_file_id={data_file_id}: все записи обработаны (итераций: {iteration})")
            break
        
        log.info(f"📥 Батч #{iteration}: извлечено {len(rows)} записей")
        total_loaded += len(rows)
        
        for row in rows:
            h_data_rec_sk, h_obj_prop_sk, d_file_id, data_src_id, load_dttm, value, timestamp = row
            
            rec_sk_str = ensure_uuid_string(h_data_rec_sk)
            norm_timestamp = _normalize_ts(timestamp)
            day_key = norm_timestamp.date()
            norm_load_dttm = _normalize_ts(load_dttm)
            
            if isinstance(value, timedelta):
                safe_value = str(value)
            else:
                safe_value = value
                
            # 🔥 Инициализация колонок при первом обращении к дню
            if day_key not in partition_buffers:
                partition_buffers[day_key] = {
                    'data_record_sk': [], 'h_object_property_sk': [], 'data_file_id': [],
                    'data_source_id': [], 'load_dttm': [], 'timestamp': [], 'value': []
                }
                
            buf = partition_buffers[day_key]
            buf['data_record_sk'].append(rec_sk_str)
            buf['h_object_property_sk'].append(ensure_uuid_string(h_obj_prop_sk))
            buf['data_file_id'].append(ensure_uuid_string(d_file_id))
            buf['data_source_id'].append(ensure_uuid_string(data_src_id))
            buf['load_dttm'].append(norm_load_dttm.isoformat())
            buf['timestamp'].append(norm_timestamp)
            buf['value'].append(safe_value)
            
            current_buffered_rows += 1
            
        # ========================================================================
        # СБРОС БУФЕРА ПРИ ДОСТИЖЕНИИ ЛИМИТА
        # ========================================================================
        if current_buffered_rows >= TARGET_ROWS_PER_FLUSH and partition_buffers:
            log.info(f"🚀 ДОСТИГНУТ ЛИМИТ ({current_buffered_rows:,} строк). Сбрасываем буферы в S3...")
            
            current_batch_sequence_number = table_metadata.get("last-sequence-number", 0) + 1
            current_batch_snapshot_id = abs(hash(f"{table_path}_{datetime.now(timezone.utc).isoformat()}_{iteration}"))
            
            for day_date, buffer_to_flush in list(partition_buffers.items()):
                if not buffer_to_flush['data_record_sk']:
                    continue
                    
                manifest = _flush_common_partition_to_s3(
                    day_date, buffer_to_flush, s3_client, file_io, bucket, data_prefix, metadata_prefix,
                    partition_spec, partition_by_source, iceberg_type, value_is_nullable, pa_schema, table_metadata, data_file_id,
                    snapshot_id=current_batch_snapshot_id, sequence_number=current_batch_sequence_number
                )
                if manifest:
                    pending_manifests.append(manifest)
                    
                # 🔥 Собираем УНИКАЛЬНЫЕ ID для обновления БД (защищает от передачи миллионов дубликатов)
                pending_record_ids.extend(list(set(buffer_to_flush['data_record_sk'])))
            
            # Полная очистка словаря
            del partition_buffers
            partition_buffers = {}
            current_buffered_rows = 0
            
            if pending_manifests:
                _commit_and_update_db_common(
                    pending_manifests, pending_record_ids, s3_client, file_io, bucket, table_path, 
                    metadata_prefix, partition_spec, table_metadata, dwh_table_name,
                    snapshot_id=current_batch_snapshot_id, sequence_number=current_batch_sequence_number,
                    target_column="exported_to_s3_storage" # 🔥 Передаем правильную колонку
                )
                
                total_marked += len(pending_record_ids)
                
                pending_manifests = []
                pending_record_ids = []
                
                table_metadata = _init_or_load_table_metadata(
                    s3_client, bucket, table_path, metadata_prefix, iceberg_type, pa_type, partition_by_source
                )
                
                gc.collect()
                _release_arrow_memory()
                _force_malloc_trim()
        
        last_timestamp = rows[-1][6]
        last_record_id = ensure_uuid_string(rows[-1][0])
        
        del rows
        gc.collect()

    # ========================================================================
    # ФИНАЛЬНЫЙ СБРОС ОСТАТКОВ БУФЕРА
    # ========================================================================
    if current_buffered_rows > 0:
        log.info(f"🚀 ФИНАЛЬНЫЙ СБРОС: Осталось {current_buffered_rows:,} строк в буфере")
        
        current_batch_sequence_number = table_metadata.get("last-sequence-number", 0) + 1
        current_batch_snapshot_id = abs(hash(f"{table_path}_{datetime.now(timezone.utc).isoformat()}_final"))
        
        for day_date, buf in list(partition_buffers.items()):
            if not buf['data_record_sk']:
                continue
                
            manifest = _flush_common_partition_to_s3(
                day_date, buf, s3_client, file_io, bucket, data_prefix, metadata_prefix,
                partition_spec, partition_by_source, iceberg_type, value_is_nullable, pa_schema, table_metadata, data_file_id,
                snapshot_id=current_batch_snapshot_id, sequence_number=current_batch_sequence_number
            )
            if manifest:
                pending_manifests.append(manifest)
                pending_record_ids.extend(list(set(buf['data_record_sk'])))
        
        if pending_manifests:
            _commit_and_update_db_common(
                pending_manifests, pending_record_ids, s3_client, file_io, bucket, table_path,
                metadata_prefix, partition_spec, table_metadata, dwh_table_name,
                snapshot_id=current_batch_snapshot_id, sequence_number=current_batch_sequence_number,
                target_column="exported_to_s3_storage"
            )
            total_marked += len(pending_record_ids)
            
        del partition_buffers
        partition_buffers = {}
        gc.collect()
        _release_arrow_memory()
        _force_malloc_trim()

    log.info(f"🎉 data_file_id={data_file_id} завершён: загружено={total_loaded}, помечено={total_marked}")
    
    return {
        'data_file_id': data_file_id,
        'dwh_table_name': dwh_table_name,
        'records_processed': total_loaded,
        'records_marked': total_marked,
    }



    


import uuid
import tempfile
import gc
import json
from pathlib import Path
from datetime import datetime, date, timezone
from collections import defaultdict
from typing import Optional, List, Tuple, Any, Dict
from sqlalchemy import text
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import pandas as pd
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import DoubleType, StringType, TimestampType, PrimitiveType, ListType
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat, ManifestEntry, ManifestEntryStatus

from pyiceberg.typedef import Record
import boto3
from botocore.config import Config
from airflow.hooks.base import BaseHook

# Глобальная оценка размера одной строки в байтах для расчета порога буферизации
# 7 колонок (3x UUID-string, 1x string, 1x float, 1x timestamp, 1x string) ≈ 120-150 байт
ESTIMATED_BYTES_PER_ROW = 150




import sys
import gc
from datetime import datetime, date, timezone
from typing import Optional, List, Tuple, Any, Dict
from collections import defaultdict
from sqlalchemy import text
import pyarrow as pa
import boto3
from botocore.config import Config
from airflow.hooks.base import BaseHook
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import DoubleType


import uuid
import tempfile
import gc
import json
from pathlib import Path
from datetime import datetime, date, timezone
from collections import defaultdict
from typing import Optional, List, Tuple, Any, Dict
from sqlalchemy import text
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import pandas as pd
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import DoubleType, StringType, TimestampType, PrimitiveType, ListType
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat, ManifestEntry, ManifestEntryStatus
from pyiceberg.typedef import Record
import boto3
from botocore.config import Config
from airflow.hooks.base import BaseHook

# Глобальная оценка размера одной строки (для справки, основная логика теперь опирается на количество строк)
ESTIMATED_BYTES_PER_ROW = 150

import os
import gc
from datetime import datetime, date, timezone
from typing import Optional, List, Tuple, Any, Dict
from collections import defaultdict
from sqlalchemy import text
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.config import Config
from airflow.hooks.base import BaseHook
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import DoubleType

def _log_memory_usage(prefix: str = ""):
    """Логирует использование памяти и CPU текущим процессом."""
    try:
        import psutil
        process = psutil.Process(os.getpid())
        mem_mb = process.memory_info().rss / (1024 * 1024)
        cpu_pct = process.cpu_percent(interval=0.1)
        log.info(f"💾 {prefix} | RAM: {mem_mb:.1f} MB, CPU: {cpu_pct:.1f}%")
    except ImportError:
        log.info(f"💾 {prefix} | (установите пакет 'psutil' для детального мониторинга RAM/CPU)")
    except Exception as e:
        log.warning(f"⚠️ Ошибка логирования ресурсов: {e}")


import os
import gc
import logging
import psutil
from datetime import datetime, date, timezone
from typing import Optional, List, Tuple, Any, Dict
from collections import defaultdict
from sqlalchemy import text
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.config import Config
from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)

def _log_worker_resources(iteration: int, context: str = ""):
    """
    Логирует потребление ресурсов текущим процессом (Airflow Worker).
    """
    try:
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()
        rss_mb = mem_info.rss / (1024 * 1024)      # Физическая память
        vms_mb = mem_info.vms / (1024 * 1024)      # Виртуальная память
        cpu_pct = process.cpu_percent(interval=0.1) # Загрузка CPU (с небольшой задержкой для точности)
        gc_counts = gc.get_count()                 # Счетчики сборщика мусора (gen0, gen1, gen2)
        
        log.info(
            f"📊 [Iter {iteration}] {context} | "
            f"RAM (RSS): {rss_mb:,.1f} MB | "
            f"RAM (VMS): {vms_mb:,.1f} MB | "
            f"CPU: {cpu_pct:.1f}% | "
            f"GC: {gc_counts}"
        )
    except Exception as e:
        log.warning(f"⚠️ Не удалось получить метрики ресурсов: {e}")




import ctypes
import gc
from collections import defaultdict
from datetime import datetime, timezone, date
from typing import Dict, Tuple, List, Any, Optional
from sqlalchemy import text
import boto3
from botocore.config import Config
import pyarrow as pa
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import DoubleType
from pyiceberg.io.pyarrow import PyArrowFileIO
from airflow.hooks.base import BaseHook

def _force_malloc_trim():
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception:
        pass

def _release_arrow_memory():
    try:
        pa.default_memory_pool().release_unused()
    except Exception:
        pass

def load_creyt_time_series_to_iceberg_for_file_id(
    dwh_table_name: str,
    data_file_id: str,
    is_hist_data: bool,
    target_file_size_mb: int = 256,
    fetch_batch_size: int = 1000,
    partition_by_source: bool = True,
) -> dict:
    """
    Загрузка временных рядов Creyt в Iceberg с КОЛОНОЧНЫМ хранением в памяти.
    """
    # 🔥 ВАЖНО: Снизьте лимит! 75 млн словарей убивают Python из-за фрагментации.
    # 25 млн точек при колоночном хранении займут ~4-5 ГБ RAM и дадут файл ~100 МБ.
    TARGET_POINTS_PER_FLUSH = 70_000_000 
    
    # ... (Инициализация S3, Iceberg, SQL запросов без изменений) ...
    s3_conn = BaseHook.get_connection('minio_conn')
    s3_endpoint = s3_conn.extra_dejson.get("host", "http://minio:9000")
    if not s3_endpoint.startswith("http"):
        s3_endpoint = f"http://{s3_endpoint}"
    
    bucket = s3_conn.extra_dejson.get("bucket", "iceberg-warehouse")
    region = s3_conn.extra_dejson.get("region", "us-east-1")
    
    s3_client = boto3.client(
        "s3", endpoint_url=s3_endpoint, aws_access_key_id=s3_conn.login,
        aws_secret_access_key=s3_conn.password, region_name=region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"})
    )
    
    file_io = PyArrowFileIO(properties={
        "s3.endpoint": s3_endpoint, "s3.access-key-id": s3_conn.login,
        "s3.secret-access-key": s3_conn.password, "s3.region": region,
        "s3.path-style-access": "true"
    })
    
    data_mart_table_name = "creyt_timeseries_data"
    hist_path = 'history-data' if is_hist_data else 'non-history-data'
    table_path = f"iceberg_warehouse/object_data_values/{hist_path}/{data_mart_table_name}"
    metadata_prefix = f"{table_path}/metadata"
    data_prefix = f"{table_path}/data"
    table_location = f"s3://{bucket}/{table_path}"
    
    log.info(f"📍 Iceberg: {table_location} | file_id={data_file_id} | TARGET_POINTS={TARGET_POINTS_PER_FLUSH:,}")
    
    iceberg_type = DoubleType()
    pa_type = pa.float64()
    value_is_nullable = False
    
    partition_spec = PartitionSpec(
        # 🔥 ИСПРАВЛЕНИЕ: Сначала день, потом источник (соответствует структуре папок в S3)
        PartitionField(source_id=7, field_id=1000, transform=DayTransform(), name="dt_day"),
        PartitionField(source_id=3, field_id=1001, transform=IdentityTransform(), name="source_id"),
        spec_id=0
    )
    
    pa_schema = pa.schema([
        pa.field("data_record_sk", pa.string(), nullable=False),
        pa.field("h_object_property_sk", pa.string(), nullable=True),
        pa.field("data_file_id", pa.string(), nullable=False),
        pa.field("data_source_id", pa.string(), nullable=False),
        pa.field("load_dttm", pa.string(), nullable=True),
        pa.field("value", pa_type, nullable=value_is_nullable),
        pa.field("timestamp", pa.timestamp("us"), nullable=True),
    ])
    
    table_metadata = _init_or_load_table_metadata(
        s3_client, bucket, table_path, metadata_prefix, iceberg_type, pa_type, partition_by_source
    )
    
    last_timestamp: Optional[datetime] = None
    last_record_id: Optional[str] = None
    
    extract_sql_no_cursor = text(f"""
        SELECT h.data_record_sk, h.h_object_property_sk, h.data_file_id, h.data_source_id,
               h.load_dttm, h.sample_rate, h.colibration_factor_a, h.colibration_factor_b,
               h.raw_values, h.min_raw, h.max_raw, h.min_eu, h.max_eu, h.timestamp as base_timestamp
        FROM public.{dwh_table_name} h
        WHERE h.data_file_id = :file_id AND h.exported_to_s3_storage = FALSE
        ORDER BY h.timestamp ASC, h.data_record_sk ASC LIMIT :limit
    """)
    
    extract_sql_with_cursor = text(f"""
        SELECT h.data_record_sk, h.h_object_property_sk, h.data_file_id, h.data_source_id,
               h.load_dttm, h.sample_rate, h.colibration_factor_a, h.colibration_factor_b,
               h.raw_values, h.min_raw, h.max_raw, h.min_eu, h.max_eu, h.timestamp as base_timestamp
        FROM public.{dwh_table_name} h
        WHERE h.data_file_id = :file_id AND h.exported_to_s3_storage = FALSE
          AND (h.timestamp, h.data_record_sk) > (:last_ts, :last_id)
        ORDER BY h.timestamp ASC, h.data_record_sk ASC LIMIT :limit
    """)
    
    total_source_records = 0
    total_decoded_points = 0
    total_uploaded_points = 0
    total_marked_records = 0
    iteration = 0
    
    # 🔥 КОЛОНОЧНОЕ ХРАНЕНИЕ: Словарь списков вместо списка словарей
    partition_buffers: Dict[Tuple[str, date], Dict[str, list]] = {}
    pending_record_ids: List[str] = []
    pending_manifests: List[Any] = []
    
    current_buffered_points = 0 
    buffered_source_ids = set()
    
    while True:
        iteration += 1
        _log_worker_resources(iteration, "Начало итерации")
        
        try:
            with cloudberry_engine.begin() as conn:
                sql = extract_sql_no_cursor if last_timestamp is None else extract_sql_with_cursor
                params = {"file_id": data_file_id, "limit": fetch_batch_size} if last_timestamp is None else \
                         {"file_id": data_file_id, "limit": fetch_batch_size, "last_ts": last_timestamp, "last_id": last_record_id}
                rows = conn.execute(sql, params).fetchall()
        except Exception as e:
            log.error(f"❌ Ошибка извлечения чанка #{iteration}: {e}", exc_info=True)
            raise
        
        if not rows:
            log.info(f"✅ Чтение завершено (итераций: {iteration})")
            break
        
        total_source_records += len(rows)
        batch_decoded_count = 0
        
        for row in rows:
            (h_data_rec_sk, h_obj_prop_sk, row_data_file_id, data_src_id, load_dttm, sample_rate,
             calib_a, calib_b, raw_vals, min_raw, max_raw, min_eu, max_eu, timestamp) = row
            
            rec_sk_str = ensure_uuid_string(h_data_rec_sk)
            try:
                raw_bytes = bytes(raw_vals) if isinstance(raw_vals, (bytes, bytearray, memoryview)) else bytes(raw_vals)
                timeseries = decode_creyt_timeseries(
                    raw_values=raw_bytes, calibration_factor_a=calib_a, calibration_factor_b=calib_b,
                    min_raw=min_raw, max_raw=max_raw, min_eu=min_eu, max_eu=max_eu,
                    sample_rate=sample_rate, first_timestamp=timestamp
                )
            except Exception as e:
                log.error(f"❌ Ошибка декодирования {h_data_rec_sk}: {e}", exc_info=True)
                continue
            
            if not timeseries:
                continue
            
            buffered_source_ids.add(rec_sk_str)
            
            file_id_str = ensure_uuid_string(row_data_file_id)
            src_id_str = ensure_uuid_string(data_src_id)
            load_dttm_str = load_dttm.isoformat() if isinstance(load_dttm, datetime) else str(load_dttm)
            h_obj_prop_str = ensure_uuid_string(h_obj_prop_sk)
            
            for ts, value in timeseries:
                day_key = ts.date() if isinstance(ts, datetime) else datetime.now(timezone.utc).date()
                key = (file_id_str, day_key) if partition_by_source else ("all", day_key)
                
                # 🔥 Инициализация колонок при первом обращении к партиции
                if key not in partition_buffers:
                    partition_buffers[key] = {
                        'data_record_sk': [], 'h_object_property_sk': [], 'data_file_id': [],
                        'data_source_id': [], 'load_dttm': [], 'timestamp': [], 'value': []
                    }
                
                buf = partition_buffers[key]
                buf['data_record_sk'].append(rec_sk_str)
                buf['h_object_property_sk'].append(h_obj_prop_str)
                buf['data_file_id'].append(file_id_str)
                buf['data_source_id'].append(src_id_str)
                buf['load_dttm'].append(load_dttm_str)
                buf['timestamp'].append(ts)
                buf['value'].append(float(value) if value is not None else None)
                
                batch_decoded_count += 1
        
        total_decoded_points += batch_decoded_count
        current_buffered_points += batch_decoded_count 
        
        _log_worker_resources(iteration, "После декодирования")
        
        if current_buffered_points >= TARGET_POINTS_PER_FLUSH:
            log.info(f"🚀 ДОСТИГНУТ ЛИМИТ ({current_buffered_points:,} точек). Сбрасываем буфер в S3...")
            
            flushed_points_count = 0
            for key, buffer_to_flush in list(partition_buffers.items()):
                if not buffer_to_flush['data_record_sk']:
                    continue
                    
                file_id_str_flush, day_date = key
                
                current_sequence_number = table_metadata.get("last-sequence-number", 0) + 1
                current_snapshot_id = abs(hash(f"{table_path}_{datetime.now(timezone.utc).isoformat()}_{iteration}"))
                
                # Передаем словарь списков в функцию сброса
                manifest = _flush_common_partition_to_s3(
                    day_date, buffer_to_flush, s3_client, file_io, bucket, data_prefix, metadata_prefix,
                    partition_spec, partition_by_source, iceberg_type, value_is_nullable, pa_schema, table_metadata, data_file_id,
                    snapshot_id=current_snapshot_id, sequence_number=current_sequence_number
                )
                
                if manifest:
                    pending_manifests.append(manifest)
                    flushed_points_count += len(buffer_to_flush['data_record_sk'])
                
                # Удаляем ссылку на словарь
                del partition_buffers[key]
            
            pending_record_ids.extend(list(buffered_source_ids))
            buffered_source_ids.clear()
            current_buffered_points = 0 
            
            if pending_manifests:
                _commit_and_update_db_common(
                    pending_manifests, pending_record_ids, s3_client, file_io, bucket, table_path,
                    metadata_prefix, partition_spec, table_metadata, dwh_table_name,
                    snapshot_id=current_snapshot_id, sequence_number=current_sequence_number,
                    target_column="exported_to_s3_storage" 
                )
                
                total_uploaded_points += flushed_points_count
                total_marked_records += len(pending_record_ids)
                
                pending_manifests = []
                pending_record_ids = []
                
                table_metadata = _init_or_load_table_metadata(
                    s3_client, bucket, table_path, metadata_prefix, iceberg_type, pa_type, partition_by_source
                )
                
                # 🔥 Тройной удар по памяти
                del partition_buffers
                partition_buffers = {}
                
                gc.collect()
                _release_arrow_memory()
                _force_malloc_trim() 
                _log_worker_resources(iteration, "После сброса и очистки всех пулов")

        if rows:
            last_timestamp = rows[-1].base_timestamp
            last_record_id = ensure_uuid_string(rows[-1].data_record_sk)

    if current_buffered_points > 0:
        log.info(f"🚀 ФИНАЛЬНЫЙ СБРОС: Осталось {current_buffered_points:,} точек в буфере")
        
        pending_record_ids.extend(list(buffered_source_ids))
        buffered_source_ids.clear()
        
        flushed_points_count = 0
        for key, buf in list(partition_buffers.items()):
            if not buf['data_record_sk']:
                continue
                
            file_id_str_flush, day_date = key
            
            current_sequence_number = table_metadata.get("last-sequence-number", 0) + 1
            current_snapshot_id = abs(hash(f"{table_path}_{datetime.now(timezone.utc).isoformat()}_final"))
            
            manifest = _flush_common_partition_to_s3(
                day_date, buf, s3_client, file_io, bucket, data_prefix, metadata_prefix,
                partition_spec, partition_by_source, iceberg_type, value_is_nullable, pa_schema, table_metadata, data_file_id,
                snapshot_id=current_snapshot_id, sequence_number=current_sequence_number
            )
            if manifest:
                pending_manifests.append(manifest)
                flushed_points_count += len(buf['data_record_sk'])
            
            del partition_buffers[key]
        
        if pending_manifests:
            _commit_and_update_db_common(
                pending_manifests, pending_record_ids, s3_client, file_io, bucket, table_path,
                metadata_prefix, partition_spec, table_metadata, dwh_table_name,
                snapshot_id=current_snapshot_id, sequence_number=current_sequence_number,
                target_column="exported_to_s3_storage" 
            )
            
            total_uploaded_points += flushed_points_count
            total_marked_records += len(pending_record_ids)
            
            del partition_buffers
            partition_buffers = {}
            
            gc.collect()
            _release_arrow_memory()
            _force_malloc_trim()
            _log_worker_resources(iteration, "После финального сброса")
    
    log.info(f"🎉 Завершён file_id={data_file_id}: источник={total_source_records}, "
             f"декодировано={total_decoded_points}, загружено={total_uploaded_points}, помечено={total_marked_records}")
    
    return {
        'data_file_id': data_file_id,
        'source_records': total_source_records,
        'decoded_points': total_decoded_points,
        'uploaded_points': total_uploaded_points,
        'marked_records': total_marked_records
    }





def load_lcard_time_series_to_iceberg(
    dwh_table_name: str,
    is_hist_data: bool,
    target_file_size_mb: int = 256,
    fetch_batch_size: int = 1000,
    partition_by_source: bool = True,
) -> dict:
    """
    Загрузка временных рядов LCard в Iceberg с колоночным хранением и защитой от OOM.
    """
    # 🔥 Безопасный лимит для колоночного формата (~2-4 ГБ RAM)
    TARGET_POINTS_PER_FLUSH = 20_000_000 
    
    # 0. ИНИЦИАЛИЗАЦИЯ S3 и ICEBERG
    s3_conn = BaseHook.get_connection('minio_conn')
    s3_endpoint = s3_conn.extra_dejson.get("host", "http://minio:9000")
    if not s3_endpoint.startswith("http"): s3_endpoint = f"http://{s3_endpoint}"
    bucket = s3_conn.extra_dejson.get("bucket", "iceberg-warehouse")
    region = s3_conn.extra_dejson.get("region", "us-east-1")
    
    s3_client = boto3.client("s3", endpoint_url=s3_endpoint, aws_access_key_id=s3_conn.login,
                             aws_secret_access_key=s3_conn.password, region_name=region,
                             config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))
    
    file_io = PyArrowFileIO(properties={"s3.endpoint": s3_endpoint, "s3.access-key-id": s3_conn.login,
                                        "s3.secret-access-key": s3_conn.password, "s3.region": region,
                                        "s3.path-style-access": "true"})
    
    data_mart_table_name = "lcard_timeseries_data"
    hist_path = 'history-data' if is_hist_data else 'non-history-data'
    table_path = f"iceberg_warehouse/object_data_values/{hist_path}/{data_mart_table_name}"
    metadata_prefix, data_prefix = f"{table_path}/metadata", f"{table_path}/data"
    table_location = f"s3://{bucket}/{table_path}"
    
    log.info(f"📍 Iceberg: {table_location} | TARGET_POINTS={TARGET_POINTS_PER_FLUSH:,}")
    
    iceberg_type = DoubleType()
    pa_type = pa.float64()
    value_is_nullable = False
    
    # 🔥 День первым, источник вторым (Hive-style)
    partition_spec = PartitionSpec(
        PartitionField(source_id=7, field_id=1000, transform=DayTransform(), name="dt_day"),
        PartitionField(source_id=3, field_id=1001, transform=IdentityTransform(), name="source_id"),
        spec_id=0
    )
    
    pa_schema = pa.schema([
        pa.field("data_record_sk", pa.string(), nullable=False),
        pa.field("h_object_property_sk", pa.string(), nullable=True),
        pa.field("data_file_id", pa.string(), nullable=False),
        pa.field("data_source_id", pa.string(), nullable=False),
        pa.field("load_dttm", pa.string(), nullable=True),
        pa.field("value", pa_type, nullable=value_is_nullable),
        pa.field("timestamp", pa.timestamp("us"), nullable=True),
    ])
    
    table_metadata = _init_or_load_table_metadata(s3_client, bucket, table_path, metadata_prefix, 
                                                  iceberg_type, pa_type, partition_by_source)
    
    # 1. SQL ЗАПРОСЫ (Keyset pagination без фильтра по data_file_id)
    extract_sql_no_cursor = text(f"""
        SELECT h.data_record_sk, h.h_object_property_sk, h.data_file_id, h.data_source_id, h.load_dttm,
               h.sample_rate, h.step, h.is_scale, h.raw_values, h.min_raw, h.max_raw,
               h.min_eu, h.max_eu, h.timestamp as base_timestamp
        FROM public.{dwh_table_name} h
        WHERE h.exported_to_s3_storage = FALSE
        ORDER BY h.timestamp ASC, h.data_record_sk ASC LIMIT :limit
    """)
    
    extract_sql_with_cursor = text(f"""
        SELECT h.data_record_sk, h.h_object_property_sk, h.data_file_id, h.data_source_id, h.load_dttm,
               h.sample_rate, h.step, h.is_scale, h.raw_values, h.min_raw, h.max_raw,
               h.min_eu, h.max_eu, h.timestamp as base_timestamp
        FROM public.{dwh_table_name} h
        WHERE h.exported_to_s3_storage = FALSE
          AND (h.timestamp, h.data_record_sk) > (:last_ts, :last_id)
        ORDER BY h.timestamp ASC, h.data_record_sk ASC LIMIT :limit
    """)
    
    # 2. БУФЕРИЗАЦИЯ (КОЛОНОЧНОЕ ХРАНЕНИЕ)
    total_source_records = 0
    total_decoded_points = 0
    total_uploaded_points = 0
    total_marked_records = 0
    iteration = 0
    
    partition_buffers: Dict[Tuple[str, date], Dict[str, list]] = {}
    pending_record_ids: List[str] = []
    pending_manifests: List[Any] = []
    
    current_buffered_points = 0 
    buffered_source_ids = set() # 🔥 Уникальные исходные ID
    
    last_timestamp: Optional[datetime] = None
    last_record_id: Optional[str] = None
    
    while True:
        iteration += 1
        
        try:
            with cloudberry_engine.begin() as conn:
                sql = extract_sql_no_cursor if last_timestamp is None else extract_sql_with_cursor
                params = {"limit": fetch_batch_size} if last_timestamp is None else \
                         {"limit": fetch_batch_size, "last_ts": last_timestamp, "last_id": last_record_id}
                rows = conn.execute(sql, params).fetchall()
        except Exception as e:
            log.error(f"❌ Ошибка извлечения чанка #{iteration}: {e}", exc_info=True)
            raise
        
        if not rows:
            log.info(f"✅ Чтение завершено (итераций: {iteration})")
            break
        
        total_source_records += len(rows)
        batch_decoded_count = 0
        
        for row in rows:
            (h_data_rec_sk, h_obj_prop_sk, row_data_file_id, data_src_id, load_dttm, 
             sample_rate, step, is_scale, raw_vals, min_raw, max_raw, min_eu, max_eu, timestamp) = row
            
            rec_sk_str = ensure_uuid_string(h_data_rec_sk)
            
            try:
                if isinstance(raw_vals, (bytes, bytearray, memoryview)):
                    raw_bytes = bytes(raw_vals) if isinstance(raw_vals, memoryview) else raw_vals
                    raw_array = np.frombuffer(raw_bytes, dtype='<f8')
                else:
                    continue
                    
                timeseries = decode_lcard_timeseries(
                    raw_values=raw_array, step=step, is_scale=bool(is_scale),
                    min_raw=min_raw, max_raw=max_raw, min_eu=min_eu, max_eu=max_eu,
                    sample_rate=sample_rate, first_timestamp=timestamp
                )
            except Exception as e:
                log.error(f"❌ Ошибка декодирования {rec_sk_str}: {e}", exc_info=True)
                continue
            
            if not timeseries: continue
            
            buffered_source_ids.add(rec_sk_str)
            file_id_str = ensure_uuid_string(row_data_file_id)
            src_id_str = ensure_uuid_string(data_src_id)
            load_dttm_str = load_dttm.isoformat() if isinstance(load_dttm, datetime) else str(load_dttm)
            h_obj_prop_str = ensure_uuid_string(h_obj_prop_sk)
            
            for ts, value in timeseries:
                day_key = ts.date() if isinstance(ts, datetime) else datetime.now(timezone.utc).date()
                key = (file_id_str, day_key) if partition_by_source else ("all", day_key)
                
                if key not in partition_buffers:
                    partition_buffers[key] = {
                        'data_record_sk': [], 'h_object_property_sk': [], 'data_file_id': [],
                        'data_source_id': [], 'load_dttm': [], 'timestamp': [], 'value': []
                    }
                
                buf = partition_buffers[key]
                buf['data_record_sk'].append(rec_sk_str)
                buf['h_object_property_sk'].append(h_obj_prop_str)
                buf['data_file_id'].append(file_id_str)
                buf['data_source_id'].append(src_id_str)
                buf['load_dttm'].append(load_dttm_str)
                buf['timestamp'].append(ts)
                buf['value'].append(float(value) if value is not None else None)
                
                batch_decoded_count += 1
        
        total_decoded_points += batch_decoded_count
        current_buffered_points += batch_decoded_count 
        
        # 3. СБРОС БУФЕРА
        if current_buffered_points >= TARGET_POINTS_PER_FLUSH and partition_buffers:
            log.info(f"🚀 ДОСТИГНУТ ЛИМИТ ({current_buffered_points:,} точек). Сбрасываем буферы в S3...")
            
            current_sequence_number = table_metadata.get("last-sequence-number", 0) + 1
            current_snapshot_id = abs(hash(f"{table_path}_{datetime.now(timezone.utc).isoformat()}_{iteration}"))
            
            flushed_points_count = 0
            for key, buffer_to_flush in list(partition_buffers.items()):
                if not buffer_to_flush.get('data_record_sk'): continue
                file_id_str_flush, day_date = key 
                
                manifest = _flush_common_partition_to_s3(
                    day_date, buffer_to_flush, s3_client, file_io, bucket, data_prefix, metadata_prefix,
                    partition_spec, partition_by_source, iceberg_type, value_is_nullable, pa_schema, table_metadata, file_id_str_flush,
                    snapshot_id=current_snapshot_id, sequence_number=current_sequence_number
                )
                if manifest:
                    pending_manifests.append(manifest)
                    flushed_points_count += len(buffer_to_flush['data_record_sk'])
            
            pending_record_ids.extend(list(buffered_source_ids))
            buffered_source_ids.clear()
            current_buffered_points = 0 
            
            if pending_manifests:
                _commit_and_update_db_common(
                    pending_manifests, pending_record_ids, s3_client, file_io, bucket, table_path,
                    metadata_prefix, partition_spec, table_metadata, dwh_table_name,
                    snapshot_id=current_snapshot_id, sequence_number=current_sequence_number,
                    target_column="exported_to_s3_storage" # 🔥 Обязательно!
                )
                
                total_uploaded_points += flushed_points_count
                total_marked_records += len(pending_record_ids)
                
                pending_manifests = []
                pending_record_ids = []
                
                table_metadata = _init_or_load_table_metadata(s3_client, bucket, table_path, metadata_prefix, 
                                                              iceberg_type, pa_type, partition_by_source)
                
                del partition_buffers
                partition_buffers = {}
                gc.collect()
                _release_arrow_memory()
                _force_malloc_trim()

        if rows:
            last_timestamp = rows[-1][-1] # base_timestamp
            last_record_id = ensure_uuid_string(rows[-1][0]) # data_record_sk

    # 4. ФИНАЛЬНЫЙ СБРОС
    if current_buffered_points > 0:
        log.info(f"🚀 ФИНАЛЬНЫЙ СБРОС: Осталось {current_buffered_points:,} точек в буфере")
        
        pending_record_ids.extend(list(buffered_source_ids))
        buffered_source_ids.clear()
        
        current_sequence_number = table_metadata.get("last-sequence-number", 0) + 1
        current_snapshot_id = abs(hash(f"{table_path}_{datetime.now(timezone.utc).isoformat()}_final"))
        
        flushed_points_count = 0
        for key, buf in list(partition_buffers.items()):
            if not buf.get('data_record_sk'): continue
            file_id_str_flush, day_date = key
            
            manifest = _flush_common_partition_to_s3(
                day_date, buf, s3_client, file_io, bucket, data_prefix, metadata_prefix,
                partition_spec, partition_by_source, iceberg_type, value_is_nullable, pa_schema, table_metadata, file_id_str_flush,
                snapshot_id=current_snapshot_id, sequence_number=current_sequence_number
            )
            if manifest:
                pending_manifests.append(manifest)
                flushed_points_count += len(buf['data_record_sk'])
        
        if pending_manifests:
            _commit_and_update_db_common(
                pending_manifests, pending_record_ids, s3_client, file_io, bucket, table_path,
                metadata_prefix, partition_spec, table_metadata, dwh_table_name,
                snapshot_id=current_snapshot_id, sequence_number=current_sequence_number,
                target_column="exported_to_s3_storage"
            )
            total_uploaded_points += flushed_points_count
            total_marked_records += len(pending_record_ids)
            
        del partition_buffers
        partition_buffers = {}
        gc.collect()
        _release_arrow_memory()
        _force_malloc_trim()
    
    log.info(f"🎉 Завершён LCard: источник={total_source_records}, "
             f"декодировано={total_decoded_points}, загружено={total_uploaded_points}, помечено={total_marked_records}")
    
    return {
        'dwh_table_name': dwh_table_name,
        'source_records': total_source_records,
        'decoded_points': total_decoded_points,
        'uploaded_points': total_uploaded_points,
        'marked_records': total_marked_records
    }



def load_bode_data_to_iceberg(
    dwh_table_name: str,
    is_hist_data: bool,
    target_file_size_mb: int = 256,
    fetch_batch_size: int = 1000,
    partition_by_source: bool = True,
) -> dict:
    """
    Загрузка данных BODE в Iceberg с колоночным хранением и защитой от OOM/VMEM Runaway.
    """
    # 🔥 Безопасный лимит для колоночного формата (~2-4 ГБ RAM)
    TARGET_POINTS_PER_FLUSH = 20_000_000 
    
    # 0. ИНИЦИАЛИЗАЦИЯ S3 и ICEBERG
    s3_conn = BaseHook.get_connection('minio_conn')
    s3_endpoint = s3_conn.extra_dejson.get("host", "http://minio:9000")
    if not s3_endpoint.startswith("http"): s3_endpoint = f"http://{s3_endpoint}"
    bucket = s3_conn.extra_dejson.get("bucket", "iceberg-warehouse")
    region = s3_conn.extra_dejson.get("region", "us-east-1")
    
    s3_client = boto3.client("s3", endpoint_url=s3_endpoint, aws_access_key_id=s3_conn.login,
                             aws_secret_access_key=s3_conn.password, region_name=region,
                             config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))
    
    file_io = PyArrowFileIO(properties={"s3.endpoint": s3_endpoint, "s3.access-key-id": s3_conn.login,
                                        "s3.secret-access-key": s3_conn.password, "s3.region": region,
                                        "s3.path-style-access": "true"})
    
    data_mart_table_name = "bode_data"
    hist_path = 'history-data' if is_hist_data else 'non-history-data'
    table_path = f"iceberg_warehouse/object_data_values/{hist_path}/{data_mart_table_name}"
    metadata_prefix, data_prefix = f"{table_path}/metadata", f"{table_path}/data"
    table_location = f"s3://{bucket}/{table_path}"
    
    log.info(f"📍 Iceberg: {table_location} | TARGET_POINTS={TARGET_POINTS_PER_FLUSH:,}")
    
    iceberg_type = DoubleType()
    pa_type = pa.float64()
    value_is_nullable = False
    
    # 🔥 День первым, источник вторым (Hive-style)
    partition_spec = PartitionSpec(
        PartitionField(source_id=7, field_id=1000, transform=DayTransform(), name="dt_day"),
        PartitionField(source_id=3, field_id=1001, transform=IdentityTransform(), name="source_id"),
        spec_id=0
    )
    
    pa_schema = pa.schema([
        pa.field("data_record_sk", pa.string(), nullable=False),
        pa.field("h_object_property_sk", pa.string(), nullable=True),
        pa.field("data_file_id", pa.string(), nullable=False),
        pa.field("data_source_id", pa.string(), nullable=False),
        pa.field("load_dttm", pa.string(), nullable=True),
        pa.field("value", pa_type, nullable=value_is_nullable),
        pa.field("timestamp", pa.timestamp("us"), nullable=True),
        pa.field("phase_value", pa.float64(), nullable=True),
        pa.field("turnover_frequency_value", pa.float64(), nullable=True),
    ])
    
    table_metadata = _init_or_load_table_metadata(s3_client, bucket, table_path, metadata_prefix, 
                                                  iceberg_type, pa_type, partition_by_source)
    
    # 1. SQL ЗАПРОСЫ (Keyset pagination без фильтра по data_file_id)
    extract_sql_no_cursor = text(f"""
        SELECT h.data_record_sk, h.h_object_property_sk, h.data_file_id, h.data_source_id, h.load_dttm,
               h.magnitude_value, h.phase_value, h.turnover_frequency_value, h.timestamp as base_timestamp
        FROM public.{dwh_table_name} h
        WHERE h.exported_to_s3_storage = FALSE
        ORDER BY h.timestamp ASC, h.data_record_sk ASC LIMIT :limit
    """)
    
    extract_sql_with_cursor = text(f"""
        SELECT h.data_record_sk, h.h_object_property_sk, h.data_file_id, h.data_source_id, h.load_dttm,
               h.magnitude_value, h.phase_value, h.turnover_frequency_value, h.timestamp as base_timestamp
        FROM public.{dwh_table_name} h
        WHERE h.exported_to_s3_storage = FALSE
          AND (h.timestamp, h.data_record_sk) > (:last_ts, :last_id)
        ORDER BY h.timestamp ASC, h.data_record_sk ASC LIMIT :limit
    """)
    
    # 2. БУФЕРИЗАЦИЯ (КОЛОНОЧНОЕ ХРАНЕНИЕ)
    total_source_records = 0
    total_decoded_points = 0
    total_uploaded_points = 0
    total_marked_records = 0
    iteration = 0
    
    partition_buffers: Dict[Tuple[str, date], Dict[str, list]] = {}
    pending_record_ids: List[str] = []
    pending_manifests: List[Any] = []
    
    current_buffered_points = 0 
    buffered_source_ids = set() # 🔥 Уникальные исходные ID
    
    last_timestamp: Optional[datetime] = None
    last_record_id: Optional[str] = None
    
    while True:
        iteration += 1
        
        try:
            with cloudberry_engine.begin() as conn:
                sql = extract_sql_no_cursor if last_timestamp is None else extract_sql_with_cursor
                params = {"limit": fetch_batch_size} if last_timestamp is None else \
                         {"limit": fetch_batch_size, "last_ts": last_timestamp, "last_id": last_record_id}
                rows = conn.execute(sql, params).fetchall()
        except Exception as e:
            log.error(f"❌ Ошибка извлечения чанка #{iteration}: {e}", exc_info=True)
            raise
        
        if not rows:
            log.info(f"✅ Чтение завершено (итераций: {iteration})")
            break
        
        total_source_records += len(rows)
        batch_decoded_count = 0
        
        for row in rows:
            (h_data_rec_sk, h_obj_prop_sk, row_data_file_id, data_src_id, load_dttm,
             magnitude_value, phase_value, turnover_frequency_value, timestamp) = row
            
            rec_sk_str = ensure_uuid_string(h_data_rec_sk)
            
            if magnitude_value is None:
                continue
                
            buffered_source_ids.add(rec_sk_str)
            file_id_str = ensure_uuid_string(row_data_file_id)
            src_id_str = ensure_uuid_string(data_src_id)
            load_dttm_str = load_dttm.isoformat() if isinstance(load_dttm, datetime) else str(load_dttm)
            h_obj_prop_str = ensure_uuid_string(h_obj_prop_sk)
            
            day_key = timestamp.date() if isinstance(timestamp, datetime) else datetime.now(timezone.utc).date()
            key = (file_id_str, day_key) if partition_by_source else ("all", day_key)
            
            if key not in partition_buffers:
                partition_buffers[key] = {
                    'data_record_sk': [], 'h_object_property_sk': [], 'data_file_id': [],
                    'data_source_id': [], 'load_dttm': [], 'timestamp': [], 'value': [],
                    'phase_value': [], 'turnover_frequency_value': []
                }
            
            buf = partition_buffers[key]
            buf['data_record_sk'].append(rec_sk_str)
            buf['h_object_property_sk'].append(h_obj_prop_str)
            buf['data_file_id'].append(file_id_str)
            buf['data_source_id'].append(src_id_str)
            buf['load_dttm'].append(load_dttm_str)
            buf['timestamp'].append(timestamp)
            buf['value'].append(float(magnitude_value))
            buf['phase_value'].append(float(phase_value) if phase_value is not None else None)
            buf['turnover_frequency_value'].append(float(turnover_frequency_value) if turnover_frequency_value is not None else None)
            
            batch_decoded_count += 1
        
        total_decoded_points += batch_decoded_count
        current_buffered_points += batch_decoded_count 
        
        # 3. СБРОС БУФЕРА
        if current_buffered_points >= TARGET_POINTS_PER_FLUSH and partition_buffers:
            log.info(f"🚀 ДОСТИГНУТ ЛИМИТ ({current_buffered_points:,} точек). Сбрасываем буферы в S3...")
            
            current_sequence_number = table_metadata.get("last-sequence-number", 0) + 1
            current_snapshot_id = abs(hash(f"{table_path}_{datetime.now(timezone.utc).isoformat()}_{iteration}"))
            
            flushed_points_count = 0
            for key, buffer_to_flush in list(partition_buffers.items()):
                if not buffer_to_flush.get('data_record_sk'): continue
                file_id_str_flush, day_date = key 
                
                manifest = _flush_common_partition_to_s3(
                    day_date, buffer_to_flush, s3_client, file_io, bucket, data_prefix, metadata_prefix,
                    partition_spec, partition_by_source, iceberg_type, value_is_nullable, pa_schema, table_metadata, file_id_str_flush,
                    snapshot_id=current_snapshot_id, sequence_number=current_sequence_number
                )
                if manifest:
                    pending_manifests.append(manifest)
                    flushed_points_count += len(buffer_to_flush['data_record_sk'])
            
            pending_record_ids.extend(list(buffered_source_ids))
            buffered_source_ids.clear()
            current_buffered_points = 0 
            
            if pending_manifests:
                _commit_and_update_db_common(
                    pending_manifests, pending_record_ids, s3_client, file_io, bucket, table_path,
                    metadata_prefix, partition_spec, table_metadata, dwh_table_name,
                    snapshot_id=current_snapshot_id, sequence_number=current_sequence_number,
                    target_column="exported_to_s3_storage" # 🔥 Обязательно!
                )
                
                total_uploaded_points += flushed_points_count
                total_marked_records += len(pending_record_ids)
                
                pending_manifests = []
                pending_record_ids = []
                
                table_metadata = _init_or_load_table_metadata(s3_client, bucket, table_path, metadata_prefix, 
                                                              iceberg_type, pa_type, partition_by_source)
                
                del partition_buffers
                partition_buffers = {}
                gc.collect()
                _release_arrow_memory()
                _force_malloc_trim()

        if rows:
            last_timestamp = rows[-1][-1] # base_timestamp
            last_record_id = ensure_uuid_string(rows[-1][0]) # data_record_sk

    # 4. ФИНАЛЬНЫЙ СБРОС
    if current_buffered_points > 0:
        log.info(f"🚀 ФИНАЛЬНЫЙ СБРОС: Осталось {current_buffered_points:,} точек в буфере")
        
        pending_record_ids.extend(list(buffered_source_ids))
        buffered_source_ids.clear()
        
        current_sequence_number = table_metadata.get("last-sequence-number", 0) + 1
        current_snapshot_id = abs(hash(f"{table_path}_{datetime.now(timezone.utc).isoformat()}_final"))
        
        flushed_points_count = 0
        for key, buf in list(partition_buffers.items()):
            if not buf.get('data_record_sk'): continue
            file_id_str_flush, day_date = key
            
            manifest = _flush_common_partition_to_s3(
                day_date, buf, s3_client, file_io, bucket, data_prefix, metadata_prefix,
                partition_spec, partition_by_source, iceberg_type, value_is_nullable, pa_schema, table_metadata, file_id_str_flush,
                snapshot_id=current_snapshot_id, sequence_number=current_sequence_number
            )
            if manifest:
                pending_manifests.append(manifest)
                flushed_points_count += len(buf['data_record_sk'])
        
        if pending_manifests:
            _commit_and_update_db_common(
                pending_manifests, pending_record_ids, s3_client, file_io, bucket, table_path,
                metadata_prefix, partition_spec, table_metadata, dwh_table_name,
                snapshot_id=current_snapshot_id, sequence_number=current_sequence_number,
                target_column="exported_to_s3_storage"
            )
            total_uploaded_points += flushed_points_count
            total_marked_records += len(pending_record_ids)
            
        del partition_buffers
        partition_buffers = {}
        gc.collect()
        _release_arrow_memory()
        _force_malloc_trim()
    
    log.info(f"🎉 Завершён BODE: источник={total_source_records}, "
             f"декодировано={total_decoded_points}, загружено={total_uploaded_points}, помечено={total_marked_records}")
    
    return {
        'dwh_table_name': dwh_table_name,
        'source_records': total_source_records,
        'decoded_points': total_decoded_points,
        'uploaded_points': total_uploaded_points,
        'marked_records': total_marked_records
    }




def load_spectrum_data_to_iceberg(
    dwh_table_name: str,
    is_hist_data: bool,
    target_file_size_mb: int = 256,
    fetch_batch_size: int = 1000,
    partition_by_source: bool = True,
) -> dict:
    """
    Загрузка данных Spectrum в Iceberg с колоночным хранением и защитой от OOM/VMEM Runaway.
    """
    # 🔥 Безопасный лимит: 500 тысяч записей. 
    # Каждая запись содержит массив (спектр), поэтому 500k строк могут занимать несколько ГБ RAM.
    TARGET_ROWS_PER_FLUSH = 5_000_000 
    
    # 0. ИНИЦИАЛИЗАЦИЯ S3 и ICEBERG
    s3_conn = BaseHook.get_connection('minio_conn')
    s3_endpoint = s3_conn.extra_dejson.get("host", "http://minio:9000")
    if not s3_endpoint.startswith("http"): s3_endpoint = f"http://{s3_endpoint}"
    bucket = s3_conn.extra_dejson.get("bucket", "iceberg-warehouse")
    region = s3_conn.extra_dejson.get("region", "us-east-1")
    
    s3_client = boto3.client("s3", endpoint_url=s3_endpoint, aws_access_key_id=s3_conn.login,
                             aws_secret_access_key=s3_conn.password, region_name=region,
                             config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))
    
    file_io = PyArrowFileIO(properties={"s3.endpoint": s3_endpoint, "s3.access-key-id": s3_conn.login,
                                        "s3.secret-access-key": s3_conn.password, "s3.region": region,
                                        "s3.path-style-access": "true"})
    
    data_mart_table_name = "spectrum_data"
    hist_path = 'history-data' if is_hist_data else 'non-history-data'
    table_path = f"iceberg_warehouse/object_data_values/{hist_path}/{data_mart_table_name}"
    metadata_prefix, data_prefix = f"{table_path}/metadata", f"{table_path}/data"
    table_location = f"s3://{bucket}/{table_path}"
    
    log.info(f"📍 Iceberg: {table_location} | TARGET_ROWS={TARGET_ROWS_PER_FLUSH:,}")
    
    iceberg_type = ListType(element_id=501, element_type=DoubleType(), element_required=False)
    pa_type = pa.list_(pa.float64())
    value_is_nullable = False
    
    # 🔥 День первым, источник вторым (Hive-style)
    partition_spec = PartitionSpec(
        PartitionField(source_id=7, field_id=1000, transform=DayTransform(), name="dt_day"),
        PartitionField(source_id=3, field_id=1001, transform=IdentityTransform(), name="source_id"),
        spec_id=0
    )
    
    pa_schema = pa.schema([
        pa.field("data_record_sk", pa.string(), nullable=False),
        pa.field("h_object_property_sk", pa.string(), nullable=True),
        pa.field("data_file_id", pa.string(), nullable=False),
        pa.field("data_source_id", pa.string(), nullable=False),
        pa.field("load_dttm", pa.string(), nullable=True),
        pa.field("value", pa_type, nullable=value_is_nullable),
        pa.field("timestamp", pa.timestamp("us"), nullable=True),
        pa.field("multiplier", pa.float64(), nullable=True),
        pa.field("h_spectrum_type_sk", pa.string(), nullable=True),
    ])
    
    table_metadata = _init_or_load_table_metadata(s3_client, bucket, table_path, metadata_prefix, 
                                                  iceberg_type, pa_type, partition_by_source)
    
    # 1. SQL ЗАПРОСЫ (Keyset pagination без фильтра по data_file_id)
    extract_sql_no_cursor = text(f"""
        SELECT h.data_record_sk, h.h_object_property_sk, h.data_file_id, h.data_source_id, h.load_dttm,
               h.raw_values, h.multiplier, h.h_spectrum_type_sk, h.timestamp as base_timestamp
        FROM public.{dwh_table_name} h
        WHERE h.exported_to_s3_storage = FALSE
        ORDER BY h.timestamp ASC, h.data_record_sk ASC LIMIT :limit
    """)
    
    extract_sql_with_cursor = text(f"""
        SELECT h.data_record_sk, h.h_object_property_sk, h.data_file_id, h.data_source_id, h.load_dttm,
               h.raw_values, h.multiplier, h.h_spectrum_type_sk, h.timestamp as base_timestamp
        FROM public.{dwh_table_name} h
        WHERE h.exported_to_s3_storage = FALSE
          AND (h.timestamp, h.data_record_sk) > (:last_ts, :last_id)
        ORDER BY h.timestamp ASC, h.data_record_sk ASC LIMIT :limit
    """)
    
    # 2. БУФЕРИЗАЦИЯ (КОЛОНОЧНОЕ ХРАНЕНИЕ)
    total_source_records = 0
    total_decoded_points = 0
    total_uploaded_points = 0
    total_marked_records = 0
    iteration = 0
    
    partition_buffers: Dict[Tuple[str, date], Dict[str, list]] = {}
    pending_record_ids: List[str] = []
    pending_manifests: List[Any] = []
    
    current_buffered_rows = 0 
    buffered_source_ids = set() # 🔥 Уникальные исходные ID
    
    last_timestamp: Optional[datetime] = None
    last_record_id: Optional[str] = None
    
    while True:
        iteration += 1
        
        try:
            with cloudberry_engine.begin() as conn:
                sql = extract_sql_no_cursor if last_timestamp is None else extract_sql_with_cursor
                params = {"limit": fetch_batch_size} if last_timestamp is None else \
                         {"limit": fetch_batch_size, "last_ts": last_timestamp, "last_id": last_record_id}
                rows = conn.execute(sql, params).fetchall()
        except Exception as e:
            log.error(f"❌ Ошибка извлечения чанка #{iteration}: {e}", exc_info=True)
            raise
        
        if not rows:
            log.info(f"✅ Чтение завершено (итераций: {iteration})")
            break
        
        total_source_records += len(rows)
        batch_decoded_count = 0
        
        for row in rows:
            (h_data_rec_sk, h_obj_prop_sk, row_data_file_id, data_src_id, load_dttm,
             raw_vals, multiplier, h_spectrum_type_sk, timestamp) = row
            
            rec_sk_str = ensure_uuid_string(h_data_rec_sk)
            
            if raw_vals is None or len(raw_vals) == 0:
                continue
                
            # Конвертация raw_values в список float64
            try:
                if isinstance(raw_vals, (bytes, bytearray)):
                    raw_values_list = np.frombuffer(raw_vals, dtype='<f8').tolist()
                elif isinstance(raw_vals, memoryview):
                    raw_values_list = np.frombuffer(bytes(raw_vals), dtype='<f8').tolist()
                elif isinstance(raw_vals, (list, tuple)):
                    raw_values_list = [float(v) for v in raw_vals]
                else:
                    continue
            except Exception as e:
                log.warning(f"⚠️ Ошибка конвертации raw_values для {rec_sk_str}: {e}")
                continue
            
            buffered_source_ids.add(rec_sk_str)
            file_id_str = ensure_uuid_string(row_data_file_id)
            src_id_str = ensure_uuid_string(data_src_id)
            load_dttm_str = load_dttm.isoformat() if isinstance(load_dttm, datetime) else str(load_dttm)
            h_obj_prop_str = ensure_uuid_string(h_obj_prop_sk)
            h_spec_type_str = ensure_uuid_string(h_spectrum_type_sk) if h_spectrum_type_sk is not None else None
            
            day_key = timestamp.date() if isinstance(timestamp, datetime) else datetime.now(timezone.utc).date()
            key = (file_id_str, day_key) if partition_by_source else ("all", day_key)
            
            if key not in partition_buffers:
                partition_buffers[key] = {
                    'data_record_sk': [], 'h_object_property_sk': [], 'data_file_id': [],
                    'data_source_id': [], 'load_dttm': [], 'timestamp': [], 'value': [],
                    'multiplier': [], 'h_spectrum_type_sk': []
                }
            
            buf = partition_buffers[key]
            buf['data_record_sk'].append(rec_sk_str)
            buf['h_object_property_sk'].append(h_obj_prop_str)
            buf['data_file_id'].append(file_id_str)
            buf['data_source_id'].append(src_id_str)
            buf['load_dttm'].append(load_dttm_str)
            buf['timestamp'].append(timestamp)
            buf['value'].append(raw_values_list)
            buf['multiplier'].append(float(multiplier) if multiplier is not None else None)
            buf['h_spectrum_type_sk'].append(h_spec_type_str)
            
            batch_decoded_count += 1
        
        total_decoded_points += batch_decoded_count
        current_buffered_rows += batch_decoded_count 
        
        # 3. СБРОС БУФЕРА
        if current_buffered_rows >= TARGET_ROWS_PER_FLUSH and partition_buffers:
            log.info(f"🚀 ДОСТИГНУТ ЛИМИТ ({current_buffered_rows:,} записей). Сбрасываем буферы в S3...")
            
            current_sequence_number = table_metadata.get("last-sequence-number", 0) + 1
            current_snapshot_id = abs(hash(f"{table_path}_{datetime.now(timezone.utc).isoformat()}_{iteration}"))
            
            flushed_points_count = 0
            for key, buffer_to_flush in list(partition_buffers.items()):
                if not buffer_to_flush.get('data_record_sk'): continue
                file_id_str_flush, day_date = key 
                
                manifest = _flush_common_partition_to_s3(
                    day_date, buffer_to_flush, s3_client, file_io, bucket, data_prefix, metadata_prefix,
                    partition_spec, partition_by_source, iceberg_type, value_is_nullable, pa_schema, table_metadata, file_id_str_flush,
                    snapshot_id=current_snapshot_id, sequence_number=current_sequence_number
                )
                if manifest:
                    pending_manifests.append(manifest)
                    flushed_points_count += len(buffer_to_flush['data_record_sk'])
            
            pending_record_ids.extend(list(buffered_source_ids))
            buffered_source_ids.clear()
            current_buffered_rows = 0 
            
            if pending_manifests:
                _commit_and_update_db_common(
                    pending_manifests, pending_record_ids, s3_client, file_io, bucket, table_path,
                    metadata_prefix, partition_spec, table_metadata, dwh_table_name,
                    snapshot_id=current_snapshot_id, sequence_number=current_sequence_number,
                    target_column="exported_to_s3_storage" # 🔥 Обязательно!
                )
                
                total_uploaded_points += flushed_points_count
                total_marked_records += len(pending_record_ids)
                
                pending_manifests = []
                pending_record_ids = []
                
                table_metadata = _init_or_load_table_metadata(s3_client, bucket, table_path, metadata_prefix, 
                                                              iceberg_type, pa_type, partition_by_source)
                
                del partition_buffers
                partition_buffers = {}
                gc.collect()
                _release_arrow_memory()
                _force_malloc_trim()

        if rows:
            last_timestamp = rows[-1][-1] # base_timestamp
            last_record_id = ensure_uuid_string(rows[-1][0]) # data_record_sk

    # 4. ФИНАЛЬНЫЙ СБРОС
    if current_buffered_rows > 0:
        log.info(f"🚀 ФИНАЛЬНЫЙ СБРОС: Осталось {current_buffered_rows:,} записей в буфере")
        
        pending_record_ids.extend(list(buffered_source_ids))
        buffered_source_ids.clear()
        
        current_sequence_number = table_metadata.get("last-sequence-number", 0) + 1
        current_snapshot_id = abs(hash(f"{table_path}_{datetime.now(timezone.utc).isoformat()}_final"))
        
        flushed_points_count = 0
        for key, buf in list(partition_buffers.items()):
            if not buf.get('data_record_sk'): continue
            file_id_str_flush, day_date = key
            
            manifest = _flush_common_partition_to_s3(
                day_date, buf, s3_client, file_io, bucket, data_prefix, metadata_prefix,
                partition_spec, partition_by_source, iceberg_type, value_is_nullable, pa_schema, table_metadata, file_id_str_flush,
                snapshot_id=current_snapshot_id, sequence_number=current_sequence_number
            )
            if manifest:
                pending_manifests.append(manifest)
                flushed_points_count += len(buf['data_record_sk'])
        
        if pending_manifests:
            _commit_and_update_db_common(
                pending_manifests, pending_record_ids, s3_client, file_io, bucket, table_path,
                metadata_prefix, partition_spec, table_metadata, dwh_table_name,
                snapshot_id=current_snapshot_id, sequence_number=current_sequence_number,
                target_column="exported_to_s3_storage"
            )
            total_uploaded_points += flushed_points_count
            total_marked_records += len(pending_record_ids)
            
        del partition_buffers
        partition_buffers = {}
        gc.collect()
        _release_arrow_memory()
        _force_malloc_trim()
    
    log.info(f"🎉 Завершён Spectrum: источник={total_source_records}, "
             f"записей={total_decoded_points}, загружено={total_uploaded_points}, помечено={total_marked_records}")
    
    return {
        'dwh_table_name': dwh_table_name,
        'source_records': total_source_records,
        'decoded_points': total_decoded_points,
        'uploaded_points': total_uploaded_points,
        'marked_records': total_marked_records
    }




def load_sample_data_to_iceberg(
    dwh_table_name: str,
    is_hist_data: bool,
    target_file_size_mb: int = 256,
    fetch_batch_size: int = 1000,
    partition_by_source: bool = True,
) -> dict:
    """
    Загрузка данных SampleData в Iceberg с колоночным хранением и защитой от OOM/VMEM Runaway.
    """
    # 🔥 Безопасный лимит: 500 тысяч записей. 
    # Каждая запись содержит массив (семпл), поэтому 500k строк могут занимать несколько ГБ RAM.
    TARGET_ROWS_PER_FLUSH = 5_000_000 
    
    # 0. ИНИЦИАЛИЗАЦИЯ S3 и ICEBERG
    s3_conn = BaseHook.get_connection('minio_conn')
    s3_endpoint = s3_conn.extra_dejson.get("host", "http://minio:9000")
    if not s3_endpoint.startswith("http"): s3_endpoint = f"http://{s3_endpoint}"
    bucket = s3_conn.extra_dejson.get("bucket", "iceberg-warehouse")
    region = s3_conn.extra_dejson.get("region", "us-east-1")
    
    s3_client = boto3.client("s3", endpoint_url=s3_endpoint, aws_access_key_id=s3_conn.login,
                             aws_secret_access_key=s3_conn.password, region_name=region,
                             config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))
    
    file_io = PyArrowFileIO(properties={"s3.endpoint": s3_endpoint, "s3.access-key-id": s3_conn.login,
                                        "s3.secret-access-key": s3_conn.password, "s3.region": region,
                                        "s3.path-style-access": "true"})
    
    data_mart_table_name = "sample_data"
    hist_path = 'history-data' if is_hist_data else 'non-history-data'
    table_path = f"iceberg_warehouse/object_data_values/{hist_path}/{data_mart_table_name}"
    metadata_prefix, data_prefix = f"{table_path}/metadata", f"{table_path}/data"
    table_location = f"s3://{bucket}/{table_path}"
    
    log.info(f"📍 Iceberg: {table_location} | TARGET_ROWS={TARGET_ROWS_PER_FLUSH:,}")
    
    iceberg_type = ListType(element_id=501, element_type=DoubleType(), element_required=False)
    pa_type = pa.list_(pa.float64())
    value_is_nullable = False
    
    # 🔥 День первым, источник вторым (Hive-style)
    # ВНИМАНИЕ: timestamp имеет индекс 6 в pa_schema (0-based)!
    partition_spec = PartitionSpec(
        PartitionField(source_id=6, field_id=1000, transform=DayTransform(), name="dt_day"),
        PartitionField(source_id=3, field_id=1001, transform=IdentityTransform(), name="source_id"),
        spec_id=0
    )
    
    pa_schema = pa.schema([
        pa.field("data_record_sk", pa.string(), nullable=False),
        pa.field("h_object_property_sk", pa.string(), nullable=True),
        pa.field("data_file_id", pa.string(), nullable=False),
        pa.field("data_source_id", pa.string(), nullable=False),
        pa.field("load_dttm", pa.string(), nullable=True),
        pa.field("value", pa_type, nullable=value_is_nullable),
        pa.field("timestamp", pa.timestamp("us"), nullable=True),
        pa.field("sample_rate", pa.float64(), nullable=True),
    ])
    
    table_metadata = _init_or_load_table_metadata(s3_client, bucket, table_path, metadata_prefix, 
                                                  iceberg_type, pa_type, partition_by_source)
    
    # 1. SQL ЗАПРОСЫ (Keyset pagination без фильтра по data_file_id)
    extract_sql_no_cursor = text(f"""
        SELECT h.data_record_sk, h.h_object_property_sk, h.data_file_id, h.data_source_id, h.load_dttm,
               h.raw_values, h.sample_rate, h.timestamp as base_timestamp
        FROM public.{dwh_table_name} h
        WHERE h.exported_to_s3_storage = FALSE
        ORDER BY h.timestamp ASC, h.data_record_sk ASC LIMIT :limit
    """)
    
    extract_sql_with_cursor = text(f"""
        SELECT h.data_record_sk, h.h_object_property_sk, h.data_file_id, h.data_source_id, h.load_dttm,
               h.raw_values, h.sample_rate, h.timestamp as base_timestamp
        FROM public.{dwh_table_name} h
        WHERE h.exported_to_s3_storage = FALSE
          AND (h.timestamp, h.data_record_sk) > (:last_ts, :last_id)
        ORDER BY h.timestamp ASC, h.data_record_sk ASC LIMIT :limit
    """)
    
    # 2. БУФЕРИЗАЦИЯ (КОЛОНОЧНОЕ ХРАНЕНИЕ)
    total_source_records = 0
    total_decoded_points = 0
    total_uploaded_points = 0
    total_marked_records = 0
    iteration = 0
    
    partition_buffers: Dict[Tuple[str, date], Dict[str, list]] = {}
    pending_record_ids: List[str] = []
    pending_manifests: List[Any] = []
    
    current_buffered_rows = 0 
    buffered_source_ids = set() # 🔥 Уникальные исходные ID
    
    last_timestamp: Optional[datetime] = None
    last_record_id: Optional[str] = None
    
    while True:
        iteration += 1
        
        try:
            with cloudberry_engine.begin() as conn:
                sql = extract_sql_no_cursor if last_timestamp is None else extract_sql_with_cursor
                params = {"limit": fetch_batch_size} if last_timestamp is None else \
                         {"limit": fetch_batch_size, "last_ts": last_timestamp, "last_id": last_record_id}
                rows = conn.execute(sql, params).fetchall()
        except Exception as e:
            log.error(f"❌ Ошибка извлечения чанка #{iteration}: {e}", exc_info=True)
            raise
        
        if not rows:
            log.info(f"✅ Чтение завершено (итераций: {iteration})")
            break
        
        total_source_records += len(rows)
        batch_decoded_count = 0
        
        for row in rows:
            (h_data_rec_sk, h_obj_prop_sk, row_data_file_id, data_src_id, load_dttm,
             raw_vals, sample_rate, timestamp) = row
            
            rec_sk_str = ensure_uuid_string(h_data_rec_sk)
            
            if raw_vals is None or len(raw_vals) == 0:
                continue
                
            # Конвертация raw_values в список float64
            try:
                if isinstance(raw_vals, (bytes, bytearray)):
                    raw_values_list = np.frombuffer(raw_vals, dtype='<f8').tolist()
                elif isinstance(raw_vals, memoryview):
                    raw_values_list = np.frombuffer(bytes(raw_vals), dtype='<f8').tolist()
                elif isinstance(raw_vals, (list, tuple)):
                    raw_values_list = [float(v) for v in raw_vals]
                else:
                    continue
            except Exception as e:
                log.warning(f"⚠️ Ошибка конвертации raw_values для {rec_sk_str}: {e}")
                continue
            
            buffered_source_ids.add(rec_sk_str)
            file_id_str = ensure_uuid_string(row_data_file_id)
            src_id_str = ensure_uuid_string(data_src_id)
            load_dttm_str = load_dttm.isoformat() if isinstance(load_dttm, datetime) else str(load_dttm)
            h_obj_prop_str = ensure_uuid_string(h_obj_prop_sk)
            
            day_key = timestamp.date() if isinstance(timestamp, datetime) else datetime.now(timezone.utc).date()
            key = (file_id_str, day_key) if partition_by_source else ("all", day_key)
            
            if key not in partition_buffers:
                partition_buffers[key] = {
                    'data_record_sk': [], 'h_object_property_sk': [], 'data_file_id': [],
                    'data_source_id': [], 'load_dttm': [], 'timestamp': [], 'value': [],
                    'sample_rate': []
                }
            
            buf = partition_buffers[key]
            buf['data_record_sk'].append(rec_sk_str)
            buf['h_object_property_sk'].append(h_obj_prop_str)
            buf['data_file_id'].append(file_id_str)
            buf['data_source_id'].append(src_id_str)
            buf['load_dttm'].append(load_dttm_str)
            buf['timestamp'].append(timestamp)
            buf['value'].append(raw_values_list)
            buf['sample_rate'].append(float(sample_rate) if sample_rate is not None else None)
            
            batch_decoded_count += 1
        
        total_decoded_points += batch_decoded_count
        current_buffered_rows += batch_decoded_count 
        
        # 3. СБРОС БУФЕРА
        if current_buffered_rows >= TARGET_ROWS_PER_FLUSH and partition_buffers:
            log.info(f"🚀 ДОСТИГНУТ ЛИМИТ ({current_buffered_rows:,} записей). Сбрасываем буферы в S3...")
            
            current_sequence_number = table_metadata.get("last-sequence-number", 0) + 1
            current_snapshot_id = abs(hash(f"{table_path}_{datetime.now(timezone.utc).isoformat()}_{iteration}"))
            
            flushed_points_count = 0
            for key, buffer_to_flush in list(partition_buffers.items()):
                if not buffer_to_flush.get('data_record_sk'): continue
                file_id_str_flush, day_date = key 
                
                manifest = _flush_common_partition_to_s3(
                    day_date, buffer_to_flush, s3_client, file_io, bucket, data_prefix, metadata_prefix,
                    partition_spec, partition_by_source, iceberg_type, value_is_nullable, pa_schema, table_metadata, file_id_str_flush,
                    snapshot_id=current_snapshot_id, sequence_number=current_sequence_number
                )
                if manifest:
                    pending_manifests.append(manifest)
                    flushed_points_count += len(buffer_to_flush['data_record_sk'])
            
            pending_record_ids.extend(list(buffered_source_ids))
            buffered_source_ids.clear()
            current_buffered_rows = 0 
            
            if pending_manifests:
                _commit_and_update_db_common(
                    pending_manifests, pending_record_ids, s3_client, file_io, bucket, table_path,
                    metadata_prefix, partition_spec, table_metadata, dwh_table_name,
                    snapshot_id=current_snapshot_id, sequence_number=current_sequence_number,
                    target_column="exported_to_s3_storage" # 🔥 Обязательно!
                )
                
                total_uploaded_points += flushed_points_count
                total_marked_records += len(pending_record_ids)
                
                pending_manifests = []
                pending_record_ids = []
                
                table_metadata = _init_or_load_table_metadata(s3_client, bucket, table_path, metadata_prefix, 
                                                              iceberg_type, pa_type, partition_by_source)
                
                del partition_buffers
                partition_buffers = {}
                gc.collect()
                _release_arrow_memory()
                _force_malloc_trim()

        if rows:
            last_timestamp = rows[-1][-1] # base_timestamp
            last_record_id = ensure_uuid_string(rows[-1][0]) # data_record_sk

    # 4. ФИНАЛЬНЫЙ СБРОС
    if current_buffered_rows > 0:
        log.info(f"🚀 ФИНАЛЬНЫЙ СБРОС: Осталось {current_buffered_rows:,} записей в буфере")
        
        pending_record_ids.extend(list(buffered_source_ids))
        buffered_source_ids.clear()
        
        current_sequence_number = table_metadata.get("last-sequence-number", 0) + 1
        current_snapshot_id = abs(hash(f"{table_path}_{datetime.now(timezone.utc).isoformat()}_final"))
        
        flushed_points_count = 0
        for key, buf in list(partition_buffers.items()):
            if not buf.get('data_record_sk'): continue
            file_id_str_flush, day_date = key
            
            manifest = _flush_common_partition_to_s3(
                day_date, buf, s3_client, file_io, bucket, data_prefix, metadata_prefix,
                partition_spec, partition_by_source, iceberg_type, value_is_nullable, pa_schema, table_metadata, file_id_str_flush,
                snapshot_id=current_snapshot_id, sequence_number=current_sequence_number
            )
            if manifest:
                pending_manifests.append(manifest)
                flushed_points_count += len(buf['data_record_sk'])
        
        if pending_manifests:
            _commit_and_update_db_common(
                pending_manifests, pending_record_ids, s3_client, file_io, bucket, table_path,
                metadata_prefix, partition_spec, table_metadata, dwh_table_name,
                snapshot_id=current_snapshot_id, sequence_number=current_sequence_number,
                target_column="exported_to_s3_storage"
            )
            total_uploaded_points += flushed_points_count
            total_marked_records += len(pending_record_ids)
            
        del partition_buffers
        partition_buffers = {}
        gc.collect()
        _release_arrow_memory()
        _force_malloc_trim()
    
    log.info(f"🎉 Завершён SampleData: источник={total_source_records}, "
             f"записей={total_decoded_points}, загружено={total_uploaded_points}, помечено={total_marked_records}")
    
    return {
        'dwh_table_name': dwh_table_name,
        'source_records': total_source_records,
        'decoded_points': total_decoded_points,
        'uploaded_points': total_uploaded_points,
        'marked_records': total_marked_records
    }




def load_siemens_sample_data_to_iceberg(
    dwh_table_name: str,
    is_hist_data: bool,
    target_file_size_mb: int = 256,
    fetch_batch_size: int = 1000,
    partition_by_source: bool = True,
) -> dict:
    """
    Загрузка данных Siemens Sample в Iceberg с колоночным хранением и защитой от OOM/VMEM Runaway.
    """
    # 🔥 Безопасный лимит: 500 тысяч записей. 
    # Каждая запись содержит массив (семпл), поэтому 500k строк могут занимать несколько ГБ RAM.
    TARGET_ROWS_PER_FLUSH = 5_000_000 
    
    # 0. ИНИЦИАЛИЗАЦИЯ S3 и ICEBERG
    s3_conn = BaseHook.get_connection('minio_conn')
    s3_endpoint = s3_conn.extra_dejson.get("host", "http://minio:9000")
    if not s3_endpoint.startswith("http"): s3_endpoint = f"http://{s3_endpoint}"
    bucket = s3_conn.extra_dejson.get("bucket", "iceberg-warehouse")
    region = s3_conn.extra_dejson.get("region", "us-east-1")
    
    s3_client = boto3.client("s3", endpoint_url=s3_endpoint, aws_access_key_id=s3_conn.login,
                             aws_secret_access_key=s3_conn.password, region_name=region,
                             config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))
    
    file_io = PyArrowFileIO(properties={"s3.endpoint": s3_endpoint, "s3.access-key-id": s3_conn.login,
                                        "s3.secret-access-key": s3_conn.password, "s3.region": region,
                                        "s3.path-style-access": "true"})
    
    data_mart_table_name = "siemens_sample_data"
    hist_path = 'history-data' if is_hist_data else 'non-history-data'
    table_path = f"iceberg_warehouse/object_data_values/{hist_path}/{data_mart_table_name}"
    metadata_prefix, data_prefix = f"{table_path}/metadata", f"{table_path}/data"
    table_location = f"s3://{bucket}/{table_path}"
    
    log.info(f"📍 Iceberg: {table_location} | TARGET_ROWS={TARGET_ROWS_PER_FLUSH:,}")
    
    iceberg_type = ListType(element_id=501, element_type=DoubleType(), element_required=False)
    pa_type = pa.list_(pa.float64())
    value_is_nullable = False
    
    # 🔥 День первым, источник вторым (Hive-style)
    # ВНИМАНИЕ: timestamp имеет индекс 6 в pa_schema (0-based)!
    partition_spec = PartitionSpec(
        PartitionField(source_id=6, field_id=1000, transform=DayTransform(), name="dt_day"),
        PartitionField(source_id=3, field_id=1001, transform=IdentityTransform(), name="source_id"),
        spec_id=0
    )
    
    pa_schema = pa.schema([
        pa.field("data_record_sk", pa.string(), nullable=False),
        pa.field("h_object_property_sk", pa.string(), nullable=True),
        pa.field("data_file_id", pa.string(), nullable=False),
        pa.field("data_source_id", pa.string(), nullable=False),
        pa.field("load_dttm", pa.string(), nullable=True),
        pa.field("value", pa_type, nullable=value_is_nullable),
        pa.field("timestamp", pa.timestamp("us"), nullable=True),
        pa.field("sample_rate", pa.float64(), nullable=True),
    ])
    
    table_metadata = _init_or_load_table_metadata(s3_client, bucket, table_path, metadata_prefix, 
                                                  iceberg_type, pa_type, partition_by_source)
    
    # 1. SQL ЗАПРОСЫ (Keyset pagination без фильтра по data_file_id)
    extract_sql_no_cursor = text(f"""
        SELECT h.data_record_sk, h.h_object_property_sk, h.data_file_id, h.data_source_id, h.load_dttm,
               h.raw_values, h.sample_rate, h.timestamp as base_timestamp
        FROM public.{dwh_table_name} h
        WHERE h.exported_to_s3_storage = FALSE
        ORDER BY h.timestamp ASC, h.data_record_sk ASC LIMIT :limit
    """)
    
    extract_sql_with_cursor = text(f"""
        SELECT h.data_record_sk, h.h_object_property_sk, h.data_file_id, h.data_source_id, h.load_dttm,
               h.raw_values, h.sample_rate, h.timestamp as base_timestamp
        FROM public.{dwh_table_name} h
        WHERE h.exported_to_s3_storage = FALSE
          AND (h.timestamp, h.data_record_sk) > (:last_ts, :last_id)
        ORDER BY h.timestamp ASC, h.data_record_sk ASC LIMIT :limit
    """)
    
    # 2. БУФЕРИЗАЦИЯ (КОЛОНОЧНОЕ ХРАНЕНИЕ)
    total_source_records = 0
    total_decoded_points = 0
    total_uploaded_points = 0
    total_marked_records = 0
    iteration = 0
    
    partition_buffers: Dict[Tuple[str, date], Dict[str, list]] = {}
    pending_record_ids: List[str] = []
    pending_manifests: List[Any] = []
    
    current_buffered_rows = 0 
    buffered_source_ids = set() # 🔥 Уникальные исходные ID
    
    last_timestamp: Optional[datetime] = None
    last_record_id: Optional[str] = None
    
    while True:
        iteration += 1
        
        try:
            with cloudberry_engine.begin() as conn:
                sql = extract_sql_no_cursor if last_timestamp is None else extract_sql_with_cursor
                params = {"limit": fetch_batch_size} if last_timestamp is None else \
                         {"limit": fetch_batch_size, "last_ts": last_timestamp, "last_id": last_record_id}
                rows = conn.execute(sql, params).fetchall()
        except Exception as e:
            log.error(f"❌ Ошибка извлечения чанка #{iteration}: {e}", exc_info=True)
            raise
        
        if not rows:
            log.info(f"✅ Чтение завершено (итераций: {iteration})")
            break
        
        total_source_records += len(rows)
        batch_decoded_count = 0
        
        for row in rows:
            (h_data_rec_sk, h_obj_prop_sk, row_data_file_id, data_src_id, load_dttm,
             raw_vals, sample_rate, timestamp) = row
            
            rec_sk_str = ensure_uuid_string(h_data_rec_sk)
            
            if raw_vals is None or len(raw_vals) == 0:
                continue
                
            # Конвертация raw_values в список float64
            try:
                if isinstance(raw_vals, (bytes, bytearray)):
                    raw_values_list = np.frombuffer(raw_vals, dtype='<f8').tolist()
                elif isinstance(raw_vals, memoryview):
                    raw_values_list = np.frombuffer(bytes(raw_vals), dtype='<f8').tolist()
                elif isinstance(raw_vals, (list, tuple)):
                    raw_values_list = [float(v) for v in raw_vals]
                else:
                    continue
            except Exception as e:
                log.warning(f"⚠️ Ошибка конвертации raw_values для {rec_sk_str}: {e}")
                continue
            
            buffered_source_ids.add(rec_sk_str)
            file_id_str = ensure_uuid_string(row_data_file_id)
            src_id_str = ensure_uuid_string(data_src_id)
            load_dttm_str = load_dttm.isoformat() if isinstance(load_dttm, datetime) else str(load_dttm)
            h_obj_prop_str = ensure_uuid_string(h_obj_prop_sk)
            
            day_key = timestamp.date() if isinstance(timestamp, datetime) else datetime.now(timezone.utc).date()
            key = (file_id_str, day_key) if partition_by_source else ("all", day_key)
            
            if key not in partition_buffers:
                partition_buffers[key] = {
                    'data_record_sk': [], 'h_object_property_sk': [], 'data_file_id': [],
                    'data_source_id': [], 'load_dttm': [], 'timestamp': [], 'value': [],
                    'sample_rate': []
                }
            
            buf = partition_buffers[key]
            buf['data_record_sk'].append(rec_sk_str)
            buf['h_object_property_sk'].append(h_obj_prop_str)
            buf['data_file_id'].append(file_id_str)
            buf['data_source_id'].append(src_id_str)
            buf['load_dttm'].append(load_dttm_str)
            buf['timestamp'].append(timestamp)
            buf['value'].append(raw_values_list)
            buf['sample_rate'].append(float(sample_rate) if sample_rate is not None else None)
            
            batch_decoded_count += 1
        
        total_decoded_points += batch_decoded_count
        current_buffered_rows += batch_decoded_count 
        
        # 3. СБРОС БУФЕРА
        if current_buffered_rows >= TARGET_ROWS_PER_FLUSH and partition_buffers:
            log.info(f"🚀 ДОСТИГНУТ ЛИМИТ ({current_buffered_rows:,} записей). Сбрасываем буферы в S3...")
            
            current_sequence_number = table_metadata.get("last-sequence-number", 0) + 1
            current_snapshot_id = abs(hash(f"{table_path}_{datetime.now(timezone.utc).isoformat()}_{iteration}"))
            
            flushed_points_count = 0
            for key, buffer_to_flush in list(partition_buffers.items()):
                if not buffer_to_flush.get('data_record_sk'): continue
                file_id_str_flush, day_date = key 
                
                manifest = _flush_common_partition_to_s3(
                    day_date, buffer_to_flush, s3_client, file_io, bucket, data_prefix, metadata_prefix,
                    partition_spec, partition_by_source, iceberg_type, value_is_nullable, pa_schema, table_metadata, file_id_str_flush,
                    snapshot_id=current_snapshot_id, sequence_number=current_sequence_number
                )
                if manifest:
                    pending_manifests.append(manifest)
                    flushed_points_count += len(buffer_to_flush['data_record_sk'])
            
            pending_record_ids.extend(list(buffered_source_ids))
            buffered_source_ids.clear()
            current_buffered_rows = 0 
            
            if pending_manifests:
                _commit_and_update_db_common(
                    pending_manifests, pending_record_ids, s3_client, file_io, bucket, table_path,
                    metadata_prefix, partition_spec, table_metadata, dwh_table_name,
                    snapshot_id=current_snapshot_id, sequence_number=current_sequence_number,
                    target_column="exported_to_s3_storage" # 🔥 Обязательно!
                )
                
                total_uploaded_points += flushed_points_count
                total_marked_records += len(pending_record_ids)
                
                pending_manifests = []
                pending_record_ids = []
                
                table_metadata = _init_or_load_table_metadata(s3_client, bucket, table_path, metadata_prefix, 
                                                              iceberg_type, pa_type, partition_by_source)
                
                del partition_buffers
                partition_buffers = {}
                gc.collect()
                _release_arrow_memory()
                _force_malloc_trim()

        if rows:
            last_timestamp = rows[-1][-1] # base_timestamp
            last_record_id = ensure_uuid_string(rows[-1][0]) # data_record_sk

    # 4. ФИНАЛЬНЫЙ СБРОС
    if current_buffered_rows > 0:
        log.info(f"🚀 ФИНАЛЬНЫЙ СБРОС: Осталось {current_buffered_rows:,} записей в буфере")
        
        pending_record_ids.extend(list(buffered_source_ids))
        buffered_source_ids.clear()
        
        current_sequence_number = table_metadata.get("last-sequence-number", 0) + 1
        current_snapshot_id = abs(hash(f"{table_path}_{datetime.now(timezone.utc).isoformat()}_final"))
        
        flushed_points_count = 0
        for key, buf in list(partition_buffers.items()):
            if not buf.get('data_record_sk'): continue
            file_id_str_flush, day_date = key
            
            manifest = _flush_common_partition_to_s3(
                day_date, buf, s3_client, file_io, bucket, data_prefix, metadata_prefix,
                partition_spec, partition_by_source, iceberg_type, value_is_nullable, pa_schema, table_metadata, file_id_str_flush,
                snapshot_id=current_snapshot_id, sequence_number=current_sequence_number
            )
            if manifest:
                pending_manifests.append(manifest)
                flushed_points_count += len(buf['data_record_sk'])
        
        if pending_manifests:
            _commit_and_update_db_common(
                pending_manifests, pending_record_ids, s3_client, file_io, bucket, table_path,
                metadata_prefix, partition_spec, table_metadata, dwh_table_name,
                snapshot_id=current_snapshot_id, sequence_number=current_sequence_number,
                target_column="exported_to_s3_storage"
            )
            total_uploaded_points += flushed_points_count
            total_marked_records += len(pending_record_ids)
            
        del partition_buffers
        partition_buffers = {}
        gc.collect()
        _release_arrow_memory()
        _force_malloc_trim()
    
    log.info(f"🎉 Завершён Siemens Sample: источник={total_source_records}, "
             f"записей={total_decoded_points}, загружено={total_uploaded_points}, помечено={total_marked_records}")
    
    return {
        'dwh_table_name': dwh_table_name,
        'source_records': total_source_records,
        'decoded_points': total_decoded_points,
        'uploaded_points': total_uploaded_points,
        'marked_records': total_marked_records
    }






def load_diagnostic_data_to_iceberg(
    dwh_table_name: str,
    is_hist_data: bool,
    target_file_size_mb: int = 256,
    fetch_batch_size: int = 1000,
    partition_by_source: bool = True,
) -> dict:
    """
    Загрузка диагностических данных (DiagnosticArray) в Iceberg с колоночным хранением и защитой от OOM/VMEM Runaway.
    """
    # 🔥 Безопасный лимит: 2 млн записей. Строки занимают больше памяти, чем float64.
    TARGET_ROWS_PER_FLUSH = 5_000_000 
    
    # 0. ИНИЦИАЛИЗАЦИЯ S3 и ICEBERG
    s3_conn = BaseHook.get_connection('minio_conn')
    s3_endpoint = s3_conn.extra_dejson.get("host", "http://minio:9000")
    if not s3_endpoint.startswith("http"): s3_endpoint = f"http://{s3_endpoint}"
    bucket = s3_conn.extra_dejson.get("bucket", "iceberg-warehouse")
    region = s3_conn.extra_dejson.get("region", "us-east-1")
    
    s3_client = boto3.client("s3", endpoint_url=s3_endpoint, aws_access_key_id=s3_conn.login,
                             aws_secret_access_key=s3_conn.password, region_name=region,
                             config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))
    
    file_io = PyArrowFileIO(properties={"s3.endpoint": s3_endpoint, "s3.access-key-id": s3_conn.login,
                                        "s3.secret-access-key": s3_conn.password, "s3.region": region,
                                        "s3.path-style-access": "true"})
    
    data_mart_table_name = "diagnostic_array_data"
    hist_path = 'history-data' if is_hist_data else 'non-history-data'
    table_path = f"iceberg_warehouse/object_data_values/{hist_path}/{data_mart_table_name}"
    metadata_prefix, data_prefix = f"{table_path}/metadata", f"{table_path}/data"
    table_location = f"s3://{bucket}/{table_path}"
    
    log.info(f"📍 Iceberg: {table_location} | TARGET_ROWS={TARGET_ROWS_PER_FLUSH:,}")
    
    iceberg_type = StringType()
    pa_type = pa.string()
    value_is_nullable = True
    
    # Маппинг defect_state -> UUID
    defect_state_uuid_map = {
        0: 'ca96189c-0685-9e77-22f3-f4a0c5b77c7c',   # Unknown
        1: 'de52b683-9997-c776-7e0d-332b90db8e7d',   # NotProcessed
        2: '96ef9f95-3e13-63de-5454-0b300019e116',   # Normal
        3: '465afb20-695a-212f-c26d-81b25a71939e',   # Low
        4: '23991a7f-3993-52ea-f991-9e55fee651c2',   # Medium
        5: '2427d0a4-37ec-1612-60a0-d92b28ce4888',   # High
    }
    
    # 🔥 День первым (индекс 5), источник вторым (индекс 2)
    partition_spec = PartitionSpec(
        PartitionField(source_id=5, field_id=1000, transform=DayTransform(), name="dt_day"),
        PartitionField(source_id=2, field_id=1001, transform=IdentityTransform(), name="source_id"),
        spec_id=0
    )
    
    pa_schema = pa.schema([
        pa.field("data_record_sk", pa.string(), nullable=False),
        pa.field("h_object_property_sk", pa.string(), nullable=True),
        pa.field("data_file_id", pa.string(), nullable=False),
        pa.field("data_source_id", pa.string(), nullable=False),
        pa.field("load_dttm", pa.string(), nullable=True),
        pa.field("timestamp", pa.timestamp("us"), nullable=True),
        pa.field("id", pa.string(), nullable=True),
        pa.field("h_diagnostic_defect_state_sk", pa.string(), nullable=True),
        pa.field("priority", pa.int32(), nullable=True),
        pa.field("tag_name", pa.string(), nullable=True),
        pa.field("defect_name", pa.string(), nullable=True),
        pa.field("defect_details", pa.string(), nullable=True),
        pa.field("group_name", pa.string(), nullable=True),
        pa.field("recommendations", pa.string(), nullable=True),
    ])
    
    table_metadata = _init_or_load_table_metadata(s3_client, bucket, table_path, metadata_prefix, 
                                                  iceberg_type, pa_type, partition_by_source)
    
    # 1. SQL ЗАПРОСЫ
    extract_sql_no_cursor = text(f"""
        SELECT h.data_record_sk, h.h_object_property_sk, h.data_file_id, h.data_source_id, h.load_dttm,
               h.raw_values, h.timestamp as base_timestamp
        FROM public.{dwh_table_name} h
        WHERE h.exported_to_s3_storage = FALSE
        ORDER BY h.timestamp ASC, h.data_record_sk ASC LIMIT :limit
    """)
    
    extract_sql_with_cursor = text(f"""
        SELECT h.data_record_sk, h.h_object_property_sk, h.data_file_id, h.data_source_id, h.load_dttm,
               h.raw_values, h.timestamp as base_timestamp
        FROM public.{dwh_table_name} h
        WHERE h.exported_to_s3_storage = FALSE
          AND (h.timestamp, h.data_record_sk) > (:last_ts, :last_id)
        ORDER BY h.timestamp ASC, h.data_record_sk ASC LIMIT :limit
    """)
    
    # 2. БУФЕРИЗАЦИЯ (КОЛОНОЧНОЕ ХРАНЕНИЕ)
    total_source_records = 0
    total_diagnostic_records = 0
    total_uploaded_points = 0
    total_marked_records = 0
    iteration = 0
    
    partition_buffers: Dict[Tuple[str, date], Dict[str, list]] = {}
    pending_record_ids: List[str] = []
    pending_manifests: List[Any] = []
    
    current_buffered_rows = 0 
    buffered_source_ids = set() # 🔥 Уникальные исходные ID
    
    last_timestamp: Optional[datetime] = None
    last_record_id: Optional[str] = None
    
    while True:
        iteration += 1
        
        try:
            with cloudberry_engine.begin() as conn:
                sql = extract_sql_no_cursor if last_timestamp is None else extract_sql_with_cursor
                params = {"limit": fetch_batch_size} if last_timestamp is None else \
                         {"limit": fetch_batch_size, "last_ts": last_timestamp, "last_id": last_record_id}
                rows = conn.execute(sql, params).fetchall()
        except Exception as e:
            log.error(f"❌ Ошибка извлечения чанка #{iteration}: {e}", exc_info=True)
            raise
        
        if not rows:
            log.info(f"✅ Чтение завершено (итераций: {iteration})")
            break
        
        total_source_records += len(rows)
        batch_decoded_count = 0
        
        for row in rows:
            h_data_rec_sk, h_obj_prop_sk, row_data_file_id, data_src_id, load_dttm, raw_vals, timestamp = row
            rec_sk_str = ensure_uuid_string(h_data_rec_sk)
            
            if not raw_vals:
                continue
                
            try:
                blob_bytes = bytes(raw_vals) if isinstance(raw_vals, memoryview) else raw_vals
                if not isinstance(blob_bytes, (bytes, bytearray)):
                    blob_bytes = bytes(blob_bytes)
                diagnostic_records = deserialize_diagnostic_array_blob(blob_bytes)
            except Exception as e:
                log.error(f"❌ Ошибка десериализации {rec_sk_str}: {e}", exc_info=True)
                continue
            
            if not diagnostic_records:
                continue
            
            # 🔥 Добавляем исходный ID в set (один раз на source row, даже если записей много)
            buffered_source_ids.add(rec_sk_str)
            
            file_id_str = ensure_uuid_string(row_data_file_id)
            src_id_str = ensure_uuid_string(data_src_id)
            load_dttm_str = load_dttm.isoformat() if isinstance(load_dttm, datetime) else str(load_dttm)
            h_obj_prop_str = ensure_uuid_string(h_obj_prop_sk)
            
            day_key = timestamp.date() if isinstance(timestamp, datetime) else datetime.now(timezone.utc).date()
            key = (file_id_str, day_key) if partition_by_source else ("all", day_key)
            
            if key not in partition_buffers:
                partition_buffers[key] = {
                    'data_record_sk': [], 'h_object_property_sk': [], 'data_file_id': [],
                    'data_source_id': [], 'load_dttm': [], 'timestamp': [],
                    'id': [], 'h_diagnostic_defect_state_sk': [], 'priority': [],
                    'tag_name': [], 'defect_name': [], 'defect_details': [],
                    'group_name': [], 'recommendations': []
                }
            
            buf = partition_buffers[key]
            
            for diag_rec in diagnostic_records:
                defect_state = diag_rec.get('defect_state', 0)
                defect_state_uuid = defect_state_uuid_map.get(defect_state, defect_state_uuid_map[0])
                
                buf['data_record_sk'].append(rec_sk_str)
                buf['h_object_property_sk'].append(h_obj_prop_str)
                buf['data_file_id'].append(file_id_str)
                buf['data_source_id'].append(src_id_str)
                buf['load_dttm'].append(load_dttm_str)
                buf['timestamp'].append(timestamp)
                
                buf['id'].append(str(diag_rec.get('id')) if diag_rec.get('id') is not None else None)
                buf['h_diagnostic_defect_state_sk'].append(defect_state_uuid)
                buf['priority'].append(int(diag_rec.get('priority')) if diag_rec.get('priority') is not None else None)
                buf['tag_name'].append(diag_rec.get('tag_name'))
                buf['defect_name'].append(diag_rec.get('defect_name'))
                buf['defect_details'].append(diag_rec.get('defect_details'))
                buf['group_name'].append(diag_rec.get('group_name'))
                buf['recommendations'].append(diag_rec.get('recommendations'))
                
                batch_decoded_count += 1
        
        total_diagnostic_records += batch_decoded_count
        current_buffered_rows += batch_decoded_count 
        
        # 3. СБРОС БУФЕРА
        if current_buffered_rows >= TARGET_ROWS_PER_FLUSH and partition_buffers:
            log.info(f"🚀 ДОСТИГНУТ ЛИМИТ ({current_buffered_rows:,} записей). Сбрасываем буферы в S3...")
            
            current_sequence_number = table_metadata.get("last-sequence-number", 0) + 1
            current_snapshot_id = abs(hash(f"{table_path}_{datetime.now(timezone.utc).isoformat()}_{iteration}"))
            
            flushed_points_count = 0
            for key, buffer_to_flush in list(partition_buffers.items()):
                if not buffer_to_flush.get('data_record_sk'): continue
                file_id_str_flush, day_date = key 
                
                manifest = _flush_common_partition_to_s3(
                    day_date, buffer_to_flush, s3_client, file_io, bucket, data_prefix, metadata_prefix,
                    partition_spec, partition_by_source, iceberg_type, value_is_nullable, pa_schema, table_metadata, file_id_str_flush,
                    snapshot_id=current_snapshot_id, sequence_number=current_sequence_number
                )
                if manifest:
                    pending_manifests.append(manifest)
                    flushed_points_count += len(buffer_to_flush['data_record_sk'])
            
            # 🔥 Передаем в БД ТОЛЬКО уникальные исходные ID
            pending_record_ids.extend(list(buffered_source_ids))
            buffered_source_ids.clear()
            current_buffered_rows = 0 
            
            if pending_manifests:
                _commit_and_update_db_common(
                    pending_manifests, pending_record_ids, s3_client, file_io, bucket, table_path,
                    metadata_prefix, partition_spec, table_metadata, dwh_table_name,
                    snapshot_id=current_snapshot_id, sequence_number=current_sequence_number,
                    target_column="exported_to_s3_storage" # 🔥 Обязательно!
                )
                
                total_uploaded_points += flushed_points_count
                total_marked_records += len(pending_record_ids)
                
                pending_manifests = []
                pending_record_ids = []
                
                table_metadata = _init_or_load_table_metadata(s3_client, bucket, table_path, metadata_prefix, 
                                                              iceberg_type, pa_type, partition_by_source)
                
                del partition_buffers
                partition_buffers = {}
                gc.collect()
                _release_arrow_memory()
                _force_malloc_trim()

        if rows:
            last_timestamp = rows[-1][-1] # base_timestamp
            last_record_id = ensure_uuid_string(rows[-1][0]) # data_record_sk

    # 4. ФИНАЛЬНЫЙ СБРОС
    if current_buffered_rows > 0:
        log.info(f"🚀 ФИНАЛЬНЫЙ СБРОС: Осталось {current_buffered_rows:,} записей в буфере")
        
        pending_record_ids.extend(list(buffered_source_ids))
        buffered_source_ids.clear()
        
        current_sequence_number = table_metadata.get("last-sequence-number", 0) + 1
        current_snapshot_id = abs(hash(f"{table_path}_{datetime.now(timezone.utc).isoformat()}_final"))
        
        flushed_points_count = 0
        for key, buf in list(partition_buffers.items()):
            if not buf.get('data_record_sk'): continue
            file_id_str_flush, day_date = key
            
            manifest = _flush_common_partition_to_s3(
                day_date, buf, s3_client, file_io, bucket, data_prefix, metadata_prefix,
                partition_spec, partition_by_source, iceberg_type, value_is_nullable, pa_schema, table_metadata, file_id_str_flush,
                snapshot_id=current_snapshot_id, sequence_number=current_sequence_number
            )
            if manifest:
                pending_manifests.append(manifest)
                flushed_points_count += len(buf['data_record_sk'])
        
        if pending_manifests:
            _commit_and_update_db_common(
                pending_manifests, pending_record_ids, s3_client, file_io, bucket, table_path,
                metadata_prefix, partition_spec, table_metadata, dwh_table_name,
                snapshot_id=current_snapshot_id, sequence_number=current_sequence_number,
                target_column="exported_to_s3_storage"
            )
            total_uploaded_points += flushed_points_count
            total_marked_records += len(pending_record_ids)
            
        del partition_buffers
        partition_buffers = {}
        gc.collect()
        _release_arrow_memory()
        _force_malloc_trim()
    
    log.info(f"🎉 Завершён Diagnostic: источник={total_source_records}, "
             f"записей={total_diagnostic_records}, загружено={total_uploaded_points}, помечено={total_marked_records}")
    
    return {
        'dwh_table_name': dwh_table_name,
        'source_records': total_source_records,
        'decoded_points': total_diagnostic_records,
        'uploaded_points': total_uploaded_points,
        'marked_records': total_marked_records
    }



import uuid
import struct
import psycopg2.extras  # type: ignore
from datetime import datetime
from typing import List

def insert_fh_values_to_staging(
    values: list,
    data_file_id: str,
    data_source_id: str,
    main_db_id: str,
    data_type_id: str = '05229302-46d6-f9af-261c-6e0ea91996a2', # UUID типа Double
    batch_size: int = 500_000,
    page_size: int = 50_000,
    stg_table_name: str = 'stg_fh_double_object_data_values_hist',
) -> int:
    """
    Батчевая вставка распарсенных значений из .fh файла в staging таблицу.
    🔥 Пропускает строки с нулевым значением (0.0 / \x00...), так как это NO_DATA.
    🔥 Правильно маппит все 11 колонок staging таблицы (без data_record_sk).
    """
    import struct
    import hashlib
    from datetime import datetime, timezone
    import psycopg2
    import psycopg2.extras
    from airflow.hooks.postgres_hook import PostgresHook
    from functions.common_functions import calc_hash_sk, calc_hub_hash_for_column
    
    log.info(f"📥 Вставка {len(values):,} значений в staging таблицу {stg_table_name}...")
    
    hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    # 🔥 ИСПРАВЛЕНО: Убран data_record_sk, добавлен id (object_id из парсера)
    # Порядок колонок точно соответствует failing row из логов и extract_object_data_values_to_staging
    insert_template = f"""
        INSERT INTO {stg_table_name} (
            id, timestamp, value, datatypeid, 
            h_object_property_sk, h_data_type_sk, hash_sk,
            main_db_id, data_file_id, data_source_id, load_dttm
        ) VALUES %s
    """
    
    load_dttm = datetime.now(timezone.utc)
    total_inserted = 0
    skipped_zero_count = 0
    
    def _get_attr(obj, name, default=None):
        if isinstance(obj, dict):
            return obj.get(name, default)
        return getattr(obj, name, default)
    
    try:
        for i in range(0, len(values), batch_size):
            chunk = values[i:i + batch_size]
            values_list = []
            
            for rec in chunk:
                # Извлечение полей из FhParsedValue
                object_id = _get_attr(rec, 'object_id') or _get_attr(rec, 'id') or _get_attr(rec, 'tag_name')
                timestamp = _get_attr(rec, 'timestamp')
                value_raw = _get_attr(rec, 'value') or _get_attr(rec, 'data_value')
                
                if object_id is None or timestamp is None:
                    continue
                
                # 🔥 Пропускаем нулевые значения (NO_DATA в .fh)
                is_zero = False
                if value_raw is None:
                    is_zero = True
                elif isinstance(value_raw, (int, float)):
                    if float(value_raw) == 0.0:
                        is_zero = True
                elif isinstance(value_raw, (bytes, bytearray, memoryview)):
                    if bytes(value_raw) == b'\x00\x00\x00\x00\x00\x00\x00\x00':
                        is_zero = True
                        
                if is_zero:
                    skipped_zero_count += 1
                    continue
                
                # Конвертация значения в bytea (little-endian double)
                bytea_val = None
                if isinstance(value_raw, (bytes, bytearray, memoryview)):
                    bytea_val = bytes(value_raw)
                else:
                    try:
                        bytea_val = struct.pack('<d', float(value_raw))
                    except (ValueError, TypeError, OverflowError):
                        continue
                
                # 🔥 Расчет всех хешей (как в extract_object_data_values_to_staging)
                row_dict_for_hash = {
                    'id': str(object_id),
                    'datatypeid': data_type_id,
                    'timestamp': timestamp.isoformat() if hasattr(timestamp, 'isoformat') else str(timestamp),
                    'data_file_id': data_file_id,
                    'main_db_id': main_db_id
                }
                
                hash_sk = calc_hash_sk(row_dict_for_hash)
                
                h_object_property_sk = calc_hub_hash_for_column(
                    row_dict_for_hash, 'id', False, '', main_db_id
                )
                
                h_data_type_sk = calc_hub_hash_for_column(
                    row_dict_for_hash, 'datatypeid', True, 'h_data_types', main_db_id
                )
                        
                values_list.append((
                    str(object_id),              # 1. id (🔥 вместо data_record_sk)
                    timestamp,                   # 2. timestamp
                    psycopg2.Binary(bytea_val),  # 3. value (bytea)
                    data_type_id,                # 4. datatypeid (🔥 NOT NULL)
                    str(h_object_property_sk),   # 5. h_object_property_sk
                    str(h_data_type_sk),         # 6. h_data_type_sk
                    str(hash_sk),                # 7. hash_sk
                    main_db_id,                  # 8. main_db_id
                    data_file_id,                # 9. data_file_id
                    data_source_id,              # 10. data_source_id
                    load_dttm,                   # 11. load_dttm
                ))
            
            if values_list:
                psycopg2.extras.execute_values(
                    cursor,
                    insert_template,
                    values_list,
                    page_size=page_size,
                    template=None
                )
                conn.commit()
                total_inserted += len(values_list)
                log.info(f"📊 Прогресс вставки: {total_inserted:,} / {len(values):,} "
                         f"({100 * total_inserted / len(values):.1f}%)")
                
    except Exception as e:
        conn.rollback()
        log.error(f"❌ Ошибка вставки в staging: {e}", exc_info=True)
        raise
    finally:
        cursor.close()
        conn.close()
        
    log.info(f"✅ Успешно вставлено в {stg_table_name}: {total_inserted:,} записей")
    log.info(f"⏭️ Пропущено нулевых значений (NO_DATA): {skipped_zero_count:,}")
    return total_inserted



from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException

def merge_fh_data_values_to_target(
    stg_table_name: str, 
    target_table_name: str
) -> dict:
    """
    Оптимизированный перенос данных из staging в целевую таблицу.
    
    Изменения:
    - Убран двойной COUNT(*)
    - Используется стриминг через серверный курсор
    - Добавлен ANALYZE после вставки
    """
    log.info(f"📤 Загрузка данных из {stg_table_name} в {target_table_name}...")
    
    hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    conn = hook.get_conn()
    
    try:
        # Проверяем наличие данных без полного COUNT(*)
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT EXISTS (
                SELECT 1 FROM {stg_table_name} LIMIT 1
            )
        """)
        has_data = cursor.fetchone()[0]
        cursor.close()
        
        if not has_data:
            log.warning(f"⚠️ Staging таблица пуста - нечего переносить")
            return {
                'status': 'skipped',
                'records_transferred': 0,
                'staging_count_before': 0
            }
        
        # Получаем примерное количество (быстрее чем COUNT(*))
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT reltuples::bigint 
            FROM pg_class 
            WHERE oid = '{stg_table_name}'::regclass
        """)
        estimated_count = max(0, cursor.fetchone()[0])
        cursor.close()
        log.info(f"📊 Примерное количество записей в staging: {estimated_count:,}")
        
        # Выполняем перенос с оптимизированной функцией
        records_transferred = merge_fh_data_optimized(
            stg_table_name=stg_table_name,
            target_table_name=target_table_name,
            batch_size=500_000 
        )
        
        if records_transferred == 0:
            log.warning(f"⚠️ Ни одна запись не была перенесена в целевую таблицу")
            return {
                'status': 'warning',
                'records_transferred': 0,
                'staging_count_before': estimated_count,
                'staging_count_after': estimated_count
            }
        
        log.info(f"✅ Успешно перенесено {records_transferred:,} записей в целевую таблицу")
        
        return {
            'status': 'success',
            'records_transferred': records_transferred,
            'staging_count_before': estimated_count,
            'records_transferred': records_transferred
        }
        
    except Exception as e:
        log.error(f"❌ Ошибка при переносе данных: {e}", exc_info=True)
        raise
    finally:
        conn.close()



def merge_fh_data(
    stg_table_name: str,
    target_table_name: str,
    batch_size: int = 10000
) -> int:
    """
    Переносит данные из staging в целевую таблицу для .fh файлов.
    Работает со схемой stg_fh_double_object_data_values_hist.
    """
    hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    total_inserted = 0
    
    try:
        # Получаем общее количество записей
        cursor.execute(f"SELECT COUNT(*) FROM {stg_table_name}")
        total_count = cursor.fetchone()[0]
        log.info(f"📊 Всего записей в staging для переноса: {total_count}")
        
        if total_count == 0:
            return 0
        
        # Читаем данные из staging батчами
        offset = 0
        while offset < total_count:
            cursor.execute(f"""
                SELECT 
                    hash_sk,
                    h_object_property_sk,
                    value,
                    timestamp,
                    data_file_id,
                    data_source_id,
                    load_dttm
                FROM {stg_table_name}
                ORDER BY load_dttm
                LIMIT {batch_size} OFFSET {offset}
            """)
            
            rows = cursor.fetchall()
            if not rows:
                break
            
            # Формируем данные для вставки в целевую таблицу
            insert_data = []
            for row in rows:
                hash_sk, h_object_property_sk, value_bytes, timestamp, data_file_id, data_source_id, load_dttm = row
                
                # Распаковываем bytea обратно в float
                data_value = struct.unpack('<d', value_bytes)[0] if value_bytes else None
                
                insert_data.append((
                    hash_sk,
                    h_object_property_sk,
                    data_value,
                    timestamp,
                    data_file_id,
                    data_source_id,
                    load_dttm,
                    False,
                    None
                ))
            
            # Вставляем в целевую таблицу (с проверкой на дубликаты по hash_sk)
            insert_query = f"""
                INSERT INTO {target_table_name} (
                    data_record_sk,
                    h_object_property_sk,
                    value,
                    timestamp,
                    data_file_id,
                    data_source_id,
                    load_dttm,
                    exported_to_s3_storage,
                    s3_storage_path
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (data_record_sk) DO NOTHING
            """
            
            psycopg2.extras.execute_batch(cursor, insert_query, insert_data)
            conn.commit()
            
            total_inserted += len(insert_data)
            offset += batch_size
            
            log.info(f"   📤 Перенесено {total_inserted}/{total_count} записей")
        
        log.info(f"✅ Всего перенесено в целевую таблицу: {total_inserted} записей")
        return total_inserted
    
    except Exception as e:
        conn.rollback()
        log.error(f"❌ Ошибка переноса данных: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
        
        
def extract_fh_data_to_staging(record_info: dict, stg_table_name:str) -> dict:
    """
    Парсит .fh файл и загружает значения в staging таблицу.
    """
    hdfs_path = record_info['hdfs_path']
    data_file_id = record_info['data_file_id']
    data_source_id = record_info['data_source_id']
    
    log.info(f"📂 Начало загрузки .fh файла: {hdfs_path}")
    log.info(f"   data_file_id: {data_file_id}")
    log.info(f"   data_source_id: {data_source_id}")
    
    with hdfs_tempfile(hdfs_path=hdfs_path) as local_fh_path:
        # Парсинг .fh файла
        log.info(f"🔍 Парсинг .fh файла...")
        parsed_file = parse_fh_file(local_fh_path)
        
        log.info(
            f"✅ Файл распарсен: "
            f"MainDb={parsed_file.main_db_name}, "
            f"строк={parsed_file.rows_count}, "
            f"значений для загрузки={len(parsed_file.values)}"
        )
        
        # Вставка в staging
        if parsed_file.values:
            log.info(f"📥 Загрузка значений в staging таблицу {stg_table_name}...")
            inserted_count = insert_fh_values_to_staging(
                values=parsed_file.values,
                data_file_id=data_file_id,
                data_source_id=data_source_id,
                batch_size=500_000,   
                page_size=50_000    
            )
            
            log.info(f"✅ Загружено {inserted_count} записей в staging")
        else:
            log.warning(f"⚠️ Файл не содержит значений для загрузки (все NO_DATA/UNKNOWN)")
            inserted_count = 0
    
    return {
        'status': 'success',
        'rows_count': parsed_file.rows_count,
        'values_loaded': inserted_count,
        'main_db_name': parsed_file.main_db_name,
        'from_date': parsed_file.from_date.isoformat(),
        'to_date': parsed_file.to_date.isoformat(),
    }
        

    
    
def load_creyt_time_series_for_file_id(
    dwh_table_name: str,
    clickhouse_table_name: str,
    data_file_id: str,
    clickhouse_batch_size: int = 1_000_000,
    cloudberry_fetch_chunk: int = 500,
    clickhouse_conn_id: str = 'clickhouse_conn',
) -> dict:
    """
    🔥 ОПТИМИЗИРОВАННАЯ ВЕРСИЯ с clickhouse_connect.insert_df
    
    Преимущества перед clickhouse_driver:
    1. insert_df в 5-10 раз быстрее execute с кортежами
    2. HTTP-протокол с LZ4 сжатием
    3. Автоматический маппинг pandas dtypes → ClickHouse types
    4. Поддержка InsertContext для переиспользования
    
    Returns:
        dict: {'data_file_id': str, 'records_processed': int, 'points_inserted': int}
    """
    from sqlalchemy import text
    from airflow.hooks.base import BaseHook
    from datetime import timezone
    import numpy as np
    import pandas as pd
    import clickhouse_connect
    
    total_points = 0
    total_records = 0
    
    log.info(f"🚀 Начало обработки data_file_id={data_file_id}")
    
    # =====================================================================
    # 🔥 ШАГ 0: Создаём clickhouse_connect клиент (HTTP, порт 8123)
    # =====================================================================
    conn_config = BaseHook.get_connection(clickhouse_conn_id)
    
    # 🔥 clickhouse_connect ВСЕГДА работает через HTTP (порт 8123)
    # Игнорируем порт из connection (там может быть 9000 для clickhouse-driver)
    client = clickhouse_connect.get_client(
        host=conn_config.host,
        port=8123,                          # 🔥 ЖЁСТКО задаём HTTP порт
        database=conn_config.schema or 'default',
        username=conn_config.login or 'default',
        password=conn_config.password or '',
        compress=True,                      # 🔥 LZ4 сжатие HTTP
        query_limit=0,                      # Без лимита строк
    )
    
    try:
        last_timestamp = None
        last_record_id = None
        iteration = 0
        
        while True:
            iteration += 1
            
            # =================================================================
            # ШАГ 1: Извлечение чанка (SQL без изменений)
            # =================================================================
            if last_timestamp is None:
                extract_sql = text(f"""
                    SELECT h.data_record_sk, h.h_object_property_sk, h.data_file_id,
                           h.data_source_id, h.load_dttm, h.sample_rate,
                           h.colibration_factor_a, h.colibration_factor_b,
                           h.raw_values, h.min_raw, h.max_raw, h.min_eu, h.max_eu, h.timestamp
                    FROM public.{dwh_table_name} h
                    WHERE h.data_file_id = :file_id AND h.exported_to_datamart = FALSE
                    ORDER BY h.timestamp ASC, h.data_record_sk ASC LIMIT :limit
                """)
                params = {"file_id": data_file_id, "limit": cloudberry_fetch_chunk}
            else:
                extract_sql = text(f"""
                    SELECT h.data_record_sk, h.h_object_property_sk, h.data_file_id,
                           h.data_source_id, h.load_dttm, h.sample_rate,
                           h.colibration_factor_a, h.colibration_factor_b,
                           h.raw_values, h.min_raw, h.max_raw, h.min_eu, h.max_eu, h.timestamp
                    FROM public.{dwh_table_name} h
                    WHERE h.data_file_id = :file_id AND h.exported_to_datamart = FALSE
                      AND (h.timestamp, h.data_record_sk) > (:last_ts, :last_id)
                    ORDER BY h.timestamp ASC, h.data_record_sk ASC LIMIT :limit
                """)
                params = {
                    "file_id": data_file_id, 
                    "limit": cloudberry_fetch_chunk,
                    "last_ts": last_timestamp, 
                    "last_id": last_record_id
                }
            
            with cloudberry_engine.begin() as conn:
                rows = conn.execute(extract_sql, params).fetchall()
            
            if not rows:
                log.info(f"✅ data_file_id={data_file_id}: все записи обработаны (итераций: {iteration})")
                break
            
            log.info(f"📥 Чанк #{iteration}: {len(rows)} записей")
            
            # =================================================================
            # 🔥 ШАГ 2: Сбор данных в словарь numpy массивов
            # =================================================================
            batch_cols = {
                'data_record_sk': [],
                'h_object_property_sk': [],
                'data_file_id': [],
                'data_source_id': [],
                'timestamp': [],
                'value': [],
                'load_dttm': []
            }
            
            processed_ids = []
            chunk_points = 0
            
            for row in rows:
                (dr_sk, hop_sk, df_id, ds_id, load_dttm, sample_rate, 
                 calib_a, calib_b, raw_vals, min_raw, max_raw, min_eu, max_eu, ts) = row
                
                if not raw_vals:
                    continue
                
                record_id = ensure_uuid_string(dr_sk)
                raw_bytes = bytes(raw_vals) if not isinstance(raw_vals, bytes) else raw_vals
                
                # 🔥 Векторное декодирование → numpy arrays
                ts_arr, val_arr = decode_creyt_timeseries_numpy(
                    raw_bytes, calib_a, calib_b, min_raw, max_raw, 
                    min_eu, max_eu, sample_rate, ts
                )
                
                if len(ts_arr) == 0:
                    continue
                
                n_points = len(ts_arr)
                
                # Добавляем массивы в батч
                batch_cols['data_record_sk'].append(
                    np.full(n_points, record_id, dtype=object)
                )
                batch_cols['h_object_property_sk'].append(
                    np.full(n_points, ensure_uuid_string(hop_sk), dtype=object)
                )
                batch_cols['data_file_id'].append(
                    np.full(n_points, ensure_uuid_string(df_id), dtype=object)
                )
                batch_cols['data_source_id'].append(
                    np.full(n_points, ensure_uuid_string(ds_id), dtype=object)
                )
                
                # Timestamps (numpy datetime64)
                batch_cols['timestamp'].append(ts_arr)
                
                # Values (numpy float64)
                batch_cols['value'].append(val_arr)
                
                # load_dttm (константа для всей записи)
                ld_aware = load_dttm if load_dttm.tzinfo else load_dttm.replace(tzinfo=timezone.utc)
                batch_cols['load_dttm'].append(
                    np.full(n_points, ld_aware, dtype=object)
                )
                
                processed_ids.append(record_id)
                chunk_points += n_points
            
            # =================================================================
            # 🔥 ШАГ 3: Формирование DataFrame и вставка через insert_df
            # =================================================================
            if processed_ids:
                # Конкатенируем numpy массивы
                df_data = {k: np.concatenate(v) for k, v in batch_cols.items()}
                df = pd.DataFrame(df_data)
                
                # 🔥 Гарантируем правильные dtypes для ClickHouse
                # UUID → string
                for col in ['data_record_sk', 'h_object_property_sk', 
                           'data_file_id', 'data_source_id']:
                    df[col] = df[col].astype(str)
                
                # Timestamps → datetime64[us, UTC]
                df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
                df['load_dttm'] = pd.to_datetime(df['load_dttm'], utc=True)
                
                # Values → float64
                df['value'] = df['value'].astype('float64')
                
                # Порядок колонок
                cols_order = [
                    'data_record_sk', 'h_object_property_sk', 'data_file_id',
                    'data_source_id', 'timestamp', 'value', 'load_dttm'
                ]
                df = df[cols_order]
                
                try:
                    # 🔥 insert_df — в 5-10 раз быстрее execute с кортежами
                    # Принимает имя таблицы и DataFrame
                    client.insert_df(clickhouse_table_name, df)
                    
                    total_points += chunk_points
                    log.info(f"✅ Вставлено {chunk_points} точек через insert_df")
                except Exception as e:
                    log.error(f"❌ Ошибка вставки DataFrame: {e}", exc_info=True)
                    raise
                
                # Пометка экспортированных через temp table + COPY
                marked = mark_records_as_exported_via_temp_table(
                    cloudberry_engine, 
                    dwh_table_name, 
                    processed_ids,
                    data_file_id=data_file_id, 
                    use_copy=True
                )
                total_records += len(processed_ids)
                log.info(f"✅ Помечено {marked} записей")
            
            # =================================================================
            # ШАГ 4: Обновление курсора
            # =================================================================
            last_timestamp = rows[-1].timestamp
            last_record_id = ensure_uuid_string(rows[-1].data_record_sk)
            
            # Очистка памяти
            del rows, batch_cols, df_data, df
    
    finally:
        # 🔥 clickhouse_connect использует close() вместо disconnect()
        client.close()
    
    log.info(f"✅ data_file_id={data_file_id} завершён: "
             f"{total_records} записей, {total_points} точек")
    
    return {
        'data_file_id': data_file_id,
        'records_processed': total_records,
        'points_inserted': total_points,
    }





def merge_fh_data_values_to_target_by_file(
    stg_table_name: str,
    target_table_name: str,  # 🔥 Имя таблицы в ClickHouse и Iceberg (например, 'hist_double_data')
    main_db_name: str,
    is_hist_data: bool = None,   # 🔥 Автоопределяется по имени таблицы
    data_file_id: str = None,
    # =====================================================================
    # 🔥 ПАРАМЕТРЫ БАТЧИНГА (вынесены в сигнатуру)
    # =====================================================================
    ch_batch_size: int = 2_500_000,     # Строк за один insert_df в CH
    s3_batch_size: int = 50_000_000,    # Строк на один Parquet-файл (~125 МБ)
    read_chunk_size: int = 500_000,     # Размер чанка чтения из staging (yield_per)
) -> dict:
    """
    🔥 STREAMING merge .fh данных из staging СРАЗУ в ClickHouse и S3 (Iceberg).
    Минует промежуточное хранение в DWH (Cloudberry).
    Десериализует bytea (little-endian double) в float64 на лету.
    Использует column-wise буферы и ОДИН snapshot для всего прогона.
    """
    import gc, uuid, struct, boto3, pandas as pd, numpy as np, clickhouse_connect
    from botocore.config import Config
    from datetime import datetime, timezone, date, timedelta
    from typing import Dict, Tuple, Any, List
    from sqlalchemy import text, inspect
    from airflow.hooks.base import BaseHook
    import pyarrow as pa
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import DayTransform, IdentityTransform
    from pyiceberg.types import DoubleType
    from pyiceberg.io.pyarrow import PyArrowFileIO

    if data_file_id is not None and type(data_file_id).__name__ in ('XComArg', 'PlainXComArg'):
        raise TypeError("❌ Передан XComArg вместо строки!")

    # 🔥 АВТООПРЕДЕЛЕНИЕ is_hist_data
    if is_hist_data is None:
        is_hist_data = '_hist' in stg_table_name.lower()

    ZERO_UUID = '00000000-0000-0000-0000-000000000000'

    dwh_engine = get_dwh_engine('cloudberry_test_dwh')

    # =====================================================================
    # 1. ClickHouse Client Setup
    # =====================================================================
    conn_config = BaseHook.get_connection('clickhouse_conn')
    ch_client = clickhouse_connect.get_client(
        host=conn_config.host, port=8123,
        database=conn_config.schema or 'default',
        username=conn_config.login or 'default',
        password=conn_config.password or '',
        compress=True, query_limit=0,
    )

    # =====================================================================
    # 2. S3 / Iceberg Setup
    # =====================================================================
    s3_conn = BaseHook.get_connection('minio_conn')
    s3_endpoint = s3_conn.extra_dejson.get("host", "http://minio:9000")
    if not s3_endpoint.startswith("http"):
        s3_endpoint = f"http://{s3_endpoint}"
    bucket = s3_conn.extra_dejson.get("bucket", "iceberg-warehouse")
    region = s3_conn.extra_dejson.get("region", "us-east-1")

    s3_client = boto3.client(
        "s3", endpoint_url=s3_endpoint,
        aws_access_key_id=s3_conn.login, aws_secret_access_key=s3_conn.password,
        region_name=region, config=Config(signature_version="s3v4", s3={"addressing_style": "path"})
    )
    file_io = PyArrowFileIO(properties={
        "s3.endpoint": s3_endpoint, "s3.access-key-id": s3_conn.login,
        "s3.secret-access-key": s3_conn.password, "s3.region": region,
        "s3.path-style-access": "true"
    })

    hist_path = 'history-data' if is_hist_data else 'non-history-data'
    table_path = f"{main_db_name}/{hist_path}/{target_table_name}"
    metadata_prefix = f"{table_path}/metadata"
    data_prefix = f"{table_path}/data"

    iceberg_type = DoubleType()
    pa_type = pa.float64()
    value_is_nullable = False

    table_metadata = _init_or_load_table_metadata(
        s3_client, bucket, table_path, metadata_prefix,
        iceberg_type, pa_type, partition_by_source=True
    )

    def _get_field_id(metadata_dict: dict, field_names: list, schema_obj: pa.Schema) -> int:
        def find_field_id(obj, target_names):
            if isinstance(obj, dict):
                if obj.get("name") in target_names and "id" in obj: return obj.get("id")
                for v in obj.values():
                    res = find_field_id(v, target_names)
                    if res is not None: return res
            elif isinstance(obj, list):
                for item in obj:
                    res = find_field_id(item, target_names)
                    if res is not None: return res
            return None
        found_id = find_field_id(metadata_dict, field_names)
        if found_id is not None: return found_id
        for i, field in enumerate(schema_obj):
            if field.name in field_names: return i + 1
        raise ValueError(f"Field {field_names} not found")

    pa_schema = pa.schema([
        pa.field("data_record_sk", pa.string(), nullable=False),
        pa.field("h_object_property_sk", pa.string(), nullable=True),
        pa.field("data_file_id", pa.string(), nullable=False),
        pa.field("data_source_id", pa.string(), nullable=False),
        pa.field("main_db_id", pa.string(), nullable=True),
        pa.field("load_dttm", pa.string(), nullable=True),
        pa.field("value", pa.float64(), nullable=value_is_nullable),
        pa.field("timestamp", pa.timestamp("us"), nullable=True),
    ])

    ts_field_id = _get_field_id(table_metadata, ["timestamp", "date", "base_timestamp"], pa_schema)
    src_field_id = _get_field_id(table_metadata, ["data_source_id", "source_id"], pa_schema)

    partition_spec = PartitionSpec(
        PartitionField(source_id=ts_field_id, field_id=1000, transform=DayTransform(), name="dt_day"),
        PartitionField(source_id=src_field_id, field_id=1001, transform=IdentityTransform(), name="source_id"),
        spec_id=0
    )

    def _normalize_ts(ts_val: Any) -> datetime:
        if ts_val is None or (isinstance(ts_val, float) and pd.isna(ts_val)):
            return datetime(1970, 1, 1, tzinfo=timezone.utc)
        if hasattr(ts_val, 'total_seconds'):
            return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=ts_val.total_seconds())
        if isinstance(ts_val, datetime):
            return ts_val if ts_val.tzinfo else ts_val.replace(tzinfo=timezone.utc)
        try:
            ts_str = str(ts_val).replace(" ", "T")
            if "." not in ts_str and "T" in ts_str:
                ts_str += ".000000"
            dt = datetime.fromisoformat(ts_str)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return datetime(1970, 1, 1, tzinfo=timezone.utc)

    # =====================================================================
    # 3. Определяем наличие колонки main_db_id в staging
    # =====================================================================
    has_main_db = False
    try:
        insp = inspect(dwh_engine)
        columns = [c['name'] for c in insp.get_columns(stg_table_name, schema='public')]
        has_main_db = 'main_db_id' in columns
    except Exception as e:
        log.warning(f"⚠️ Не удалось проверить схему staging ({e}), предполагаем main_db_id=False")
        has_main_db = False

    main_db_col = "stg.main_db_id," if has_main_db else ""

    # =====================================================================
    # 4. SQL Query (параметризованный, с фильтром по data_file_id)
    # =====================================================================
    sql_fetch = text(f"""
        SELECT stg.hash_sk, stg.h_object_property_sk, stg.value, stg.timestamp,
               stg.data_file_id, stg.data_source_id, {main_db_col} stg.load_dttm
        FROM public.{stg_table_name} stg
        WHERE stg.data_file_id = :data_file_id
          AND stg.hash_sk IS NOT NULL
          AND stg.value IS NOT NULL
        ORDER BY stg.hash_sk
    """)
    sql_params = {"data_file_id": data_file_id}

    log.info(f"📥 STREAMING FH (file={data_file_id}, has_main_db={has_main_db}): "
             f"ch_batch={ch_batch_size:,}, s3_batch={s3_batch_size:,}, read_chunk={read_chunk_size}")

    # 🔥 Фиксируем snapshot ОДИН раз на весь прогон
    base_seq = table_metadata.get("last-sequence-number", 0) + 1
    base_snap_id = abs(hash(f"{table_path}_{datetime.now(timezone.utc).isoformat()}_{uuid.uuid4().hex}"))
    all_manifests = []
    total_s3_rows = 0
    total_processed = 0
    error_count = 0

    # 🔥 Column-wise буферы (dict-of-lists) — ускоряют insert_df в 3-5 раз
    ch_buf = {
        'data_record_sk': [], 'h_object_property_sk': [], 'data_file_id': [],
        'data_source_id': [], 'main_db_id': [], 'load_dttm': [], 'value': [], 'timestamp': []
    }
    s3_partition_buffers: Dict[Tuple[str, date], Dict[str, list]] = {}
    s3_buffered_rows = 0

    def _flush_ch():
        if not ch_buf['value']: return
        df = pd.DataFrame(ch_buf)
        for col in ['data_record_sk', 'h_object_property_sk', 'data_file_id', 'data_source_id', 'main_db_id']:
            if col in df.columns:
                df[col] = df[col].fillna(ZERO_UUID).astype(str)
        if 'timestamp' in df.columns: df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        if 'load_dttm' in df.columns: df['load_dttm'] = pd.to_datetime(df['load_dttm'], utc=True)
        if 'value' in df.columns: df['value'] = pd.to_numeric(df['value'], errors='coerce').astype('float64')
        cols_order = ['data_record_sk', 'h_object_property_sk', 'data_file_id', 'data_source_id', 'main_db_id',
                      'timestamp', 'value', 'load_dttm']
        for c in cols_order:
            if c not in df.columns: df[c] = None
        ch_client.insert_df(target_table_name, df[cols_order])
        log.info(f"  ✅ ClickHouse: {len(ch_buf['value']):,} записей")
        for k in ch_buf:
            ch_buf[k] = []

    def _flush_s3():
        nonlocal s3_partition_buffers, s3_buffered_rows, all_manifests, total_s3_rows
        if not s3_partition_buffers: return
        for (fid, day), buf in list(s3_partition_buffers.items()):
            if not buf['data_record_sk']: continue
            m = _flush_common_partition_to_s3(
                day, buf, s3_client, file_io, bucket, data_prefix, metadata_prefix,
                partition_spec, True, iceberg_type, value_is_nullable, pa_schema,
                table_metadata, fid, base_snap_id, base_seq
            )
            if m: all_manifests.append(m)
        total_s3_rows += s3_buffered_rows
        log.info(f"  ✅ S3: {s3_buffered_rows:,} записей → {len(all_manifests)} файлов всего")
        s3_partition_buffers = {}
        s3_buffered_rows = 0
        gc.collect()
        try:
            pa.default_memory_pool().release_unused()
        except Exception:
            pass

    # =====================================================================
    # 5. Main Streaming Loop (через SQLAlchemy server-side cursor)
    # =====================================================================
    try:
        with dwh_engine.begin() as conn:
            result = conn.execution_options(stream_results=True, yield_per=read_chunk_size).execute(
                sql_fetch, sql_params
            )

            for row in result:
                # Распаковываем строку с учётом наличия main_db_id
                if has_main_db:
                    hash_sk, h_obj_prop_sk, value_bytes, timestamp, df_id, ds_id, main_db_id, load_dttm = row
                else:
                    hash_sk, h_obj_prop_sk, value_bytes, timestamp, df_id, ds_id, load_dttm = row
                    main_db_id = None

                # 🔥 Десериализация bytea -> float64 (little-endian double)
                if value_bytes is None:
                    error_count += 1
                    continue
                try:
                    vb = bytes(value_bytes) if not isinstance(value_bytes, (bytes, bytearray)) else value_bytes
                    if len(vb) != 8:
                        raise ValueError(f"bad bytea length={len(vb)}")
                    data_value = struct.unpack('<d', vb)[0]
                except Exception as e:
                    error_count += 1
                    continue

                rec_sk_str = ensure_uuid_string(hash_sk)
                h_obj_prop_str = ensure_uuid_string(h_obj_prop_sk)
                file_id_str = ensure_uuid_string(df_id)
                src_id_str = ensure_uuid_string(ds_id)
                main_db_str = ensure_uuid_string(main_db_id) if main_db_id else ZERO_UUID
                norm_ts = _normalize_ts(timestamp)
                norm_ld = _normalize_ts(load_dttm)
                day_key = norm_ts.date()

                # 🔥 CH буфер (column-wise append)
                ch_buf['data_record_sk'].append(rec_sk_str)
                ch_buf['h_object_property_sk'].append(h_obj_prop_str)
                ch_buf['data_file_id'].append(file_id_str)
                ch_buf['data_source_id'].append(src_id_str)
                ch_buf['main_db_id'].append(main_db_str)
                ch_buf['load_dttm'].append(norm_ld)
                ch_buf['value'].append(data_value)
                ch_buf['timestamp'].append(norm_ts)

                if len(ch_buf['value']) >= ch_batch_size:
                    _flush_ch()

                # 🔥 S3 буфер (по партициям file_id+day)
                key = (file_id_str, day_key)
                if key not in s3_partition_buffers:
                    s3_partition_buffers[key] = {
                        'data_record_sk': [], 'h_object_property_sk': [], 'data_file_id': [],
                        'data_source_id': [], 'main_db_id': [], 'load_dttm': [], 'value': [], 'timestamp': []
                    }
                buf = s3_partition_buffers[key]
                buf['data_record_sk'].append(rec_sk_str)
                buf['h_object_property_sk'].append(h_obj_prop_str)
                buf['data_file_id'].append(file_id_str)
                buf['data_source_id'].append(src_id_str)
                buf['main_db_id'].append(main_db_str)
                buf['load_dttm'].append(norm_ld.isoformat() if isinstance(norm_ld, datetime) else str(norm_ld))
                buf['value'].append(data_value)
                buf['timestamp'].append(norm_ts)
                s3_buffered_rows += 1
                total_processed += 1

                if s3_buffered_rows >= s3_batch_size:
                    _flush_s3()

                # 🗑️ Освобождаем память после обработки
                del vb
                if total_processed % 100_000 == 0:
                    gc.collect()

                if total_processed % 500_000 == 0:
                    log.info(f"📊 FH progress: {total_processed:,} записей из file_id={data_file_id[:8]}")

        # Финальные флеши
        if ch_buf['value']:
            _flush_ch()
        if s3_partition_buffers:
            _flush_s3()

        # ОДИН commit snapshot на весь прогон
        if all_manifests:
            log.info(f"📝 Commit snapshot: {len(all_manifests)} манифестов, {total_s3_rows:,} записей")
            ml = _write_manifest_list(
                s3_client, file_io, bucket, metadata_prefix,
                all_manifests, table_metadata, partition_spec, base_snap_id, base_seq
            )
            _commit_snapshot(
                s3_client, bucket, table_path, metadata_prefix, table_metadata,
                ml, all_manifests, total_s3_rows, base_snap_id, base_seq
            )

    except Exception as e:
        log.error(f"❌ FH streaming failed for {data_file_id}: {e}", exc_info=True)
        raise
    finally:
        ch_client.close()

    log.info(f"✅ FH [{data_file_id[:8] if data_file_id else '?'}] completed: "
             f"{total_processed:,} записей, errors={error_count}")

    return {
        'data_file_id': data_file_id,
        'stg_table_name': stg_table_name,
        'target_table_name': target_table_name,
        'records_processed': total_processed,
        'errors': error_count,
        'manifests_created': len(all_manifests),
        's3_rows': total_s3_rows,
    }




    
    
def load_common_object_data_values_for_file_id(
    dwh_table_name: str,
    data_mart_table_name: str,
    data_type: str,
    data_file_id: str,
    value_is_nullable: bool = False,
    value_default_on_null: Any = None,
    fetch_batch_size: int = 100_000,
) -> dict:
    """
    🔥 ОПТИМИЗИРОВАННАЯ ВЕРСИЯ с clickhouse_connect.insert_df
    
    Загрузка common object data values для конкретного data_file_id.
    """
    from sqlalchemy import text
    from datetime import timezone
    import numpy as np
    import pandas as pd
    import clickhouse_connect
    
    total_loaded = 0
    total_marked = 0
    iteration = 0
    
    last_timestamp = None
    last_record_id = None
    
    log.info(f"🚀 Начало обработки {dwh_table_name} для data_file_id={data_file_id}")
    
    # =====================================================================
    # 🔥 Создаём clickhouse_connect клиент (HTTP, порт 8123)
    # =====================================================================
    conn_config = BaseHook.get_connection('clickhouse_conn')
    client = clickhouse_connect.get_client(
        host=conn_config.host,
        port=8123,
        database=conn_config.schema or 'default',
        username=conn_config.login or 'default',
        password=conn_config.password or '',
        compress=True,
        query_limit=0,
    )
    
    try:
        while True:
            iteration += 1
            
            # 1️⃣ Извлечение батча
            if last_timestamp is None:
                extract_sql = text(f"""
                    SELECT h.data_record_sk, h.h_object_property_sk, h.data_file_id,
                           h.data_source_id, h.load_dttm, h.value, h.timestamp
                    FROM public.{dwh_table_name} h
                    WHERE h.data_file_id = :file_id
                      AND h.exported_to_datamart = FALSE
                    ORDER BY h.timestamp ASC, h.data_record_sk ASC
                    LIMIT :limit
                """)
                params = {"file_id": data_file_id, "limit": fetch_batch_size}
            else:
                extract_sql = text(f"""
                    SELECT h.data_record_sk, h.h_object_property_sk, h.data_file_id,
                           h.data_source_id, h.load_dttm, h.value, h.timestamp
                    FROM public.{dwh_table_name} h
                    WHERE h.data_file_id = :file_id
                      AND h.exported_to_datamart = FALSE
                      AND (h.timestamp, h.data_record_sk) > (:last_ts, :last_id)
                    ORDER BY h.timestamp ASC, h.data_record_sk ASC
                    LIMIT :limit
                """)
                params = {
                    "file_id": data_file_id,
                    "limit": fetch_batch_size,
                    "last_ts": last_timestamp,
                    "last_id": last_record_id
                }
            
            try:
                with cloudberry_engine.begin() as conn:
                    result = conn.execute(extract_sql, params)
                    rows = result.fetchall()
            except Exception as e:
                log.error(f"❌ Ошибка извлечения батча #{iteration}: {e}", exc_info=True)
                raise
            
            if not rows:
                log.info(f"✅ data_file_id={data_file_id}: все записи обработаны (итераций: {iteration})")
                break
            
            log.info(f"📥 Батч #{iteration}: извлечено {len(rows)} записей для {data_file_id}")
            
            # 2️⃣ Подготовка данных для ClickHouse
            records = [
                {
                    'data_record_sk': ensure_uuid_string(row[0]),
                    'h_object_property_sk': ensure_uuid_string(row[1]),
                    'data_file_id': ensure_uuid_string(row[2]),
                    'data_source_id': ensure_uuid_string(row[3]),
                    'load_dttm': row[4],
                    'value': row[5],
                    'timestamp': row[6],
                }
                for row in rows
            ]
            
            exported_record_ids = [rec['data_record_sk'] for rec in records]
            
            # 🔥 Подготовка через prepare_record_for_clickhouse
            prepared_records = [
                prepare_record_for_clickhouse(
                    rec, data_type, value_is_nullable, value_default_on_null
                )
                for rec in records
            ]
            
            # 3️⃣ 🔥 Формирование DataFrame и вставка через insert_df
            if prepared_records:
                # Распаковка кортежей в DataFrame
                df = pd.DataFrame(
                    prepared_records,
                    columns=[
                        'data_record_sk', 'h_object_property_sk', 'data_file_id',
                        'data_source_id', 'timestamp', 'value', 'load_dttm'
                    ]
                )
                
                # 🔥 Гарантируем правильные dtypes
                for col in ['data_record_sk', 'h_object_property_sk', 
                           'data_file_id', 'data_source_id']:
                    df[col] = df[col].astype(str)
                
                df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
                df['load_dttm'] = pd.to_datetime(df['load_dttm'], utc=True)
                
                try:
                    client.insert_df(data_mart_table_name, df)
                    total_loaded += len(prepared_records)
                    log.info(f"✅ Батч #{iteration}: загружено {len(prepared_records)} в {data_mart_table_name}")
                except Exception as e:
                    log.error(f"❌ Ошибка вставки DataFrame: {e}", exc_info=True)
                    raise
            
            # 4️⃣ 🔥 Пометка через temp table + COPY
            if exported_record_ids:
                try:
                    updated = mark_records_as_exported_via_temp_table(
                        cloudberry_engine,
                        dwh_table_name,
                        exported_record_ids,
                        insert_batch_size=500_000,
                        data_file_id=data_file_id,
                        use_copy=True,
                        target_column = "exported_to_datamart",
                    )
                    total_marked += updated
                except Exception as e:
                    log.error(f"⚠️ Не удалось обновить флаги для батча #{iteration}: {e}", exc_info=True)
            
            # 5️⃣ Обновление курсора
            if rows:
                last_timestamp = rows[-1][6]
                last_record_id = ensure_uuid_string(rows[-1][0])
            
            # 6️⃣ Очистка памяти
            del rows, records, prepared_records, exported_record_ids
    
    finally:
        client.close()
    
    log.info(f"🎉 data_file_id={data_file_id} завершён: загружено={total_loaded}, помечено={total_marked}")
    
    return {
        'data_file_id': data_file_id,
        'dwh_table_name': dwh_table_name,
        'records_processed': total_loaded,
        'records_marked': total_marked,
    }

def stream_fh_to_staging(
    local_path: str,
    data_file_id: str,
    data_source_id: str,
    main_db_id: str,
    data_type_id: str = '05229302-46d6-f9af-261c-6e0ea91996a2', # UUID типа Double
    stg_table_name: str = 'stg_fh_double_object_data_values_hist',
) -> int:
    """Парсит .fh файл и загружает значения в staging таблицу."""
    log.info(f"🔍 Парсинг .fh файла: {local_path}")
    parsed_file = parse_fh_file(local_path)
    log.info(
        f"✅ Файл распарсен: "
        f"MainDb={parsed_file.main_db_name}, "
        f"строк={parsed_file.rows_count}, "
        f"значений для загрузки={len(parsed_file.values)}"
    )
    
    if not parsed_file.values:
        log.warning(f"⚠️ Файл не содержит значений для загрузки (все NO_DATA/UNKNOWN)")
        return 0
        
    inserted_count = insert_fh_values_to_staging(
        values=parsed_file.values,
        data_file_id=data_file_id,
        data_source_id=data_source_id,
        main_db_id=main_db_id,
        data_type_id=data_type_id,  # 🔥 Передаем UUID типа данных
        batch_size=500_000,
        page_size=50_000,
        stg_table_name=stg_table_name
    )
    return inserted_count


def merge_by_file_common_object_data_values(
    stg_table_name: str,
    target_table_name: str,
    data_type_id: str,
    is_hist_data: bool,
    data_file_id: str,  # 🔥 ОБЯЗАТЕЛЬНЫЙ ПАРАМЕТР
    batch_size: int = 50_000,
) -> bool:
    """
    🔥 ИЗОЛИРОВАННЫЙ merge для common object data values.
    Читает и вставляет ТОЛЬКО для конкретного data_file_id.
    """
    dwh_engine = get_dwh_engine(DWH_CONN_ID)
    timestamp_column_name = 'timestamp' if is_hist_data else 'date'
    
    sql_fetch = text(f"""
        SELECT
            stg.hash_sk, stg.h_object_property_sk, stg.load_dttm,
            stg.data_file_id, stg.data_source_id, stg.value,
            stg.{timestamp_column_name} AS timestamp
        FROM public.{stg_table_name} stg
        WHERE stg.h_data_type_sk = :data_type_id
          AND stg.data_file_id = :data_file_id  -- 🔥 ИЗОЛЯЦИЯ
          AND stg.hash_sk IS NOT NULL
          AND stg.value IS NOT NULL
        ORDER BY stg.hash_sk
    """)
    
    sql_insert_template = f"""
        INSERT INTO public.{target_table_name} (
            data_record_sk, h_object_property_sk, load_dttm, data_file_id,
            data_source_id, value, timestamp, exported_to_datamart, exported_to_s3_storage
        ) VALUES %s
        ON CONFLICT (data_record_sk) DO NOTHING
    """
    
    try:
        with dwh_engine.begin() as conn:
            dbapi_conn = conn.connection
            result = conn.execution_options(
                stream_results=True,
                yield_per=batch_size
            ).execute(sql_fetch, {
                "data_type_id": data_type_id,
                "data_file_id": data_file_id
            })
            
            inserted_count = 0
            error_count = 0
            batch_number = 0
            
            for partition in result.partitions():
                batch_number += 1
                values_list = []
                
                for row in partition:
                    hash_sk, h_obj_prop_sk, load_dttm, df_id, data_src_id, bytea_val, timestamp = row
                    try:
                        deserialized_val = deserialize_value(bytea_val, data_type_id)
                        values_list.append((
                            hash_sk, h_obj_prop_sk, load_dttm, df_id,
                            data_src_id, deserialized_val, timestamp, False, False
                        ))
                    except Exception as e:
                        error_count += 1
                        log.warning(f"⚠️ Failed to deserialize hash_sk={hash_sk}: {e}")
                        continue
                
                if values_list:
                    _insert_batch_with_retry(dbapi_conn, sql_insert_template, values_list, batch_size)
                    inserted_count += len(values_list)
                
                if batch_number % 10 == 0:
                    log.info(
                        f"📊 [{data_file_id[:8]}] batch {batch_number}, "
                        f"inserted {inserted_count}, errors {error_count}"
                    )
                del values_list
            
            log.info(
                f"✅ [{data_file_id[:8]}] merge to {target_table_name}: "
                f"inserted {inserted_count}, errors {error_count}"
            )
            return True
    
    except Exception as e:
        log.error(f"❌ Error during merge for {data_file_id}: {e}", exc_info=True)
        raise

def merge_by_file_creyt_data(
    stg_table_name: str,
    target_table_name: str,
    is_hist_data: bool,
    data_file_id: str,  # 🔥 ОБЯЗАТЕЛЬНЫЙ ПАРАМЕТР
    data_type_id: str = "1b134c95-43d8-2793-ba0f-b6d0ba22c624",
    batch_size: int = 2000,
) -> bool:
    """🔥 ИЗОЛИРОВАННЫЙ merge для CreytFastSample data."""
    dwh_engine = get_dwh_engine(DWH_CONN_ID)
    timestamp_column_name = 'timestamp' if is_hist_data else 'date'
    
    sql_fetch = text(f"""
        SELECT stg.hash_sk, stg.h_object_property_sk, stg.load_dttm,
               stg.data_file_id, stg.data_source_id, stg.value,
               stg.{timestamp_column_name} AS timestamp
        FROM public.{stg_table_name} stg
        WHERE stg.h_data_type_sk = :data_type_id
          AND stg.data_file_id = :data_file_id
          AND stg.hash_sk IS NOT NULL AND stg.value IS NOT NULL
        ORDER BY stg.hash_sk
    """)
    
    sql_insert = text(f"""
        INSERT INTO public.{target_table_name} (
            data_record_sk, h_object_property_sk, load_dttm, data_file_id,
            data_source_id, sample_rate, colibration_factor_a, colibration_factor_b,
            raw_values, min_raw, max_raw, min_eu, max_eu, timestamp,
            exported_to_datamart, exported_to_s3_storage
        ) VALUES (
            :data_record_sk, :h_object_property_sk, :load_dttm, :data_file_id,
            :data_source_id, :sample_rate, :colibration_factor_a, :colibration_factor_b,
            :raw_values, :min_raw, :max_raw, :min_eu, :max_eu, :timestamp,
            :exported_to_datamart, :exported_to_s3_storage
        )
        ON CONFLICT (data_record_sk) DO NOTHING
    """)
    
    try:
        with dwh_engine.begin() as conn:
            result = conn.execution_options(
                stream_results=True, yield_per=batch_size
            ).execute(sql_fetch, {
                "data_type_id": data_type_id,
                "data_file_id": data_file_id
            })
            
            inserted_count = 0
            error_count = 0
            batch_number = 0
            
            for partition in result.partitions():
                batch_number += 1
                params_list = []
                
                for row in partition:
                    hash_sk, h_obj_prop_sk, load_dttm, df_id, data_src_id, bytea_val, timestamp = row
                    try:
                        if not bytea_val:
                            continue
                        blob_bytes = bytes(bytea_val) if hasattr(bytea_val, 'tobytes') else bytea_val
                        creyt = deserialize_creyt_blob(blob_bytes)
                        del blob_bytes
                        
                        params_list.append({
                            "data_record_sk": hash_sk,
                            "h_object_property_sk": h_obj_prop_sk,
                            "load_dttm": load_dttm,
                            "data_file_id": df_id,
                            "data_source_id": data_src_id,
                            "timestamp": timestamp,
                            "sample_rate": creyt.sample_rate,
                            "colibration_factor_a": creyt.calibration_factor_a,
                            "colibration_factor_b": creyt.calibration_factor_b,
                            "raw_values": creyt.raw_values,
                            "min_raw": creyt.scale_min_raw,
                            "max_raw": creyt.scale_max_raw,
                            "min_eu": creyt.scale_min_eu,
                            "max_eu": creyt.scale_max_eu,
                            "exported_to_datamart": False,
                            "exported_to_s3_storage": False
                        })
                        del creyt
                    except Exception as e:
                        error_count += 1
                        log.error(f"❌ Creyt deserialize hash_sk={hash_sk}: {e}")
                        continue
                
                if params_list:
                    _insert_with_retry(conn, sql_insert, params_list, batch_number)
                    inserted_count += len(params_list)
                del params_list
                
                if batch_number % 5 == 0:
                    log.info(f"📊 Creyt [{data_file_id[:8]}]: batch {batch_number}, inserted {inserted_count}")
            
            log.info(f"✅ Creyt [{data_file_id[:8]}]: inserted {inserted_count}, errors {error_count}")
            return True
    
    except Exception as e:
        log.error(f"❌ Creyt merge error for {data_file_id}: {e}", exc_info=True)
        raise

def merge_by_file_lcard_data(
    stg_table_name: str,
    target_table_name: str,
    is_hist_data: bool,
    data_file_id: str,
    data_type_id: str = "548dc301-340b-4257-17a3-1f8b350af42a",
    batch_size: int = 2000,
) -> bool:
    """🔥 ИЗОЛИРОВАННЫЙ merge для LCard data."""
    dwh_engine = get_dwh_engine(DWH_CONN_ID)
    timestamp_column_name = 'timestamp' if is_hist_data else 'date'
    
    sql_fetch = text(f"""
        SELECT stg.hash_sk, stg.h_object_property_sk, stg.load_dttm,
               stg.data_file_id, stg.data_source_id, stg.value,
               stg.{timestamp_column_name} AS timestamp
        FROM public.{stg_table_name} stg
        WHERE stg.h_data_type_sk = :data_type_id
          AND stg.data_file_id = :data_file_id
          AND stg.hash_sk IS NOT NULL AND stg.value IS NOT NULL
        ORDER BY stg.hash_sk
    """)
    
    sql_insert = text(f"""
        INSERT INTO public.{target_table_name} (
            data_record_sk, h_object_property_sk, load_dttm, data_file_id,
            data_source_id, sample_rate, step, is_scale, raw_values,
            min_raw, max_raw, min_eu, max_eu, timestamp,
            exported_to_datamart, exported_to_s3_storage
        ) VALUES (
            :data_record_sk, :h_object_property_sk, :load_dttm, :data_file_id,
            :data_source_id, :sample_rate, :step, :is_scale, :raw_values,
            :min_raw, :max_raw, :min_eu, :max_eu, :timestamp,
            :exported_to_datamart, :exported_to_s3_storage
        )
        ON CONFLICT (data_record_sk) DO NOTHING
    """)
    
    try:
        with dwh_engine.begin() as conn:
            result = conn.execution_options(
                stream_results=True, yield_per=batch_size
            ).execute(sql_fetch, {
                "data_type_id": data_type_id,
                "data_file_id": data_file_id
            })
            
            inserted_count = 0
            error_count = 0
            batch_number = 0
            
            for partition in result.partitions():
                batch_number += 1
                params_list = []
                
                for row in partition:
                    hash_sk, h_obj_prop_sk, load_dttm, df_id, data_src_id, bytea_val, timestamp = row
                    try:
                        if not bytea_val:
                            continue
                        blob_bytes = bytes(bytea_val) if hasattr(bytea_val, 'tobytes') else bytea_val
                        lcard = deserialize_lcard_blob(blob_bytes)
                        del blob_bytes
                        
                        params_list.append({
                            "data_record_sk": hash_sk,
                            "h_object_property_sk": h_obj_prop_sk,
                            "load_dttm": load_dttm,
                            "data_file_id": df_id,
                            "data_source_id": data_src_id,
                            "timestamp": timestamp,
                            "sample_rate": int(lcard.sample_rate),
                            "step": lcard.step,
                            "is_scale": lcard.is_scale,
                            "raw_values": lcard.raw_values,
                            "min_raw": lcard.scale_min_raw,
                            "max_raw": lcard.scale_max_raw,
                            "min_eu": lcard.scale_min_eu,
                            "max_eu": lcard.scale_max_eu,
                            "exported_to_datamart": False,
                            "exported_to_s3_storage": False
                        })
                        del lcard
                    except Exception as e:
                        error_count += 1
                        log.error(f"❌ LCard deserialize hash_sk={hash_sk}: {e}")
                        continue
                
                if params_list:
                    _insert_with_retry(conn, sql_insert, params_list, batch_number)
                    inserted_count += len(params_list)
                del params_list
                
                if batch_number % 5 == 0:
                    log.info(f"📊 LCard [{data_file_id[:8]}]: batch {batch_number}, inserted {inserted_count}")
            
            log.info(f"✅ LCard [{data_file_id[:8]}]: inserted {inserted_count}, errors {error_count}")
            return True
    
    except Exception as e:
        log.error(f"❌ LCard merge error for {data_file_id}: {e}", exc_info=True)
        raise


def merge_by_file_staging_to_hub(
    hub_table_name: str,
    sk_hub_column_name: str,
    stg_table_name: str,
    data_file_id: str,  # 🔥 ОБЯЗАТЕЛЬНЫЙ ПАРАМЕТР
    stg_hash_column_name: str = 'hash_sk',
    business_id_column_name: str = None
):
    """🔥 ИЗОЛИРОВАННЫЙ merge staging → Hub."""
    dwh_engine = get_dwh_engine(DWH_CONN_ID)
    
    if business_id_column_name:
        business_id_stg_source = f"stg.{business_id_column_name},"
        hub_business_id = "business_id,"
    else:
        business_id_stg_source = ""
        hub_business_id = ""
    
    try:
        with dwh_engine.begin() as conn:
            insert_hub_sql = text(f"""
                INSERT INTO {hub_table_name} (
                    {sk_hub_column_name},
                    {hub_business_id}
                    load_dttm,
                    data_file_id,
                    data_source_id
                )
                SELECT
                    stg.{stg_hash_column_name},
                    {business_id_stg_source}
                    stg.load_dttm,
                    stg.data_file_id,
                    stg.data_source_id
                FROM {stg_table_name} stg
                WHERE stg.data_file_id = :data_file_id  -- 🔥 ИЗОЛЯЦИЯ
                ON CONFLICT ({sk_hub_column_name}) DO NOTHING
            """)
            result = conn.execute(insert_hub_sql, {"data_file_id": data_file_id})
            hub_inserted = result.rowcount
            log.info(f"✅ Hub {hub_table_name} [{data_file_id[:8]}]: inserted {hub_inserted}")
            return {"hub_inserted": hub_inserted, "status": "success"}
    
    except Exception as e:
        log.error(f"❌ Hub merge error for {data_file_id}: {e}", exc_info=True)
        raise


def merge_by_file_staging_to_satellite(
    sat_table_name: str,
    hab_sk_column_name: str,
    sat_column_names: list[str],
    stg_table_name: str,
    stg_column_names: list[str],
    data_file_id: str,  # 🔥 ОБЯЗАТЕЛЬНЫЙ ПАРАМЕТР
    stg_hash_sk_column_name: str = 'hash_sk',
    stg_hash_sat_diff_column_name: str = 'hash_sat_diff'
):
    """🔥 ИЗОЛИРОВАННЫЙ merge staging → Satellite."""
    dwh_engine = get_dwh_engine(DWH_CONN_ID)
    
    base_sat_cols = [
        hab_sk_column_name, 'load_dttm', 'valid_from_dttm', 'valid_to_dttm',
        'active_flag', 'data_file_id', 'data_source_id', 'hash_sat_diff'
    ]
    all_sat_cols = base_sat_cols + [c.lower() for c in sat_column_names]
    satellite_columns = ',\n'.join(all_sat_cols)
    
    base_select_cols = [
        f'stg.{stg_hash_sk_column_name}', 'stg.load_dttm', 'stg.load_dttm',
        'NULL', 'true', 'stg.data_file_id', 'stg.data_source_id',
        f'stg.{stg_hash_sat_diff_column_name}'
    ]
    all_select_cols = base_select_cols + [f'stg.{c.lower()}' for c in stg_column_names]
    staging_columns = ',\n'.join(all_select_cols)
    
    try:
        with dwh_engine.begin() as conn:
            # 1. Закрытие старых записей
            close_old_sat_sql = text(f"""
                UPDATE public.{sat_table_name} AS sat
                SET valid_to_dttm = NOW(), active_flag = false
                FROM public.{stg_table_name} AS stg
                WHERE sat.{hab_sk_column_name} = stg.{stg_hash_sk_column_name}
                  AND sat.data_file_id = stg.data_file_id
                  AND stg.data_file_id = :data_file_id  -- 🔥 ИЗОЛЯЦИЯ
                  AND sat.active_flag = true
                  AND sat.hash_sat_diff IS DISTINCT FROM stg.{stg_hash_sat_diff_column_name}
            """)
            result_close = conn.execute(close_old_sat_sql, {"data_file_id": data_file_id})
            sat_closed = result_close.rowcount
            
            # 2. Вставка новых
            insert_sat_sql = text(f"""
                INSERT INTO public.{sat_table_name} ({satellite_columns})
                SELECT {staging_columns}
                FROM public.{stg_table_name} stg
                LEFT JOIN public.{sat_table_name} sat
                    ON stg.{stg_hash_sk_column_name} = sat.{hab_sk_column_name}
                    AND stg.data_file_id = sat.data_file_id
                    AND sat.active_flag = true
                    AND stg.{stg_hash_sat_diff_column_name} = sat.hash_sat_diff
                WHERE stg.data_file_id = :data_file_id  -- 🔥 ИЗОЛЯЦИЯ
                  AND sat.{hab_sk_column_name} IS NULL
            """)
            result_insert = conn.execute(insert_sat_sql, {"data_file_id": data_file_id})
            sat_inserted = result_insert.rowcount
            
            log.info(f"✅ Sat [{data_file_id[:8]}]: closed {sat_closed}, inserted {sat_inserted}")
            return {"sat_closed": sat_closed, "sat_inserted": sat_inserted, "status": "success"}
    
    except Exception as e:
        log.error(f"❌ Satellite merge error for {data_file_id}: {e}", exc_info=True)
        raise



def merge_by_file_staging_to_link(
    stg_table_name: str,
    link_table_name: str,
    link_sk_column_name: str,
    link_first_hub_sk_column_name: str,
    link_second_hub_sk_column_name: str,
    stg_link_sk_column_name: str,
    stg_first_hub_sk_column_name: str,
    stg_second_hub_sk_column_name: str,
    data_file_id: str  # 🔥 ОБЯЗАТЕЛЬНЫЙ ПАРАМЕТР
) -> bool:
    """🔥 ИЗОЛИРОВАННЫЙ merge staging → Link."""
    dwh_engine = get_dwh_engine(DWH_CONN_ID)
    
    try:
        with dwh_engine.begin() as conn:
            # 1. Закрытие
            sql_close = text(f"""
                WITH missing_in_stg AS (
                    SELECT link.{link_sk_column_name} as link_sk_val, link.data_file_id as ds_id
                    FROM public.{link_table_name} link
                    LEFT JOIN public.{stg_table_name} stg
                        ON link.{link_sk_column_name} = stg.{stg_link_sk_column_name}
                        AND link.data_file_id = stg.data_file_id
                    WHERE link.active_flag = true
                      AND link.data_file_id = :data_file_id  -- 🔥 ИЗОЛЯЦИЯ
                      AND stg.{stg_link_sk_column_name} IS NULL
                )
                UPDATE public.{link_table_name} AS link
                SET valid_to_dttm = NOW(), active_flag = false
                FROM missing_in_stg mis
                WHERE link.{link_sk_column_name} = mis.link_sk_val
                  AND link.data_file_id = mis.ds_id
            """)
            result_close = conn.execute(sql_close, {"data_file_id": data_file_id})
            
            # 2. Вставка
            sql_insert = text(f"""
                INSERT INTO public.{link_table_name} (
                    {link_sk_column_name}, load_dttm, valid_from_dttm, valid_to_dttm,
                    active_flag, data_file_id, data_source_id,
                    {link_first_hub_sk_column_name}, {link_second_hub_sk_column_name}
                )
                SELECT
                    stg.{stg_link_sk_column_name}, stg.load_dttm, stg.load_dttm, NULL,
                    true, stg.data_file_id, stg.data_source_id,
                    stg.{stg_first_hub_sk_column_name}, stg.{stg_second_hub_sk_column_name}
                FROM public.{stg_table_name} stg
                LEFT JOIN public.{link_table_name} link
                    ON stg.{stg_link_sk_column_name} = link.{link_sk_column_name}
                    AND stg.data_file_id = link.data_file_id
                WHERE stg.data_file_id = :data_file_id  -- 🔥 ИЗОЛЯЦИЯ
                  AND link.{link_sk_column_name} IS NULL
            """)
            result_insert = conn.execute(sql_insert, {"data_file_id": data_file_id})
            
            log.info(
                f"✅ Link [{data_file_id[:8]}]: closed {result_close.rowcount}, "
                f"inserted {result_insert.rowcount}"
            )
            return True
    
    except Exception as e:
        log.error(f"❌ Link merge error for {data_file_id}: {e}", exc_info=True)
        raise


def merge_by_file_sample_data(
    stg_table_name: str,
    target_table_name: str,
    is_hist_data: bool,
    data_file_id: str,  # 🔥 ОБЯЗАТЕЛЬНЫЙ ПАРАМЕТР
    data_type_id: str = "969420b8-fd94-4476-b06e-ed7408d42a61",
    batch_size: int = 2000,
) -> bool:
    """
    🔥 ИЗОЛИРОВАННЫЙ streaming merge для SampleData.
    Читает и вставляет ТОЛЬКО для конкретного data_file_id.
    """
    dwh_engine = get_dwh_engine(DWH_CONN_ID)
    timestamp_column_name = 'timestamp' if is_hist_data else 'date'

    sql_fetch = text(f"""
        SELECT
            stg.hash_sk,
            stg.h_object_property_sk,
            stg.load_dttm,
            stg.data_file_id,
            stg.data_source_id,
            stg.value,
            stg.{timestamp_column_name} AS timestamp
        FROM public.{stg_table_name} stg
        WHERE stg.h_data_type_sk = :data_type_id
          AND stg.data_file_id = :data_file_id  -- 🔥 ИЗОЛЯЦИЯ
          AND stg.hash_sk IS NOT NULL
          AND stg.value IS NOT NULL
        ORDER BY stg.hash_sk
    """)

    sql_insert = text(f"""
        INSERT INTO public.{target_table_name} (
            data_record_sk,
            h_object_property_sk,
            load_dttm,
            data_file_id,
            data_source_id,
            sample_rate,
            raw_values,
            timestamp,
            exported_to_datamart,
            exported_to_s3_storage
        ) VALUES (
            :data_record_sk,
            :h_object_property_sk,
            :load_dttm,
            :data_file_id,
            :data_source_id,
            :sample_rate,
            :raw_values,
            :timestamp,
            :exported_to_datamart,
            :exported_to_s3_storage
        )
        ON CONFLICT (data_record_sk) DO NOTHING
    """)

    try:
        with dwh_engine.begin() as conn:
            result = conn.execution_options(
                stream_results=True,
                yield_per=batch_size
            ).execute(sql_fetch, {
                "data_type_id": data_type_id,
                "data_file_id": data_file_id  # 🔥 ПЕРЕДАЁМ
            })

            inserted_count = 0
            error_count = 0
            batch_number = 0

            for partition in result.partitions():
                batch_number += 1
                params_list = []

                for row in partition:
                    hash_sk, h_obj_prop_sk, load_dttm, df_id, data_src_id, bytea_val, timestamp = row
                    try:
                        if not bytea_val:
                            log.warning(f"Empty BLOB for hash_sk={hash_sk}")
                            continue

                        blob_bytes = bytes(bytea_val) if hasattr(bytea_val, 'tobytes') else bytea_val
                        sample = deserialize_sample_data_blob(blob_bytes)
                        del blob_bytes

                        params_list.append({
                            "data_record_sk": hash_sk,
                            "h_object_property_sk": h_obj_prop_sk,
                            "load_dttm": load_dttm,
                            "data_file_id": df_id,
                            "data_source_id": data_src_id,
                            "timestamp": timestamp,
                            "sample_rate": sample['sample_rate'],
                            "raw_values": sample['raw_values'],
                            "exported_to_datamart": False,
                            "exported_to_s3_storage": False
                        })
                        del sample

                    except Exception as e:
                        error_count += 1
                        log.error(f"❌ SampleData deserialize hash_sk={hash_sk}: {e}")
                        continue

                if params_list:
                    _insert_with_retry(conn, sql_insert, params_list, batch_number)
                    inserted_count += len(params_list)
                del params_list

                if batch_number % 5 == 0:
                    log.info(
                        f"📊 SampleData [{data_file_id[:8]}]: batch {batch_number}, "
                        f"inserted {inserted_count}, errors {error_count}"
                    )

            log.info(
                f"✅ SampleData [{data_file_id[:8]}]: inserted {inserted_count}, "
                f"errors {error_count}, batches {batch_number}"
            )
            return True

    except SQLAlchemyError as e:
        log.error(f"❌ SQLAlchemy error during SampleData merge: {e}", exc_info=True)
        raise e
    except Exception as e:
        log.error(f"❌ Unexpected error during SampleData merge: {e}", exc_info=True)
        raise e


def merge_by_file_siemens_sample_data(
    stg_table_name: str,
    target_table_name: str,
    is_hist_data: bool,
    data_file_id: str,  # 🔥 ОБЯЗАТЕЛЬНЫЙ ПАРАМЕТР
    data_type_id: str = '80bdd702-d6f9-e90e-e3fa-1b9866940654',
    batch_size: int = 2000,
) -> bool:
    """
    🔥 ИЗОЛИРОВАННЫЙ streaming merge для Siemens SampleData.
    Читает и вставляет ТОЛЬКО для конкретного data_file_id.
    """
    dwh_engine = get_dwh_engine(DWH_CONN_ID)
    timestamp_column_name = 'timestamp' if is_hist_data else 'date'

    sql_fetch = text(f"""
        SELECT
            stg.hash_sk,
            stg.h_object_property_sk,
            stg.load_dttm,
            stg.data_file_id,
            stg.data_source_id,
            stg.value,
            stg.{timestamp_column_name} AS timestamp
        FROM public.{stg_table_name} stg
        WHERE stg.h_data_type_sk = :data_type_id
          AND stg.data_file_id = :data_file_id  -- 🔥 ИЗОЛЯЦИЯ
          AND stg.hash_sk IS NOT NULL
          AND stg.value IS NOT NULL
        ORDER BY stg.hash_sk
    """)

    sql_insert = text(f"""
        INSERT INTO public.{target_table_name} (
            data_record_sk,
            h_object_property_sk,
            load_dttm,
            data_file_id,
            data_source_id,
            sample_rate,
            raw_values,
            timestamp,
            exported_to_datamart,
            exported_to_s3_storage
        ) VALUES (
            :data_record_sk,
            :h_object_property_sk,
            :load_dttm,
            :data_file_id,
            :data_source_id,
            :sample_rate,
            :raw_values,
            :timestamp,
            :exported_to_datamart,
            :exported_to_s3_storage
        )
        ON CONFLICT (data_record_sk) DO NOTHING
    """)

    try:
        with dwh_engine.begin() as conn:
            result = conn.execution_options(
                stream_results=True,
                yield_per=batch_size
            ).execute(sql_fetch, {
                "data_type_id": data_type_id,
                "data_file_id": data_file_id  # 🔥 ПЕРЕДАЁМ
            })

            inserted_count = 0
            error_count = 0
            batch_number = 0

            for partition in result.partitions():
                batch_number += 1
                params_list = []

                for row in partition:
                    hash_sk, h_obj_prop_sk, load_dttm, df_id, data_src_id, bytea_val, timestamp = row
                    try:
                        if not bytea_val:
                            log.warning(f"Empty BLOB for hash_sk={hash_sk}")
                            continue

                        # Поддержка разных типов bytea
                        if isinstance(bytea_val, memoryview):
                            blob_bytes = bytea_val.tobytes()
                        elif isinstance(bytea_val, (bytes, bytearray)):
                            blob_bytes = bytes(bytea_val)
                        else:
                            blob_bytes = bytes(bytea_val)

                        sample = deserialize_siemens_sample_blob(blob_bytes)
                        del blob_bytes

                        params_list.append({
                            "data_record_sk": hash_sk,
                            "h_object_property_sk": h_obj_prop_sk,
                            "load_dttm": load_dttm,
                            "data_file_id": df_id,
                            "data_source_id": data_src_id,
                            "timestamp": timestamp,
                            "sample_rate": sample['sample_rate'],
                            "raw_values": sample['raw_values'],
                            "exported_to_datamart": False,
                            "exported_to_s3_storage": False
                        })
                        del sample

                    except Exception as e:
                        error_count += 1
                        log.error(f"❌ Siemens SampleData deserialize hash_sk={hash_sk}: {e}")
                        continue

                if params_list:
                    _insert_with_retry(conn, sql_insert, params_list, batch_number)
                    inserted_count += len(params_list)
                del params_list

                if batch_number % 5 == 0:
                    log.info(
                        f"📊 Siemens [{data_file_id[:8]}]: batch {batch_number}, "
                        f"inserted {inserted_count}, errors {error_count}"
                    )

            log.info(
                f"✅ Siemens [{data_file_id[:8]}]: inserted {inserted_count}, "
                f"errors {error_count}, batches {batch_number}"
            )
            return True

    except SQLAlchemyError as e:
        log.error(f"❌ SQLAlchemy error during Siemens SampleData merge: {e}", exc_info=True)
        raise e
    except Exception as e:
        log.error(f"❌ Unexpected error during Siemens SampleData merge: {e}", exc_info=True)
        raise e


def merge_by_file_spectrum_data(
    stg_table_name: str,
    target_table_name: str,
    is_hist_data: bool,
    data_file_id: str,  # 🔥 ОБЯЗАТЕЛЬНЫЙ ПАРАМЕТР
    data_type_id: str = '195542f6-0ed3-6f80-91ca-296219cf3f8b',
    batch_size: int = 2000,
) -> bool:
    """
    🔥 ИЗОЛИРОВАННЫЙ streaming merge для Spectrum Data.
    Читает и вставляет ТОЛЬКО для конкретного data_file_id.
    """
    dwh_engine = get_dwh_engine(DWH_CONN_ID)
    timestamp_column_name = 'timestamp' if is_hist_data else 'date'

    sql_fetch = text(f"""
        SELECT
            stg.hash_sk,
            stg.h_object_property_sk,
            stg.load_dttm,
            stg.data_file_id,
            stg.data_source_id,
            stg.value,
            stg.{timestamp_column_name} AS timestamp
        FROM public.{stg_table_name} stg
        WHERE stg.h_data_type_sk = :data_type_id
          AND stg.data_file_id = :data_file_id  -- 🔥 ИЗОЛЯЦИЯ
          AND stg.hash_sk IS NOT NULL
          AND stg.value IS NOT NULL
        ORDER BY stg.hash_sk
    """)

    sql_insert = text(f"""
        INSERT INTO public.{target_table_name} (
            data_record_sk,
            h_object_property_sk,
            load_dttm,
            data_file_id,
            data_source_id,
            raw_values,
            multiplier,
            h_spectrum_type_sk,
            timestamp,
            exported_to_datamart,
            exported_to_s3_storage
        ) VALUES (
            :data_record_sk,
            :h_object_property_sk,
            :load_dttm,
            :data_file_id,
            :data_source_id,
            :raw_values,
            :multiplier,
            :h_spectrum_type_sk,
            :timestamp,
            :exported_to_datamart,
            :exported_to_s3_storage
        )
        ON CONFLICT (data_record_sk) DO NOTHING
    """)

    try:
        with dwh_engine.begin() as conn:
            result = conn.execution_options(
                stream_results=True,
                yield_per=batch_size
            ).execute(sql_fetch, {
                "data_type_id": data_type_id,
                "data_file_id": data_file_id  # 🔥 ПЕРЕДАЁМ
            })

            inserted_count = 0
            error_count = 0
            batch_number = 0

            for partition in result.partitions():
                batch_number += 1
                params_list = []

                for row in partition:
                    hash_sk, h_obj_prop_sk, load_dttm, df_id, data_src_id, bytea_val, timestamp = row
                    try:
                        if not bytea_val:
                            log.warning(f"Empty BLOB for hash_sk={hash_sk}")
                            continue

                        # Поддержка разных типов bytea
                        if isinstance(bytea_val, memoryview):
                            blob_bytes = bytea_val.tobytes()
                        elif isinstance(bytea_val, (bytes, bytearray)):
                            blob_bytes = bytes(bytea_val)
                        else:
                            blob_bytes = bytes(bytea_val)

                        spectrum = deserialize_spectrum_blob(blob_bytes)
                        del blob_bytes

                        params_list.append({
                            "data_record_sk": hash_sk,
                            "h_object_property_sk": h_obj_prop_sk,
                            "load_dttm": load_dttm,
                            "data_file_id": df_id,
                            "data_source_id": data_src_id,
                            "timestamp": timestamp,
                            "raw_values": spectrum['raw_values'],
                            "multiplier": spectrum['multiplier'],
                            "h_spectrum_type_sk": spectrum['h_spectrum_type_sk'],
                            "exported_to_datamart": False,
                            "exported_to_s3_storage": False
                        })
                        del spectrum

                    except Exception as e:
                        error_count += 1
                        log.error(f"❌ Spectrum deserialize hash_sk={hash_sk}: {e}")
                        continue

                if params_list:
                    _insert_with_retry(conn, sql_insert, params_list, batch_number)
                    inserted_count += len(params_list)
                del params_list

                if batch_number % 5 == 0:
                    log.info(
                        f"📊 Spectrum [{data_file_id[:8]}]: batch {batch_number}, "
                        f"inserted {inserted_count}, errors {error_count}"
                    )

            log.info(
                f"✅ Spectrum [{data_file_id[:8]}]: inserted {inserted_count}, "
                f"errors {error_count}, batches {batch_number}"
            )
            return True

    except SQLAlchemyError as e:
        log.error(f"❌ SQLAlchemy error during Spectrum merge: {e}", exc_info=True)
        raise e
    except Exception as e:
        log.error(f"❌ Unexpected error during Spectrum merge: {e}", exc_info=True)
        raise e


def merge_by_file_bode_data(
    stg_table_name: str,
    target_table_name: str,
    is_hist_data: bool,
    data_file_id: str,  # 🔥 ОБЯЗАТЕЛЬНЫЙ ПАРАМЕТР
    data_type_id: str = "98015170-fd7e-23a2-8d27-c9506dc968d2",
    batch_size: int = 5000,
) -> bool:
    """
    🔥 ИЗОЛИРОВАННЫЙ streaming merge для BODE data.
    Читает и вставляет ТОЛЬКО для конкретного data_file_id.
    """
    dwh_engine = get_dwh_engine(DWH_CONN_ID)
    timestamp_column_name = 'timestamp' if is_hist_data else 'date'

    sql_fetch = text(f"""
        SELECT
            stg.hash_sk,
            stg.h_object_property_sk,
            stg.load_dttm,
            stg.data_file_id,
            stg.data_source_id,
            stg.value,
            stg.{timestamp_column_name} AS timestamp
        FROM public.{stg_table_name} stg
        WHERE stg.h_data_type_sk = :data_type_id
          AND stg.data_file_id = :data_file_id  -- 🔥 ИЗОЛЯЦИЯ
          AND stg.hash_sk IS NOT NULL
          AND stg.value IS NOT NULL
        ORDER BY stg.hash_sk
    """)

    sql_insert = text(f"""
        INSERT INTO public.{target_table_name} (
            data_record_sk,
            h_object_property_sk,
            load_dttm,
            data_file_id,
            data_source_id,
            magnitude_value,
            phase_value,
            turnover_frequency_value,
            timestamp,
            exported_to_datamart,
            exported_to_s3_storage
        ) VALUES (
            :data_record_sk,
            :h_object_property_sk,
            :load_dttm,
            :data_file_id,
            :data_source_id,
            :magnitude_value,
            :phase_value,
            :turnover_frequency_value,
            :timestamp,
            :exported_to_datamart,
            :exported_to_s3_storage
        )
        ON CONFLICT (data_record_sk) DO NOTHING
    """)

    try:
        with dwh_engine.begin() as conn:
            result = conn.execution_options(
                stream_results=True,
                yield_per=batch_size
            ).execute(sql_fetch, {
                "data_type_id": data_type_id,
                "data_file_id": data_file_id  # 🔥 ПЕРЕДАЁМ
            })

            inserted_count = 0
            error_count = 0
            batch_number = 0

            for partition in result.partitions():
                batch_number += 1
                params_list = []

                for row in partition:
                    hash_sk, h_obj_prop_sk, load_dttm, df_id, data_src_id, bytea_val, timestamp = row
                    try:
                        if not bytea_val:
                            log.warning(f"Empty BLOB for hash_sk={hash_sk}")
                            continue

                        blob_bytes = bytes(bytea_val) if hasattr(bytea_val, 'tobytes') else bytea_val
                        bode = deserialize_bode_blob(blob_bytes)
                        del blob_bytes

                        params_list.append({
                            "data_record_sk": hash_sk,
                            "h_object_property_sk": h_obj_prop_sk,
                            "load_dttm": load_dttm,
                            "data_file_id": df_id,
                            "data_source_id": data_src_id,
                            "timestamp": timestamp,
                            "magnitude_value": bode['magnitude'],
                            "phase_value": bode['phase'],
                            "turnover_frequency_value": bode['turnover_frequency'],
                            "exported_to_datamart": False,
                            "exported_to_s3_storage": False
                        })
                        del bode

                    except Exception as e:
                        error_count += 1
                        log.error(f"❌ BODE deserialize hash_sk={hash_sk}: {e}")
                        continue

                if params_list:
                    _insert_with_retry(conn, sql_insert, params_list, batch_number)
                    inserted_count += len(params_list)
                del params_list

                if batch_number % 5 == 0:
                    log.info(
                        f"📊 BODE [{data_file_id[:8]}]: batch {batch_number}, "
                        f"inserted {inserted_count}, errors {error_count}"
                    )

            log.info(
                f"✅ BODE [{data_file_id[:8]}]: inserted {inserted_count}, "
                f"errors {error_count}, batches {batch_number}"
            )
            return True

    except SQLAlchemyError as e:
        log.error(f"❌ SQLAlchemy error during BODE merge: {e}", exc_info=True)
        raise e
    except Exception as e:
        log.error(f"❌ Unexpected error during BODE merge: {e}", exc_info=True)
        raise e


def merge_by_file_diagnostic_array_data(
    stg_table_name: str,
    data_file_id: str,  # 🔥 ОБЯЗАТЕЛЬНЫЙ ПАРАМЕТР
    target_table_name: str = "nonhist_diagnostic_array_data_records",
    is_hist_data: bool = False,
    data_type_id: str = "5550953e-6500-4d3f-f5a0-00a7f1034c0f",
    batch_size: int = 2000,
) -> bool:
    """
    🔥 ИЗОЛИРОВАННЫЙ merge для Diagnostic Array Data.
    Без десериализации: raw_values сохраняются как bytea "как есть".
    Читает и вставляет ТОЛЬКО для конкретного data_file_id.
    """
    dwh_engine = get_dwh_engine(DWH_CONN_ID)
    timestamp_column_name = 'timestamp' if is_hist_data else 'date'

    sql_fetch = text(f"""
        SELECT
            stg.hash_sk,
            stg.h_object_property_sk,
            stg.load_dttm,
            stg.data_file_id,
            stg.data_source_id,
            stg.value,
            stg.{timestamp_column_name} AS timestamp
        FROM public.{stg_table_name} stg
        WHERE stg.h_data_type_sk = :data_type_id
          AND stg.data_file_id = :data_file_id  -- 🔥 ИЗОЛЯЦИЯ
          AND stg.hash_sk IS NOT NULL
          AND stg.value IS NOT NULL
        ORDER BY stg.hash_sk
    """)

    sql_insert = text(f"""
        INSERT INTO public.{target_table_name} (
            data_record_sk,
            h_object_property_sk,
            load_dttm,
            data_file_id,
            data_source_id,
            raw_values,
            timestamp,
            exported_to_datamart,
            exported_to_s3_storage
        ) VALUES (
            :data_record_sk,
            :h_object_property_sk,
            :load_dttm,
            :data_file_id,
            :data_source_id,
            :raw_values,
            :timestamp,
            :exported_to_datamart,
            :exported_to_s3_storage
        )
        ON CONFLICT (data_record_sk) DO NOTHING
    """)

    try:
        with dwh_engine.begin() as conn:
            result = conn.execution_options(
                stream_results=True,
                yield_per=batch_size
            ).execute(sql_fetch, {
                "data_type_id": data_type_id,
                "data_file_id": data_file_id  # 🔥 ПЕРЕДАЁМ
            })

            inserted_count = 0
            skipped_count = 0
            error_count = 0
            batch_number = 0

            for partition in result.partitions():
                batch_number += 1
                params_list = []

                for row in partition:
                    hash_sk, h_obj_prop_sk, load_dttm, df_id, data_src_id, bytea_val, timestamp = row
                    try:
                        if not bytea_val:
                            skipped_count += 1
                            continue

                        # Без десериализации — сохраняем "как есть"
                        if isinstance(bytea_val, memoryview):
                            raw_bytes = bytea_val.tobytes()
                        elif isinstance(bytea_val, (bytes, bytearray)):
                            raw_bytes = bytes(bytea_val)
                        else:
                            raw_bytes = bytes(bytea_val)

                        params_list.append({
                            "data_record_sk": hash_sk,
                            "h_object_property_sk": h_obj_prop_sk,
                            "load_dttm": load_dttm,
                            "data_file_id": df_id,
                            "data_source_id": data_src_id,
                            "raw_values": raw_bytes,
                            "timestamp": timestamp,
                            "exported_to_datamart": False,
                            "exported_to_s3_storage": False
                        })

                    except Exception as e:
                        error_count += 1
                        log.error(f"❌ DiagnosticArray process hash_sk={hash_sk}: {e}")
                        continue

                if params_list:
                    _insert_with_retry(conn, sql_insert, params_list, batch_number)
                    inserted_count += len(params_list)
                del params_list

                if batch_number % 5 == 0:
                    log.info(
                        f"📊 DiagArray [{data_file_id[:8]}]: batch {batch_number}, "
                        f"inserted {inserted_count}, errors {error_count}"
                    )

            log.info(
                f"✅ DiagArray [{data_file_id[:8]}]: inserted {inserted_count}, "
                f"skipped {skipped_count}, errors {error_count}"
            )
            return True

    except SQLAlchemyError as e:
        log.error(f"❌ SQLAlchemy error during DiagnosticArray merge: {e}", exc_info=True)
        raise e
    except Exception as e:
        log.error(f"❌ Unexpected error during DiagnosticArray merge: {e}", exc_info=True)
        raise e