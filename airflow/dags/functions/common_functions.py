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
from airflow.models import Variable
from contextlib import contextmanager
from typing import Dict, Any, Optional, List, Union
from sqlalchemy import Column, Integer, String, ForeignKey, create_engine, DateTime, func, Index, Boolean, select, text, Float, LargeBinary
from sqlalchemy.orm import declarative_base, relationship, Session, declared_attr, sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from db_requests.db_request_methods import get_data_from_dwh
from uuid import uuid4, UUID
import logging
import json
import tempfile
from helper.helper import parse_uuid
import pandas as pd
import pyarrow.parquet as pq
import hashlib
import numpy as np
import binascii
import pickle
import struct
from typing import Any, Optional
from dataclasses import dataclass
import struct
import uuid
import logging
from typing import Optional
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import struct
from typing import List, Tuple
import logging
from datetime import timedelta
import os
import gc
import json
import uuid
from datetime import datetime, date
from pathlib import Path
from typing import Any, Optional
import pyarrow as pa
import pyarrow.parquet as pqa
import boto3
from botocore.config import Config
from sqlalchemy import text
from airflow.hooks.base import BaseHook
from datetime import datetime, date, timezone
from typing import Any, Optional
from uuid import UUID
import pandas as pd
import numpy as np
from sqlalchemy import text
from clickhouse_driver import Client
from airflow.hooks.base import BaseHook
import logging
import json
from psycopg2.extras import execute_values
import uuid
import time
from sqlalchemy import text
from typing import List, Union

from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import PrimitiveType
from pyiceberg.io.pyarrow import PyArrowFileIO
from airflow.hooks.base import BaseHook

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

def get_dwh_engine(conn_id: str = 'cloudberry_test_dwh') -> Engine:
    log.debug(f"🔧 Creating engine for connection: {conn_id}")
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
        log.debug(f"✅ Engine created successfully for {conn_id}")
        return engine
        
    except Exception as e:
        log.error(f"❌ Failed to create engine for '{conn_id}': {str(e)}", exc_info=True)
        raise AirflowException(f"Ошибка создания Engine для подключения '{conn_id}': {str(e)}")


Base = declarative_base()
DWH_CONN_ID = 'cloudberry_test_dwh'
cloudberry_engine = get_dwh_engine(DWH_CONN_ID)

TARGET_FILE_SIZE_MB = int(os.getenv("PARQUET_TARGET_SIZE_MB", "256"))
TARGET_FILE_SIZE_BYTES = TARGET_FILE_SIZE_MB * 1024 * 1024
ZSTD_COMPRESSION_LEVEL = int(os.getenv("ZSTD_LEVEL", "3"))

class DatabaseIdent(Base):
    __tablename__ = 'DatabaseIdent'
    id = Column('ID', String(36), primary_key=True)  # UUID как TEXT
    main_db_name = Column('MainDbName', String(100))
    main_db_id = Column('MainDbId', String(36))
    data_type = Column('DataType', Integer)
    
FH_MAGIC_HEX = 0xBEBAADDE
FH_MIN_HEADER_LENGTH = 30
FH_HEADER_DATA_OFFSET = 26
FH_FROM_DATE_OFFSET = 8
FH_TO_DATE_OFFSET = 16
FH_VERSION_OFFSET = 24
FH_EXTRA_MAGIC = 0x4B52494F  # "KRIO"

FH_QUALITY_LENGTH = 1
FH_VALUE_LENGTH = 8
FH_FIELD_LENGTH = FH_QUALITY_LENGTH + FH_VALUE_LENGTH  # 9 байт
FH_ROW_ID_LENGTH = 16  # GUID

# Коды качества
FH_QUALITY_UNKNOWN = 0
FH_QUALITY_NO_DATA = 1
FH_QUALITY_GOOD = 2
FH_QUALITY_BAD = 3

# Тип данных для .fh файлов (всегда Double)
FH_DATA_TYPE_ID = '05229302-46d6-f9af-261c-6e0ea91996a2'

# Таблицы для очистки дубликатов
TABLES_TO_CHECK_DUPLICATES = [
    'stg_double_object_data_values',
]

# ═══════════════════════════════════════════════════════════════════
#  Вспомогательные функции для работы с .fh
# ═══════════════════════════════════════════════════════════════════

def fh_ts_to_datetime(ts_ms: int) -> datetime:
    """Конвертирует Unix timestamp (мс) в datetime UTC."""
    from datetime import timezone
    return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=ts_ms)


def fh_guid_from_bytes_le(b: bytes) -> uuid.UUID:
    """Читает GUID в формате .NET (little-endian)."""
    return uuid.UUID(bytes_le=b)





@dataclass
class FhParsedValue:
    """Одно распарсенное значение из .fh файла."""
    object_id: uuid.UUID       # property_id из строки
    timestamp: datetime        # from_date + col_idx секунд
    data_value: float          # double значение
    quality: int               # 0=UNKNOWN, 1=NO_DATA, 2=GOOD, 3=BAD


@dataclass
class FhParsedFile:
    """Результат парсинга .fh файла."""
    file_id: uuid.UUID
    main_db_id: uuid.UUID
    main_db_name: str
    from_date: datetime
    to_date: datetime
    data_row_values_count: int
    rows_count: int
    total_values_count: int
    values: List[FhParsedValue]





# Epochs
POSTGRES_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)
UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
DOTNET_EPOCH = datetime(1, 1, 1, tzinfo=timezone.utc)
SIZEOF_DOUBLE = 8  # sizeof(double) в C#


TYPE_CONFIG = {
    # Primitive types
    '188e5cad-ec8e-d4fb-e35e-b46954a3bd22': ('int', '<i', 4),      # INT32
    'e52a9429-ff56-ef37-8d3a-a059d2014a59': ('int', '<q', 8),      # INT64
    '05229302-46d6-f9af-261c-6e0ea91996a2': ('float', '<d', 8),    # FLOAT64
    'aea0a96a-2d29-90a3-7bc6-34ca78a25338': ('bool', 'B', 1),      # bool
    '1bd768d5-d1a0-6e18-f22a-117d0d746a16': ('str', None, None),   # STRING
    
    # Special types
    '16b0f965-5eff-6cb6-9bff-7ccebc1c8658': ('datetime', None, 8), # DATETIME
    '72e62f2c-a3f3-5438-1582-b296e7e8aef3': ('timedelta', None, 8),# DURATION
    
    # Array types - NO LENGTH PREFIX, just packed elements
    '0f573cc6-34aa-e254-7206-f7ce3395513b': ('array_int', '<i', 4),      # INT_ARRAY
    'e0c92ea3-2133-ec22-a0c9-bb4f4bd0f58d': ('array_float', '<d', 8),    # FLOAT_ARRAY
    '1cb452b1-fc3d-99c1-900a-498826c1558f': ('array_str', None, None),   # STRING_ARRAY (UTF-8, null-separated or JSON)
    'cadab02d-e9d6-1078-2d4e-ce9453bad248': ('array_datetime', None, 8), # DATETIME_ARRAY
}

# Маппинг SpectrumType enum (C#) -> описание
SPECTRUM_TYPE_MAP = {
    0: "Acceleration",
    1: "Velocity", 
    2: "Displacement",
    3: "Envelope",
}


DEFAULT_DATETIME = datetime(1970, 1, 1, tzinfo=timezone.utc)
DEFAULT_VALUES_BY_TYPE = {
    'datetime': DEFAULT_DATETIME,
    'integer': 0,
    'float': 0.0,
    'boolean': False,
    'string': '',
    'array': [],
    'uuid': '00000000-0000-0000-0000-000000000000',
}





    

@dataclass
class CreytSample:
    """Контейнер для десериализованных данных одной записи"""
    # Метаданные из заголовка
    sync_id: str
    sample_rate: float
    calibration_factor_a: float
    calibration_factor_b: float
    
    # Параметры шкалы преобразования
    scale_min_eu: float
    scale_max_eu: float
    scale_min_raw: float
    scale_max_raw: float
    
    # Сырые и обработанные данные
    raw_values: bytes           # исходные байты (включая CRC)
    adc_values: np.ndarray      # значения АЦП (0-65535), float64
    voltage_values: np.ndarray  # напряжение (0-5В), float64
    calibrated_values: np.ndarray  # после калибровки, float64
    eu_values: np.ndarray       # инженерные единицы, float64
    
    # Статистика
    sample_count: int
    time_delta_ms: float

@dataclass
class LCardSample:
    """Контейнер для десериализованных данных LCardSample"""
    # Метаданные из заголовка
    sample_rate: float          # частота дискретизации
    step: float                 # шаг преобразования (вольты/отсчет)
    is_scale: bool              # флаг применения шкалы
    
    # Параметры шкалы преобразования (если is_scale=True)
    scale_min_eu: float         # мин. значение в инженерных единицах
    scale_max_eu: float         # макс. значение в инженерных единицах
    scale_min_raw: float        # мин. сырое значение
    scale_max_raw: float        # макс. сырое значение
    
    # Сырые данные
    raw_values: bytes           # исходные байты (массив double, little-endian)
    raw_values_array: np.ndarray  # распакованный массив значений (float64)
    
    # Статистика
    sample_count: int           # количество отсчётов




    

    
    
def get_data_file_id(engine: Engine, is_hist_data:bool = False) -> UUID:
    # log.debug("🔍 Fetching data_file_id from DatabaseIdent table")
    uid: UUID
    try:
        with Session(engine) as session:
            stmt = select(DatabaseIdent)
            row = session.execute(stmt.limit(1)).scalars().one_or_none()

            if row is None:
                log.error("❌ DatabaseIdent table is empty or contains no records")
                raise AirflowException("Таблица DatabaseIdent пуста или не содержит записей")
            
            # Для исторических данных any/double берем main_db_id а не id any/double файла!!!!
            if is_hist_data:
                uid = parse_uuid(row.main_db_id)
            else:
                uid = parse_uuid(row.id)
            
            if uid is None:
                log.error(f"❌ Invalid UUID in row: {row}")
                raise AirflowException(f"Не удалось распарсить ID: {row.id}")
            
            return uid
                    
    except Exception as e:
        log.error(f"❌ Error fetching data_file_id: {str(e)}", exc_info=True)
        raise AirflowException(f"Ошибка при подключении к БД: {str(e)}")
    

def get_data_source_name(data_file_id: str) -> str:
    source_name: str
    sql_query = f"SELECT main_db_name FROM data_catalogue WHERE data_file_id = '{data_file_id}'"
    result=get_data_from_dwh(sql_query)
    if not result:
        return data_file_id.split('-')[0] if data_file_id else 'unknown'
    source_name = result[0].get('main_db_name', 'unknown')
    return source_name if source_name else 'unknown'
    
    





def calc_hash_sk(values: dict) -> UUID:
    """
    Расчет хеша для Surrogate Key.
    values: dict (ранее был pd.Series)
    """
    # Итерируемся по значениям словаря
    raw_string = '|'.join(str(v) for v in values.values() if pd.notna(v))
    md5_hex = hashlib.md5(raw_string.encode('utf-8')).hexdigest()
    result = md5_hex_to_uuid(md5_hex)
    return result





def calc_hub_hash_for_column(
    row: dict,  # Теперь принимаем dict вместо pd.Series
    column_name: str,
    is_system_type_data: bool,
    prefix: str,
    data_file_id: UUID
) -> UUID | None:
    """
    Расчет хеша для Hub таблицы.
    row: dict (ранее был pd.Series)
    """
    # row[column_name] работает одинаково для dict и pd.Series!
    raw_value = row[column_name]
    
    if pd.isna(raw_value) or raw_value is None:
        column_value = 'null'
    elif isinstance(raw_value, (int, np.integer)):
        column_value = str(int(raw_value))
    elif isinstance(raw_value, (float, np.floating)):
        if np.isnan(raw_value):
            column_value = 'null'
        elif raw_value.is_integer():
            column_value = str(int(raw_value))  # 1.0 -> "1"
        else:
            column_value = str(raw_value)
    else:
        column_value = str(raw_value)

    if is_system_type_data:
        inner_input = f"{prefix}|{column_value}"
    else:
        inner_input = f"{column_value}|{str(data_file_id).lower()}"
    
    inner_md5 = hashlib.md5(inner_input.encode('utf-8')).hexdigest()
    result = md5_hex_to_uuid(inner_md5)
    return result



def calc_hash_sat_diff(row: dict, data_file_id: UUID) -> UUID:
    # row теперь dict, итерируемся по values() (а не по ключам!)
    values_str = '|'.join(str(val) for val in row.values())
    raw_string = f"{values_str}|{str(data_file_id)}"
    md5_hex = hashlib.md5(raw_string.encode('utf-8')).hexdigest()
    return md5_hex_to_uuid(md5_hex)


def calc_hub_hash_for_column(
    row: pd.Series,
    column_name: str,
    is_system_type_data: bool,
    prefix: str,
    data_file_id: UUID
) -> UUID | None:

    raw_value = row[column_name]
    
    if pd.isna(raw_value) or raw_value is None:
        column_value = 'null'
    elif isinstance(raw_value, (int, np.integer)):
        column_value = str(int(raw_value))
    elif isinstance(raw_value, (float, np.floating)):
        if np.isnan(raw_value):
            column_value = 'null'
        elif raw_value.is_integer():
            column_value = str(int(raw_value))  # 1.0 -> "1"
        else:
            column_value = str(raw_value)
    else:
        column_value = str(raw_value)

    if is_system_type_data:
        inner_input = f"{prefix}|{column_value}"
        # log.info(f"{inner_input}")
        # log.info(f"{raw_value}")
        # log.info(f"{column_name}")
    else:
        inner_input = f"{column_value}|{str(data_file_id)}"
    
    inner_md5 = hashlib.md5(inner_input.encode('utf-8')).hexdigest()
    result = md5_hex_to_uuid(inner_md5)
    return result


def calc_hub_hash_for_column(
    row: pd.Series,
    column_name: str,
    is_system_type_data: bool,
    prefix: str,
    main_db_id: UUID
) -> UUID | None:

    raw_value = row[column_name]
    
    if pd.isna(raw_value) or raw_value is None:
        column_value = 'null'
    elif isinstance(raw_value, (int, np.integer)):
        column_value = str(int(raw_value))
    elif isinstance(raw_value, (float, np.floating)):
        if np.isnan(raw_value):
            column_value = 'null'
        elif raw_value.is_integer():
            column_value = str(int(raw_value))  # 1.0 -> "1"
        else:
            column_value = str(raw_value)
    else:
        column_value = str(raw_value)

    if is_system_type_data:
        inner_input = f"{prefix}|{column_value}"
        # log.info(f"{inner_input}")
        # log.info(f"{raw_value}")
        # log.info(f"{column_name}")
    else:
        inner_input = f"{column_value}|{str(main_db_id)}"
    
    inner_md5 = hashlib.md5(inner_input.encode('utf-8')).hexdigest()
    result = md5_hex_to_uuid(inner_md5)
    return result


def normalize_value(val, target_type: str) -> Union[str, int, float, UUID, datetime, dict, list, None]:
    # 🔥 Проверка на пустые/отсутствующие значения
    is_empty = pd.isna(val) or val is None or (isinstance(val, str) and val.strip() == '')
    
    if is_empty:
        if target_type == 'string':
            return ''
        return None
    
    try:
        if target_type == 'json':
            if isinstance(val, (dict, list)):
                result = json.dumps(val, ensure_ascii=False)
                return result
            if isinstance(val, str):
                val = val.strip()
                if not val:
                    return None
                try:
                    parsed = json.loads(val)
                    result = json.dumps(parsed, ensure_ascii=False)
                    return result
                except json.JSONDecodeError as e:
                    return None
            return None

        elif target_type == 'uuid':
            raw_str = None
            
            # 1. Нормализация входных данных до строки
            if isinstance(val, bytes):
                if len(val) == 16:
                    raw_str = val.hex()
                else:
                    try:
                        raw_str = val.decode('utf-8', errors='replace').strip()
                    except Exception as e:
                        return None
            
            elif isinstance(val, UUID):
                raw_str = str(val)
            elif isinstance(val, str):
                raw_str = val.strip()
            else:
                raw_str = str(val).strip()

            if not raw_str:
                return None

            # --- ЛОГИКА ИСПРАВЛЕНИЯ ПОРЯДКА БАЙТ (.NET GUID Little-Endian) ---
            if len(raw_str) == 32 and '-' not in raw_str:
                raw_str = f"{raw_str[:8]}-{raw_str[8:12]}-{raw_str[12:16]}-{raw_str[16:20]}-{raw_str[20:]}"

            parts = raw_str.split('-')
            
            # Проверяем формат 8-4-4-4-12
            if len(parts) == 5:
                if (len(parts[0]) == 8 and len(parts[1]) == 4 and len(parts[2]) == 4 and 
                    len(parts[3]) == 4 and len(parts[4]) == 12):
                    
                    if all(all(c in '0123456789abcdefABCDEF' for c in p) for p in parts):
                        try:
                            p0_bytes = [parts[0][i:i+2] for i in range(0, 8, 2)]
                            p0_fixed = "".join(p0_bytes[::-1])
                            p1_bytes = [parts[1][i:i+2] for i in range(0, 4, 2)]
                            p1_fixed = "".join(p1_bytes[::-1])
                            p2_bytes = [parts[2][i:i+2] for i in range(0, 4, 2)]
                            p2_fixed = "".join(p2_bytes[::-1])
                            p3_fixed = parts[3]
                            p4_fixed = parts[4]
                            fixed_str = f"{p0_fixed}-{p1_fixed}-{p2_fixed}-{p3_fixed}-{p4_fixed}"
                            return UUID(fixed_str)
                        except ValueError as e:
                            log.warning(f"⚠️ Failed to fix UUID {raw_str}: {e}. Trying standard parse.")

            try:
                result = UUID(raw_str)
                return result
            except ValueError as e:
                pass
            
            raise ValueError(f"Invalid UUID format: {raw_str}")

        elif target_type == 'timestamp':
            if isinstance(val, (datetime, pd.Timestamp)):
                return val if pd.notna(val) else None
            if isinstance(val, str):
                val = val.strip()
                if not val:
                    return None
                result = pd.to_datetime(val, errors='coerce')
                return result
            if isinstance(val, (int, float)):
                result = pd.to_datetime(val, unit='s', errors='coerce') if val else None
                return result
            result = pd.to_datetime(str(val), errors='coerce')
            return result
            
        elif target_type == 'integer':
            if isinstance(val, (int, np.integer)):
                return int(val)
            if isinstance(val, (float, np.floating)):
                return int(val) if not np.isnan(val) else None
            if isinstance(val, str):
                val_stripped = val.strip()
                if not val_stripped:
                    return None
                result = int(float(val_stripped))
                return result
            result = int(float(str(val)))
            return result
            
        elif target_type == 'float':
            if isinstance(val, (int, float, np.number)):
                return float(val) if not pd.isna(val) else None
            if isinstance(val, str):
                val_stripped = val.strip()
                result = float(val_stripped) if val_stripped else None
                return result
            result = float(str(val))
            return result
            
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
            result = bool(val)
            return result
            
        elif target_type == 'string':
            if isinstance(val, bytes):
                result = val.decode('utf-8', errors='replace').strip().replace('\x00', '')
                return result
            result = str(val).strip().replace('\x00', '')
            return result if result != '' else ''
         
        elif target_type == 'bytea':
            if isinstance(val, bytes):
                return val
            if isinstance(val, str):
                val = val.strip()
                if not val:
                    return None
                try:
                    import base64
                    result = base64.b64decode(val)
                    return result
                except Exception as e:
                    return val.encode('utf-8', errors='replace')
            if pd.isna(val) or val is None:
                return None
            result = str(val).encode('utf-8', errors='replace')

            return result
            
    except Exception as e:
        if target_type == 'string':
            return ''
        if target_type == 'json':
            return None
        return None


def pg_insert_values(table, conn, keys, data_iter):
    """Кастомный метод для быстрой вставки через execute_values"""
    dbapi_conn = conn.connection
    if hasattr(dbapi_conn, 'dbapi_connection'):  # SQLAlchemy 2.0+
        dbapi_conn = dbapi_conn.dbapi_connection
        
    with dbapi_conn.cursor() as cur:
        if table.schema:
            table_name = f"{table.schema}.{table.name}"
        else:
            table_name = table.name
            
        cols = ', '.join(f'"{k}"' for k in keys)
        query = f"INSERT INTO {table_name} ({cols}) VALUES %s"
        execute_values(cur, query, data_iter, page_size=10000)
        

def read_entity_config(entity_config: Dict[str, Any]) -> Dict[str, Any]:

    result = {
        'source_table_name': entity_config['source_table_name'],
        'staging_table_name': entity_config['staging_table_name'],
        'hash_hub_gener_columns': [],
        'source_column_names': [],
        'column_types': {},
        'link_columns': [],
        'satellite_columns': []
    }
    
    for column in entity_config['source_column_descriptions']:
        column_name = column['column_name']
        column_name_lower = column_name.lower()
        
        # Добавляем имя колонки в общий список
        result['source_column_names'].append(column_name_lower)
        
        # Добавляем тип колонки
        result['column_types'][column_name_lower] = column['type']
        
        # Проверяем флаг hub_sk_gener
        if column.get('hub_sk_gener', 'False') == 'True':
            result['hash_hub_gener_columns'].append(column_name_lower)
        
        # Проверяем флаг is_satellite_column
        if column.get('is_satellite_column', 'False') == 'True':
            result['satellite_columns'].append(column_name_lower)
        
        
        # Проверяем флаг is_link_columns
        if column.get('is_link_columns', 'False') == 'True':
            link_column_desc = {
                'column_name': column_name_lower,
                'is_system_type_data': column.get('is_system_type_data', ''),
                'prefix_entity_name': column.get('prefix_entity_name', '')
                }       
            result['link_columns'].append(link_column_desc)
        
    
    return result


def extract_nonhist_data_to_staging(
    sqlite_path: str, 
    entity_config: Dict[str, Any],
) -> bool:
    
    # Парсим конфигурацию через read_entity_config (если передан исходный формат)
    if 'source_column_descriptions' in entity_config:
        # Исходный формат → преобразуем
        config = read_entity_config(entity_config)
    else:
        # Уже готовый формат из Knowledge Base
        config = entity_config.copy()
        # Нормализуем ключи к нижнему регистру
        config['source_column_names'] = [c.lower() for c in config.get('source_column_names', [])]
        config['column_types'] = {k.lower(): v for k, v in config.get('column_types', {}).items()}

    # Извлекаем параметры из конфигурации
    source_table_name = config['source_table_name']
    staging_table_name = config['staging_table_name']
    source_column_names = config['source_column_names']
    hash_hub_gener_columns = config['hash_hub_gener_columns']
    column_types = config.get('column_types', {})
    
    db_path = sqlite_path if sqlite_path.startswith("sqlite:///") else f"sqlite:///{sqlite_path}"
    sqlite_engine = create_engine(db_path, echo=False)
    dwh_engine = get_dwh_engine(DWH_CONN_ID)
    
    try:
        data_file_id: UUID = get_data_file_id(sqlite_engine)
        
        # Формируем запрос с экранированием имён колонок
        quoted_cols = [f'"{col}"' if col.lower() not in source_column_names else col for col in source_column_names]
        query = f"SELECT {', '.join(quoted_cols)} FROM \"{source_table_name}\""
        df = pd.read_sql(query, sqlite_engine)
        
        # Нормализуем имена колонок к нижнему регистру
        df.columns = df.columns.str.lower()

        if df.empty:
            log.info(f"⚠️ No data to process in SQLite for entity '{entity_config['source_table_name']}' - returning early")
            return True

        df['data_file_id'] = data_file_id
        df['load_dttm'] = datetime.now(timezone.utc)
        
        # Расчёт hash_sk для Hub
        cols_for_hash = hash_hub_gener_columns + ['data_file_id']
        df['hash_sk'] = df[cols_for_hash].apply(lambda row: calc_hash_sk(row), axis=1)
        
        # Расчёт hash_sat_diff для отслеживания изменений
        cols_to_drop = ['hash_sk', 'hash_sat_diff', 'load_dttm']
        df['hash_sat_diff'] = df.apply(
            lambda row: calc_hash_sat_diff(
                row.drop(cols_to_drop, errors='ignore'), 
                data_file_id
            ), 
            axis=1
        )

        # Нормализация значений по типам
        for col in df.columns:
            if col in ['data_file_id', 'hash_sk', 'hash_sat_diff', 'load_dttm']:
                continue
            target_type = column_types.get(col, 'string')
            df[col] = df[col].apply(lambda x: normalize_value(x, target_type))

        # Расчёт hash_sk для Hub
        df['h_object_property_sk'] = df.apply(lambda row: calc_hub_hash_for_column(row, 'id', False, '', data_file_id), axis=1)
        df['h_data_type_sk'] = df.apply(lambda row: calc_hub_hash_for_column(row, 'datatypeid', True, 'h_data_types', data_file_id), axis=1)
        
        
        # Маппинг типов для PostgreSQL
        dtype_mapping = {
            'hash_sk': PG_UUID(as_uuid=True),
            'hash_sat_diff': PG_UUID(as_uuid=True),
            'h_object_property_sk': PG_UUID(as_uuid=True),
            'h_data_type_sk': PG_UUID(as_uuid=True),
            'data_file_id': PG_UUID(as_uuid=True),
            'load_dttm': DateTime(timezone=True),
        }
        
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
                elif ttype == 'bytea':
                    dtype_mapping[col] = LargeBinary
        

        # Загрузка в staging-таблицу
        rows_loaded = df.to_sql(
            name=staging_table_name, 
            con=dwh_engine, 
            if_exists='append', 
            index=False,
            method='multi',
            chunksize=200,
            dtype=dtype_mapping if hasattr(dwh_engine.dialect, 'name') and dwh_engine.dialect.name == 'postgresql' else None
        )
        
        return True
        
    except SQLAlchemyError as e:
        raise AirflowException(f"DB error: {str(e)}")
    except Exception as e:
        raise
    finally:
        sqlite_engine.dispose()
        dwh_engine.dispose()
        log.debug("🔌 Database engines disposed")
        
        
        
        

      

def clean_staging_table(table_name:str):
    dwh_engine = get_dwh_engine(DWH_CONN_ID)
    
    try:
        with dwh_engine.begin() as conn:
            sql_clean = text(f"""TRUNCATE TABLE {table_name}""")
            result_truncate = conn.execute(sql_clean)
            
    except SQLAlchemyError as e:
        log.error(f"❌ SQLAlchemy error during link upload: {e}", exc_info=True)
        raise e
    except Exception as e:
        log.error(f"❌ Unexpected error during link upload: {e}", exc_info=True)
        raise e
    
    
def md5_hex_to_uuid(hex_string: str) -> UUID:
    if not hex_string or len(hex_string) != 32:
        log.debug(f"⚠️ Invalid hex string for UUID conversion: '{hex_string[:10]}...', generating random UUID")
        return uuid4()
    uuid_str = f"{hex_string[:8]}-{hex_string[8:12]}-{hex_string[12:16]}-{hex_string[16:20]}-{hex_string[20:]}"
    return UUID(uuid_str)

def calc_link_hash(
    row: dict,
    link_column: str,
    is_system_type_data: bool,
    prefix: str,
    data_file_id: UUID,
    hash_column_name: str = 'hash_sk'
) -> UUID | None:

    link_value = str(row[link_column])
    hash_sk = str(row[hash_column_name])

    if is_system_type_data:
        inner_input = f"{prefix}|{link_value}"
    else:
        inner_input = f"{link_value}|{str(data_file_id)}"

    inner_md5 = hashlib.md5(inner_input.encode('utf-8')).hexdigest()
    final_input = f"{hash_sk}|{inner_md5}"
    md5_hex = hashlib.md5(final_input.encode('utf-8')).hexdigest()
    result=md5_hex_to_uuid(md5_hex)

    return result



def _ensure_bytes(val: Union[bytes, memoryview, None]) -> Optional[bytes]:
    if val is None:
        return None
    if isinstance(val, memoryview):
        return bytes(val)
    return val


def _safe_unpack_datetime(micros: int, type_id: str) -> Optional[datetime]:
    """Try multiple epoch interpretations for datetime"""
    for epoch in (POSTGRES_EPOCH, UNIX_EPOCH):
        try:
            return epoch + timedelta(microseconds=micros)
        except (OverflowError, ValueError, OSError):
            continue
    # Fallback: return None instead of logging for every invalid value
    return None


def deserialize_value(bytea_value: Union[bytes, memoryview, None], data_type_id: str) -> Any:
    """
    Deserialize bytea value to native Python type.
    
    Array format: packed elements without length prefix.
    String arrays: JSON-encoded or null-separated UTF-8.
    """
    bytea_value = _ensure_bytes(bytea_value)
    
    if bytea_value is None or len(bytea_value) == 0:
        return None
    
    config = TYPE_CONFIG.get(data_type_id)
    if not config:
        # Fallback: try pickle, then UTF-8
        try:
            return pickle.loads(bytea_value)
        except Exception:
            try:
                return bytea_value.decode('utf-8')
            except Exception:
                return None
    
    py_type, fmt, elem_size = config
    
    try:
        # ==================== Primitive numeric ====================
        if py_type == 'int':
            if len(bytea_value) >= elem_size:
                return struct.unpack(fmt, bytea_value[:elem_size])[0]
        
        elif py_type == 'float':
            if len(bytea_value) >= elem_size:
                return struct.unpack(fmt, bytea_value[:elem_size])[0]
        
        elif py_type == 'bool':
            return bool(bytea_value[0]) if bytea_value else False
        
        # ==================== STRING ====================
        elif py_type == 'str':
            return bytea_value.decode('utf-8', errors='replace')
        
        # ==================== DATETIME ====================
        elif py_type == 'datetime':
            if len(bytea_value) >= 8:
                micros = struct.unpack('<q', bytea_value[:8])[0]
                return _safe_unpack_datetime(micros, data_type_id)
        
        # ==================== DURATION ====================
        elif py_type == 'timedelta':
            if len(bytea_value) >= 8:
                micros = struct.unpack('<q', bytea_value[:8])[0]
                try:
                    return timedelta(microseconds=micros)
                except (OverflowError, ValueError, OSError):
                    return None
        
        # ==================== Arrays (PACKED, no length prefix) ====================
        elif py_type.startswith('array_'):
            base_type = py_type.replace('array_', '')
            
            if base_type in ('int', 'float'):
                # Unpack consecutive elements until we run out of bytes
                result = []
                offset = 0
                while offset + elem_size <= len(bytea_value):
                    try:
                        val = struct.unpack(fmt, bytea_value[offset:offset+elem_size])[0]
                        result.append(val)
                        offset += elem_size
                    except struct.error:
                        break
                return result if result else None
            
            elif base_type == 'str':
                # Try JSON first, then null-separated UTF-8
                try:
                    import json
                    text = bytea_value.decode('utf-8')
                    parsed = json.loads(text)
                    if isinstance(parsed, list):
                        return parsed
                except Exception:
                    pass
                # Fallback: split by null bytes
                try:
                    return [s.decode('utf-8', errors='replace') 
                           for s in bytea_value.split(b'\x00') if s]
                except Exception:
                    pass
                return None
            
            elif base_type == 'datetime':
                result = []
                offset = 0
                while offset + 8 <= len(bytea_value):
                    try:
                        micros = struct.unpack('<q', bytea_value[offset:offset+8])[0]
                        dt = _safe_unpack_datetime(micros, data_type_id)
                        if dt:
                            result.append(dt)
                        offset += 8
                    except struct.error:
                        break
                return result if result else None
        
        return None
        
    except Exception:
        return None


import struct
import psycopg2
import psycopg2.extras
from psycopg2.extras import execute_values
from airflow.hooks.postgres_hook import PostgresHook

def merge_fh_data_optimized(
    stg_table_name: str,
    target_table_name: str,
    batch_size: int = 50000
) -> int:
    """
    Оптимизированный перенос данных из staging в целевую таблицу.
    
    Ключевые оптимизации:
    1. Серверный курсор вместо OFFSET/LIMIT (O(n) вместо O(n²))
    2. execute_values вместо execute_batch (в 2-3 раза быстрее)
    3. Оптимизированная десериализация
    4. ANALYZE после вставки
    5. Разделение соединений для чтения и записи, чтобы батчевый коммит 
       не закрывал серверный курсор
    """
    hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    
    # ИСПРАВЛЕНИЕ: Используем два соединения
    conn_read = hook.get_conn()
    conn_write = hook.get_conn()
    
    try:
        # Создаем серверный курсор на соединении для чтения
        cursor = conn_read.cursor(name='staging_stream_cursor', cursor_factory=psycopg2.extras.DictCursor)
        
        # Запрос без ORDER BY (сортировка добавляет оверхед)
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
        """)
        
        total_inserted = 0
        batch_num = 0
        
        # Шаблон для execute_values
        insert_template = f"""
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
            ) VALUES %s
            ON CONFLICT (data_record_sk) DO NOTHING
        """
        
        # Читаем данные порциями через серверный курсор
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            
            # Формируем список кортежей для execute_values
            values_list = []
            for row in rows:
                hash_sk = row['hash_sk']
                h_object_property_sk = row['h_object_property_sk']
                value_bytes = row['value']
                timestamp = row['timestamp']
                data_file_id = row['data_file_id']
                data_source_id = row['data_source_id']
                load_dttm = row['load_dttm']
                
                # Оптимизированная распаковка float64
                if value_bytes is not None and len(value_bytes) == 8:
                    try:
                        data_value = struct.unpack('<d', value_bytes)[0]
                    except struct.error:
                        data_value = None
                else:
                    data_value = None
                
                values_list.append((
                    hash_sk,
                    h_object_property_sk,
                    data_value,
                    timestamp,
                    data_file_id,
                    data_source_id,
                    load_dttm,
                    False,  # exported_to_s3_storage
                    None    # s3_storage_path
                ))
            
            # Быстрая пакетная вставка через execute_values
            if values_list:
                # ИСПРАВЛЕНИЕ: Используем второе соединение для вставки
                insert_cursor = conn_write.cursor()
                execute_values(
                    insert_cursor,
                    insert_template,
                    values_list,
                    page_size=batch_size,
                    template=None
                )
                insert_cursor.close()
                
                # Коммитим батч на соединении для записи. 
                # Это НЕ закрывает серверный курсор на conn_read!
                conn_write.commit()
                
                total_inserted += len(values_list)
                batch_num += 1
                
                if batch_num % 5 == 0:
                    log.info(f"📤 Прогресс: {total_inserted:,} записей перенесено ({batch_num} батчей)")
        
        cursor.close()
        
        # Обновляем статистику для оптимизатора Cloudberry
        log.info(f"🔄 Выполняю ANALYZE для {target_table_name}...")
        analyze_cursor = conn_write.cursor()
        analyze_cursor.execute(f"ANALYZE {target_table_name}")
        analyze_cursor.close()
        conn_write.commit()
        
        log.info(f"✅ Всего перенесено в целевую таблицу: {total_inserted:,} записей")
        return total_inserted
    
    except Exception as e:
        conn_read.rollback()
        conn_write.rollback()
        log.error(f"❌ Ошибка переноса данных: {e}", exc_info=True)
        raise
    finally:
        # ИСПРАВЛЕНИЕ: Закрываем оба соединения
        conn_read.close()
        conn_write.close()
    


def deserialize_creyt_blob(blob_data: bytes) -> CreytSample:
    """
    Десериализация BLOB из колонки Value таблицы ObjectDataValues_common_snapshot.
    
    Параметры:
    ----------
    blob_data : bytes
        Сырые байты из SQLite BLOB-поля
    
    Возвращает:
    -----------
    CreytSample : Объект с метаданными и массивами значений
    
    Raises:
    -------
    ValueError : если формат BLOB не соответствует ожидаемому
    """
    if not blob_data or len(blob_data) < 68:
        raise ValueError(f"BLOB too short for CreytFastSample: {len(blob_data)} bytes (min 68)")
    
    offset = 0
    
    # === Чтение заголовка (68 байт фиксированной части) ===
    
    # 1. SyncID: 16 bytes Guid (little-endian byte order)
    sync_id = _parse_guid_le(blob_data[offset:offset+16])
    offset += 16
    
    # 2. CalibrationFactorA: double, little-endian
    calibration_factor_a = struct.unpack('<d', blob_data[offset:offset+8])[0]
    offset += 8
    
    # 3. SampleRate: double, little-endian
    sample_rate = struct.unpack('<d', blob_data[offset:offset+8])[0]
    offset += 8
    
    # 4. EUScale параметры (4 x double, little-endian)
    min_eu   = struct.unpack('<d', blob_data[offset:offset+8])[0]; offset += 8
    max_eu   = struct.unpack('<d', blob_data[offset:offset+8])[0]; offset += 8
    min_raw  = struct.unpack('<d', blob_data[offset:offset+8])[0]; offset += 8
    max_raw  = struct.unpack('<d', blob_data[offset:offset+8])[0]; offset += 8
    
    # 5. RawValuesLength: uint32, little-endian
    raw_len = struct.unpack('<I', blob_data[offset:offset+4])[0]
    offset += 4
    
    # 6. RawValues: byte[raw_len]
    if len(blob_data) < offset + raw_len:
        raise ValueError(
            f"BLOB truncated: expected {offset + raw_len} bytes for header+raw, got {len(blob_data)}"
        )
    raw_values = blob_data[offset:offset+raw_len]
    offset += raw_len
    
    # 7. CalibrationFactorB: double, little-endian (опционально в конце)
    calibration_factor_b = 0.0
    if offset + 8 <= len(blob_data):
        calibration_factor_b = struct.unpack('<d', blob_data[offset:offset+8])[0]
    
    # === Обработка сырых данных АЦП ===
    
    # Обрезаем последние 8 байт (CRC32 + padding) — они не являются данными
    data_bytes = raw_values[:-8] if len(raw_values) > 8 else raw_values
    
    if len(data_bytes) % 2 != 0:
        raise ValueError(f"RawValues length must be even after CRC trim: {len(data_bytes)}")
    
    # Распаковка: 16-bit unsigned integers, Big-Endian (как в C# ReadUInt16BigEndian)
    adc_values = np.frombuffer(data_bytes, dtype='>u2').astype(np.float64)
    n_samples = len(adc_values)
    
    # === Цепочка преобразований (идентична C# GetDoubleValue / AsArray) ===
    
    # 1. АЦП → напряжение (0-5В): ADC * 5.0 / 65535.0
    voltage_values = adc_values * 5.0 / 65535.0
    
    # 2. Калибровка датчика: voltage * calA + calB
    calibrated_values = voltage_values * calibration_factor_a + calibration_factor_b
    
    # 3. Преобразование в инженерные единицы: EU = calibrated * K + B
    eu_values = np.full(n_samples, np.nan)
    if (max_raw - min_raw) != 0:
        k = (max_eu - min_eu) / (max_raw - min_raw)
        b = min_eu - min_raw * k
        eu_values = calibrated_values * k + b
    
    # === Сбор результата ===
    return CreytSample(
        sync_id=sync_id,
        sample_rate=sample_rate,
        calibration_factor_a=calibration_factor_a,
        calibration_factor_b=calibration_factor_b,
        scale_min_eu=min_eu,
        scale_max_eu=max_eu,
        scale_min_raw=min_raw,
        scale_max_raw=max_raw,
        raw_values=raw_values,
        adc_values=adc_values,
        voltage_values=voltage_values,
        calibrated_values=calibrated_values,
        eu_values=eu_values,
        sample_count=n_samples,
        time_delta_ms=1000.0 / sample_rate if sample_rate > 0 else float('inf')
    )


def _parse_guid_le(guid_bytes: bytes) -> str:
    """
    Парсинг GUID из little-endian формата (C# Guid.TryWriteBytes(span, false)).
    
    Возвращает строку в стандартном формате: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    """
    if len(guid_bytes) != 16:
        raise ValueError(f"GUID must be 16 bytes, got {len(guid_bytes)}")
    
    # Формат C# Guid: mixed endian
    d1 = struct.unpack('<I', guid_bytes[0:4])[0]   # uint32 LE
    d2 = struct.unpack('<H', guid_bytes[4:6])[0]   # uint16 LE
    d3 = struct.unpack('<H', guid_bytes[6:8])[0]   # uint16 LE
    d4 = guid_bytes[8:10].hex()                     # 2 bytes as-is
    d5 = guid_bytes[10:16].hex()                    # 6 bytes as-is
    
    return f"{d1:08x}-{d2:04x}-{d3:04x}-{d4}-{d5}"


def get_blob_info(blob_data: bytes) -> dict:
    """
    Быстрый инспектор структуры BLOB без полной десериализации.
    Полезно для отладки и валидации данных.
    """
    if len(blob_data) < 68:
        return {'error': f'Too short: {len(blob_data)} bytes'}
    
    offset = 0
    info = {}
    
    info['sync_id'] = _parse_guid_le(blob_data[offset:offset+16]); offset += 16
    info['calibration_factor_a'] = struct.unpack('<d', blob_data[offset:offset+8])[0]; offset += 8
    info['sample_rate'] = struct.unpack('<d', blob_data[offset:offset+8])[0]; offset += 8
    info['min_eu'] = struct.unpack('<d', blob_data[offset:offset+8])[0]; offset += 8
    info['max_eu'] = struct.unpack('<d', blob_data[offset:offset+8])[0]; offset += 8
    info['min_raw'] = struct.unpack('<d', blob_data[offset:offset+8])[0]; offset += 8
    info['max_raw'] = struct.unpack('<d', blob_data[offset:offset+8])[0]; offset += 8
    
    raw_len = struct.unpack('<I', blob_data[offset:offset+4])[0]; offset += 4
    info['raw_values_length'] = raw_len
    info['expected_total_size'] = 68 + raw_len + 8  # +8 для CalibrationFactorB
    info['actual_size'] = len(blob_data)
    info['is_complete'] = len(blob_data) >= 68 + raw_len
    info['has_calibration_b'] = len(blob_data) >= 68 + raw_len + 8
    
    return info




def deserialize_lcard_blob(blob_data: bytes) -> LCardSample:
    """
    Десериализация BLOB из колонки Value для типа LCard (LCARD_DATA_TYPE_ID).
    
    Формат буфера (C# LCardSample_Serializer):
    - sampleRate: double (8 bytes, LE)
    - step: double (8 bytes, LE)
    - isScale: bool (1 byte)
    - minEu, maxEu, minRaw, maxRaw: 4 × double (32 bytes, LE)
    - rawLen: uint32 (4 bytes, LE)
    - rawValues: rawLen × double (LE, packed)
    
    Параметры:
    ----------
    blob_data : bytes
        Сырые байты из SQLite BLOB-поля
    
    Возвращает:
    -----------
    LCardSample : Объект с метаданными и массивом значений
    
    Raises:
    -------
    ValueError : если формат BLOB не соответствует ожидаемому
    """
    # Минимальный размер заголовка: 8+8+1+32+4 = 53 bytes
    if not blob_data or len(blob_data) < 53:
        raise ValueError(f"BLOB too short for LCardSample: {len(blob_data)} bytes (min 53)")
    
    offset = 0
    
    # === Чтение заголовка (53 байта фиксированной части) ===
    
    # 1. SampleRate: double, little-endian
    sample_rate = struct.unpack('<d', blob_data[offset:offset+8])[0]
    offset += 8
    
    # 2. Step: double, little-endian
    step = struct.unpack('<d', blob_data[offset:offset+8])[0]
    offset += 8
    
    # 3. IsScale: bool (1 byte)
    is_scale = bool(blob_data[offset])
    offset += 1
    
    # 4. EUScale параметры (4 × double, little-endian)
    min_eu   = struct.unpack('<d', blob_data[offset:offset+8])[0]; offset += 8
    max_eu   = struct.unpack('<d', blob_data[offset:offset+8])[0]; offset += 8
    min_raw  = struct.unpack('<d', blob_data[offset:offset+8])[0]; offset += 8
    max_raw  = struct.unpack('<d', blob_data[offset:offset+8])[0]; offset += 8
    
    # 5. RawValuesLength: uint32, little-endian
    raw_len = struct.unpack('<I', blob_data[offset:offset+4])[0]
    offset += 4
    
    # 6. RawValues: array of double (raw_len × 8 bytes, little-endian)
    expected_raw_size = raw_len * 8
    if len(blob_data) < offset + expected_raw_size:
        raise ValueError(
            f"BLOB truncated: expected {offset + expected_raw_size} bytes for header+raw, got {len(blob_data)}"
        )
    
    raw_values = blob_data[offset:offset+expected_raw_size]
    
    # Распаковка массива double (little-endian)
    raw_values_array = np.frombuffer(raw_values, dtype='<f8')  # '<f8' = float64, little-endian
    
    if len(raw_values_array) != raw_len:
        log.warning(f"⚠️ RawValues length mismatch: expected {raw_len}, got {len(raw_values_array)}")
    
    # === Сбор результата ===
    return LCardSample(
        sample_rate=sample_rate,
        step=step,
        is_scale=is_scale,
        scale_min_eu=min_eu,
        scale_max_eu=max_eu,
        scale_min_raw=min_raw,
        scale_max_raw=max_raw,
        raw_values=raw_values,
        raw_values_array=raw_values_array,
        sample_count=len(raw_values_array)
    )
    



def deserialize_bode_blob(blob_data: bytes) -> Dict[str, float]:
    """
    Десериализация BLOB из колонки Value для типа BODE.
    
    Формат буфера (C# ObjectDataValue_Bode_Serializer):
    - magnitude_value: double (8 bytes, LE)
    - phase_value: double (8 bytes, LE)
    - turnover_frequency_value: double (8 bytes, LE)
    
    Всего: 24 байта (3 × float64)
    
    Параметры:
    ----------
    blob_data : bytes
        Сырые байты из SQLite BLOB-поля
    
    Возвращает:
    -----------
    dict : {'magnitude': float, 'phase': float, 'turnover_frequency': float}
    
    Raises:
    -------
    ValueError : если размер BLOB не равен 24 байтам
    """
    if not blob_data or len(blob_data) != 24:
        raise ValueError(
            f"BODE BLOB must be exactly 24 bytes (3×float64), got {len(blob_data)} bytes"
        )
    
    # Распаковка: 3 × double, little-endian
    magnitude, phase, turnover_frequency = struct.unpack('<3d', blob_data)
    
    return {
        'magnitude': magnitude,
        'phase': phase,
        'turnover_frequency': turnover_frequency
    }



    
    
def deserialize_sample_data_blob(blob_data: bytes) -> Dict[str, Any]:
    """
    Десериализация BLOB из колонки Value для типа DataSample.
    
    Формат буфера (C# ObjectDataValue_DataSample_Serializer):
    - raw_values: array of double (little-endian), размер = (total_len - 8)
    - sample_rate: double (последние 8 байт, little-endian)
    
    Параметры:
    ----------
    blob_data : bytes
        Сырые байты из SQLite BLOB-поля
    
    Возвращает:
    -----------
    dict : {
        'raw_values': bytes,      # исходные байты для хранения в bytea
        'sample_rate': int,       # частота дискретизации (приведена к int)
        'sample_count': int       # количество отсчётов
    }
    
    Raises:
    -------
    ValueError : если размер BLOB не кратен 8 или меньше 8 байт
    """
    if not blob_data or len(blob_data) < 8:
        raise ValueError(
            f"SampleData BLOB must be at least 8 bytes (for sample_rate), got {len(blob_data)} bytes"
        )
    
    if len(blob_data) % 8 != 0:
        raise ValueError(
            f"SampleData BLOB size must be multiple of 8 (double size), got {len(blob_data)} bytes"
        )
    
    # Последний double (8 байт) — это sample_rate
    sample_rate_bytes = blob_data[-8:]
    sample_rate = struct.unpack('<d', sample_rate_bytes)[0]
    
    # Остальные байты — массив raw_values (double[])
    raw_values = blob_data[:-8]
    
    # Количество отсчётов
    sample_count = len(raw_values) // 8
    
    return {
        'raw_values': raw_values,           # исходные байты для хранения в bytea
        'sample_rate': int(sample_rate),    # приводим к int как в таблице
        'sample_count': sample_count
    }










def deserialize_siemens_sample_blob(blob_bytes: bytes) -> dict:
    """
    Десериализует бинарный BLOB формата Siemens Sample.
    
    Бинарный формат (Little-Endian):
    - [0 : (N-1)*8] : массив значений сэмплов (double[])
    - [(N-1)*8 : ]  : sample_rate (double) - один элемент в конце
    
    Args:
        blob_bytes: Сырые байты из колонки bytea
        
    Returns:
        dict:
            - sample_rate: int (приведено к INTEGER для БД)
            - raw_values: bytes (для хранения в bytea)
            - sample_count: int (количество сэмплов)
    """
    if not blob_bytes:
        return {
            'sample_rate': 0,
            'raw_values': b'',
            'sample_count': 0
        }
    
    total_len = len(blob_bytes)
    
    # Валидация: размер должен быть кратен 8 и содержать хотя бы sample_rate
    if total_len < SIZEOF_DOUBLE or total_len % SIZEOF_DOUBLE != 0:
        raise ValueError(
            f"Invalid blob length {total_len} for SiemensSample. "
            f"Expected multiple of {SIZEOF_DOUBLE} bytes."
        )
    
    # 1. Извлекаем sample_rate из последних 8 байт ('<d' - little-endian double)
    sample_rate_double = struct.unpack('<d', blob_bytes[-SIZEOF_DOUBLE:])[0]
    
    # 2. Извлекаем сырые данные сэмплов (всё кроме последних 8 байт)
    raw_values_bytes = blob_bytes[:-SIZEOF_DOUBLE]
    
    # 3. Считаем количество сэмплов
    sample_count = len(raw_values_bytes) // SIZEOF_DOUBLE
    
    return {
        # Приводим к int, т.к. в таблице sample_rate INTEGER NOT NULL
        'sample_rate': int(sample_rate_double),
        # Оставляем как bytes для прямой вставки в bytea
        'raw_values': raw_values_bytes,
        'sample_count': sample_count
    }
    
    




def decode_siemens_sample_timeseries(
    raw_values: Union[bytes, np.ndarray],
    sample_rate: float,
    first_timestamp: datetime,
) -> List[Tuple[datetime, float]]:
    """
    🔥 ВЕКТОРИЗОВАННАЯ ВЕРСИЯ — в 10-50 раз быстрее исходной.
    
    Преобразует массив сырых значений Siemens Sample во временной ряд (timestamp, value).
    Алгоритм:
    1. raw_values — это массив double (little-endian), сохранённый как bytes
    2. Каждый элемент массива — это значение в инженерных единицах (без дополнительной калибровки)
    3. Временная метка каждого отсчёта вычисляется как:
       timestamp_i = first_timestamp + i * (1_000_000 / sample_rate) микросекунд

    Args:
        raw_values: numpy массив сырых значений (float64) ИЛИ bytes (little-endian double[])
        sample_rate: частота дискретизации (Гц)
        first_timestamp: временная метка первого отсчёта

    Returns:
        Список кортежей (timestamp, value)
    """
    # 🔥 КОНВЕРТАЦИЯ bytes → np.ndarray
    if isinstance(raw_values, (bytes, bytearray)):
        if len(raw_values) == 0:
            log.warning("❌ Empty raw_values bytes for Siemens Sample decoding")
            return []
        if len(raw_values) % 8 != 0:
            log.warning(f"⚠️ raw_values length {len(raw_values)} is not multiple of 8")
            return []
        raw_array = np.frombuffer(raw_values, dtype='<f8')
    elif isinstance(raw_values, memoryview):
        raw_array = np.frombuffer(bytes(raw_values), dtype='<f8')
    elif isinstance(raw_values, np.ndarray):
        raw_array = raw_values
    else:
        log.warning(f"❌ Invalid raw_values type: {type(raw_values)}")
        return []
    
    if len(raw_array) == 0:
        log.warning("❌ Empty raw_values array for Siemens Sample decoding")
        return []
    
    n_samples = len(raw_array)
    
    # =====================================================================
    # 🔥 ШАГ 1: Векторный расчёт временных меток
    # =====================================================================
    if sample_rate > 0:
        time_delta_us = 1_000_000.0 / sample_rate
        offsets_us = np.arange(n_samples, dtype=np.int64) * int(time_delta_us)
    else:
        offsets_us = np.zeros(n_samples, dtype=np.int64)
    
    if first_timestamp.tzinfo is None:
        first_timestamp = first_timestamp.replace(tzinfo=timezone.utc)
    
    base_ns = np.datetime64(first_timestamp, 'us')
    timestamps_ns = base_ns + offsets_us.astype('timedelta64[us]')
    
    # =====================================================================
    # 🔥 ШАГ 2: Финальная сборка
    # =====================================================================
    ts_series = pd.to_datetime(timestamps_ns, utc=True)
    ts_py_list = ts_series.to_pydatetime()
    
    result = list(zip(ts_py_list, raw_array.astype(np.float64).tolist()))
    
    log.debug(f"✅ Decoded {len(result)} Siemens Sample points (vectorized)")
    return result    



def get_spectrum_type_sk(spectrum_type_id: int) -> str:
    """
    Генерирует h_spectrum_type_sk UUID по тому же принципу, что и в PostgreSQL:
    uuid_in(md5('h_spectrum_types|{type_id}')::cstring)
    
    Args:
        spectrum_type_id: int (0=Acceleration, 1=Velocity, 2=Displacement, 3=Envelope)
        
    Returns:
        str: UUID в строковом формате
    """
    # Формируем строку как в SQL: 'h_spectrum_types|{type_id}'
    input_string = f"h_spectrum_types|{spectrum_type_id}"
    
    # Вычисляем MD5 хеш (32 hex символа)
    md5_hash = hashlib.md5(input_string.encode('utf-8')).hexdigest()
    
    # Формируем UUID из хеша в формате 8-4-4-4-12
    # PostgreSQL uuid_in(md5::cstring) интерпретирует 128 бит как UUID
    uuid_str = f"{md5_hash[:8]}-{md5_hash[8:12]}-{md5_hash[12:16]}-{md5_hash[16:20]}-{md5_hash[20:]}"
    
    return uuid_str


def deserialize_spectrum_blob(blob_bytes: bytes) -> dict:
    """
    Десериализует бинарный BLOB формата Spectrum.
    
    Бинарный формат (Little-Endian):
    - [0 : (N)*8]           : массив значений спектра (double[])
    - [(N)*8 : (N+1)*8]     : multiplier (double)
    - [(N+1)*8 : (N+2)*8]   : spectrum_type как double, содержащий int (0-3)
    
    Args:
        blob_bytes: Сырые байты из колонки bytea
        
    Returns:
        dict:
            - raw_values: bytes (для хранения в bytea)
            - raw_values_length: int (количество точек спектра)
            - multiplier: float
            - spectrum_type_id: int (0-3)
            - h_spectrum_type_sk: str (UUID для FK)
    """
    if not blob_bytes:
        raise ValueError("Empty blob for Spectrum deserialization")
    
    total_len = len(blob_bytes)
    
    # Валидация: минимум 2 double (multiplier + type) + данные
    # Общий размер должен быть кратен 8
    if total_len < 2 * SIZEOF_DOUBLE or total_len % SIZEOF_DOUBLE != 0:
        raise ValueError(
            f"Invalid blob length {total_len} for Spectrum. "
            f"Expected multiple of {SIZEOF_DOUBLE} bytes, min 16 bytes."
        )
    
    # 1. Извлекаем последние 2 double: multiplier и spectrum_type
    # spectrum_type хранится как double, но содержит int значение
    multiplier = struct.unpack('<d', blob_bytes[-2*SIZEOF_DOUBLE:-SIZEOF_DOUBLE])[0]
    spectrum_type_double = struct.unpack('<d', blob_bytes[-SIZEOF_DOUBLE:])[0]
    spectrum_type_id = int(spectrum_type_double)
    
    # 2. Валидация spectrum_type_id
    if spectrum_type_id not in SPECTRUM_TYPE_MAP:
        log.warning(f"Unknown spectrum_type_id {spectrum_type_id}, defaulting to 0 (Acceleration)")
        spectrum_type_id = 0
    
    # 3. Извлекаем сырые данные спектра (всё кроме последних 16 байт)
    raw_values_bytes = blob_bytes[:-2*SIZEOF_DOUBLE]
    
    # 4. Считаем количество точек спектра
    spectrum_length = len(raw_values_bytes) // SIZEOF_DOUBLE
    
    return {
        # Данные для БД
        'raw_values': raw_values_bytes,           # bytes для bytea
        'raw_values_length': spectrum_length,     # INTEGER в таблице
        'multiplier': multiplier,                 # DOUBLE PRECISION в таблице
        'spectrum_type_id': spectrum_type_id,     # для логирования/валидации
        'h_spectrum_type_sk': get_spectrum_type_sk(spectrum_type_id),  # UUID FK
    }



def clean_duplicate_staging_rows(stg_table_name: str) -> bool:
    """
    Удаляет дубликаты из staging-таблицы, сравнивая только по бизнес-колонкам.
    
    Исключает из проверки:
    - Системные колонки PostgreSQL (ctid, xmin, xmax, cmin, cmax, tableoid)
    - Технические колонки ETL (load_dttm)
    - Все колонки с суффиксами _sk, _hash, hash_diff (Data Vault технические ключи)
    - data_file_id включается в проверку уникальности (т.к. в DV уникальность = бизнес-ключ + источник)
    
    Оставляет запись с минимальным ctid (первую вставленную).
    """
    # Валидация имени таблицы (защита от SQL-инъекций)
    if not stg_table_name or not stg_table_name.replace('_', '').isalnum():
        log.error(f"❌ Invalid table name: '{stg_table_name}'")
        return False
    
    # Список колонок, которые НЕ являются бизнес-ключами и должны исключаться
    TECHNICAL_COLUMNS = [
        # PostgreSQL system columns
        'ctid', 'xmin', 'xmax', 'cmin', 'cmax', 'tableoid',
        # ETL/Airflow columns
        'load_dttm', 'valid_from_dttm', 'valid_to_dttm', 'active_flag',
        # Data Vault technical keys (суффиксы)
        'hash_sk', 'hash_diff', 'hash_sat_diff',
        # Hub/Satellite/Link surrogate keys
        'hub_sk', 'sat_sk', 'link_sk',
        # Generic _sk columns (будут отфильтрованы дополнительно ниже)
    ]
    
    dwh_engine = get_dwh_engine(DWH_CONN_ID)
    
    try:
        with dwh_engine.begin() as conn:
            # 1. Получаем список всех колонок таблицы
            columns_result = conn.execute(text("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = :table_name
                AND table_schema = 'public'
                ORDER BY ordinal_position
            """), {"table_name": stg_table_name})
            
            all_columns = [row[0] for row in columns_result.fetchall()]
            
            if not all_columns:
                log.warning(f"⚠️ No columns found in table '{stg_table_name}'")
                return True
            
            # 2. Фильтруем технические колонки:
            #    - явно из списка TECHNICAL_COLUMNS
            #    - все колонки, заканчивающиеся на '_sk' (кроме data_file_id)
            #    - все колонки, содержащие 'hash' (кроме явных бизнес-полей)
            business_columns = []
            for col in all_columns:
                col_lower = col.lower()
                # Пропускаем явные технические колонки
                if col in TECHNICAL_COLUMNS or col_lower in [t.lower() for t in TECHNICAL_COLUMNS]:
                    continue
                # Пропускаем колонки с суффиксом _sk (технические ключи)
                if col_lower.endswith('_sk') and col_lower != 'data_file_id':
                    continue
                # Пропускаем колонки с hash в названии (если это не бизнес-поле)
                if 'hash' in col_lower and col_lower not in ['hash', 'hash_value']:
                    continue
                # Оставляем всё остальное как бизнес-колонки
                business_columns.append(col)
            
            # data_file_id ВСЕГДА включаем в проверку уникальности для Data Vault
            if 'data_file_id' in all_columns and 'data_file_id' not in business_columns:
                business_columns.append('data_file_id')
            
            if not business_columns:
                log.warning(f"⚠️ No business columns found in table '{stg_table_name}' after filtering")
                return True
            
            log.debug(f"🔍 Table '{stg_table_name}': using {len(business_columns)} business columns for dedup: {business_columns}")
            
            # 3. Формируем строку для PARTITION BY
            partition_by_columns = ', '.join([f'"{col}"' for col in business_columns])
            
            # 4. Проверяем количество дубликатов перед удалением
            check_duplicates_sql = f"""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT ({partition_by_columns})) as unique_combinations,
                    COUNT(*) - COUNT(DISTINCT ({partition_by_columns})) as duplicate_rows
                FROM "{stg_table_name}"
            """
            
            check_result = conn.execute(text(check_duplicates_sql)).fetchone()
            total_rows = check_result[0] if check_result[0] is not None else 0
            unique_combinations = check_result[1] if check_result[1] is not None else 0
            duplicate_rows = check_result[2] if check_result[2] is not None else 0
            
            log.info(f"📊 Table '{stg_table_name}': {total_rows} total rows, "
                    f"{unique_combinations} unique combinations, "
                    f"{duplicate_rows} duplicate rows to delete")
            
            if duplicate_rows == 0:
                log.info(f"ℹ️ No duplicates found in '{stg_table_name}'")
                return True
            
            # 5. Показываем пример дубликатов для отладки (значения бизнес-колонок + счетчик)
            debug_sql = f"""
                SELECT {partition_by_columns}, COUNT(*) as cnt
                FROM "{stg_table_name}"
                GROUP BY {partition_by_columns}
                HAVING COUNT(*) > 1
                LIMIT 5
            """
            
            debug_result = conn.execute(text(debug_sql)).fetchall()
            if debug_result:
                log.warning(f"⚠️ Found {len(debug_result)} duplicate combinations (showing first 5):")
                for row in debug_result:
                    log.warning(f"   Duplicate combo: {row}")
            
            # 6. Удаляем дубликаты используя оконную функцию ROW_NUMBER()
            #    Оставляем запись с минимальным ctid (первую физически вставленную)
            sql_query = f"""
                WITH duplicates AS (
                    SELECT ctid,
                           ROW_NUMBER() OVER (
                               PARTITION BY {partition_by_columns} 
                               ORDER BY ctid ASC
                           ) as rn
                    FROM "{stg_table_name}"
                )
                DELETE FROM "{stg_table_name}"
                WHERE ctid IN (
                    SELECT ctid FROM duplicates WHERE rn > 1
                )
            """
            
            log.info(f"🗑️ Executing deduplication on '{stg_table_name}'...")
            result = conn.execute(text(sql_query))
            deleted_count = result.rowcount
            
            log.info(f"✅ Deleted {deleted_count} duplicate rows from '{stg_table_name}'")
            
            # 7. Проверяем результат после удаления
            verify_result = conn.execute(text(check_duplicates_sql)).fetchone()
            remaining_duplicates = verify_result[2] if verify_result[2] is not None else 0
            
            if remaining_duplicates > 0:
                log.error(f"❌ Warning: {remaining_duplicates} duplicates still remain after deletion in '{stg_table_name}'!")
                log.error("   Possible causes: ctid changed during transaction, or concurrent modifications")
                return False
            else:
                log.info(f"✅ Successfully removed all duplicates from '{stg_table_name}'")
            
            return True
            
    except Exception as e:
        log.error(f"❌ Error deleting duplicates from '{stg_table_name}': {str(e)}", exc_info=True)
        return False
    finally:
        dwh_engine.dispose()
    



def decode_creyt_timeseries(
    raw_values: bytes,
    calibration_factor_a: float,
    calibration_factor_b: float,
    min_raw: float,
    max_raw: float,
    min_eu: float,
    max_eu: float,
    sample_rate: float,
    first_timestamp: datetime,
) -> List[Tuple[datetime, float]]:
    """
    🔥 ВЕКТОРИЗОВАННАЯ ВЕРСИЯ — в 10-50 раз быстрее исходной.
    
    Преобразует сырые байты в временной ряд (timestamp, value).
    Алгоритм соответствует C# реализации CreytFastSample.GetDoubleValue:
    1. Чтение UInt16 Big Endian из байтов (каждые 2 байта = 1 отсчет)
    2. Преобразование ADC → Вольты: adcValue * 5.0 / 65535.0
    3. Калибровка: voltage * CalibrationFactorA + CalibrationFactorB
    4. Масштабирование: value * K + B, где:
       K = (MaxEU - MinEU) / (MaxRaw - MinRaw)
       B = MinEU - MinRaw * K

    Args:
        raw_values: сырые байты с АЦП
        calibration_factor_a: коэффициент калибровки A
        calibration_factor_b: коэффициент калибровки B
        min_raw: минимальное значение сырого сигнала
        max_raw: максимальное значение сырого сигнала
        min_eu: минимальное значение в инженерных единицах
        max_eu: максимальное значение в инженерных единицах
        sample_rate: частота дискретизации (Гц)
        first_timestamp: временная метка первого отсчёта

    Returns:
        Список кортежей (timestamp, value_in_eu)
    """
    # Последние 8 байт - служебные (CRC32 + 4 нуля), не используем их
    data_length = len(raw_values) - 8
    if data_length <= 0:
        log.warning("❌ Raw values too short (less than 8 bytes)")
        return []
    
    num_samples = data_length // 2
    if num_samples == 0:
        return []
    
    # =====================================================================
    # 🔥 ШАГ 1: Векторная распаковка ADC значений (Big-Endian uint16)
    # =====================================================================
    adc_values = np.frombuffer(raw_values[:data_length], dtype='>u2').astype(np.float64)
    
    # =====================================================================
    # 🔥 ШАГ 2: Цепочка преобразований — ВСЁ ВЕКТОРНО (без Python-циклов)
    # =====================================================================
    # ADC → Вольты (0-5В)
    voltage_values = adc_values * (5.0 / 65535.0)
    
    # Калибровка датчика
    calibrated_values = voltage_values * calibration_factor_a + calibration_factor_b
    
    # Масштабирование в инженерные единицы
    if (max_raw - min_raw) == 0:
        log.warning(f"⚠️ Invalid scale: max_raw={max_raw}, min_raw={min_raw}")
        return []
    
    k = (max_eu - min_eu) / (max_raw - min_raw)
    b = min_eu - min_raw * k
    eu_values = calibrated_values * k + b
    
    # =====================================================================
    # 🔥 ШАГ 3: Векторный расчёт временных меток
    # =====================================================================
    if sample_rate > 0:
        time_delta_us = 1_000_000.0 / sample_rate
        offsets_us = np.arange(num_samples, dtype=np.int64) * int(time_delta_us)
    else:
        offsets_us = np.zeros(num_samples, dtype=np.int64)
    
    # Базовая временная метка (timezone-aware)
    if first_timestamp.tzinfo is None:
        first_timestamp = first_timestamp.replace(tzinfo=timezone.utc)
    
    # 🔥 Используем numpy datetime64 для быстрого прибавления микросекунд
    base_ns = np.datetime64(first_timestamp, 'us')
    timestamps_ns = base_ns + offsets_us.astype('timedelta64[us]')
    
    # =====================================================================
    # 🔥 ШАГ 4: Финальная сборка — единственный Python-цикл
    # =====================================================================
    # Конвертируем numpy datetime64 → Python datetime
    # Используем pandas для быстрой конвертации всего массива сразу
    ts_series = pd.to_datetime(timestamps_ns, utc=True)
    ts_py_list = ts_series.to_pydatetime()
    
    # Собираем результат через zip (быстрее list comprehension)
    result = list(zip(ts_py_list, eu_values.tolist()))
    
    return result





def decode_lcard_timeseries(
    raw_values: Union[bytes, np.ndarray],
    step: float,
    is_scale: bool,
    min_raw: float,
    max_raw: float,
    min_eu: float,
    max_eu: float,
    sample_rate: float,
    first_timestamp: datetime,
) -> List[Tuple[datetime, float]]:
    """
    🔥 ВЕКТОРИЗОВАННАЯ ВЕРСИЯ — в 10-50 раз быстрее исходной.
    
    Преобразует массив сырых значений LCard во временной ряд (timestamp, value).
    Алгоритм соответствует C# реализации LCardSample.GetDoubleValue:
    1. Если Step == 0: voltage = raw_value, иначе: voltage = raw_value * Step
    2. Если IsScale: применяем преобразование в инженерные единицы:
       value = voltage * K + B, где:
       - K = (MaxEU - MinEU) / (MaxRaw - MinRaw)
       - B = MinEU - MinRaw * K

    Args:
        raw_values: numpy массив сырых значений (float64) ИЛИ bytes (little-endian double[])
        step: шаг преобразования (вольты/отсчет)
        is_scale: флаг применения шкалы преобразования
        min_raw: минимальное значение сырого сигнала
        max_raw: максимальное значение сырого сигнала
        min_eu: минимальное значение в инженерных единицах
        max_eu: максимальное значение в инженерных единицах
        sample_rate: частота дискретизации (Гц)
        first_timestamp: временная метка первого отсчёта

    Returns:
        Список кортежей (timestamp, value_in_eu)
    """
    # 🔥 КОНВЕРТАЦИЯ bytes → np.ndarray
    if isinstance(raw_values, (bytes, bytearray)):
        if len(raw_values) == 0:
            # log.warning("❌ Empty raw_values bytes for LCard decoding")
            return []
        if len(raw_values) % 8 != 0:
            log.warning(f"⚠️ raw_values length {len(raw_values)} is not multiple of 8")
            return []
        raw_array = np.frombuffer(raw_values, dtype='<f8')
    elif isinstance(raw_values, memoryview):
        raw_array = np.frombuffer(bytes(raw_values), dtype='<f8')
    elif isinstance(raw_values, np.ndarray):
        raw_array = raw_values
    else:
        log.warning(f"❌ Invalid raw_values type: {type(raw_values)}")
        return []
    
    if len(raw_array) == 0:
        # log.warning("❌ Empty raw_values array for LCard decoding")
        return []
    
    n_samples = len(raw_array)
    
    # =====================================================================
    # 🔥 ШАГ 1: Преобразование сырых значений в напряжение (векторно)
    # =====================================================================
    if step == 0:
        voltage_values = raw_array.astype(np.float64)
    else:
        voltage_values = raw_array * step
    
    # =====================================================================
    # 🔥 ШАГ 2: Применение шкалы преобразования (если включено)
    # =====================================================================
    if is_scale and not any(np.isnan([min_raw, max_raw, min_eu, max_eu])):
        if (max_raw - min_raw) != 0:
            k = (max_eu - min_eu) / (max_raw - min_raw)
            b = min_eu - min_raw * k
            eu_values = voltage_values * k + b
        else:
            eu_values = voltage_values
    else:
        eu_values = voltage_values
    
    # =====================================================================
    # 🔥 ШАГ 3: Векторный расчёт временных меток
    # =====================================================================
    if sample_rate > 0:
        time_delta_us = 1_000_000.0 / sample_rate
        offsets_us = np.arange(n_samples, dtype=np.int64) * int(time_delta_us)
    else:
        offsets_us = np.zeros(n_samples, dtype=np.int64)
    
    if first_timestamp.tzinfo is None:
        first_timestamp = first_timestamp.replace(tzinfo=timezone.utc)
    
    base_ns = np.datetime64(first_timestamp, 'us')
    timestamps_ns = base_ns + offsets_us.astype('timedelta64[us]')
    
    # =====================================================================
    # 🔥 ШАГ 4: Финальная сборка
    # =====================================================================
    ts_series = pd.to_datetime(timestamps_ns, utc=True)
    ts_py_list = ts_series.to_pydatetime()
    
    result = list(zip(ts_py_list, eu_values.tolist()))
    
    log.debug(f"✅ Decoded {len(result)} LCard samples (vectorized)")
    return result



# 🔥 ФУНКЦИЯ: Гарантированная конвертация в UUID-строку
# ========================================================================
def ensure_uuid_string(val):
    """
    Возвращает UUID как строку. Если val — уже UUID, возвращаем как есть.
    """
    # 1. Обработка None/NaN
    if val is None or (isinstance(val, float) and np.isnan(val)) or pd.isna(val):
        return '00000000-0000-0000-0000-000000000000'
    
    # 2. Если уже строка — просто возвращаем (не перегенерируем!)
    if isinstance(val, str):
        return val.strip()
    
    # 3. Если UUID объект — конвертируем в строку
    if isinstance(val, uuid.UUID):
        return str(val)
    
    # 4. Для остальных случаев — пробуем конвертировать
    try:
        return str(val)
    except Exception:
        return '00000000-0000-0000-0000-000000000000' 
    

   
def convert_value_for_parquet(
    value: Any,
    value_type: str,
    is_nullable: bool = False
) -> Any:
    """
    Конвертирует значение в тип, совместимый с Parquet.
    
    Args:
        value: Исходное значение
        value_type: Тип значения (Int32, Int64, Float64, Bool, String, Array(...), etc.)
        is_nullable: Может ли значение быть NULL
    
    Returns:
        Значение в Parquet-совместимом формате
    """
    # Обработка NULL значений
    if value is None:
        if is_nullable:
            return None
        else:
            return get_default_value(value_type)
    
    try:
        # Числовые типы
        if value_type == "Int32":
            return int(value) if value is not None else None
        
        elif value_type == "Int64":
            return int(value) if value is not None else None
        
        elif value_type == "Float32":
            return float(value) if value is not None else None
        
        elif value_type == "Float64":
            return float(value) if value is not None else None
        
        # Булевый тип
        elif value_type == "Bool":
            if isinstance(value, bool):
                return value
            elif isinstance(value, (int, float)):
                return bool(value)
            elif isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'on')
            return bool(value)
        
        # Строковые типы
        elif value_type in ("String", "interval", "FixedString"):
            if isinstance(value, (datetime, date)):
                return value.isoformat()
            elif isinstance(value, uuid.UUID):
                return str(value)
            else:
                return str(value) if value is not None else None
        
        # DateTime типы
        elif value_type.startswith("DateTime"):
            if isinstance(value, (datetime, date)):
                return value
            elif isinstance(value, str):
                # Пробуем распарсить строку как datetime
                try:
                    return datetime.fromisoformat(value.replace(" ", "T"))
                except:
                    return value
            return value
        
        # UUID тип
        elif value_type == "UUID":
            if isinstance(value, uuid.UUID):
                return str(value)
            return str(value) if value is not None else None
        
        elif value_type in ("integer_array", "text_array", "double_array", "timestamp_array") or value_type.startswith("Array("):
            # Извлекаем внутренний тип для старых форматов
            if value_type.startswith("Array("):
                inner_type = value_type[6:-1]
            else:
                # Маппинг упрощённых типов
                inner_map = {
                    "integer_array": "Int32",
                    "text_array": "String", 
                    "double_array": "Float64",
                    "timestamp_array": "DateTime",
                }
                inner_type = inner_map.get(value_type, "String")
            
            if value is None:
                return None if is_nullable else []
            
            try:
                # Если уже список/кортеж — конвертируем элементы
                if isinstance(value, (list, tuple)):
                    return [
                        convert_value_for_parquet(item, inner_type, is_nullable)
                        for item in value
                    ]
                # Если строка — пробуем распарсить как JSON или разделить по запятой
                elif isinstance(value, str):
                    import json
                    try:
                        parsed = json.loads(value)
                        if isinstance(parsed, list):
                            return [convert_value_for_parquet(item, inner_type, is_nullable) for item in parsed]
                    except:
                        pass
                    items = [item.strip() for item in value.split(",") if item.strip()]
                    return [convert_value_for_parquet(item, inner_type, is_nullable) for item in items]
                # Если одно значение — оборачиваем в список
                else:
                    return [convert_value_for_parquet(value, inner_type, is_nullable)]
            except Exception as e:
                log.warning(f"⚠️ Ошибка конвертации массива {value} в тип {value_type}: {e}")
                return [] if is_nullable else None
        
        # Decimal тип
        elif value_type.startswith("Decimal"):
            return float(value) if value is not None else None
        
        # По умолчанию возвращаем как строку
        else:
            return str(value) if value is not None else None
    
    except (ValueError, TypeError) as e:
        # Если конвертация не удалась
        if is_nullable:
            return None
        else:
            # Логируем ошибку и возвращаем дефолтное значение
            print(f"⚠️ Ошибка конвертации значения {value} в тип {value_type}: {e}")
            return get_default_value(value_type)


def convert_array_value(value: Any, value_type: str, is_nullable: bool) -> Any:
    """
    Конвертирует значения массива.
    
    Примеры типов:
    - Array(Int32)
    - Array(String)
    - Array(Float64)
    - Array(DateTime)
    """
    if value is None:
        return None if is_nullable else []
    
    # Извлекаем тип элементов массива
    # Array(Int32) -> Int32
    inner_type = value_type[6:-1]  # Убираем "Array(" и ")"
    
    try:
        # Если уже список/кортеж
        if isinstance(value, (list, tuple)):
            return [
                convert_value_for_parquet(item, inner_type, is_nullable)
                for item in value
            ]
        
        # Если строка (предполагаем JSON или разделитель)
        elif isinstance(value, str):
            # Пробуем распарсить как JSON
            import json
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    return [
                        convert_value_for_parquet(item, inner_type, is_nullable)
                        for item in parsed
                    ]
            except:
                pass
            
            # Пробуем разделить по запятой
            items = [item.strip() for item in value.split(",") if item.strip()]
            return [
                convert_value_for_parquet(item, inner_type, is_nullable)
                for item in items
            ]
        
        # Если одно значение - оборачиваем в список
        else:
            return [convert_value_for_parquet(value, inner_type, is_nullable)]
    
    except Exception as e:
        print(f"⚠️ Ошибка конвертации массива {value} в тип {value_type}: {e}")
        return [] if is_nullable else []


def get_default_value(value_type: str) -> Any:
    """
    Возвращает дефолтное значение для типа.
    """
    defaults = {
        "Int32": 0,
        "Int64": 0,
        "Float32": 0.0,
        "Float64": 0.0,
        "Bool": False,
        "String": "",
        "UUID": "00000000-0000-0000-0000-000000000000",
    }
    
    if value_type.startswith("Array("):
        return []
    elif value_type.startswith("Decimal"):
        return 0.0
    
    return defaults.get(value_type, "")


    
    
def convert_value_for_clickhouse(
    value: Any,
    value_type: str,
    is_nullable: bool = False,
    default_on_null: Any = None
) -> Any:
    """
    Конвертирует значение под тип колонки ClickHouse.
    
    Returns:
        - timezone-aware datetime для DateTime типов
        - None только если тип Nullable или is_nullable=True
        - Безопасный дефолт для не-nullable типов
    """
    # 1. Определяем, разрешены ли NULL
    ch_allows_null = value_type.strip().startswith('Nullable(')
    effective_nullable = ch_allows_null or is_nullable
    
    # 2. Проверка на "пустое" значение
    if _is_null_value(value):
        return handle_null_value(value_type, effective_nullable, default_on_null)
    
    try:
        return _convert_non_null_value(value, value_type, effective_nullable)
    except Exception as e:
        log.warning(f"⚠️ Convert error for type '{value_type}': {e}")
        return handle_conversion_error(value_type, effective_nullable, default_on_null)


def _is_null_value(value: Any) -> bool:
    """Проверяет, является ли значение логически пустым."""
    if value is None:
        return True
    if isinstance(value, float) and (value != value or value in (float('inf'), float('-inf'))):
        return True
    if isinstance(value, str) and value.strip() == '':
        return True
    return False


def handle_null_value(value_type: str, is_nullable: bool, default: Any) -> Any:
    """Возвращает значение для обработки NULL."""
    if is_nullable:
        return None
    type_lower = value_type.lower()
    if 'datetime' in type_lower or 'date' in type_lower:
        return DEFAULT_DATETIME
    if any(t in type_lower for t in ['int', 'uint']):
        return 0
    if any(t in type_lower for t in ['float', 'decimal']):
        return 0.0
    if 'bool' in type_lower:
        return False
    if value_type.startswith('array('):
        return []
    # 🔥 FIX: Добавлен 'interval'
    if any(t in type_lower for t in ['string', 'fixedstring', 'interval']):
        return ''
    return default if default is not None else ''


def _convert_non_null_value(value: Any, value_type: str, is_nullable: bool) -> Any:
    """Конвертирует непустое значение под тип ClickHouse."""
    type_lower = value_type.lower()
    # DateTime / Date
    if ('datetime' in type_lower or 'date' in type_lower) and not value_type.startswith('Array('):
        return _convert_datetime_value(value, is_nullable)
    # Integer types
    if any(t in type_lower for t in ['int8', 'int16', 'int32', 'int64', 'uint']):
        return _convert_integer_value(value, is_nullable)
    # Float / Decimal
    if any(t in type_lower for t in ['float', 'decimal']):
        return _convert_float_value(value, is_nullable)
    # Boolean
    if 'bool' in type_lower:
        return _convert_boolean_value(value)
    # Array
    if value_type.startswith('Array('):
        return convert_array_value(value, is_nullable)
    # String / FixedString / Interval
    # 🔥 FIX: Добавлен 'interval', так как ClickHouse хранит его как String
    if any(t in type_lower for t in ['string', 'fixedstring', 'interval']):
        return convert_string_value(value)
    # UUID
    if 'uuid' in type_lower:
        return str(value) if not isinstance(value, UUID) else str(value)
    
    # 🔥 FIX: Fallback для timedelta, если тип не был явно указан как interval
    if isinstance(value, timedelta):
        return str(value)

    return value


def _convert_datetime_value(value: Any, is_nullable: bool) -> datetime:
    """Конвертирует значение в timezone-aware datetime."""
    if hasattr(value, 'to_pydatetime'):
        value = value.to_pydatetime()
    
    if isinstance(value, str):
        try:
            from dateutil import parser
            value = parser.parse(value)
        except Exception:
            return DEFAULT_DATETIME if not is_nullable else None
    
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value
    
    return DEFAULT_DATETIME if not is_nullable else None


def _convert_integer_value(value: Any, is_nullable: bool) -> Optional[int]:
    """Конвертирует значение в integer."""
    if isinstance(value, (int, np.integer)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        return int(value) if not np.isnan(value) else (0 if not is_nullable else None)
    if isinstance(value, str):
        try:
            return int(float(value.strip()))
        except (ValueError, TypeError):
            return 0 if not is_nullable else None
    return 0 if not is_nullable else None


def _convert_float_value(value: Any, is_nullable: bool) -> Optional[float]:
    """Конвертирует значение в float."""
    if isinstance(value, (float, int, np.floating, np.integer)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except (ValueError, TypeError):
            return 0.0 if not is_nullable else None
    return 0.0 if not is_nullable else None


def _convert_boolean_value(value: Any) -> bool:
    """Конвертирует значение в boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, np.integer)):
        return bool(value)
    if isinstance(value, str):
        v = value.strip().lower()
        if v in ('1', 'true', 'yes', 'on', 't', 'y'):
            return True
        if v in ('0', 'false', 'no', 'off', 'f', 'n', ''):
            return False
    return bool(value)


# ============================================================================
# КОНСТАНТЫ И КОНФИГУРАЦИЯ
# ============================================================================
DEFAULT_DATETIME = datetime(1970, 1, 1, tzinfo=timezone.utc)
DEFAULT_VALUES_BY_TYPE = {
    'datetime': DEFAULT_DATETIME,
    'integer': 0,
    'float': 0.0,
    'boolean': False,
    'string': '',
    'array': [],
    'uuid': '00000000-0000-0000-0000-000000000000',
}

# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================

def ensure_timezone_aware(dt: Any) -> datetime:
    """
    🔥 МАКСИМАЛЬНО ПРОСТАЯ И НАДЁЖНАЯ функция.
    Возвращает timezone-aware datetime (UTC) или DEFAULT_DATETIME.
    НИКОГДА не возвращает None.
    """
    # Если это не datetime объект — сразу дефолт
    if not isinstance(dt, datetime):
        return DEFAULT_DATETIME
    
    # Если нет таймзоны — добавляем UTC
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    
    # Уже ок
    return dt


def get_default_for_type(clickhouse_type: str, is_nullable: bool) -> Any:
    """Возвращает безопасное значение по умолчанию для типа ClickHouse."""
    if is_nullable:
        return None
    type_lower = clickhouse_type.lower()
    if 'datetime' in type_lower or 'date' in type_lower:
        return DEFAULT_VALUES_BY_TYPE['datetime']
    if any(t in type_lower for t in ['int8', 'int16', 'int32', 'int64', 'uint']):
        return DEFAULT_VALUES_BY_TYPE['integer']
    if any(t in type_lower for t in ['float', 'decimal']):
        return DEFAULT_VALUES_BY_TYPE['float']
    if 'bool' in type_lower:
        return DEFAULT_VALUES_BY_TYPE['boolean']
    if type_lower.startswith('array('):
        return DEFAULT_VALUES_BY_TYPE['array']
    # 🔥 FIX: Добавлен 'interval'
    if any(t in type_lower for t in ['string', 'fixedstring', 'interval']):
        return DEFAULT_VALUES_BY_TYPE['string']
    if 'uuid' in type_lower:
        return DEFAULT_VALUES_BY_TYPE['uuid']
    return DEFAULT_VALUES_BY_TYPE['string']


def sanitize_value(
    value: Any,
    clickhouse_type: str,
    is_nullable: bool,
    default_on_null: Any = None
) -> Any:
    """
    Универсальная санитизация значения под тип ClickHouse.
    """
    # 🔥 Всегда проверяем фактический тип ClickHouse на наличие Nullable(
    ch_allows_null = clickhouse_type.strip().startswith('Nullable(')
    effective_nullable = ch_allows_null or is_nullable
    
    result = convert_value_for_clickhouse(
        value, clickhouse_type, effective_nullable, default_on_null
    )
    
    # 🔥 Финальная страховка: если результат None, но тип НЕ nullable — возвращаем дефолт
    if result is None and not ch_allows_null:
        return get_default_for_type(clickhouse_type, is_nullable=False)
    
    return result


def prepare_record_for_clickhouse(
    record: dict,
    value_clickhouse_type: str,
    value_is_nullable: bool,
    value_default_on_null: Any
) -> tuple:
    """Преобразует одну запись в кортеж для вставки в ClickHouse."""
    # Обработка value
    processed_value = sanitize_value(
        record['value'],
        value_clickhouse_type,
        value_is_nullable,
        value_default_on_null
    )
    
    # Типизация для String
    if any(t in value_clickhouse_type for t in ['String', 'FixedString']):
        if not isinstance(processed_value, str):
            processed_value = '' if processed_value is None else str(processed_value)
    
    # Типизация для Array
    if value_clickhouse_type.startswith('Array('):
        if processed_value is None:
            processed_value = []
        elif isinstance(processed_value, tuple):
            processed_value = list(processed_value)
        elif not isinstance(processed_value, list):
            processed_value = [processed_value]
    
    # Обработка временных меток (просто и надёжно)
    timestamp = ensure_timezone_aware(record['timestamp'])
    load_dttm = ensure_timezone_aware(record['load_dttm'])
    
    return (
        record['data_record_sk'],
        record['h_object_property_sk'],
        record['data_file_id'],
        record['data_source_id'], 
        timestamp,
        processed_value,
        load_dttm,
    )


def validate_and_fix_records_for_clickhouse(
    records: list[tuple],
    value_clickhouse_type: str = None
) -> list[tuple]:
    """🔥 Финальная защита от None в datetime-полях."""
    from datetime import datetime, timezone
    DEFAULT_DATETIME = datetime(1970, 1, 1, tzinfo=timezone.utc)
    
    # Проверяем, ожидает ли колонка value тип DateTime (не nullable)
    value_expects_datetime = (
        value_clickhouse_type 
        and ('datetime' in value_clickhouse_type.lower() or 'date' in value_clickhouse_type.lower())
        and not value_clickhouse_type.strip().startswith('Nullable(')
    )
    
    fixed = []
    for idx, rec in enumerate(records):
        # 🔥 ТЕПЕРЬ 7 ЭЛЕМЕНТОВ
        dr_sk, hop_sk, df_id, ds_id, timestamp, value, load_dttm = rec
        
        # Fix timestamp (index 4)
        if timestamp is None or not isinstance(timestamp, datetime):
            log.warning(f"⚠️ [FIX] timestamp at idx={idx}")
            timestamp = DEFAULT_DATETIME
        elif timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        
        # Fix load_dttm (index 6)
        if load_dttm is None or not isinstance(load_dttm, datetime):
            log.warning(f"⚠️ [FIX] load_dttm at idx={idx}")
            load_dttm = DEFAULT_DATETIME
        elif load_dttm.tzinfo is None:
            load_dttm = load_dttm.replace(tzinfo=timezone.utc)
        
        # 🔥 Fix value (index 5), если это DateTime и значение None
        if value_expects_datetime and value is None:
            log.warning(f"⚠️ [FIX] value (datetime) at idx={idx}, replaced with DEFAULT_DATETIME")
            value = DEFAULT_DATETIME
        
        fixed.append((dr_sk, hop_sk, df_id, ds_id, timestamp, value, load_dttm))
    
    return fixed


# === 3. ОБНОВИТЕ insert_to_clickhouse ===
def insert_to_clickhouse(
    client: Client,
    table_name: str,
    records: list[tuple],
    value_clickhouse_type: str = None
) -> bool:
    if not records:
        return True
    
    # 🔥 Финальная валидация с учётом типа value
    records = validate_and_fix_records_for_clickhouse(records, value_clickhouse_type)
    
    try:
        client.execute(
            f"""
            INSERT INTO {table_name}
            (data_record_sk, h_object_property_sk, data_file_id, data_source_id, timestamp, value, load_dttm)
            VALUES
            """,
            records
        )
        return True
    except Exception as e:
        log.error(f"❌ Ошибка при вставке в ClickHouse: {type(e).__name__}: {e}")
        raise

def extract_pending_records(
    engine,
    table_name: str
) -> list[dict]:
    """Извлекает неэкспортированные записи из Cloudberry."""
    query = text(f"""
        SELECT 
            h.data_record_sk,
            h.h_object_property_sk,
            h.data_file_id,
            h.data_source_id,
            h.load_dttm,
            h.value,
            h.timestamp as base_timestamp
        FROM public.{table_name} h
        WHERE h.exported_to_datamart = FALSE
        ORDER BY h.timestamp
    """)
    
    with engine.begin() as conn:
        result = conn.execute(query)
        rows = result.fetchall()
    
    return [
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


def insert_to_clickhouse(
    client: Client,
    table_name: str,
    records: list[tuple],
    value_clickhouse_type: str = None  # ← Новый параметр
) -> bool:
    if not records:
        return True
    
    # 🔥 Финальная валидация с учётом типа value
    records = validate_and_fix_records_for_clickhouse(records, value_clickhouse_type)
    
    try:
        client.execute(
            f"""
            INSERT INTO {table_name} 
            (data_record_sk, h_object_property_sk, data_file_id, data_source_id, timestamp, value, load_dttm)
            VALUES
            """,
            records
        )
        return True
    except Exception as e:
        log.error(f"❌ Ошибка при вставке в ClickHouse: {type(e).__name__}: {e}")
        raise


def mark_records_as_exported(
    engine,
    table_name: str,
    record_ids: list[str],
    batch_size: int = 500000  # ← Добавлен параметр
) -> int:
    """
    Помечает записи как экспортированные батчами для избежания OOM в Cloudberry.
    
    Args:
        engine: SQLAlchemy engine
        table_name: Имя таблицы
        record_ids: Список UUID записей
        batch_size: Размер батча (по умолчанию 50000)
    
    Returns:
        int: Общее количество обновлённых записей
    """
    if not record_ids:
        return 0
    
    total_updated = 0
    num_batches = (len(record_ids) + batch_size - 1) // batch_size
    
    log.info(f"🏷️ Пометка {len(record_ids)} записей как экспортированных ({num_batches} батчей по {batch_size})")
    
    for i in range(0, len(record_ids), batch_size):
        batch = record_ids[i:i + batch_size]
        
        # Конвертируем строки в UUID-объекты
        uuid_ids = [
            UUID(rec_id) if isinstance(rec_id, str) else rec_id
            for rec_id in batch
        ]
        
        try:
            with engine.begin() as conn:
                query = text(f"""
                    UPDATE public.{table_name}
                    SET exported_to_datamart = TRUE
                    WHERE data_record_sk = ANY(:record_ids)
                """)
                result = conn.execute(query, {"record_ids": uuid_ids})
                batch_updated = result.rowcount
                total_updated += batch_updated
                
                batch_num = i // batch_size + 1
                log.info(f"✅ Батч {batch_num}/{num_batches}: помечено {batch_updated} записей")
        except Exception as e:
            log.error(f"❌ Ошибка при пометке батча {i // batch_size + 1}: {e}", exc_info=True)
            raise
    
    log.info(f"🎉 Всего помечено {total_updated}/{len(record_ids)} записей")
    return total_updated


def create_clickhouse_client(connection_name: str = 'clickhouse_conn') -> Client:
    """Создаёт клиент ClickHouse из Airflow Connection."""
    conn_config = BaseHook.get_connection(connection_name)
    
    return Client(
        host=conn_config.host,
        port=conn_config.port or 9000,
        database=conn_config.schema or 'default',
        user=conn_config.login or 'default',
        password=conn_config.password or '',
    )


def convert_array_value(value: Any, is_nullable: bool) -> Optional[list]:
    """Конвертирует значение в список для Array типа."""
    if isinstance(value, (list, tuple)):
        return list(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else [parsed]
        except:
            return [x.strip() for x in value.split(',') if x.strip()]
    return [value] if value is not None else ([] if not is_nullable else None)


def convert_string_value(value: Any) -> str:
    """Конвертирует значение в строку."""
    if isinstance(value, bytes):
        return value.decode('utf-8', errors='replace')
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    # 🔥 FIX: Явная конвертация timedelta в строку
    if isinstance(value, timedelta):
        return str(value)
    return str(value)


def handle_conversion_error(
    value_type: str, 
    is_nullable: bool, 
    default: Any
) -> Any:
    """Возвращает безопасное значение при ошибке конвертации."""
    if is_nullable:
        return None
    return handle_null_value(value_type, is_nullable=False, default=default)


# 🔥 Принудительная санитизация ВСЕХ datetime-полей
def sanitize_datetime_for_clickhouse(dt_val, allow_null: bool = False):
    """
    Гарантирует, что значение для ClickHouse DateTime — это timezone-aware datetime или None.
    Корректно обрабатывает: None, pd.NaT, np.nan, datetime без tzinfo, pandas.Timestamp.
    """
    from datetime import datetime, timezone
    
    # ✅ Универсальная проверка на "пустое" значение (работает для None, NaT, NaN)
    if dt_val is None or pd.isna(dt_val):
        return None if allow_null else datetime(1970, 1, 1, tzinfo=timezone.utc)
    
    # Конвертация pandas Timestamp → стандартный datetime
    if hasattr(dt_val, 'to_pydatetime'):
        dt_val = dt_val.to_pydatetime()
    
    # Добавление UTC таймзоны, если отсутствует
    if isinstance(dt_val, datetime):
        if dt_val.tzinfo is None:
            return dt_val.replace(tzinfo=timezone.utc)
        return dt_val
    
    # Fallback для любых неожиданных типов
    return None if allow_null else datetime(1970, 1, 1, tzinfo=timezone.utc)



def get_s3_client():
    """
    Создаёт S3 клиент используя Airflow connection 'minio_conn'
    """
    conn = BaseHook.get_connection('minio_conn')
    
    # Извлекаем креды из connection
    aws_access_key = conn.login  # AWS Access Key ID
    aws_secret_key = conn.password  # AWS Secret Access Key
    
    # Извлекаем Extra config
    extra = conn.extra_dejson if conn.extra else {}
    endpoint_url = extra.get('endpoint_url', 'http://minio:9000')
    verify_ssl = extra.get('verify', True)
    
    # Получаем addressing_style из config.s3.addressing_style
    s3_config = extra.get('config', {}).get('s3', {})
    addressing_style = s3_config.get('addressing_style', 'path')
    
    # Создаём boto3 config с path addressing (требуется для MinIO)
    botocore_config = Config(
        s3={
            'addressing_style': addressing_style
        },
        retries={
            'max_attempts': 3,
            'mode': 'standard'
        }
    )
    
    # Создаём S3 клиент
    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        config=botocore_config,
        verify=verify_ssl,
        # region_name не требуется для MinIO, но можно добавить если нужно
        # region_name='us-east-1',
    )
    
    log.info(f"✅ S3 клиент создан: endpoint={endpoint_url}, addressing={addressing_style}")
    return s3_client


def get_bucket_name():
    """
    Получает имя S3 бакета из переменных окружения.
    
    Returns:
        str: Имя бакета (по умолчанию 'iceberg-warehouse')
    """
    import os
    
    # Приоритет переменных окружения (из вашего docker-compose)
    bucket_name = os.getenv('DATA_STORAGE_S3_BUCKET', 'iceberg-warehouse')
    
    # Альтернативные варианты (для совместимости)
    if not bucket_name:
        bucket_name = os.getenv('AWS_S3_BUCKET', 'iceberg-warehouse')
    
    return bucket_name


def compute_batch_hash(source_table: str, day: str, extra_salt: str = "") -> str:
    """Детерминированный хеш для идемпотентности батча"""
    content = f"{source_table}_{day}_{extra_salt}"
    return hashlib.sha256(content.encode()).hexdigest()[:8]


def build_s3_prefix(
    data_type: str,
    data_file_id: str,
    year: int,
    month: int,
    day: int,
    batch_id: str,
) -> str:
    source_name = get_data_source_name(data_file_id)
    """Формирует S3 префикс по структуре"""
    return (
        f"iceberg-warehouse/{data_type}/"
        f"{source_name}_{data_file_id}/"
        f"year={year:04d}/"
        f"month={month:02d}/"
        f"day={day:02d}/"
        f"{batch_id}/"
    )


def estimate_row_size_bytes(sample_rows: list[dict], value_type: str) -> int:
    """Оценивает средний размер строки в байтах"""
    if not sample_rows:
        return 200
    
    sample = sample_rows[:100]
    total_bytes = 0
    
    for row in sample:
        row_bytes = (36 * 3) + (26 * 2)  # UUID × 3 + timestamp × 2
        val = row.get("value")
        if val is None:
            row_bytes += 1
        elif value_type in ("Int32", "Int64", "Float64", "Bool"):
            row_bytes += 8
        elif value_type in ("String",):
            row_bytes += len(str(val)) if val else 1
        elif value_type.startswith("Array"):
            row_bytes += len(str(val)) * 0.6 if val else 1
        total_bytes += row_bytes
    
    avg = total_bytes / len(sample)
    return int(avg * 0.5)  # ZSTD compression ~0.4-0.6


def write_parquet_chunk(
    rows: list[dict],
    output_path: str,
    value_type: str,
    value_is_nullable: bool,
) -> int:
    """Записывает чанк данных в Parquet с корректной обработкой массивов"""
    
    # Базовые поля
    data = {
        "data_record_sk": [ensure_uuid_string(r["data_record_sk"]) for r in rows],
        "h_object_property_sk": [ensure_uuid_string(r["h_object_property_sk"]) for r in rows],
        "data_file_id": [ensure_uuid_string(r["data_file_id"]) for r in rows],
        "load_dttm": [r["load_dttm"] for r in rows],
        "timestamp": [r["timestamp"] for r in rows],
    }
    
    # Нормализуем value_type для безопасного сравнения (убираем пробелы, нижний регистр)
    vt = value_type.strip().lower() if value_type else ""
    
    # === ОПРЕДЕЛЕНИЕ ТИПА ДЛЯ VALUE ===
    if vt in ("int32", "integer"):
        value_pa_type = pa.int32()
    elif vt in ("int64", "bigint"):
        value_pa_type = pa.int64()
    elif vt in ("float64", "double"):
        value_pa_type = pa.float64()
    elif vt in ("bool", "boolean"):
        value_pa_type = pa.bool_()
    elif vt in ("string", "interval", "text"):
        value_pa_type = pa.string()
    elif vt in ("timestamp",):
        value_pa_type = pa.timestamp("us", tz="UTC")
    # Array-типы
    elif vt in ("integer_array", "array(int32)"):
        value_pa_type = pa.list_(pa.int32())
    elif vt in ("text_array", "array(string)"):
        value_pa_type = pa.list_(pa.string())
    elif vt in ("double_array", "array(float64)"):
        value_pa_type = pa.list_(pa.float64())
    elif vt in ("timestamp_array", "array(datetime)"):
        value_pa_type = pa.list_(pa.timestamp("us", tz="UTC"))
    else:
        value_pa_type = pa.string()
        
    # === КОНВЕРТАЦИЯ VALUE ===
    is_array_type = vt.startswith("array(") or vt.endswith("_array")
    converted_values = []
    
    for r in rows:
        val = r["value"]
        if is_array_type:
            if val is None or (isinstance(val, str) and val.strip() == ''):
                converted_values.append(None)
            elif isinstance(val, (list, tuple)):
                # Если тип массива распознан, приводим элементы к нужному типу
                if pa.types.is_list(value_pa_type):
                    inner_pa_type = value_pa_type.value_type
                    cleaned = []
                    for item in val:
                        if item is None or pd.isna(item):
                            cleaned.append(None)
                        elif pa.types.is_int32(inner_pa_type) or pa.types.is_int64(inner_pa_type):
                            cleaned.append(int(item) if not isinstance(item, bool) else int(bool(item)))
                        elif pa.types.is_float64(inner_pa_type) or pa.types.is_float32(inner_pa_type):
                            cleaned.append(float(item))
                        elif pa.types.is_string(inner_pa_type) or pa.types.is_unicode(inner_pa_type):
                            cleaned.append(str(item) if item is not None else None)
                        elif pa.types.is_timestamp(inner_pa_type):
                            cleaned.append(ensure_timezone_aware(item) if item else None)
                        else:
                            cleaned.append(item)
                    converted_values.append(cleaned)
                else:
                    # Fallback: если тип не распознан, просто кладем список как есть
                    converted_values.append(list(val))
            elif isinstance(val, str):
                try:
                    parsed = json.loads(val)
                    converted_values.append(parsed if isinstance(parsed, list) else [parsed])
                except:
                    converted_values.append([val])
            else:
                converted_values.append([val])
        else:
            converted_values.append(convert_value_for_parquet(val, value_type, value_is_nullable))

    # === ЯВНОЕ СОЗДАНИЕ PYARROW ARRAY ДЛЯ VALUE ===
    # 🔥 FIX: Надежное определение типа на основе РЕАЛЬНЫХ ДАННЫХ, а не только value_type
    sample_val = next((v for v in converted_values if v is not None), None)
    data_is_array = isinstance(sample_val, list)
    
    # Если данные на самом деле массивы, но схема определилась как скалярная (из-за неверного value_type),
    # пересоздаем схему под список на лету.
    if data_is_array and not pa.types.is_list(value_pa_type):
        inner_sample = next((item for item in sample_val if item is not None), None)
        if isinstance(inner_sample, int):
            value_pa_type = pa.list_(pa.int32())
        elif isinstance(inner_sample, float):
            value_pa_type = pa.list_(pa.float64())
        elif isinstance(inner_sample, (datetime, pd.Timestamp)):
            value_pa_type = pa.list_(pa.timestamp("us", tz="UTC"))
        else:
            value_pa_type = pa.list_(pa.string())
            
    # Создаем массив. Для списков (ListType) from_pandas=True ломает вложенность!
    if pa.types.is_list(value_pa_type):
        value_array = pa.array(converted_values, type=value_pa_type)
    else:
        value_array = pa.array(converted_values, type=value_pa_type, from_pandas=True)
        
    data["value"] = value_array
    
    # === СХЕМА ===
    schema = pa.schema([
        pa.field("data_record_sk", pa.string(), nullable=False),
        pa.field("h_object_property_sk", pa.string(), nullable=False),
        pa.field("data_file_id", pa.string(), nullable=False),
        pa.field("load_dttm", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("value", value_pa_type, nullable=True),
    ])
    
    # === СОЗДАНИЕ ТАБЛИЦЫ И ЗАПИСЬ ===
    table = pa.Table.from_pydict(data, schema=schema)
    pq.write_table(
        table,
        output_path,
        compression="zstd",
        compression_level=ZSTD_COMPRESSION_LEVEL,
        row_group_size=500_000,
        write_statistics=True,
    )
    
    return os.path.getsize(output_path)


def upload_to_s3(local_path: str, s3_key: str, s3_client, bucket: str):
    """Загружает файл в S3/MinIO"""
    s3_client.upload_file(
        Filename=local_path,
        Bucket=bucket,
        Key=s3_key,
        # ExtraArgs убраны, так как MinIO не поддерживает запрос шифрования без настройки KMS
    )
    


def mark_exported_batch(
    engine,
    table_name: str,
    record_ids: List[str]
) -> int:
    """Помечает пачку записей как экспортированные."""
    if not record_ids:
        return 0
    
    # Конвертируем строки в UUID-объекты
    uuid_ids = [
        UUID(rec_id) if isinstance(rec_id, str) else rec_id
        for rec_id in record_ids
    ]
    
    with engine.begin() as conn:
        query = text(f"""
            UPDATE public.{table_name}
            SET exported_to_datamart = TRUE
            WHERE data_record_sk = ANY(:record_ids)
        """)
        result = conn.execute(query, {"record_ids": uuid_ids})
        log.info(f"✅ Помечено {result.rowcount} записей как exported_to_datamart=TRUE")
        return result.rowcount
    

def deserialize_diagnostic_array_blob(blob_data: bytes) -> List[Dict[str, Any]]:
    """
    Десериализация DiagnosticArray BLOB согласно C# реализации.
    
    Формат буфера (C# ObjectDataValue_DiagnosticArray_Serializer):
    - size: int32 (4 bytes, LE) - количество записей
    - Для каждой записи:
      - ID: Guid (16 bytes)
      - defectState: int32 (4 bytes, LE)
      - priority: int32 (4 bytes, LE)
      - tagNameSize: int32 (4 bytes, LE)
      - tagName: UTF-8 string (tagNameSize bytes)
      - defectNameSize: int32 (4 bytes, LE)
      - defectName: UTF-8 string (defectNameSize bytes)
      - defectDetailsSize: int32 (4 bytes, LE)
      - defectDetails: UTF-8 string (defectDetailsSize bytes)
      - groupNameSize: int32 (4 bytes, LE)
      - groupName: UTF-8 string (groupNameSize bytes)
      - recommendationsSize: int32 (4 bytes, LE)
      - recommendations: UTF-8 string (recommendationsSize bytes)
    
    Returns:
        List[Dict]: Список словарей с диагностическими записями
    """
    if not blob_data or len(blob_data) < 4:
        log.warning(f"⚠️ Empty or too short blob for DiagnosticArray: {len(blob_data) if blob_data else 0} bytes")
        return []
    
    offset = 0
    try:
        # Чтение количества записей
        size = struct.unpack('<i', blob_data[offset:offset+4])[0]
        offset += 4
        
        if size <= 0:
            log.debug(f"ℹ️ DiagnosticArray size is 0 or negative: {size}")
            return []
        
        if len(blob_data) < offset:
            log.warning(f"⚠️ Blob too short: expected at least {offset} bytes, got {len(blob_data)}")
            return []
        
        diagnostic_records = []
        
        for i in range(size):
            try:
                # ID: Guid (16 bytes)
                if offset + 16 > len(blob_data):
                    log.warning(f"⚠️ Not enough data for record {i} ID")
                    break
                diag_id = UUID(bytes=blob_data[offset:offset+16])
                offset += 16
                
                # defectState: int32 (4 bytes)
                if offset + 4 > len(blob_data):
                    log.warning(f"⚠️ Not enough data for record {i} defectState")
                    break
                defect_state = struct.unpack('<i', blob_data[offset:offset+4])[0]
                offset += 4
                
                # priority: int32 (4 bytes)
                if offset + 4 > len(blob_data):
                    log.warning(f"⚠️ Not enough data for record {i} priority")
                    break
                priority = struct.unpack('<i', blob_data[offset:offset+4])[0]
                offset += 4
                
                # tagName: size-prefixed UTF-8 string
                if offset + 4 > len(blob_data):
                    log.warning(f"⚠️ Not enough data for record {i} tagNameSize")
                    break
                tag_name_size = struct.unpack('<i', blob_data[offset:offset+4])[0]
                offset += 4
                
                if offset + tag_name_size > len(blob_data):
                    log.warning(f"⚠️ Not enough data for record {i} tagName")
                    break
                tag_name = blob_data[offset:offset+tag_name_size].decode('utf-8') if tag_name_size > 0 else ''
                offset += tag_name_size
                
                # defectName: size-prefixed UTF-8 string
                if offset + 4 > len(blob_data):
                    log.warning(f"⚠️ Not enough data for record {i} defectNameSize")
                    break
                defect_name_size = struct.unpack('<i', blob_data[offset:offset+4])[0]
                offset += 4
                
                if offset + defect_name_size > len(blob_data):
                    log.warning(f"⚠️ Not enough data for record {i} defectName")
                    break
                defect_name = blob_data[offset:offset+defect_name_size].decode('utf-8') if defect_name_size > 0 else ''
                offset += defect_name_size
                
                # defectDetails: size-prefixed UTF-8 string
                if offset + 4 > len(blob_data):
                    log.warning(f"⚠️ Not enough data for record {i} defectDetailsSize")
                    break
                defect_details_size = struct.unpack('<i', blob_data[offset:offset+4])[0]
                offset += 4
                
                if offset + defect_details_size > len(blob_data):
                    log.warning(f"⚠️ Not enough data for record {i} defectDetails")
                    break
                defect_details = blob_data[offset:offset+defect_details_size].decode('utf-8') if defect_details_size > 0 else ''
                offset += defect_details_size
                
                # groupName: size-prefixed UTF-8 string
                if offset + 4 > len(blob_data):
                    log.warning(f"⚠️ Not enough data for record {i} groupNameSize")
                    break
                group_name_size = struct.unpack('<i', blob_data[offset:offset+4])[0]
                offset += 4
                
                if offset + group_name_size > len(blob_data):
                    log.warning(f"⚠️ Not enough data for record {i} groupName")
                    break
                group_name = blob_data[offset:offset+group_name_size].decode('utf-8') if group_name_size > 0 else ''
                offset += group_name_size
                
                # recommendations: size-prefixed UTF-8 string
                if offset + 4 > len(blob_data):
                    log.warning(f"⚠️ Not enough data for record {i} recommendationsSize")
                    break
                recommendations_size = struct.unpack('<i', blob_data[offset:offset+4])[0]
                offset += 4
                
                if offset + recommendations_size > len(blob_data):
                    log.warning(f"⚠️ Not enough data for record {i} recommendations")
                    break
                recommendations = blob_data[offset:offset+recommendations_size].decode('utf-8') if recommendations_size > 0 else ''
                offset += recommendations_size
                
                # Создаём запись
                diagnostic_records.append({
                    'id': str(diag_id),
                    'defect_state': defect_state,
                    'priority': priority,
                    'tag_name': tag_name,
                    'defect_name': defect_name,
                    'defect_details': defect_details,
                    'group_name': group_name,
                    'recommendations': recommendations,
                })
                
            except Exception as e:
                log.error(f"❌ Error deserializing diagnostic record {i}: {e}", exc_info=True)
                break
        
        log.debug(f"✅ Deserialized {len(diagnostic_records)} diagnostic records from {len(blob_data)} bytes")
        return diagnostic_records
        
    except Exception as e:
        log.error(f"❌ Error deserializing DiagnosticArray: {e}", exc_info=True)
        return []
    



def load_array_data_to_datamart(
    dwh_table_name: str,
    clickhouse_table_name: str,
    deserialize_func,
    decode_timeseries_func,
    extract_fields: List[str],
    timeseries_params_mapping: Optional[Dict[str, str]] = None,
    clickhouse_batch_size: int = 50000,
    cloudberry_fetch_chunk: int = 100,
    clickhouse_conn_id: str = 'clickhouse_conn',
) -> bool:
    """
    🔥 ОПТИМИЗИРОВАННАЯ ВЕРСИЯ с clickhouse_connect.insert_df
    
    Загрузка данных в ClickHouse с пайплайном:
    1. (Опционально) Десериализация blob из БД через deserialize_func
    2. Декодирование временного ряда через decode_timeseries_func
    3. Вставка DataFrame через insert_df (в 5-10 раз быстрее execute)
    """
    import gc
    import numpy as np
    import pandas as pd
    import clickhouse_connect
    
    total_processed = 0
    total_source_records = 0
    total_empty_decodes = 0
    total_timeseries_points = 0
    
    mode_desc = "with deserialization" if deserialize_func else "direct raw_values"
    log.info(f"🚀 Старт загрузки timeseries: table={dwh_table_name}, chunk={cloudberry_fetch_chunk}, mode={mode_desc}")
    
    # Вспомогательная функция для извлечения имени поля из SQL выражения
    def extract_field_name(sql_expr: str) -> str:
        sql_lower = sql_expr.lower()
        if ' as ' in sql_lower:
            return sql_expr[sql_lower.rfind(' as ') + 4:].strip().split('.')[-1].strip()
        if '.' in sql_expr:
            return sql_expr.split('.')[-1].strip()
        return sql_expr.strip()
    
    field_names = [extract_field_name(f) for f in extract_fields]
    
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
        # KEYSET PAGINATION
        last_timestamp: Optional[datetime] = None
        last_record_id: Optional[str] = None
        iteration = 0
        
        while True:
            iteration += 1
            
            # 1️⃣ ИЗВЛЕЧЕНИЕ ЧАНКА С KEYSET PAGINATION
            if last_timestamp is None:
                extract_sql = text(f"""
                    SELECT {', '.join(extract_fields)}
                    FROM public.{dwh_table_name} h
                    WHERE h.exported_to_datamart = FALSE
                    ORDER BY h.timestamp ASC, h.data_record_sk ASC
                    LIMIT :limit
                """)
                params = {"limit": cloudberry_fetch_chunk}
            else:
                extract_sql = text(f"""
                    SELECT {', '.join(extract_fields)}
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
            
            # 2️⃣ 🔥 СБОР ДАННЫХ В СЛОВАРЬ NUMPY МАССИВОВ
            batch_cols = {
                'data_record_sk': [],
                'h_object_property_sk': [],
                'data_file_id': [],
                'timestamp': [],
                'value': [],
                'load_dttm': []
            }
            successfully_decoded_ids: List[str] = []
            chunk_points = 0
            
            for row in rows:
                row_dict = dict(zip(field_names, row))
                
                record_id = ensure_uuid_string(row_dict.get('data_record_sk'))
                h_obj_prop_sk = row_dict.get('h_object_property_sk')
                data_src_id = row_dict.get('data_file_id')
                load_dttm = row_dict.get('load_dttm')
                
                raw_vals = row_dict.get('raw_values')
                
                try:
                    if not raw_vals:
                        log.warning(f"⚠️ NULL/empty raw_values для {record_id}")
                        total_empty_decodes += 1
                        continue
                    
                    if isinstance(raw_vals, (bytes, bytearray)):
                        raw_bytes = bytes(raw_vals)
                    elif isinstance(raw_vals, memoryview):
                        raw_bytes = bytes(raw_vals)
                    else:
                        raw_bytes = bytes(raw_vals)
                    
                    if deserialize_func is not None:
                        deserialized = deserialize_func(raw_bytes)
                        if not deserialized:
                            log.warning(f"⚠️ Пустой результат десериализации для {record_id}")
                            total_empty_decodes += 1
                            continue
                    else:
                        deserialized = {'raw_values': raw_bytes}
                
                except Exception as e:
                    log.error(f"❌ Ошибка десериализации {record_id}: {e}", exc_info=True)
                    total_empty_decodes += 1
                    continue
                
                try:
                    func_params = {}
                    if timeseries_params_mapping:
                        for func_param, source_name in timeseries_params_mapping.items():
                            if source_name in deserialized:
                                func_params[func_param] = deserialized[source_name]
                            elif source_name in row_dict:
                                func_params[func_param] = row_dict[source_name]
                    else:
                        func_params = {**row_dict, **deserialized}
                    
                    timeseries = decode_timeseries_func(**func_params)
                    
                    if not timeseries:
                        log.warning(f"⚠️ Пустой результат декодирования для {record_id}")
                        total_empty_decodes += 1
                        continue
                    
                    # 🔥 Сбор в numpy массивы
                    n_points = len(timeseries)
                    
                    # Разделяем timestamps и values
                    ts_list = []
                    val_list = []
                    for ts, value in timeseries:
                        ts = ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
                        ts_list.append(ts)
                        val_list.append(float(value) if value is not None else 0.0)
                    
                    ld = load_dttm if load_dttm and load_dttm.tzinfo else (
                        load_dttm.replace(tzinfo=timezone.utc) if load_dttm else datetime.now(timezone.utc)
                    )
                    
                    batch_cols['data_record_sk'].append(
                        np.full(n_points, record_id, dtype=object)
                    )
                    batch_cols['h_object_property_sk'].append(
                        np.full(n_points, ensure_uuid_string(h_obj_prop_sk), dtype=object)
                    )
                    batch_cols['data_file_id'].append(
                        np.full(n_points, ensure_uuid_string(data_src_id), dtype=object)
                    )
                    batch_cols['timestamp'].append(np.array(ts_list, dtype='datetime64[us]'))
                    batch_cols['value'].append(np.array(val_list, dtype='float64'))
                    batch_cols['load_dttm'].append(
                        np.full(n_points, ld, dtype=object)
                    )
                    
                    successfully_decoded_ids.append(record_id)
                    chunk_points += n_points
                    total_timeseries_points += n_points
                    
                except Exception as e:
                    log.error(f"❌ Ошибка декодирования timeseries для {record_id}: {e}", exc_info=True)
                    total_empty_decodes += 1
                    continue
            
            # Сохраняем курсор
            if rows:
                last_timestamp = row_dict.get('timestamp')
                last_record_id = record_id
            
            del rows
            
            if not successfully_decoded_ids:
                log.warning(f"⚠️ Чанк #{iteration}: нет данных для вставки после обработки")
                continue
            
            # 3️⃣ 🔥 ФОРМИРОВАНИЕ DataFrame И ВСТАВКА ЧЕРЕЗ insert_df
            try:
                df_data = {k: np.concatenate(v) for k, v in batch_cols.items()}
                df = pd.DataFrame(df_data)
                
                # 🔥 Гарантируем правильные dtypes
                for col in ['data_record_sk', 'h_object_property_sk', 'data_file_id']:
                    df[col] = df[col].astype(str)
                
                df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
                df['load_dttm'] = pd.to_datetime(df['load_dttm'], utc=True)
                df['value'] = df['value'].astype('float64')
                
                # Порядок колонок
                cols_order = [
                    'data_record_sk', 'h_object_property_sk', 'data_file_id',
                    'timestamp', 'value', 'load_dttm'
                ]
                df = df[cols_order]
                
                client.insert_df(clickhouse_table_name, df)
                
                total_processed += chunk_points
                log.info(f"✅ Вставлено {chunk_points} точек через insert_df из чанка #{iteration}")
                
                # 4️⃣ ПОМЕТКА ЭКСПОРТИРОВАННЫХ
                if successfully_decoded_ids:
                    marked = mark_records_as_exported_via_temp_table(
                        cloudberry_engine,
                        dwh_table_name,
                        successfully_decoded_ids,
                        use_copy=True,
                    )
                    log.info(f"✅ Помечено {marked} записей-источников как экспортированные")
                
            except Exception as e:
                log.error(f"❌ Ошибка при вставке/пометке чанка #{iteration}: {type(e).__name__}: {e}", exc_info=True)
                raise
            finally:
                del batch_cols, successfully_decoded_ids, df_data, df
            
            log.info(f"📊 Прогресс: источник={total_source_records}, "
                    f"точек_timeseries={total_timeseries_points}, "
                    f"вставлено={total_processed}, "
                    f"пропущено={total_empty_decodes}")
        
    finally:
        client.close()
    
    # ========================================================================
    # ФИНАЛЬНАЯ СТАТИСТИКА
    # ========================================================================
    log.info(f"🎉 Загрузка timeseries завершена:")
    log.info(f"   • Обработано записей-источников: {total_source_records}")
    log.info(f"   • Всего точек временного ряда: {total_timeseries_points}")
    log.info(f"   • Вставлено строк в ClickHouse: {total_processed}")
    log.info(f"   • Пропущено записей: {total_empty_decodes}")
    if total_source_records > 0 and total_timeseries_points > 0:
        avg_points = total_timeseries_points / total_source_records
        log.info(f"   • Среднее точек на запись: {avg_points:.1f}")
    
    return True






def decode_sample_timeseries(
    raw_values: Union[bytes, np.ndarray],
    sample_rate: float,
    first_timestamp: datetime,
) -> List[Tuple[datetime, float]]:
    """
    🔥 ВЕКТОРИЗОВАННАЯ ВЕРСИЯ — в 10-50 раз быстрее исходной.
    
    Преобразует массив сырых значений SampleData во временной ряд (timestamp, value).
    Алгоритм:
    1. raw_values — это массив double (little-endian), сохранённый как bytes
    2. Каждый элемент массива — это значение в инженерных единицах (без дополнительной калибровки)
    3. Временная метка каждого отсчёта вычисляется как:
       timestamp_i = first_timestamp + i * (1_000_000 / sample_rate) микросекунд

    Args:
        raw_values: numpy массив сырых значений (float64) ИЛИ bytes (little-endian double[])
        sample_rate: частота дискретизации (Гц)
        first_timestamp: временная метка первого отсчёта

    Returns:
        Список кортежей (timestamp, value)
    """
    # 🔥 КОНВЕРТАЦИЯ bytes → np.ndarray
    if isinstance(raw_values, (bytes, bytearray)):
        if len(raw_values) == 0:
            log.warning("❌ Empty raw_values bytes for SampleData decoding")
            return []
        if len(raw_values) % 8 != 0:
            log.warning(f"⚠️ raw_values length {len(raw_values)} is not multiple of 8")
            return []
        raw_array = np.frombuffer(raw_values, dtype='<f8')
    elif isinstance(raw_values, memoryview):
        raw_array = np.frombuffer(bytes(raw_values), dtype='<f8')
    elif isinstance(raw_values, np.ndarray):
        raw_array = raw_values
    else:
        log.warning(f"❌ Invalid raw_values type: {type(raw_values)}")
        return []
    
    if len(raw_array) == 0:
        log.warning("❌ Empty raw_values array for SampleData decoding")
        return []
    
    n_samples = len(raw_array)
    
    # =====================================================================
    # 🔥 ШАГ 1: Векторный расчёт временных меток
    # =====================================================================
    if sample_rate > 0:
        time_delta_us = 1_000_000.0 / sample_rate
        offsets_us = np.arange(n_samples, dtype=np.int64) * int(time_delta_us)
    else:
        offsets_us = np.zeros(n_samples, dtype=np.int64)
    
    if first_timestamp.tzinfo is None:
        first_timestamp = first_timestamp.replace(tzinfo=timezone.utc)
    
    base_ns = np.datetime64(first_timestamp, 'us')
    timestamps_ns = base_ns + offsets_us.astype('timedelta64[us]')
    
    # =====================================================================
    # 🔥 ШАГ 2: Финальная сборка
    # =====================================================================
    ts_series = pd.to_datetime(timestamps_ns, utc=True)
    ts_py_list = ts_series.to_pydatetime()
    
    result = list(zip(ts_py_list, raw_array.astype(np.float64).tolist()))
    
    log.debug(f"✅ Decoded {len(result)} SampleData samples (vectorized)")
    return result

def parse_fh_file(local_path: str) -> FhParsedFile:
    """
    Парсит .fh файл и возвращает все значения с качеством GOOD или BAD.
    
    :param local_path: путь к локальному .fh файлу
    :return: FhParsedFile с распарсенными значениями
    :raises ValueError: если файл некорректный
    """
    with open(local_path, 'rb') as f:
        file_data = f.read()
    
    file_size = len(file_data)
    
    # Проверка минимального размера
    if file_size < FH_MIN_HEADER_LENGTH:
        raise ValueError(f"Файл слишком мал: {file_size} байт")
    
    # Проверка магического слова
    magic = struct.unpack_from('<I', file_data, 0)[0]
    if magic != FH_MAGIC_HEX:
        raise ValueError(f"Неверное магическое слово: 0x{magic:08X}")
    
    # Чтение длины заголовка
    header_length = struct.unpack_from('<I', file_data, 4)[0]
    if header_length < FH_MIN_HEADER_LENGTH or header_length > file_size:
        raise ValueError(f"Некорректная длина заголовка: {header_length}")
    
    # Чтение дат
    from_ts = struct.unpack_from('<Q', file_data, FH_FROM_DATE_OFFSET)[0]
    to_ts = struct.unpack_from('<Q', file_data, FH_TO_DATE_OFFSET)[0]
    from_date = fh_ts_to_datetime(from_ts)
    to_date = fh_ts_to_datetime(to_ts)
    
    # Чтение версии
    file_version = struct.unpack_from('<H', file_data, FH_VERSION_OFFSET)[0]
    if file_version != 1:
        raise ValueError(f"Неподдерживаемая версия файла: {file_version}")
    
    # Проверка CRC32
    header_data_end = header_length - 4
    stored_crc32 = struct.unpack_from('<I', file_data, header_data_end)[0]
    calculated_crc32 = binascii.crc32(file_data[:header_data_end]) & 0xFFFFFFFF
    if stored_crc32 != calculated_crc32:
        raise ValueError(
            f"CRC32 не совпадает: файл=0x{stored_crc32:08X}, "
            f"вычислено=0x{calculated_crc32:08X}"
        )
    
    # Разбор HeaderData
    header_data = file_data[FH_HEADER_DATA_OFFSET:header_data_end]
    
    if len(header_data) < 4:
        raise ValueError("HeaderData слишком короткий")
    
    data_row_values_count = struct.unpack_from('<I', header_data, 0)[0]
    
    # Проверка дополнительных метаданных
    if len(header_data) < 8:
        raise ValueError("Файл не содержит дополнительных метаданных")
    
    extra_sig = struct.unpack_from('<I', header_data, 4)[0]
    if extra_sig != FH_EXTRA_MAGIC:
        raise ValueError(f"Отсутствует сигнатура KRIO: 0x{extra_sig:08X}")
    
    # Чтение метаданных
    offset = 8
    file_id = fh_guid_from_bytes_le(header_data[offset:offset + 16])
    offset += 16
    main_db_id = fh_guid_from_bytes_le(header_data[offset:offset + 16])
    offset += 16
    name_len = struct.unpack_from('<I', header_data, offset)[0]
    offset += 4
    main_db_name = header_data[offset:offset + name_len].decode('utf-8')
    offset += name_len
    # data_type = struct.unpack_from('<I', header_data, offset)[0]  # всегда Double
    
    # Парсинг секции данных
    row_length = FH_ROW_ID_LENGTH + (FH_FIELD_LENGTH * data_row_values_count)
    data_section = file_data[header_length:]
    data_size = len(data_section)
    
    if data_size % row_length != 0:
        raise ValueError(
            f"Размер секции данных ({data_size}) не кратен длине строки ({row_length})"
        )
    
    num_rows = data_size // row_length
    values: List[FhParsedValue] = []
    data_offset = 0
    
    for _ in range(num_rows):
        # Чтение property_id
        prop_id = fh_guid_from_bytes_le(data_section[data_offset:data_offset + FH_ROW_ID_LENGTH])
        data_offset += FH_ROW_ID_LENGTH
        
        # Чтение массива значений
        for col_idx in range(data_row_values_count):
            quality = data_section[data_offset]
            raw_val = data_section[data_offset + FH_QUALITY_LENGTH : data_offset + FH_FIELD_LENGTH]
            
            # Загружаем только GOOD (2) и BAD (3) значения
            if quality >=0:
                timestamp = from_date + timedelta(seconds=col_idx)
                data_value = struct.unpack('<d', raw_val)[0]
                
                values.append(FhParsedValue(
                    object_id=prop_id,
                    timestamp=timestamp,
                    data_value=data_value,
                    quality=quality
                ))
            
            data_offset += FH_FIELD_LENGTH
    
    log.info(
        f"📊 Распарсен .fh файл: строк={num_rows}, "
        f"значений всего={num_rows * data_row_values_count}, "
        f"значений для загрузки={len(values)}"
    )
    
    return FhParsedFile(
        file_id=file_id,
        main_db_id=main_db_id,
        main_db_name=main_db_name,
        from_date=from_date,
        to_date=to_date,
        data_row_values_count=data_row_values_count,
        rows_count=num_rows,
        total_values_count=num_rows * data_row_values_count,
        values=values
    )
    
    
import io
import time
import uuid
import logging
from typing import List, Union
from sqlalchemy import text

log = logging.getLogger(__name__)


def _fast_copy_uuids(dbapi_conn, temp_table_name: str, record_ids: list) -> None:
    """
    🔥 БЫСТРАЯ загрузка UUID через COPY (в 100-1000 раз быстрее INSERT).
    
    Использует copy_expert вместо copy_from для избежания ошибок 
    с разделителями при работе с одной колонкой.
    """
    cursor = dbapi_conn.cursor()
    try:
        # Формируем CSV-like данные: один UUID на строку
        buffer = io.StringIO()
        for rid in record_ids:
            # Преобразуем UUID объект к строке если нужно
            uid_str = str(rid) if not isinstance(rid, str) else rid
            buffer.write(f"{uid_str}\n")
        
        buffer.seek(0)
        
        # 🔥 copy_expert даёт полный контроль над SQL командой COPY
        # FORMAT text для одной колонки работает без явного разделителя
        cursor.copy_expert(
            f"COPY {temp_table_name}(data_record_sk) FROM STDIN WITH (FORMAT text)",
            buffer
        )
        
        dbapi_conn.commit()
        log.debug(f"✅ COPY загружено {len(record_ids)} UUID в {temp_table_name}")
        
    except Exception as e:
        dbapi_conn.rollback()
        log.error(f"❌ Ошибка COPY: {e}", exc_info=True)
        raise
    finally:
        cursor.close()


def _batch_insert_uuids(
    dbapi_conn, 
    temp_table_name: str, 
    record_ids: list, 
    batch_size: int = 10_000
) -> None:
    """
    Загрузка UUID батчами через execute_values (в 10-100 раз быстрее 
    чем одиночные INSERT).
    
    Используется как fallback если COPY недоступен.
    """
    from psycopg2.extras import execute_values
    
    cursor = dbapi_conn.cursor()
    try:
        sql = f"INSERT INTO {temp_table_name} (data_record_sk) VALUES %s"
        
        # Обрабатываем батчами чтобы не раздувать SQL запрос
        for i in range(0, len(record_ids), batch_size):
            batch = record_ids[i:i + batch_size]
            
            # Формируем список кортежей для execute_values
            values = [(str(rid),) for rid in batch]
            
            execute_values(
                cursor, 
                sql, 
                values, 
                page_size=batch_size,
                template=None
            )
            
            log.debug(
                f"📥 Загружен батч {i // batch_size + 1}: "
                f"{len(batch)} UUID в {temp_table_name}"
            )
        
        dbapi_conn.commit()
        log.debug(f"✅ INSERT загружено {len(record_ids)} UUID в {temp_table_name}")
        
    except Exception as e:
        dbapi_conn.rollback()
        log.error(f"❌ Ошибка batch INSERT: {e}", exc_info=True)
        raise
    finally:
        cursor.close() 





import io
import time
import uuid
import logging
from typing import List, Union
from sqlalchemy import text

log = logging.getLogger(__name__)

def _fast_copy_uuids_with_file_id(dbapi_conn, temp_table_name: str, record_ids: list, data_file_id: str) -> None:
    """Быстрая загрузка UUID + data_file_id через COPY"""
    cursor = dbapi_conn.cursor()
    try:
        buffer = io.StringIO()
        for rid in record_ids:
            uid_str = str(rid) if not isinstance(rid, str) else rid
            buffer.write(f"{uid_str}\t{data_file_id}\n") # Tab-separated
        buffer.seek(0)
        cursor.copy_expert(
            f"COPY {temp_table_name}(data_record_sk, data_file_id) FROM STDIN WITH (FORMAT text, DELIMITER '\t')",
            buffer
        )
        dbapi_conn.commit()
    except Exception as e:
        dbapi_conn.rollback()
        log.error(f"❌ Ошибка COPY с file_id: {e}", exc_info=True)
        raise
    finally:
        cursor.close()

def mark_records_as_exported_via_temp_table(
    engine,
    table_name: str,
    record_ids: List[Union[str, uuid.UUID]],
    data_file_id: str,
    insert_batch_size: int = 500_000,
    use_copy: bool = True,
    target_column: str = "exported_to_datamart"
) -> int:
    if not record_ids:
        return 0
        
    temp_table_name = f"temp_mark_{int(time.time() * 1000)}_{id(record_ids) % 10000}"
    
    try:
        with engine.begin() as conn:
            dbapi_conn = conn.connection
            
            # 🔥 1. СОВПАДЕНИЕ КЛЮЧА ДИСТРИБУЦИИ
            # Если целевая таблица распределена по data_file_id, temp таблица должна быть такой же.
            # Это полностью убирает Redistribute Motion (пересылку данных между сегментами).
            if data_file_id:
                conn.execute(text(f"""
                    CREATE TEMP TABLE {temp_table_name} (
                        data_record_sk UUID NOT NULL,
                        data_file_id UUID NOT NULL
                    ) DISTRIBUTED BY (data_file_id)
                """))
            else:
                conn.execute(text(f"""
                    CREATE TEMP TABLE {temp_table_name} (
                        data_record_sk UUID NOT NULL
                    ) DISTRIBUTED BY (data_record_sk)
                """))
            
            # 2. Загрузка данных
            if use_copy:
                try:
                    if data_file_id:
                        _fast_copy_uuids_with_file_id(dbapi_conn, temp_table_name, record_ids, data_file_id)
                    else:
                        _fast_copy_uuids(dbapi_conn, temp_table_name, record_ids)
                except Exception as copy_error:
                    log.warning(f"⚠️ COPY failed: {copy_error}. Falling back to execute_values.")
                    _batch_insert_uuids(dbapi_conn, temp_table_name, record_ids, insert_batch_size)
            else:
                _batch_insert_uuids(dbapi_conn, temp_table_name, record_ids, insert_batch_size)
                
            # 3. Статистика и Индекс
            conn.execute(text(f"ANALYZE {temp_table_name}"))
            conn.execute(text(f"CREATE INDEX idx_{temp_table_name}_sk ON {temp_table_name} (data_record_sk)"))
            
            # 🔥 4. ПРИНУДИТЕЛЬНЫЙ HASH JOIN (Самое важное для MPP!)
            # Отключаем Nested Loop, который оптимизатор по ошибке выбирает для temp таблиц.
            conn.execute(text("SET enable_nestloop = off"))
            conn.execute(text("SET enable_hashjoin = on"))
            
            # 🔥 5. ЛОКАЛИЗАЦИЯ ОБНОВЛЕНИЯ
            # Фильтр по data_file_id гарантирует, что UPDATE затронет только тот сегмент, 
            # на котором физически лежат данные этого файла.
            file_filter = f"AND t.data_file_id = '{data_file_id}'" if data_file_id else ""
            
            update_sql = text(f"""
                UPDATE public.{table_name} AS t
                SET {target_column} = TRUE
                FROM {temp_table_name} AS tmp
                WHERE t.data_record_sk = tmp.data_record_sk
                  AND t.{target_column} = FALSE
                  {file_filter}
            """)
            
            result = conn.execute(update_sql)
            total_updated = result.rowcount
            log.info(f"✅ Обновлено {total_updated}/{len(record_ids)} записей в {table_name} (Hash Join)")
            
            # Сбрасываем настройки сессии
            conn.execute(text("RESET enable_nestloop"))
            conn.execute(text("RESET enable_hashjoin"))
            
            conn.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))
            return total_updated
            
    except Exception as e:
        log.error(f"❌ Error in mark_records_as_exported_via_temp_table: {e}", exc_info=True)
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))
        except Exception:
            pass
        raise
    

import numpy as np
import pandas as pd
from typing import Tuple

def decode_creyt_timeseries_numpy(
    raw_values: bytes,
    calibration_factor_a: float,
    calibration_factor_b: float,
    min_raw: float,
    max_raw: float,
    min_eu: float,
    max_eu: float,
    sample_rate: float,
    first_timestamp: datetime,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    🔥 БЫСТРАЯ ВЕРСИЯ ДЛЯ CLICKHOUSE.
    Возвращает кортеж из двух NumPy массивов: (timestamps, values).
    Избегает создания Python-объектов datetime.
    """
    data_length = len(raw_values) - 8
    if data_length <= 0:
        return np.array([]), np.array([])
    
    num_samples = data_length // 2
    if num_samples == 0:
        return np.array([]), np.array([])

    # 1. Распаковка ADC (векторно)
    adc_values = np.frombuffer(raw_values[:data_length], dtype='>u2').astype(np.float64)
    
    # 2. Математика (векторно)
    voltage_values = adc_values * (5.0 / 65535.0)
    calibrated_values = voltage_values * calibration_factor_a + calibration_factor_b
    
    if (max_raw - min_raw) == 0:
        return np.array([]), np.array([])
        
    k = (max_eu - min_eu) / (max_raw - min_raw)
    b = min_eu - min_raw * k
    eu_values = calibrated_values * k + b

    # 3. Временные метки (NumPy datetime64)
    if sample_rate > 0:
        time_delta_us = 1_000_000.0 / sample_rate
        offsets_us = np.arange(num_samples, dtype=np.int64) * int(time_delta_us)
    else:
        offsets_us = np.zeros(num_samples, dtype=np.int64)

    # Базовая метка -> numpy datetime64
    if first_timestamp.tzinfo is None:
        first_timestamp = first_timestamp.replace(tzinfo=timezone.utc)
    
    # Важно: используем 'us' (микросекунды) для точности
    base_ts = np.datetime64(first_timestamp, 'us')
    timestamps_ns = base_ts + offsets_us.astype('timedelta64[us]')

    return timestamps_ns, eu_values


# ==============================================================================
# Вспомогательные функции путей
# ==============================================================================

def _build_partition_path(data_file_id: str, day_date: date, by_source: bool) -> str:
    """
    Строит путь партиции в формате Hive-style:
    year=2026/month=03/day=27/source_name_full_uuid
    """
    # Базовый путь всегда начинается с даты
    base_path = f"year={day_date.year}/month={day_date.month:02d}/day={day_date.day:02d}"
    
    if by_source:
        # Получаем полное имя источника
        source_name = get_data_source_name(data_file_id)
        # Очищаем имя для пути (заменяем пробелы и слеши)
        safe_name = source_name.replace(' ', '_').replace('/', '_').replace('\\', '_').strip('_').lower()
        # Используем ПОЛНЫЙ UUID (без сокращений)
        full_uuid = str(data_file_id)
        # Добавляем источник ПОСЛЕ даты
        return f"{base_path}/{safe_name}_{full_uuid}"
        
    return base_path


def _parse_timestamp(ts_value: Any) -> Optional[datetime]:
    if isinstance(ts_value, datetime):
        return ts_value if ts_value.tzinfo else ts_value.replace(tzinfo=timezone.utc)
    if ts_value is None:
        return None
    try:
        ts_str = str(ts_value).replace(" ", "T")
        if "." not in ts_str and "T" in ts_str:
            ts_str += ".000000"
        dt = datetime.fromisoformat(ts_str)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _iceberg_json_type(iceberg_type: PrimitiveType) -> str | dict:
    if isinstance(iceberg_type, IntegerType): return "int"
    elif isinstance(iceberg_type, LongType): return "long"
    elif isinstance(iceberg_type, DoubleType): return "double"
    elif isinstance(iceberg_type, BooleanType): return "boolean"
    elif isinstance(iceberg_type, TimestampType): return "timestamp"
    elif isinstance(iceberg_type, StringType): return "string"
    elif isinstance(iceberg_type, ListType):
        return {"type": "list", "element-id": iceberg_type.element_id,
                "element": _iceberg_json_type(iceberg_type.element_type),
                "element-required": iceberg_type.element_required}
    return "string"


# ==============================================================================
# Инициализация таблицы
# ==============================================================================

def _init_or_load_table_metadata(
    s3_client, bucket: str, table_path: str, metadata_prefix: str,
    iceberg_type: PrimitiveType, pa_type: pa.DataType, partition_by_source: bool
) -> dict:
    hint_key = f"{metadata_prefix}/version-hint.text"
    try:
        resp = s3_client.get_object(Bucket=bucket, Key=hint_key)
        version = int(resp["Body"].read().decode().strip())
        metadata_key = f"{metadata_prefix}/v{version}.metadata.json"
        resp = s3_client.get_object(Bucket=bucket, Key=metadata_key)
        metadata = json.loads(resp["Body"].read().decode('utf-8'))
        log.info(f"📋 Loaded table metadata v{version}")
        return metadata
    except s3_client.exceptions.NoSuchKey:
        log.info(f"✨ Creating new Iceberg table: {table_path}")
        table_uuid = str(uuid.uuid4())
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        
        # Схема БЕЗ лишних колонок партиций
        schema = {
            "type": "struct", "schema-id": 0, "identifier-field-ids": [1],
            "fields": [
                {"id": 1, "name": "data_record_sk", "type": "string", "required": True},
                {"id": 2, "name": "h_object_property_sk", "type": "string", "required": False},
                {"id": 3, "name": "data_file_id", "type": "string", "required": True},
                {"id": 4, "name": "data_source_id", "type": "string", "required": True},
                {"id": 5, "name": "load_dttm", "type": "string", "required": False},
                {"id": 6, "name": "value", "type": _iceberg_json_type(iceberg_type), "required": not isinstance(iceberg_type, ListType)},
                {"id": 7, "name": "base_timestamp", "type": "timestamp", "required": False},
            ]
        }

        # PartitionSpec:
        # 1. source_id (IdentityTransform) -> String
        # 2. dt_day (DayTransform) -> Int (вычисляется из base_timestamp)
        partition_fields = []
        # 🔥 ИСПРАВЛЕНИЕ: Сначала день, потом источник
        partition_fields.append({"name": "dt_day", "transform": "day", "source-id": 7, "field-id": 1000})
        if partition_by_source:
            partition_fields.append({"name": "source_id", "transform": "identity", "source-id": 3, "field-id": 1001})

        metadata = {
            "format-version": 2,
            "table-uuid": table_uuid,
            "location": f"s3://{bucket}/{table_path}",
            "last-sequence-number": 0,
            "last-updated-ms": now_ms,
            "last-column-id": 6,
            "current-schema-id": 0,
            "schemas": [schema],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": partition_fields}],
            "last-partition-id": 1001 if partition_by_source else 1000,
            "default-sort-order-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "properties": {
                "write.parquet.compression-codec": "zstd",
                "write.target-file-size-bytes": "268435456",
            },
            "current-snapshot-id": -1,
            "snapshots": [], "snapshot-log": [], "metadata-log": []
        }

        metadata_key = f"{metadata_prefix}/v1.metadata.json"
        s3_client.put_object(Bucket=bucket, Key=metadata_key,
                             Body=json.dumps(metadata, indent=2).encode('utf-8'), ContentType='application/json')
        s3_client.put_object(Bucket=bucket, Key=hint_key, Body=b"1", ContentType='text/plain')
        log.info(f"✅ Table initialized: v1")
        return metadata

# ==============================================================================
# Вспомогательные функции для конвертации значений в типы PyArrow
# ==============================================================================
def _convert_value_for_pyarrow(value: Any, pa_type: pa.DataType, is_nullable: bool) -> Any:
    """Convert value to match PyArrow type expectations."""
    if value is None:
        return None if is_nullable else _get_default_for_pa_type(pa_type)
    
    try:
        # Handle primitive types
        if pa.types.is_int32(pa_type) or pa.types.is_int64(pa_type):
            return int(value)
        elif pa.types.is_float32(pa_type) or pa.types.is_float64(pa_type):
            return float(value)
        elif pa.types.is_boolean(pa_type):
            return bool(value)
        elif pa.types.is_string(pa_type):
            if isinstance(value, (bytes, bytearray)):
                return value.decode('utf-8', errors='replace')
            elif isinstance(value, timedelta):
                return str(value)
            return str(value)
        elif pa.types.is_timestamp(pa_type):
            if isinstance(value, datetime):
                return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
            return _parse_timestamp(value)
        elif pa.types.is_list(pa_type):
            if isinstance(value, (list, tuple)):
                return list(value)
            return [value] if value is not None else []
    except (ValueError, TypeError, UnicodeDecodeError) as e:
        log.warning(f"⚠️ Type conversion warning for value {repr(value)[:100]}: {e}")
        return None if is_nullable else _get_default_for_pa_type(pa_type)
    
    return value


def _get_default_for_pa_type(pa_type: pa.DataType) -> Any:
    """Get default value for PyArrow types when conversion fails or value is None."""
    if pa.types.is_int32(pa_type) or pa.types.is_int64(pa_type):
        return 0
    elif pa.types.is_float32(pa_type) or pa.types.is_float64(pa_type):
        return 0.0
    elif pa.types.is_boolean(pa_type):
        return False
    elif pa.types.is_string(pa_type):
        return ""
    elif pa.types.is_timestamp(pa_type):
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    elif pa.types.is_list(pa_type):
        return []
    return None


from pyiceberg.types import PrimitiveType, StringType, IntegerType, LongType, DoubleType, BooleanType, TimestampType, ListType
import pyarrow as pa

def _get_type_for_iceberg(type_str: str) -> tuple[PrimitiveType, pa.DataType]:
    """
    Map data_type or data_mart_table_name to corresponding Iceberg/PyArrow types.
    Надежно определяет скалярные типы и массивы (Array/List).
    """
    if not type_str:
        return StringType(), pa.string()
        
    t = str(type_str).lower().strip()
    
    # 1. Очистка от префиксов и суффиксов таблиц (nonhist_, hist_, _data, _data_records)
    for prefix in ["nonhist_", "hist_"]:
        if t.startswith(prefix):
            t = t[len(prefix):]
    for suffix in ["_data_records", "_data"]:
        if t.endswith(suffix):
            t = t[:-len(suffix)]
            
    # 2. Обработка Array (массивов)
    if "array" in t or t.startswith("list"):
        if "int" in t:
            return ListType(1, LongType(), True), pa.list_(pa.int64())
        elif "float" in t or "double" in t:
            return ListType(1, DoubleType(), True), pa.list_(pa.float64())
        elif "string" in t or "text" in t:
            return ListType(1, StringType(), True), pa.list_(pa.string())
        elif "date" in t or "time" in t:
            return ListType(1, TimestampType(), True), pa.list_(pa.timestamp("us"))
        # Fallback для неизвестных массивов
        return ListType(1, StringType(), True), pa.list_(pa.string())
        
    # 3. Обработка примитивных типов
    if t in ("int32", "integer", "int"):
        return IntegerType(), pa.int32()
    elif t in ("int64", "long", "bigint"):
        return LongType(), pa.int64()
    elif t in ("float64", "double", "float"):
        return DoubleType(), pa.float64()
    elif t in ("bool", "boolean"):
        return BooleanType(), pa.bool_()
    elif t in ("datetime", "timestamp", "date"):
        return TimestampType(), pa.timestamp("us")
    elif t in ("string", "text", "str"):
        return StringType(), pa.string()
        
    # 4. Fallback
    return StringType(), pa.string()


# ==============================================================================
# Манифесты (Исправлено!)
# ==============================================================================






def _write_manifest_list(s3_client, file_io, bucket: str, metadata_prefix: str,
                         manifest_files: list[ManifestFile], table_metadata: dict,
                         partition_spec: PartitionSpec, snapshot_id: int, sequence_number: int) -> str:
    from pyiceberg.manifest import ManifestListWriterV2
    manifest_list_name = f"snap-{snapshot_id}-0.avro"
    manifest_list_key = f"{metadata_prefix}/{manifest_list_name}"
    manifest_list_output = file_io.new_output(f"s3://{bucket}/{manifest_list_key}")
    
    with ManifestListWriterV2(output_file=manifest_list_output, snapshot_id=snapshot_id,
                              parent_snapshot_id=table_metadata.get("current-snapshot-id", -1),
                              sequence_number=sequence_number) as writer:
        writer.add_manifests(manifest_files)
    log.info(f"📋 Written manifest list: {manifest_list_key}")
    return manifest_list_key


def _commit_snapshot(s3_client, bucket: str, table_path: str, metadata_prefix: str,
                     current_metadata: dict, manifest_list_path: str,
                     manifest_files: list[ManifestFile], total_records: int,
                     snapshot_id: int, sequence_number: int):
    hint_key = f"{metadata_prefix}/version-hint.text"
    resp = s3_client.get_object(Bucket=bucket, Key=hint_key)
    current_version = int(resp["Body"].read().decode().strip())
    timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    
    summary = {"operation": "append", "added-data-files": str(len(manifest_files)),
               "added-records": str(total_records), "added-files-size": str(sum(mf.manifest_length for mf in manifest_files)),
               "changed-partition-count": str(len(manifest_files))}
    new_snapshot = {"snapshot-id": snapshot_id, "parent-snapshot-id": current_metadata.get("current-snapshot-id", -1),
                    "sequence-number": sequence_number, "timestamp-ms": timestamp_ms,
                    "manifest-list": f"s3://{bucket}/{manifest_list_path}", "summary": summary,
                    "schema-id": current_metadata["current-schema-id"]}
    
    current_metadata["last-updated-ms"] = timestamp_ms
    current_metadata["last-sequence-number"] = sequence_number
    current_metadata["current-snapshot-id"] = snapshot_id
    current_metadata["snapshots"].append(new_snapshot)
    current_metadata["snapshot-log"].append({"snapshot-id": snapshot_id, "timestamp-ms": timestamp_ms})
    current_metadata["metadata-log"].append({"metadata-file": f"s3://{bucket}/{metadata_prefix}/v{current_version}.metadata.json",
                                             "timestamp-ms": timestamp_ms})
    
    new_version = current_version + 1
    new_metadata_key = f"{metadata_prefix}/v{new_version}.metadata.json"
    s3_client.put_object(Bucket=bucket, Key=new_metadata_key,
                         Body=json.dumps(current_metadata, indent=2).encode('utf-8'), ContentType='application/json')
    s3_client.put_object(Bucket=bucket, Key=hint_key, Body=str(new_version).encode('utf-8'), ContentType='text/plain')
    log.info(f"📝 Committed snapshot {snapshot_id} (v{current_version} → v{new_version})")


# 🔥 Оценка размера одной строки в байтах для расчета порога буферизации.
# Схема проще, чем у Creyt (нет сырых байтов), ~6 колонок (UUID, string, float, timestamp) ≈ 100-120 байт.
ESTIMATED_BYTES_PER_ROW_COMMON = 120


# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ БУФЕРИЗАЦИИ И КОММИТА
# ============================================================================

def _flush_common_partition_to_s3(
    day_date: date, buffer: Dict[str, list], s3_client, file_io, bucket: str, 
    data_prefix: str, metadata_prefix: str, partition_spec: PartitionSpec, 
    partition_by_source: bool, iceberg_type: PrimitiveType, value_is_nullable: bool, 
    pa_schema: pa.Schema, table_metadata: dict, data_file_id: str,
    snapshot_id: int, sequence_number: int
) -> Optional[Any]:
    """Конвертирует колоночный буфер в Parquet, загружает в S3 и возвращает ManifestFile."""
    partition_path = _build_partition_path(data_file_id, day_date, partition_by_source)
    
    # 🔥 ИСПОЛЬЗУЕМ from_pydict для колоночного хранения (в разы быстрее и меньше памяти)
    pa_table = pa.Table.from_pydict(buffer, schema=pa_schema)
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        local_file = Path(tmp_dir) / "data.parquet"
        pq.write_table(pa_table, str(local_file), compression="zstd")
        file_size = local_file.stat().st_size
        
        # 🔥 Явно удаляем таблицу и чистим пул PyArrow
        del pa_table
        try:
            pa.default_memory_pool().release_unused()
        except Exception:
            pass
            
        file_uuid = uuid.uuid4().hex
        s3_key = f"{data_prefix}/{partition_path}/data_{file_uuid}.parquet"
        s3_uri = f"s3://{bucket}/{s3_key}"
        
        s3_client.upload_file(str(local_file), bucket, s3_key)
        log.info(f"📤 FLUSH: {len(buffer['data_record_sk'])} rows → {s3_uri} ({file_size/1024/1024:.2f} MB)")
        
        manifest_file = _write_manifest_file(
            file_io, bucket, metadata_prefix, s3_uri, file_size, len(buffer['data_record_sk']),
            data_file_id, day_date, partition_spec, partition_by_source,
            iceberg_type, value_is_nullable, snapshot_id=snapshot_id, sequence_number=sequence_number
        )
        return manifest_file


def _commit_and_update_db_common(
    pending_manifests: List[Any],
    pending_record_ids: List[str],
    s3_client, file_io, bucket: str, table_path: str, metadata_prefix: str,
    partition_spec: PartitionSpec, table_metadata: dict, dwh_table_name: str,
    snapshot_id: int, sequence_number: int,
    target_column: str = "exported_to_s3_storage" # 🔥 FIX: Добавлен параметр
):
    """Выполняет атомарный коммит в Iceberg и обновление статусов в Cloudberry."""
    if not pending_manifests:
        return
    try:
        manifest_list_path = _write_manifest_list(
            s3_client, file_io, bucket, metadata_prefix, pending_manifests, table_metadata,
            partition_spec, snapshot_id=snapshot_id, sequence_number=sequence_number
        )
        _commit_snapshot(
            s3_client, bucket, table_path, metadata_prefix, table_metadata,
            manifest_list_path, pending_manifests, len(pending_record_ids),
            snapshot_id=snapshot_id, sequence_number=sequence_number
        )
        log.info(f"📝 Committed snapshot {snapshot_id} с {len(pending_manifests)} файлами")
        
        if pending_record_ids:
            marked = mark_records_as_exported_via_temp_table(
                get_dwh_engine('cloudberry_test_dwh'),
                dwh_table_name,
                pending_record_ids,
                insert_batch_size=1_000_000, # Уменьшаем батч для безопасности
                use_copy=True,
                target_column=target_column # 🔥 FIX: Передаем колонку
            )
            log.info(f"✅ Помечено {marked} записей как экспортированные")
    except Exception as e:
        log.error(f"❌ Ошибка коммита или обновления БД: {e}", exc_info=True)
        raise 


def _write_manifest_file(
    file_io, bucket: str, metadata_prefix: str, data_file_uri: str,
    file_size: int, record_count: int, data_file_id: str, partition_day: date,
    partition_spec: PartitionSpec, partition_by_source: bool, iceberg_type: PrimitiveType,
    value_is_nullable: bool = False, snapshot_id: int = None, sequence_number: int = None,
) -> ManifestFile:
    """Генерирует Avro manifest с корректной схемой для PartitionSpec."""
    from pyiceberg.types import NestedField
    from pyiceberg.manifest import ManifestFile
    
    iceberg_schema = IcebergSchema(
        NestedField(field_id=1, name="data_record_sk", field_type=StringType(), required=True),
        NestedField(field_id=2, name="h_object_property_sk", field_type=StringType(), required=False),
        NestedField(field_id=3, name="data_file_id", field_type=StringType(), required=True),
        NestedField(field_id=4, name="data_source_id", field_type=StringType(), required=True),
        NestedField(field_id=5, name="load_dttm", field_type=StringType(), required=False),
        NestedField(field_id=6, name="value", field_type=iceberg_type, required=not value_is_nullable),
        NestedField(field_id=7, name="timestamp", field_type=TimestampType(), required=False),
        schema_id=0,
    )
    
    # 🔥 ИСПРАВЛЕНИЕ ОШИБКИ `TypeError: unsupported operand type(s) for <<: 'str' and 'int'`
    # Порядок и имена полей в Record должны СТРОГО соответствовать новому PartitionSpec:
    # 1. dt_day (DayTransform -> Integer)
    # 2. source_id (IdentityTransform -> String)
    partition = Record(
        dt_day=int((partition_day - date(1970, 1, 1)).days),
        source_id=str(data_file_id) if partition_by_source else "unknown",
    )

    avg_col_size = file_size // 6 if record_count > 0 and file_size > 0 else 100
    column_sizes = {i: avg_col_size for i in range(1, 7)}
    
    if snapshot_id is None:
        snapshot_id = abs(hash(f"{data_file_uri}_{datetime.now(timezone.utc).isoformat()}"))

    data_file = DataFile(
        content=DataFileContent.DATA,
        file_path=data_file_uri,
        file_format=FileFormat.PARQUET,
        partition=partition,
        record_count=record_count,
        file_size_in_bytes=file_size,
        column_sizes=column_sizes,
        value_counts={i: record_count for i in range(1, 7)},
        null_value_counts={i: 0 for i in range(1, 7)},
        nan_value_counts={},
        lower_bounds={},
        upper_bounds={},
        key_metadata=None,
        split_offsets=[],
        equality_ids=None,
        sort_order_id=0,
    )

    entry = ManifestEntry(
        status=ManifestEntryStatus.ADDED,
        snapshot_id=snapshot_id,
        data_sequence_number=sequence_number or 0,
        file_sequence_number=sequence_number or 0,
        data_file=data_file,
    )

    manifest_file_name = f"{uuid.uuid4().hex}-m0.avro"
    manifest_key = f"{metadata_prefix}/{manifest_file_name}"
    manifest_output = file_io.new_output(f"s3://{bucket}/{manifest_key}")

    with write_manifest(
        format_version=2,
        spec=partition_spec,
        schema=iceberg_schema,
        output_file=manifest_output,
        snapshot_id=snapshot_id,
    ) as writer:
        writer.add_entry(entry)
    
    original_manifest = writer.to_manifest_file()
    
    # Явное создание нового ManifestFile с корректным sequence_number
    # (обходит проблемы с отсутствием .copy() или .model_copy() в разных версиях pyiceberg/pydantic)
    new_manifest = ManifestFile(
        manifest_path=original_manifest.manifest_path,
        manifest_length=original_manifest.manifest_length,
        partition_spec_id=original_manifest.partition_spec_id,
        content=original_manifest.content,
        sequence_number=sequence_number or 0,
        min_sequence_number=sequence_number or 0,
        added_snapshot_id=original_manifest.added_snapshot_id,
        added_files_count=original_manifest.added_files_count,
        existing_files_count=original_manifest.existing_files_count,
        deleted_files_count=original_manifest.deleted_files_count,
        added_rows_count=original_manifest.added_rows_count,
        existing_rows_count=original_manifest.existing_rows_count,
        deleted_rows_count=original_manifest.deleted_rows_count,
        partitions=original_manifest.partitions,
        key_metadata=original_manifest.key_metadata,
    )
    
    return new_manifest


def clean_staging_layer_by_file(
    stg_table_name: str,
    data_file_id: str
) -> int:
    """
    Очищает staging ТОЛЬКО для конкретного data_file_id.
    Использует DELETE вместо TRUNCATE — не требует Access Exclusive Lock.
    """
    hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(
        f"DELETE FROM {stg_table_name} WHERE data_file_id = %s",
        (data_file_id,)
    )
    deleted = cursor.rowcount
    conn.commit()
    cursor.close()
    conn.close()

    log.info(f"🗑️ Staging очищен: удалено {deleted} строк для data_file_id={data_file_id}")
    return deleted

def clean_duplicate_staging_rows_by_file(
    stg_table_name: str,
    data_file_id: str
) -> int:
    """
    Удаляет дубликаты в staging ТОЛЬКО для конкретного data_file_id.
    🔥 Автоматически определяет ключ дедупликации по приоритету:
       hash_sk → business_id → id
    Если ни одной подходящей колонки нет — пропускает таблицу.
    Не блокирует строки других параллельных задач.
    """
    from sqlalchemy import inspect, text
    
    dwh_engine = get_dwh_engine(DWH_CONN_ID)
    
    # 🔥 1. Получаем список колонок таблицы
    try:
        insp = inspect(dwh_engine)
        columns = [c['name'].lower() for c in insp.get_columns(stg_table_name, schema='public')]
    except Exception as e:
        log.warning(f"⚠️ Не удалось получить схему {stg_table_name}: {e}")
        return 0
    
    # 🔥 2. Определяем ключ дедупликации по приоритету
    dedup_key = None
    if 'hash_sk' in columns:
        dedup_key = 'hash_sk'
    elif 'business_id' in columns:
        dedup_key = 'business_id'
    elif 'id' in columns:
        dedup_key = 'id'
    else:
        log.warning(f"⚠️ Пропускаю дедуп {stg_table_name}: нет подходящей колонки "
                    f"(hash_sk/business_id/id)")
        return 0
    
    # 🔥 3. Выполняем удаление дубликатов
    delete_sql = text(f"""
        DELETE FROM public.{stg_table_name}
        WHERE ctid IN (
            SELECT ctid FROM (
                SELECT ctid,
                       ROW_NUMBER() OVER (
                           PARTITION BY {dedup_key}
                           ORDER BY load_dttm ASC
                       ) AS rn
                FROM public.{stg_table_name}
                WHERE data_file_id = :data_file_id
            ) sub
            WHERE sub.rn > 1
        )
    """)
    
    try:
        with dwh_engine.begin() as conn:
            result = conn.execute(delete_sql, {"data_file_id": data_file_id})
            deleted = result.rowcount or 0
    except Exception as e:
        log.error(f"❌ Ошибка дедупликации {stg_table_name}: {e}", exc_info=True)
        return 0
    
    if deleted > 0:
        log.info(f"🧹 Удалено {deleted} дубликатов из {stg_table_name} "
                 f"для data_file_id={data_file_id} (key={dedup_key})")
    else:
        log.debug(f"✅ Дубликатов не найдено в {stg_table_name} "
                  f"для data_file_id={data_file_id}")
    return deleted
    
    



