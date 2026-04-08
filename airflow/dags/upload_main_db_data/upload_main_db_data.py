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
from typing import Dict, Any, Optional, List, Union
from sqlalchemy import Column, Integer, String, ForeignKey, create_engine, DateTime, func, Index, Boolean, select, text, Float, LargeBinary
from sqlalchemy.orm import declarative_base, relationship, Session, declared_attr, sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from uuid import uuid4, UUID
import logging
import json
from helper.helper import parse_uuid
import pandas as pd
import hashlib
import numpy as np
import time

Base = declarative_base()
DWH_CONN_ID = 'cloudberry_test_dwh'
log = logging.getLogger(__name__)


class DatabaseIdent(Base):
    __tablename__ = 'DatabaseIdent'
    id = Column('ID', String(36), primary_key=True)  # UUID как TEXT
    main_db_name = Column('MainDbName', String(100))
    main_db_id = Column('MainDbId', String(36))
    data_type = Column('DataType', Integer)


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


def get_data_source_id(engine: Engine) -> UUID:
    log.debug("🔍 Fetching data_source_id from DatabaseIdent table")
    uid: UUID
    try:
        with Session(engine) as session:
            stmt = select(DatabaseIdent)
            row = session.execute(stmt.limit(1)).scalars().one_or_none()

            if row is None:
                log.error("❌ DatabaseIdent table is empty or contains no records")
                raise AirflowException("Таблица DatabaseIdent пуста или не содержит записей")
            
            uid = parse_uuid(row.id)
            if uid is None:
                log.error(f"❌ Invalid UUID in row: {row}")
                raise AirflowException(f"Не удалось распарсить ID: {row.id}")
            
            return uid
                    
    except Exception as e:
        log.error(f"❌ Error fetching data_source_id: {str(e)}", exc_info=True)
        raise AirflowException(f"Ошибка при подключении к БД: {str(e)}")


def md5_hex_to_uuid(hex_string: str) -> UUID:
    if not hex_string or len(hex_string) != 32:
        log.debug(f"⚠️ Invalid hex string for UUID conversion: '{hex_string[:10]}...', generating random UUID")
        return uuid4()
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
    raw_string = '|'.join(str(v) for v in values if pd.notna(v))
    md5_hex = hashlib.md5(raw_string.encode('utf-8')).hexdigest()
    result = md5_hex_to_uuid(md5_hex)
    return result





def calc_link_hash(
    row: pd.Series,
    link_column: str,
    is_system_type_data: bool,
    prefix: str,
    data_source_id: UUID,
    hash_column_name: str = 'hash_sk'
) -> UUID | None:

    link_value = str(row[link_column]).strip().lower()
    hash_sk = str(row[hash_column_name])

    if is_system_type_data:
        inner_input = f"{prefix}|{link_value}"
    else:
        inner_input = f"{link_value}|{str(data_source_id).lower()}"

    inner_md5 = hashlib.md5(inner_input.encode('utf-8')).hexdigest()
    final_input = f"{hash_sk}|{inner_md5}"
    md5_hex = hashlib.md5(final_input.encode('utf-8')).hexdigest()
    result=md5_hex_to_uuid(md5_hex)

    return result



def calc_hub_hash_for_column(
    row: pd.Series,
    column_name: str,
    is_system_type_data: bool,
    prefix: str,
    data_source_id: UUID
) -> UUID | None:

    raw_value = row[column_name]
    
    # 🔥 Обработка NULL/NaN значений -> строка 'null' (как в SQL-эталоне)
    if pd.isna(raw_value) or raw_value is None:
        column_value = 'null'
    # 🔥 Целые числа (int, np.integer)
    elif isinstance(raw_value, (int, np.integer)):
        column_value = str(int(raw_value))
    # 🔥 Числа с плавающей точкой: если целое значение — убираем .0
    elif isinstance(raw_value, (float, np.floating)):
        if np.isnan(raw_value):
            column_value = 'null'
        elif raw_value.is_integer():
            column_value = str(int(raw_value))  # 1.0 -> "1"
        else:
            column_value = str(raw_value).strip().lower()
    # 🔥 Все остальные типы
    else:
        column_value = str(raw_value).strip().lower()
    
    # log.info(f"{column_value}")
    
    if is_system_type_data:
        inner_input = f"{prefix}|{column_value}"
        log.info(f"!!!!!!!!!!{inner_input}")
    else:
        inner_input = f"{column_value}|{str(data_source_id).lower()}"
        log.info(f"############# {inner_input}")
    
    inner_md5 = hashlib.md5(inner_input.encode('utf-8')).hexdigest()
    # log.info(f"{inner_md5}")
    result = md5_hex_to_uuid(inner_md5)
    # log.info(f"{result}")
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


def extract_from_sqlite_to_staging(
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
        link_columns.append(col_dict.get('column_name',''))
    
    db_path = sqlite_path if sqlite_path.startswith("sqlite:///") else f"sqlite:///{sqlite_path}"
    sqlite_engine = create_engine(db_path, echo=False)
    dwh_engine = get_dwh_engine(DWH_CONN_ID)
    
    try:
        data_source_id: UUID = get_data_source_id(sqlite_engine)
        
        # Формируем запрос с экранированием имён колонок
        quoted_cols = [f'"{col}"' if col.lower() not in source_column_names else col for col in source_column_names]
        query = f"SELECT {', '.join(quoted_cols)} FROM \"{source_table_name}\""
        df = pd.read_sql(query, sqlite_engine)
        
        # Нормализуем имена колонок к нижнему регистру
        df.columns = df.columns.str.lower()

        if df.empty:
            log.info(f"⚠️ No data to process in SQLite for entity '{entity_config['source_table_name']}' - returning early")
            return True

        df['data_source_id'] = data_source_id

        # Нормализация значений по типам
        for col in df.columns:
            if col in ['data_source_id', 'hash_sk', 'hash_sat_diff', 'load_dttm']:
                continue
            target_type = column_types.get(col, 'string')
            df[col] = df[col].apply(lambda x: normalize_value(x, target_type))

        # Расчёт hash_sk для Hub
        cols_for_hash = hash_hub_gener_columns + ['data_source_id']
        df['hash_sk'] = df[cols_for_hash].apply(lambda row: calc_hash_sk(row), axis=1)
        
        # Список колонок для исключения из hash_sat_diff
        cols_to_drop = ['hash_sk', 'hash_sat_diff', 'load_dttm'] + link_columns
        if link_columns:
            for column in link_columns:
                target_col = f"l_{str(column).lower()}_sk"
                cols_to_drop.append(target_col)
        
        # Расчёт hash_sat_diff для Satellite
        df['hash_sat_diff'] = df.apply(
            lambda row: calc_hash_sat_diff(
                row.drop(cols_to_drop, errors='ignore'), 
                data_source_id
            ), 
            axis=1
        )

        df['load_dttm'] = datetime.now(timezone.utc)

        # Генерация линк-колонок
        if link_columns_dict:
            for link_config in link_columns_dict:
                link_col = str(link_config.get('column_name', ''))
                is_system_type_data = str(link_config.get('is_system_type_data', 'False')).lower().strip() in ('true', '1', 'yes', 'on')
                prefix = str(link_config.get('prefix_entity_name', ''))
                if not link_col:
                    continue
                
                target_col = f"l_hub_{str(link_col).lower()}_sk"
                df[target_col] = df.apply(
                    lambda row: calc_link_hash(row, link_col, is_system_type_data, prefix, data_source_id), axis=1)
                link_hub_sk = f"{link_col.lower()}_sk"
                df[link_hub_sk] = df[[link_col, 'data_source_id']].apply(lambda row: calc_hub_hash_for_column(row, link_col, is_system_type_data, prefix, data_source_id), axis=1)

        # Маппинг типов для PostgreSQL
        dtype_mapping = {
            'hash_sk': PG_UUID(as_uuid=True),
            'hash_sat_diff': PG_UUID(as_uuid=True),
            'data_source_id': PG_UUID(as_uuid=True),
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
        
        # Добавляем dtype для сгенерированных линк-колонок
        if link_columns:
            for link_col in link_columns:
                if link_col:
                    target_col = f"l_{str(link_col).lower()}_sk"
                    dtype_mapping[target_col] = PG_UUID(as_uuid=True)

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
        

def extract_diag_data_to_staging(
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
    column_types = config.get('column_types', {})
    link_columns_dict = config.get('link_columns', {})
    link_columns = []
    
    for col_dict in link_columns_dict:
        link_columns.append(col_dict.get('column_name',''))
    
    db_path = sqlite_path if sqlite_path.startswith("sqlite:///") else f"sqlite:///{sqlite_path}"
    sqlite_engine = create_engine(db_path, echo=False)
    dwh_engine = get_dwh_engine(DWH_CONN_ID)
    
    try:
        data_source_id: UUID = get_data_source_id(sqlite_engine)
        
        # Формируем запрос с экранированием имён колонок
        quoted_cols = [f'"{col}"' if col.lower() not in source_column_names else col for col in source_column_names]
        query = f"SELECT {', '.join(quoted_cols)} FROM \"{source_table_name}\""
        df = pd.read_sql(query, sqlite_engine)
        
        # Нормализуем имена колонок к нижнему регистру
        df.columns = df.columns.str.lower()

        if df.empty:
            log.info(f"⚠️ No data to process in SQLite for entity '{entity_config['source_table_name']}' - returning early")
            return True

        df['data_source_id'] = data_source_id

        # Нормализация значений по типам
        for col in df.columns:
            if col in ['data_source_id', 'hash_sk', 'hash_sat_diff', 'load_dttm']:
                continue
            target_type = column_types.get(col, 'string')
            df[col] = df[col].apply(lambda x: normalize_value(x, target_type))

        # Расчёт hash_sk для Hub
        df['hash_diag_alarm_sk'] = df[['diagalarmid', 'date', 'alarmstate', 'data_source_id']].apply(lambda row: calc_hash_sk(row), axis=1)
        df['hash_diag_data_sk'] = df[['diagid', 'date', 'data_source_id']].apply(lambda row: calc_hash_sk(row), axis=1)
        
        # Расчёт hash_sat_diff для Satellite
        df['hash_sat_diag_alarm_diff'] = df[['date', 'confirmed', 'comment', 'data_source_id']].apply(
            lambda row: calc_hash_sat_diff(row, data_source_id), axis=1)
        
        df['hash_sat_diag_data_diff'] = df[['diagtagname', 'defectname', 'defectdetails', 'recommendation', 'priority', 'groupname','data_source_id']].apply(
            lambda row: calc_hash_sat_diff(row, data_source_id), axis=1)
        
        df['load_dttm'] = datetime.now(timezone.utc)
        
        # Генерация линк-колонок
        df['l_diag_alarm_propertyid_sk'] = df.apply(lambda row: calc_link_hash(row, 'propertyid', False, '', data_source_id, 'hash_diag_alarm_sk'), axis=1)
        df['propertyid_sk'] = df.apply(lambda row: calc_hub_hash_for_column(row, 'propertyid', False, '', data_source_id), axis=1)
        df['l_diag_alarm_alarmstate_sk'] = df.apply(lambda row: calc_link_hash(row, 'alarmstate', True, 'h_diagnostic_alarm_states', data_source_id, 'hash_diag_alarm_sk'), axis=1)
        df['alarmstate_sk'] = df.apply(lambda row: calc_hub_hash_for_column(row, 'alarmstate', True, 'h_diagnostic_alarm_states', data_source_id), axis=1)
        df['l_diag_alarm_diag_data_sk'] = df.apply(lambda row: calc_link_hash(row, 'diagid', False, '', data_source_id, 'hash_diag_alarm_sk'), axis=1)
        df['diag_data_sk'] = df.apply(lambda row: calc_hub_hash_for_column(row, 'diagid', False, '', data_source_id), axis=1)
        df['l_diag_data_defectstate_sk'] = df.apply(lambda row: calc_link_hash(row, 'defectstate', True, 'h_diagnostic_defect_states', data_source_id, 'hash_diag_data_sk'), axis=1)
        df['defectstate_sk'] = df.apply(lambda row: calc_hub_hash_for_column(row, 'defectstate', True, 'h_diagnostic_defect_states', data_source_id), axis=1)
        df['l_diag_data_defecttype_sk'] = df.apply(lambda row: calc_link_hash(row, 'defecttype', True, 'h_diagnostic_defect_types', data_source_id, 'hash_diag_data_sk'), axis=1)
        df['defecttype_sk'] = df.apply(lambda row: calc_hub_hash_for_column(row, 'defecttype', True, 'h_diagnostic_defect_types', data_source_id), axis=1)       
        
        # Маппинг типов для PostgreSQL
        dtype_mapping = {
            'hash_sk': PG_UUID(as_uuid=True),
            'hash_sat_diff': PG_UUID(as_uuid=True),
            'data_source_id': PG_UUID(as_uuid=True),
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
        
        # Добавляем dtype для сгенерированных линк-колонок
        if link_columns:
            for link_col in link_columns:
                if link_col:
                    target_col = f"l_{str(link_col).lower()}_sk"
                    dtype_mapping[target_col] = PG_UUID(as_uuid=True)

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


def clean_duplicate_staging_rows(stg_table_name: str) -> bool:
    """
    Удаляет дубликаты из staging-таблицы, сравнивая по всем бизнес-колонкам.
    Оставляет запись с минимальным ctid (первую вставленную).
    Использует подход GROUP BY + подзапрос для поиска дубликатов.
    
    Аналог SQL:
        DELETE FROM table 
        WHERE ctid NOT IN (
            SELECT MIN(ctid) 
            FROM table 
            GROUP BY col1, col2, ...
        )
    """
    from sqlalchemy import text
    
    # Валидация имени таблицы (защита от SQL-инъекций)
    if not stg_table_name or not stg_table_name.replace('_', '').isalnum():
        log.error(f"❌ Invalid table name: '{stg_table_name}'")
        return False
    
    dwh_engine = get_dwh_engine(DWH_CONN_ID)
    
    try:
        with dwh_engine.begin() as conn:
            # 1. Получаем список всех бизнес-колонок таблицы
            # Исключаем системные и служебные колонки PostgreSQL
            columns_result = conn.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = :table_name 
                  AND table_schema = 'public'
                  AND column_name NOT IN (
                      'ctid', 'xmin', 'xmax', 'cmin', 'cmax', 
                      'tableoid', 'load_dttm', 'data_source_id',
                      'hash_sk', 'hash_sat_diff'
                  )
                ORDER BY ordinal_position
            """), {"table_name": stg_table_name})
            
            columns = [row[0] for row in columns_result.fetchall()]
            
            if not columns:
                log.warning(f"⚠️ No business columns found in table '{stg_table_name}'")
                return True
            
            # 2. Формируем список колонок для GROUP BY (с экранированием имён)
            group_by_columns = ', '.join([f'"{col}"' for col in columns])
            
            # 3. Удаляем дубликаты: оставляем строку с наименьшим ctid
            # Подзапрос находит минимальный ctid для каждой уникальной комбинации значений
            sql_query = f"""
            DELETE FROM "{stg_table_name}"
            WHERE ctid NOT IN (
                SELECT MIN(ctid)
                FROM "{stg_table_name}"
                GROUP BY {group_by_columns}
            )
            """
            
            log.debug(f"🔍 Executing deduplication on '{stg_table_name}' ({len(columns)} columns)")
            result = conn.execute(text(sql_query))
            deleted_count = result.rowcount
            
            if deleted_count > 0:
                log.info(f"✅ Deleted {deleted_count} duplicate rows from '{stg_table_name}'")
            else:
                log.info(f"ℹ️ No duplicates found in '{stg_table_name}'")
            
            return True
            
    except Exception as e:
        log.error(f"❌ Error deleting duplicates from '{stg_table_name}': {str(e)}", exc_info=True)
        return False


def merge_staging_to_hub(
    hub_table_name: str, 
    sk_hub_column_name: str, 
    stg_table_name: str, 
    stg_hash_column_name: str = 'hash_sk'):

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
                    stg.{stg_hash_column_name},
                    stg.load_dttm,
                    stg.data_source_id
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
    base_sat_cols = [
        hab_sk_column_name,
        'load_dttm',
        'valid_from_dttm', 
        'valid_to_dttm',
        'active_flag',
        'data_source_id',
        'hash_sat_diff'
    ]
    # Добавляем пользовательские колонки
    all_sat_cols = base_sat_cols + [c.lower() for c in sat_column_names]
    satellite_columns = ',\n                    '.join(all_sat_cols)
    base_select_cols = [
        'stg.'+stg_hash_sk_column_name,
        'stg.load_dttm',
        'stg.load_dttm',  # valid_from_dttm
        'NULL',           # valid_to_dttm
        'true',           # active_flag
        'stg.data_source_id',
        'stg.'+stg_hash_sat_diff_column_name
    ]
    all_select_cols = base_select_cols + [f'stg.{c.lower()}' for c in stg_column_names]
    staging_columns = ',\n                    '.join(all_select_cols)
    
    try:
        with dwh_engine.begin() as conn:
            # ==================== SATELLITE: ЗАКРЫТИЕ ====================

            close_old_sat_sql = text(f"""
                UPDATE {sat_table_name} sat
                SET 
                    valid_to_dttm = NOW(),
                    active_flag = false                 
                FROM {stg_table_name} stg
                WHERE sat.hash_sat_diff = stg.{stg_hash_sk_column_name}
                  AND sat.active_flag = true
                  AND sat.hash_sat_diff IS DISTINCT FROM stg.{stg_hash_sat_diff_column_name}
            """)
            result = conn.execute(close_old_sat_sql)
            sat_closed = result.rowcount

            
            # ==================== SATELLITE: ВСТАВКА ====================

            insert_sat_sql = text(f"""
                INSERT INTO {sat_table_name} (
                    {satellite_columns}
                )
                SELECT 
                    {staging_columns}
                FROM {stg_table_name} stg
                LEFT JOIN {sat_table_name} sat
                    ON stg.{stg_hash_sk_column_name} = sat.{hab_sk_column_name}
                    AND sat.load_dttm = (SELECT MAX(load_dttm) FROM {sat_table_name} WHERE {hab_sk_column_name} = stg.{stg_hash_sk_column_name})
                WHERE sat.hash_sat_diff IS NULL
                    OR stg.{stg_hash_sat_diff_column_name} <> sat.hash_sat_diff
            """)
            result = conn.execute(insert_sat_sql)
            sat_inserted = result.rowcount

            
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



def upload_link_table(
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
    
    try:
        with dwh_engine.begin() as conn:
            # ==================== 1. ЗАКРЫТИЕ УСТАРЕВШИХ СВЯЗЕЙ ====================
            # Закрываем активные записи, где link_sk совпадает, 
            # но изменились подключённые хабы (изменение бизнес-логики связи)

            sql_close = text(f"""
                WITH rows_to_close AS (
                    SELECT 
                        link.{link_sk_column_name}
                    FROM 
                        public.{link_table_name} link
                    LEFT JOIN 
                        public.{stg_table_name} stg
                        ON  link.{link_first_hub_sk_column_name}       = stg.{stg_first_hub_sk_column_name}
                        AND link.{link_second_hub_sk_column_name}  = stg.{stg_second_hub_sk_column_name}
                        AND link.data_source_id              = stg.data_source_id
                    WHERE 
                        link.active_flag = true
                        AND stg.{stg_first_hub_sk_column_name} IS NULL             -- Фильтр Anti-Join: записей нет в staging
                )
                UPDATE public.{link_table_name} AS link
                SET 
                    valid_to_dttm   = NOW(),
                    active_flag     = false
                FROM 
                    rows_to_close AS r
                WHERE 
                    link.{link_sk_column_name} = r.{link_sk_column_name};
            """)
            
            result_close = conn.execute(sql_close)
            log.info(f"✅ Closed {result_close.rowcount} outdated links (hub keys changed)")

            # ==================== 2. ВСТАВКА НОВЫХ СВЯЗЕЙ ====================
            # Вариант А: LEFT JOIN ... WHERE IS NULL (без изменений в схеме)
            # Вариант Б: ON CONFLICT (требует уникального индекса) — см. ниже

            sql_insert = text(f"""
                INSERT INTO public.{link_table_name} (
                    {link_sk_column_name},
                    load_dttm,
                    valid_from_dttm,
                    valid_to_dttm,
                    active_flag,
                    data_source_id,
                    {link_first_hub_sk_column_name},
                    {link_second_hub_sk_column_name}
                )
                SELECT
                    stg.{stg_link_sk_column_name},
                    stg.load_dttm,
                    stg.load_dttm,
                    NULL,
                    true,
                    stg.data_source_id,
                    stg.{stg_first_hub_sk_column_name},
                    stg.{stg_second_hub_sk_column_name}
                FROM 
                    public.{stg_table_name} stg
                LEFT JOIN 
                    public.{link_table_name} link
                    ON  stg.{stg_first_hub_sk_column_name}           = link.{link_first_hub_sk_column_name}
                    AND stg.{stg_second_hub_sk_column_name}   = link.{link_second_hub_sk_column_name}
                    AND stg.data_source_id    = link.data_source_id
                    AND link.active_flag       = true                -- Проверяем только активные версии
                WHERE 
                    link.{link_sk_column_name} IS NULL; -- Anti-Join: в целевой нет
            """)
            
            result_insert = conn.execute(sql_insert)
            log.info(f"✅ Inserted {result_insert.rowcount} new links")
              
    except SQLAlchemyError as e:
        log.error(f"❌ SQLAlchemy error during link upload: {e}", exc_info=True)
        raise e
    except Exception as e:
        log.error(f"❌ Unexpected error during link upload: {e}", exc_info=True)
        raise e
    
    log.info(f"🎉 Link upload to '{link_table_name}' completed")
    return True


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
            
            


