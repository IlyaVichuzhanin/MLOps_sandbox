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
import pickle
import struct
from typing import Any, Optional
from dataclasses import dataclass
import psycopg2
import struct
import hashlib
import uuid
import logging
from typing import Optional
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError



Base = declarative_base()
DWH_CONN_ID = 'cloudberry_test_dwh'
log = logging.getLogger(__name__)








    
    










def md5_hex_to_uuid(hex_string: str) -> UUID:
    if not hex_string or len(hex_string) != 32:
        log.debug(f"⚠️ Invalid hex string for UUID conversion: '{hex_string[:10]}...', generating random UUID")
        return uuid4()
    uuid_str = f"{hex_string[:8]}-{hex_string[8:12]}-{hex_string[12:16]}-{hex_string[16:20]}-{hex_string[20:]}"
    return UUID(uuid_str)





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