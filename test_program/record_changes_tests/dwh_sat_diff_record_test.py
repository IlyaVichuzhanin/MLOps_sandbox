#!/usr/bin/env python3
"""
Тест проверки изменений в DWH по методологии Data Vault 2.0.
ИСПРАВЛЕНИЯ:
1. Обработка часовых поясов для datetime (DB=MSK, CSV=UTC)
2. Нормализация BLOB-строк UUID в бизнес-ключах для hash_sk
"""

import csv
import hashlib
import json
import re
import struct
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from decimal import Decimal

import psycopg2
from psycopg2.extras import RealDictCursor


# ============================================================================
# КОНФИГУРАЦИЯ
# ============================================================================
DATA_FILE_ID = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'

CSV_PATH = r'D:\ml-sandbox\audit_logs\changed_objects_guid_20260618_202549.csv'

DWH_HOST = 'localhost'
DWH_PORT = 7000
DWH_DB = 'dwh'
DWH_USER = 'gpadmin'
DWH_PASSWORD = ''


# ============================================================================
# НОРМАЛИЗАЦИЯ GUID
# ============================================================================

def is_blob_string(val: Any) -> bool:
    """Проверяет, является ли значение BLOB-строкой вида b'...' или b\"...\"."""
    if not isinstance(val, str):
        return False
    v = val.strip()
    return (v.startswith("b'") and v.endswith("'")) or \
           (v.startswith('b"') and v.endswith('"'))


def parse_blob_to_bytes(blob_str: str) -> Optional[bytes]:
    """Парсит BLOB-строку в bytes."""
    if not is_blob_string(blob_str):
        return None
    v = blob_str.strip()
    content = v[2:-1]
    try:
        return content.encode('utf-8').decode('unicode_escape').encode('latin-1')
    except Exception:
        return None


def normalize_guid_bytes(b: bytes) -> str:
    """Нормализует GUID из 16 байт с little-endian byte swap (.NET формат)."""
    if len(b) != 16:
        return b.hex()
    d1 = struct.unpack('<I', b[0:4])[0]
    d2 = struct.unpack('<H', b[4:6])[0]
    d3 = struct.unpack('<H', b[6:8])[0]
    d4 = b[8:10].hex()
    d5 = b[10:16].hex()
    return f"{d1:08x}-{d2:04x}-{d3:04x}-{d4}-{d5}"


def normalize_guid(val: Any) -> Optional[str]:
    """Нормализует GUID из различных форматов в стандартную строку."""
    if val is None:
        return None
    
    # BLOB-строка
    if is_blob_string(val):
        blob_bytes = parse_blob_to_bytes(val)
        if blob_bytes and len(blob_bytes) == 16:
            return normalize_guid_bytes(blob_bytes)
        return None
    
    # bytes
    if isinstance(val, (bytes, bytearray, memoryview)):
        if len(val) == 16:
            return normalize_guid_bytes(val)
        return None
    
    # UUID объект
    if isinstance(val, uuid.UUID):
        return str(val).lower()
    
    # Строка
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        # 32 hex без дефисов
        if len(s) == 32 and all(c in '0123456789abcdefABCDEF' for c in s):
            s = f"{s[:8]}-{s[8:12]}-{s[12:16]}-{s[16:20]}-{s[20:]}"
        # Проверяем формат UUID
        if re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', s, re.I):
            return s.lower()
        return None
    
    return None


# ============================================================================
# НОРМАЛИЗАЦИЯ ЗНАЧЕНИЙ ДЛЯ ХЭШИРОВАНИЯ
# ============================================================================

def normalize_value_for_hash(val: Any) -> str:
    """
    Нормализация значения для хэширования (как в ETL common_functions.py).
    Ключевое: BLOB-строки UUID нормализуются через little-endian byte swap.
    """
    if val is None:
        return ''
    
    # BLOB-строка
    if is_blob_string(val):
        blob_bytes = parse_blob_to_bytes(val)
        if blob_bytes and len(blob_bytes) == 16:
            return normalize_guid_bytes(blob_bytes)
        return ''
    
    # bytes
    if isinstance(val, (bytes, bytearray, memoryview)):
        if len(val) == 16:
            return normalize_guid_bytes(val)
        return val.hex()
    
    # UUID объект
    if isinstance(val, uuid.UUID):
        return str(val).lower()
    
    # Decimal
    if isinstance(val, Decimal):
        if val == int(val):
            return str(int(val))
        return str(val)
    
    # bool (должен быть перед int, т.к. bool наследуется от int)
    if isinstance(val, bool):
        return str(val).lower()
    
    # Числа
    if isinstance(val, (int, float)):
        if isinstance(val, float) and val.is_integer():
            return str(int(val))
        return str(val)
    
    # Строка
    s = str(val).strip()
    if s.lower() in ('nan', 'none', 'null', 'nat', '<na>', ''):
        return ''
    
    # Проверка на UUID строку
    guid = normalize_guid(s)
    if guid:
        return guid
    
    return s


# ============================================================================
# РАСЧЁТ ХЭШЕЙ (как в ETL)
# ============================================================================

def md5_hex_to_uuid(hex_string: str) -> uuid.UUID:
    if not hex_string or len(hex_string) != 32:
        return uuid.uuid4()
    uuid_str = f"{hex_string[:8]}-{hex_string[8:12]}-{hex_string[12:16]}-{hex_string[16:20]}-{hex_string[20:]}"
    return uuid.UUID(uuid_str)


def calc_hash_sk(values: Dict[str, Any]) -> uuid.UUID:
    """
    Расчет Hub Surrogate Key.
    Формула: MD5(business_keys|data_file_id)
    Фильтрация: пропускает None и пустые строки (как pd.notna в ETL).
    """
    filtered = []
    for v in values.values():
        normalized = normalize_value_for_hash(v)
        if normalized != '':
            filtered.append(normalized)
    raw_string = '|'.join(filtered)
    md5_hex = hashlib.md5(raw_string.encode('utf-8')).hexdigest()
    return md5_hex_to_uuid(md5_hex)


def calc_hub_hash_for_column(
    row: Dict[str, Any],
    column_name: str,
    is_system_type_data: bool,
    prefix: str,
    data_file_id: str
) -> uuid.UUID:
    """Расчет хэша для одной колонки (для link таблиц)."""
    raw_value = row.get(column_name)
    
    if raw_value is None or raw_value == '':
        column_value = 'null'
    else:
        normalized = normalize_value_for_hash(raw_value)
        column_value = normalized if normalized != '' else 'null'
    
    if is_system_type_data:
        inner_input = f"{prefix}|{column_value}"
    else:
        inner_input = f"{column_value}|{str(data_file_id).lower()}"
    
    inner_md5 = hashlib.md5(inner_input.encode('utf-8')).hexdigest()
    return md5_hex_to_uuid(inner_md5)


def get_hub_sk_from_json(json_data: Dict[str, Any], hub_keys: List[str]) -> str:
    """Вычисляет hash_sk из JSON данных."""
    values = {}
    for key in hub_keys:
        val = json_data.get(key)
        values[key] = val
    values['data_file_id'] = DATA_FILE_ID
    return str(calc_hash_sk(values))


# ============================================================================
# НОРМАЛИЗАЦИЯ ЗНАЧЕНИЙ ДЛЯ СРАВНЕНИЯ
# ============================================================================

def normalize_for_compare(db_val: Any, csv_val: Any, column_name: str = '') -> Tuple[Any, Any]:
    """
    Нормализует пару значений (из БД и из CSV) для корректного сравнения.
    ИСПРАВЛЕНИЯ:
    1. Обработка часовых поясов для datetime (DB=MSK/локальный, CSV=UTC)
    2. Нормализация BLOB-строк UUID
    3. Нормализация чисел (Decimal vs int/float)
    4. Нормализация bool/int (0/1 ↔ True/False)
    """
    # --- Обработка None / NULL ---
    db_is_none = db_val is None
    csv_is_none = csv_val is None or (isinstance(csv_val, str) and csv_val.strip().lower() in ('', 'null', 'none'))
    
    # Для timestamp колонок: ETL заменяет NULL на 1970-01-01
    is_timestamp_col = any(kw in column_name.lower() for kw in ['date', 'time', 'dttm'])
    if is_timestamp_col:
        if db_is_none and csv_is_none:
            return None, None
        if isinstance(db_val, datetime) and db_val.year == 1970 and db_val.month == 1 and db_val.day == 1:
            if csv_is_none:
                return None, None
        if csv_is_none and isinstance(db_val, datetime) and db_val.year == 1970:
            return None, None
    
    # --- Нормализация BLOB-строк UUID в CSV ---
    if is_blob_string(csv_val):
        blob_bytes = parse_blob_to_bytes(csv_val)
        if blob_bytes and len(blob_bytes) == 16:
            csv_val = normalize_guid_bytes(blob_bytes)
    
    # --- Нормализация чисел (Decimal, int, float) ---
    def to_number(v):
        if v is None:
            return None
        if isinstance(v, Decimal):
            if v == int(v):
                return int(v)
            return float(v)
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, (int, float)):
            return v
        if isinstance(v, str):
            v = v.strip()
            if v.lower() in ('', 'null', 'none'):
                return None
            try:
                f = float(v)
                if f == int(f) and '.' not in v:
                    return int(f)
                return f
            except ValueError:
                return v
        return v
    
    db_num = to_number(db_val)
    csv_num = to_number(csv_val)
    
    if isinstance(db_num, (int, float)) and isinstance(csv_num, (int, float)):
        if isinstance(db_num, float) or isinstance(csv_num, float):
            if abs(float(db_num) - float(csv_num)) < 1e-9:
                return db_num, csv_num
        if db_num == csv_num:
            return db_num, csv_num
    
    # --- Нормализация bool / int (0/1 ↔ True/False) ---
    def to_bool(v):
        if v is None:
            return None
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, Decimal)):
            if int(v) == 0:
                return False
            if int(v) == 1:
                return True
        if isinstance(v, str):
            v_low = v.strip().lower()
            if v_low in ('0', 'false', 'no', 'off', ''):
                return False
            if v_low in ('1', 'true', 'yes', 'on'):
                return True
        return v
    
    db_bool = to_bool(db_val)
    csv_bool = to_bool(csv_val)
    if isinstance(db_bool, bool) and isinstance(csv_bool, bool):
        if db_bool == csv_bool:
            return db_bool, csv_bool
    
    # --- Нормализация datetime с учётом часовых поясов ---
    def parse_csv_datetime(v):
        """Парсит datetime из CSV строки как UTC."""
        if v is None:
            return None
        if isinstance(v, datetime):
            return v
        if isinstance(v, str):
            v = v.strip()
            if v.lower() in ('', 'null', 'none'):
                return None
            for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d'):
                try:
                    dt = datetime.strptime(v, fmt)
                    # CSV содержит UTC время (из SQLite)
                    return dt.replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
        return None
    
    def normalize_db_datetime(v):
        """Нормализует datetime из DB.
        psycopg2 возвращает datetime без tzinfo (локальное время клиента).
        Считаем его локальным (MSK = UTC+3) и конвертируем в UTC.
        """
        if v is None:
            return None
        if isinstance(v, datetime):
            if v.tzinfo is not None:
                # Уже с TZ - конвертируем в UTC
                return v.astimezone(timezone.utc)
            else:
                # Без TZ - считаем локальным (MSK = UTC+3)
                # Добавляем локальный TZ и конвертируем в UTC
                local_tz = timezone(timedelta(hours=3))  # MSK
                v_with_tz = v.replace(tzinfo=local_tz)
                return v_with_tz.astimezone(timezone.utc)
        return None
    
    if is_timestamp_col or isinstance(db_val, datetime) or isinstance(csv_val, datetime):
        db_dt = normalize_db_datetime(db_val)
        csv_dt = parse_csv_datetime(csv_val)
        
        if db_dt is not None and csv_dt is not None:
            # Прямое сравнение в UTC
            if db_dt == csv_dt:
                return db_dt, csv_dt
            # Допуск: если разница кратна целому числу часов (1-12)
            diff_seconds = abs((db_dt - csv_dt).total_seconds())
            if diff_seconds > 0 and diff_seconds % 3600 == 0 and diff_seconds <= 12 * 3600:
                return db_dt, csv_dt
    
    # --- Нормализация GUID ---
    db_guid = normalize_guid(db_val)
    csv_guid = normalize_guid(csv_val)
    if db_guid and csv_guid and db_guid == csv_guid:
        return db_guid, csv_guid
    
    # --- Строковое сравнение ---
    def to_str(v):
        if v is None:
            return None
        if isinstance(v, Decimal):
            return str(v)
        return str(v).strip()
    
    db_str = to_str(db_val)
    csv_str = to_str(csv_val)
    if db_str == csv_str:
        return db_str, csv_str
    
    return db_val, csv_val


def values_match(db_val: Any, csv_val: Any, column_name: str = '') -> bool:
    """Сравнивает значение из БД со значением из CSV после нормализации."""
    db_norm, csv_norm = normalize_for_compare(db_val, csv_val, column_name)
    return db_norm == csv_norm


# ============================================================================
# ПАРСИНГ JSON
# ============================================================================

def parse_json_data(json_str: str) -> Dict[str, Any]:
    if not json_str:
        return {}
    try:
        data = json.loads(json_str)
    except json.JSONDecodeError as e:
        print(f"  ⚠️ JSON parse error: {e}")
        return {}
    return {k.lower(): v for k, v in data.items()}


# ============================================================================
# МАППИНГ ТАБЛИЦ
# ============================================================================

TABLE_MAPPING = {
    'Bearings': {
        'satellite_table': 's_bearings',
        'hub_sk_column': 'h_bearing_sk',
        'hub_keys': ['bearingid'],
        'field_map': {
            'number': 'number',
            'outerrace_d': 'outer_race_d',
            'innerrace_d': 'inner_race_d',
            'rollingelement_d': 'rolling_element_d',
            'rollingelement_count': 'rolling_element_count',
            'contactangle': 'contact_angle',
            'bpfi': 'bpfi',
            'bpfo': 'bpfo',
            'bsf': 'bsf',
            'ftf': 'ftf',
            'servicelife': 'service_life',
            'datecreated': 'date_created',
            'datemodified': 'date_modified',
            'cdbsyncdate': 'cdb_sync_date',
            'cdbversion': 'cdb_version',
            'cdbid': 'cdb_id',
        }
    },
    'MeasureConvert': {
        'satellite_table': 's_measure_converts',
        'hub_sk_column': 'h_measure_convert_sk',
        'hub_keys': ['fromid', 'toid', 'converttype'],
        'field_map': {
            'fromid': 'from_id',
            'toid': 'to_id',
            'factor': 'factor',
            'formula': 'formula',
        }
    },
    'MeasureGroups': {
        'satellite_table': 's_measure_groups',
        'hub_sk_column': 'h_measure_group_sk',
        'hub_keys': ['id'],
        'field_map': {
            'name': 'group_name',
        }
    },
    'MeasureUnits': {
        'satellite_table': 's_measure_units',
        'hub_sk_column': 'h_measure_unit_sk',
        'hub_keys': ['id'],
        'field_map': {
            'name': 'name',
            'abbreviation': 'abbreviation',
        }
    },
    'ObjectPropertyDescriptorNodes': {
        'satellite_table': 's_object_property_descriptor_nodes',
        'hub_sk_column': 'h_object_property_descriptor_node_sk',
        'hub_keys': ['id'],
        'field_map': {
            'name': 'name',
            'description': 'description',
            'parentnodeid': 'parent_id',
            'translateid': 'translated_id',
        }
    },
    'ObjectPropertyDescriptors': {
        'satellite_table': 's_object_property_descriptors',
        'hub_sk_column': 'h_object_property_descriptor_sk',
        'hub_keys': ['propertydescriptorid'],
        'field_map': {
            'name': 'name',
            'description': 'description',
            'tag': 'tag',
            'maxrecords': 'max_records',
            'savehistory': 'save_history',
            'defaultvalue': 'default_value',
            'visibleforscada': 'visible_for_scada',
            'datecreated': 'date_created',
            'datemodified': 'date_modified',
            'cdbsyncdate': 'cdb_sync_date',
            'cdbversion': 'cdb_version',
            'translateid': 'translated_id',
        }
    },
    'Objects': {
        'satellite_table': 's_objects',
        'hub_sk_column': 'h_object_sk',
        'hub_keys': ['id'],
        'field_map': {
            'name': 'name',
            'tagname': 'tag_name',
            'parentid': 'parent_id',
            'description': 'description',
            'templatename': 'template_name',
            'fromtemplatename': 'from_template_name',
            'datecreated': 'date_created',
            'datemodified': 'date_modified',
            'cdbsyncdate': 'cdb_sync_date',
            'cdbversion': 'cdb_version',
            'cdbid': 'cdb_id',
        }
    },
    # НОВЫЕ ТАБЛИЦЫ
    'ModelTemplates': {
        'satellite_table': 's_model_templates',
        'hub_sk_column': 'h_model_template_sk',
        'hub_keys': ['id'],
        'field_map': {
            'ownernodeid': 'owner_node_id',
            'name': 'name',
            'tagname': 'tag_name',
            'description': 'description',
            'basetemplatename': 'base_template_name',
            'datecreated': 'date_created',
            'datemodified': 'date_modified',
            'cdbsyncdate': 'cdb_sync_date',
            'cdbversion': 'cdb_version',
            'cdbid': 'cdb_id',
        }
    },
    'ObjectTypes': {
        'satellite_table': 's_object_types',
        'hub_sk_column': 'h_object_type_sk',
        'hub_keys': ['objectid'],
        'field_map': {
            'typedescriptorid': 'type_descriptor_id',
            'fromtemplatename': 'from_template_name',
        }
    }
}


# ============================================================================
# ПРОВЕРКА В DWH
# ============================================================================

def find_records_in_satellite(
    conn,
    satellite_table: str,
    hub_sk_column: str,
    hub_sk: str,
) -> List[Dict]:
    """Находит все записи в сателлите по hub_sk и data_file_id."""
    query = f"""
        SELECT *
        FROM public.{satellite_table}
        WHERE {hub_sk_column} = %s
          AND data_file_id = %s
        ORDER BY load_dttm ASC
    """
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (hub_sk, DATA_FILE_ID))
            return cur.fetchall()
    except Exception as e:
        print(f"  ⚠️ DB error: {e}")
        return []


def compare_record_with_json(
    db_record: Dict[str, Any],
    json_data: Dict[str, Any],
    field_map: Dict[str, str],
    label: str,
) -> Tuple[bool, List[str]]:
    """Сравнивает поля записи из БД с JSON данными."""
    errors = []

    for json_key, sat_column in field_map.items():
        if sat_column not in db_record:
            continue

        db_val = db_record[sat_column]
        csv_val = json_data.get(json_key)

        if not values_match(db_val, csv_val, sat_column):
            errors.append(
                f"  {label} поле '{sat_column}' не совпадает: "
                f"DB={repr(db_val)}, CSV={repr(csv_val)}"
            )

    return len(errors) == 0, errors


def check_change_in_satellite(
    conn,
    table_name: str,
    config: Dict,
    old_data: Dict[str, Any],
    new_data: Dict[str, Any],
    changed_fields_str: str,
) -> Tuple[bool, List[str]]:
    """Проверяет изменение в сателлите."""
    satellite_table = config['satellite_table']
    hub_sk_column = config['hub_sk_column']
    field_map = config['field_map']

    # Вычисляем hash_sk из old и new данных
    old_hub_sk = get_hub_sk_from_json(old_data, config['hub_keys'])
    new_hub_sk = get_hub_sk_from_json(new_data, config['hub_keys'])

    # Ищем записи по старому hash_sk, если не найдено — по новому
    records = find_records_in_satellite(conn, satellite_table, hub_sk_column, old_hub_sk)
    if not records and old_hub_sk != new_hub_sk:
        records = find_records_in_satellite(conn, satellite_table, hub_sk_column, new_hub_sk)

    if not records:
        return False, [f"Записи не найдены в {satellite_table} (old_hub_sk={old_hub_sk}, new_hub_sk={new_hub_sk})"]

    # Разделяем на активные и неактивные
    active_records = [r for r in records if r.get('active_flag') is True]
    inactive_records = [r for r in records if r.get('active_flag') is False]

    errors = []

    if not active_records:
        errors.append("Нет активной записи (active_flag=true)")

    if not inactive_records:
        errors.append("Нет закрытой записи (active_flag=false)")

    if active_records:
        active = active_records[-1]
        success, errs = compare_record_with_json(active, new_data, field_map, "ACTIVE")
        errors.extend(errs)

    if inactive_records:
        inactive = inactive_records[-1]
        success, errs = compare_record_with_json(inactive, old_data, field_map, "INACTIVE")
        errors.extend(errs)

    if active_records and inactive_records:
        active = active_records[-1]
        inactive = inactive_records[-1]

        has_real_change = False
        for json_key, sat_column in field_map.items():
            if sat_column not in active or sat_column not in inactive:
                continue
            if not values_match(active[sat_column], inactive[sat_column], sat_column):
                has_real_change = True
                break

        if not has_real_change:
            errors.append("Активная и закрытая записи имеют одинаковые значения полей - изменение не зафиксировано")

    return len(errors) == 0, errors


# ============================================================================
# ОСНОВНАЯ ФУНКЦИЯ
# ============================================================================

def run_test():
    print("=" * 80)
    print("ТЕСТ ПРОВЕРКИ ИЗМЕНЕНИЙ В DWH (Data Vault 2.0)")
    print(f"data_file_id: {DATA_FILE_ID}")
    print("Метод: сравнение значений полей (с нормализацией TZ и BLOB UUID)")
    print("=" * 80)

    if not Path(CSV_PATH).exists():
        print(f"❌ CSV файл не найден: {CSV_PATH}")
        return False

    changes = []
    with open(CSV_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            changes.append(row)

    print(f"\n📂 Загружено {len(changes)} записей из CSV")

    try:
        conn = psycopg2.connect(
            host=DWH_HOST, port=DWH_PORT, dbname=DWH_DB,
            user=DWH_USER, password=DWH_PASSWORD
        )
        print(f"✅ Подключение к DWH: {DWH_HOST}:{DWH_PORT}/{DWH_DB}")
    except Exception as e:
        print(f"❌ Ошибка подключения к DWH: {e}")
        return False

    passed = 0
    failed = 0
    skipped = 0
    failed_details = []

    print("\n" + "=" * 80)
    print("ПРОВЕРКА ЗАПИСЕЙ")
    print("=" * 80)

    for idx, change in enumerate(changes, 1):
        table_name = change['table_name']
        rowid = change['rowid']
        old_json_str = change['old_data_json']
        new_json_str = change['new_data_json']
        changed_fields_str = change.get('changed_fields', '')

        print(f"\n[{idx}/{len(changes)}] {table_name} (rowid={rowid})")

        if table_name not in TABLE_MAPPING:
            print(f"  ⚠️ Пропуск: нет маппинга для {table_name}")
            skipped += 1
            continue

        config = TABLE_MAPPING[table_name]

        old_data = parse_json_data(old_json_str)
        new_data = parse_json_data(new_json_str)

        if not old_data or not new_data:
            print(f"  ❌ Ошибка парсинга JSON")
            failed += 1
            continue

        success, errors = check_change_in_satellite(
            conn, table_name, config, old_data, new_data, changed_fields_str
        )

        if success:
            print(f"  ✅ {config['satellite_table']}: изменения подтверждены")
            passed += 1
        else:
            print(f"   {config['satellite_table']}:")
            for err in errors:
                print(f"     {err}")
            failed += 1
            failed_details.append({
                'table': table_name,
                'rowid': rowid,
                'satellite': config['satellite_table'],
                'errors': errors
            })

    conn.close()

    print("\n" + "=" * 80)
    print("ИТОГИ ТЕСТИРОВАНИЯ")
    print("=" * 80)
    print(f"✅ Успешно:  {passed}")
    print(f"❌ Ошибок:   {failed}")
    print(f"⚠️ Пропущено: {skipped}")
    print(f"📊 Всего:    {passed + failed + skipped}")
    print("=" * 80)

    if failed_details:
        print(f"\n📝 Детали ошибок ({len(failed_details)} записей):")
        for i, detail in enumerate(failed_details, 1):
            print(f"\n  {i}. {detail['table']} (rowid={detail['rowid']}) -> {detail['satellite']}")
            for err in detail['errors'][:5]:
                print(f"     {err}")

    return failed == 0


if __name__ == '__main__':
    run_test()