#!/usr/bin/env python3
"""
Тест проверки изменений в Link-таблицах DWH по методологии Data Vault 2.0.

При изменении связей в source-таблицах (MeasureUnits, ObjectProperties, 
ObjectPropertyDescriptors) проверяется, что в соответствующих Link-таблицах:
1. Старая связь закрыта (active_flag=false, valid_to_dttm установлен)
2. Новая связь активна (active_flag=true, valid_to_dttm=NULL)

Входной CSV: файл changed_links_guid_*.csv из скрипта DataAuditManager.
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

# CSV с изменениями связей (формат из DataAuditManager.save_to_csv → changed_links_guid_*.csv)
CSV_PATH = r'D:\ml-sandbox\audit_logs\changed_links_guid_20260622_100430.csv'

DWH_HOST = 'localhost'
DWH_PORT = 7000
DWH_DB = 'dwh'
DWH_USER = 'gpadmin'
DWH_PASSWORD = ''


# ============================================================================
# МАППИНГ LINK-ТАБЛИЦ
# ============================================================================
# Ключ: (source_table_name, changed_field_name) — как в CSV
# Значение: конфигурация link-таблицы
LINK_TABLE_MAPPING = {
    # MeasureUnits.MeasureGroupID → l_measure_units_measure_groups
    ('MeasureUnits', 'MeasureGroupID'): {
        'link_table': 'l_measure_units_measure_groups',
        'source_hub_sk_column': 'h_measure_unit_sk',
        'target_hub_sk_column': 'h_measure_group_sk',
        'source_hub_keys': ['id'],           # бизнес-ключ MeasureUnits
        'target_hub_keys': ['id'],           # бизнес-ключ MeasureGroups
    },
    # ObjectProperties.ObjectID → l_object_property_descriptors_objects 
    # (или l_object_properties_objects — уточнить по фактической схеме)
    ('ObjectProperties', 'ObjectID'): {
        'link_table': 'l_object_property_descriptors_objects',
        'source_hub_sk_column': 'h_object_property_sk',
        'target_hub_sk_column': 'h_object_sk',
        'source_hub_keys': ['propertyid'],
        'target_hub_keys': ['id'],
    },
    # ObjectProperties.MeasureUnitID → l_measure_units_object_properties
    ('ObjectProperties', 'MeasureUnitID'): {
        'link_table': 'l_measure_units_object_properties',
        'source_hub_sk_column': 'h_object_property_sk',
        'target_hub_sk_column': 'h_measure_unit_sk',
        'source_hub_keys': ['propertyid'],
        'target_hub_keys': ['id'],
    },
    # ObjectPropertyDescriptors.MeasureUnitID → связь с MeasureUnits
    ('ObjectPropertyDescriptors', 'MeasureUnitID'): {
        'link_table': 'l_object_property_descriptors_measure_units',
        'source_hub_sk_column': 'h_object_property_descriptor_sk',
        'target_hub_sk_column': 'h_measure_unit_sk',
        'source_hub_keys': ['propertydescriptorid'],
        'target_hub_keys': ['id'],
    },
}


# ============================================================================
# НОРМАЛИЗАЦИЯ GUID (как в тесте сателлитов + ETL)
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
    """
    Нормализует GUID из 16 байт с little-endian byte swap (.NET формат).
    Именно так ETL нормализует BLOB GUID перед хэшированием.
    """
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
    
    if is_blob_string(val):
        blob_bytes = parse_blob_to_bytes(val)
        if blob_bytes and len(blob_bytes) == 16:
            return normalize_guid_bytes(blob_bytes)
        return None
    
    if isinstance(val, (bytes, bytearray, memoryview)):
        if len(val) == 16:
            return normalize_guid_bytes(val)
        return None
    
    if isinstance(val, uuid.UUID):
        return str(val).lower()
    
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        if len(s) == 32 and all(c in '0123456789abcdefABCDEF' for c in s):
            s = f"{s[:8]}-{s[8:12]}-{s[12:16]}-{s[16:20]}-{s[20:]}"
        if re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', s, re.I):
            return s.lower()
        return None
    
    return None


def normalize_hex_guid_from_sqlite(hex_guid: str) -> Optional[str]:
    """
    Конвертирует hex GUID из SQLite BLOB (сохранённый как .hex()) 
    в стандартный UUID-строку с little-endian byte swap.
    
    В CSV changed_links_guid_*.csv GUID записаны через blob_id.hex() — 
    это сырой hex байтов BLOB. Для соответствия ETL нужно применить 
    little-endian byte swap (как в normalize_guid_bytes).
    """
    if not hex_guid or hex_guid.strip().lower() in ('none', 'null', ''):
        return None
    
    hex_guid = hex_guid.strip()
    
    # Если уже в UUID-формате — возвращаем как есть
    if '-' in hex_guid:
        if re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', hex_guid, re.I):
            return hex_guid.lower()
        return None
    
    # Hex строка (32 символа) → bytes → normalize с byte swap
    if len(hex_guid) == 32 and all(c in '0123456789abcdefABCDEF' for c in hex_guid):
        try:
            b = bytes.fromhex(hex_guid)
            return normalize_guid_bytes(b)
        except Exception:
            return None
    
    return None


# ============================================================================
# НОРМАЛИЗАЦИЯ ЗНАЧЕНИЙ ДЛЯ ХЭШИРОВАНИЯ
# ============================================================================

def normalize_value_for_hash(val: Any) -> str:
    """
    Нормализация значения для хэширования (как в ETL common_functions.py).
    """
    if val is None:
        return ''
    
    if is_blob_string(val):
        blob_bytes = parse_blob_to_bytes(val)
        if blob_bytes and len(blob_bytes) == 16:
            return normalize_guid_bytes(blob_bytes)
        return ''
    
    if isinstance(val, (bytes, bytearray, memoryview)):
        if len(val) == 16:
            return normalize_guid_bytes(val)
        return val.hex()
    
    if isinstance(val, uuid.UUID):
        return str(val).lower()
    
    if isinstance(val, Decimal):
        if val == int(val):
            return str(int(val))
        return str(val)
    
    if isinstance(val, bool):
        return str(val).lower()
    
    if isinstance(val, (int, float)):
        if isinstance(val, float) and val.is_integer():
            return str(int(val))
        return str(val)
    
    s = str(val).strip()
    if s.lower() in ('nan', 'none', 'null', 'nat', '<na>', ''):
        return ''
    
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
    Расчёт Hub Surrogate Key.
    Формула: MD5(normalized_value_1|normalized_value_2|...|data_file_id)
    """
    filtered = []
    for v in values.values():
        normalized = normalize_value_for_hash(v)
        if normalized != '':
            filtered.append(normalized)
    raw_string = '|'.join(filtered)
    md5_hex = hashlib.md5(raw_string.encode('utf-8')).hexdigest()
    return md5_hex_to_uuid(md5_hex)


def calc_hub_sk_from_hex_guid(hex_guid: str, hub_keys: List[str]) -> Optional[str]:
    """
    Вычисляет hub_sk из hex GUID (как в ETL при загрузке из SQLite).
    
    hex_guid: GUID в формате hex (32 символа) из CSV changed_links_guid_*.csv
    hub_keys: список бизнес-ключей (например, ['id'] для MeasureUnits)
    
    Возвращает hub_sk в строковом формате UUID.
    """
    normalized_guid = normalize_hex_guid_from_sqlite(hex_guid)
    if not normalized_guid:
        return None
    
    values = {}
    for key in hub_keys:
        values[key] = normalized_guid
    values['data_file_id'] = DATA_FILE_ID
    
    return str(calc_hash_sk(values))


# ============================================================================
# ПРОВЕРКА В DWH
# ============================================================================

def find_link_records(
    conn,
    link_table: str,
    source_hub_sk_column: str,
    target_hub_sk_column: str,
    source_hub_sk: str,
    target_hub_sk: str,
) -> List[Dict]:
    """Находит все записи в link-таблице по паре hub_sk и data_file_id."""
    query = f"""
        SELECT *
        FROM public.{link_table}
        WHERE {source_hub_sk_column} = %s
          AND {target_hub_sk_column} = %s
          AND data_file_id = %s
        ORDER BY load_dttm ASC
    """
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (source_hub_sk, target_hub_sk, DATA_FILE_ID))
            return cur.fetchall()
    except psycopg2.Error as e:
        print(f"  ⚠️ DB error (table={link_table}): {e}")
        conn.rollback()
        return []


def check_link_change(
    conn,
    table_name: str,
    field_name: str,
    record_guid: str,
    old_guid: str,
    new_guid: str,
) -> Tuple[bool, List[str]]:
    """
    Проверяет изменение связи в link-таблице.
    
    Ожидается:
    - Запись (source_sk, old_target_sk): active_flag=false, valid_to_dttm IS NOT NULL
    - Запись (source_sk, new_target_sk): active_flag=true, valid_to_dttm IS NULL
    """
    key = (table_name, field_name)
    if key not in LINK_TABLE_MAPPING:
        return False, [f"Нет маппинга для пары ({table_name}, {field_name})"]
    
    config = LINK_TABLE_MAPPING[key]
    link_table = config['link_table']
    source_col = config['source_hub_sk_column']
    target_col = config['target_hub_sk_column']
    source_keys = config['source_hub_keys']
    target_keys = config['target_hub_keys']
    
    # Вычисляем hub_sk для source записи
    source_hub_sk = calc_hub_sk_from_hex_guid(record_guid, source_keys)
    if not source_hub_sk:
        return False, [f"Не удалось вычислить source_hub_sk из record_guid={record_guid}"]
    
    errors = []
    
    # ── Проверяем СТАРУЮ связь (должна быть закрыта) ──
    old_guid_clean = old_guid.strip() if old_guid else ''
    if old_guid_clean and old_guid_clean.lower() not in ('none', 'null', ''):
        old_target_sk = calc_hub_sk_from_hex_guid(old_guid_clean, target_keys)
        if old_target_sk:
            old_records = find_link_records(
                conn, link_table, source_col, target_col,
                source_hub_sk, old_target_sk
            )
            
            if not old_records:
                errors.append(
                    f"❌ Старая связь НЕ найдена в {link_table}\n"
                    f"      source_sk={source_hub_sk}\n"
                    f"      target_sk={old_target_sk} (old_guid={old_guid_clean[:16]}...)"
                )
            else:
                last_old = old_records[-1]
                is_closed = last_old.get('active_flag') is False
                has_valid_to = last_old.get('valid_to_dttm') is not None
                
                if not is_closed:
                    errors.append(
                        f"❌ Старая связь НЕ закрыта: active_flag={last_old.get('active_flag')} (ожидается false)\n"
                        f"      load_dttm={last_old.get('load_dttm')}"
                    )
                if not has_valid_to:
                    errors.append(
                        f"❌ У старой связи valid_to_dttm=NULL (ожидается заполнено)"
                    )
    
    # ── Проверяем НОВУЮ связь (должна быть активна) ──
    new_guid_clean = new_guid.strip() if new_guid else ''
    if new_guid_clean and new_guid_clean.lower() not in ('none', 'null', ''):
        new_target_sk = calc_hub_sk_from_hex_guid(new_guid_clean, target_keys)
        if new_target_sk:
            new_records = find_link_records(
                conn, link_table, source_col, target_col,
                source_hub_sk, new_target_sk
            )
            
            if not new_records:
                errors.append(
                    f"❌ Новая связь НЕ найдена в {link_table}\n"
                    f"      source_sk={source_hub_sk}\n"
                    f"      target_sk={new_target_sk} (new_guid={new_guid_clean[:16]}...)"
                )
            else:
                last_new = new_records[-1]
                is_active = last_new.get('active_flag') is True
                has_null_valid_to = last_new.get('valid_to_dttm') is None
                
                if not is_active:
                    errors.append(
                        f"❌ Новая связь НЕ активна: active_flag={last_new.get('active_flag')} (ожидается true)\n"
                        f"      load_dttm={last_new.get('load_dttm')}"
                    )
                if not has_null_valid_to:
                    errors.append(
                        f"❌ У новой связи valid_to_dttm={last_new.get('valid_to_dttm')} (ожидается NULL)"
                    )
    
    return len(errors) == 0, errors


# ============================================================================
# ОСНОВНАЯ ФУНКЦИЯ
# ============================================================================

def run_test():
    print("=" * 80)
    print("ТЕСТ ПРОВЕРКИ ИЗМЕНЕНИЙ В LINK-ТАБЛИЦАХ DWH (Data Vault 2.0)")
    print(f"data_file_id: {DATA_FILE_ID}")
    print(f"CSV: {CSV_PATH}")
    print("Метод: проверка active_flag/valid_to_dttm для старой и новой связи")
    print("=" * 80)
    
    if not Path(CSV_PATH).exists():
        print(f"❌ CSV файл не найден: {CSV_PATH}")
        return False
    
    # Загружаем CSV
    changes = []
    with open(CSV_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            changes.append(row)
    
    print(f"\n📂 Загружено {len(changes)} записей из CSV")
    
    # Подключение к DWH
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
    
    # Группируем по link-таблицам для статистики
    link_table_stats = {}
    
    print("\n" + "=" * 80)
    print("ПРОВЕРКА LINK-ЗАПИСЕЙ")
    print("=" * 80)
    
    for idx, change in enumerate(changes, 1):
        table_name = change.get('table_name', '').strip()
        field_name = change.get('field_name', '').strip()
        record_guid = change.get('record_guid', '').strip()
        old_guid = change.get('old_guid', '').strip()
        new_guid = change.get('new_guid', '').strip()
        
        key = (table_name, field_name)
        
        # Определяем link-таблицу
        if key not in LINK_TABLE_MAPPING:
            print(f"\n[{idx}/{len(changes)}] ⚠️ {table_name}.{field_name} — пропуск (нет маппинга)")
            skipped += 1
            continue
        
        config = LINK_TABLE_MAPPING[key]
        link_table = config['link_table']
        
        if link_table not in link_table_stats:
            link_table_stats[link_table] = {'passed': 0, 'failed': 0}
        
        print(f"\n[{idx}/{len(changes)}] {table_name}.{field_name}")
        print(f"   Record GUID: {record_guid[:16]}...")
        print(f"   Link table:  {link_table}")
        
        if old_guid and old_guid.lower() not in ('none', 'null', ''):
            print(f"   Old target:  {old_guid[:16]}...")
        else:
            print(f"   Old target:  (пусто — связь не существовала)")
        
        if new_guid and new_guid.lower() not in ('none', 'null', ''):
            print(f"   New target:  {new_guid[:16]}...")
        else:
            print(f"   New target:  (пусто — связь удалена)")
        
        success, errors = check_link_change(
            conn, table_name, field_name, record_guid, old_guid, new_guid
        )
        
        if success:
            print(f"   ✅ {link_table}: изменения связи подтверждены")
            passed += 1
            link_table_stats[link_table]['passed'] += 1
        else:
            print(f"   ❌ {link_table}:")
            for err in errors:
                for line in err.split('\n'):
                    print(f"      {line}")
            failed += 1
            link_table_stats[link_table]['failed'] += 1
            failed_details.append({
                'table': table_name,
                'field': field_name,
                'record_guid': record_guid,
                'link_table': link_table,
                'errors': errors
            })
    
    conn.close()
    
    # ── ИТОГИ ──
    print("\n" + "=" * 80)
    print("ИТОГИ ТЕСТИРОВАНИЯ LINK-ТАБЛИЦ")
    print("=" * 80)
    print(f"✅ Успешно:   {passed}")
    print(f"❌ Ошибок:    {failed}")
    print(f"⚠️  Пропущено: {skipped}")
    print(f"📊 Всего:     {passed + failed + skipped}")
    
    if link_table_stats:
        print("\n📋 Детализация по link-таблицам:")
        for lt, stats in sorted(link_table_stats.items()):
            status = "✅" if stats['failed'] == 0 else "❌"
            print(f"   {status} {lt}: {stats['passed']} passed, {stats['failed']} failed")
    
    print("=" * 80)
    
    if failed_details:
        print(f"\n📝 Детали ошибок ({len(failed_details)} записей):")
        for i, detail in enumerate(failed_details, 1):
            print(f"\n  {i}. {detail['table']}.{detail['field']} → {detail['link_table']}")
            print(f"     record_guid: {detail['record_guid'][:32]}...")
            for err in detail['errors'][:3]:
                first_line = err.split('\n')[0]
                print(f"     • {first_line}")
    
    return failed == 0


if __name__ == '__main__':
    run_test()