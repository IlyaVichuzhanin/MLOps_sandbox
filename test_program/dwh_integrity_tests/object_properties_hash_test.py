import sqlite3
import tempfile
import os
import logging
import requests
import uuid
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from pathlib import Path
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from typing import Dict, Any, Optional, Union
import hashlib
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# =============================================================================
# КОНФИГУРАЦИЯ
# =============================================================================
DWH_HOST = 'localhost'
DWH_PORT = 7000
DWH_DB = 'dwh'
DWH_USER = 'gpadmin'
DWH_PASSWORD = ''

HDFS_HOST = 'localhost'
HDFS_PORT = 9870
HDFS_USER = 'hdfs'
HDFS_BASE_URL = f'http://{HDFS_HOST}:{HDFS_PORT}/webhdfs/v1'

HDFS_HOSTNAME_MAP = {
    'datanode1': 'localhost', 'datanode2': 'localhost', 'datanode3': 'localhost',
    'namenode': 'localhost', '172.21.0.31': 'localhost',
}

# =============================================================================
# 🔥 ИСПРАВЛЕНИЕ: Нормализация UUID
# =============================================================================

def normalize_uuid_to_string(value: Any) -> str:
    """
    🔥 КЛЮЧЕВАЯ ФУНКЦИЯ: Конвертирует любое представление UUID в строку.
    
    Обрабатывает:
    - bytes (16 байт из SQLite BLOB) → UUID строка
    - uuid.UUID объект → строка
    - строка UUID → strip().lower()
    - другие типы → str(value).strip().lower()
    """
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return ''
    
    # 🔥 Если bytes (16 байт из SQLite) — конвертируем в UUID
    if isinstance(value, (bytes, bytearray)):
        if len(value) == 16:
            try:
                return str(uuid.UUID(bytes=bytes(value)))
            except ValueError:
                # Если не валидный UUID, используем hex
                return value.hex()
        else:
            # Нестандартная длина — используем hex
            return value.hex()
    
    # Если UUID объект
    if isinstance(value, uuid.UUID):
        return str(value)
    
    # Если строка
    if isinstance(value, str):
        value = value.strip()
        # Пытаемся распарсить как UUID
        try:
            return str(uuid.UUID(value))
        except ValueError:
            return value.lower()
    
    # Fallback
    return str(value).strip().lower()


# =============================================================================
# 🔥 ИСПРАВЛЕННЫЕ ХЕШ-ФУНКЦИИ
# =============================================================================

def md5_hex_to_uuid(hex_string: str) -> uuid.UUID:
    """Конвертирует MD5 хеш в UUID."""
    if not hex_string or len(hex_string) != 32:
        return uuid.uuid4()
    uuid_str = f"{hex_string[:8]}-{hex_string[8:12]}-{hex_string[12:16]}-{hex_string[16:20]}-{hex_string[20:]}"
    return uuid.UUID(uuid_str)


def calc_hash_sk_FIXED(values: dict) -> uuid.UUID:
    """
    🔥 ИСПРАВЛЕННАЯ версия calc_hash_sk.
    Использует normalize_uuid_to_string для правильной обработки UUID.
    """
    # 🔥 Нормализуем каждое значение перед хешированием
    normalized_values = [normalize_uuid_to_string(v) for v in values.values() if v is not None and not (isinstance(v, float) and np.isnan(v))]
    raw_string = '|'.join(normalized_values)
    
    md5_hex = hashlib.md5(raw_string.encode('utf-8')).hexdigest()
    return md5_hex_to_uuid(md5_hex)


def calc_hub_hash_for_column_FIXED(
    row: dict,
    column_name: str,
    is_system_type_data: bool,
    prefix: str,
    data_file_id: uuid.UUID
) -> Optional[uuid.UUID]:
    """
    🔥 ИСПРАВЛЕННАЯ версия calc_hub_hash_for_column.
    Использует normalize_uuid_to_string для правильной обработки UUID.
    """
    raw_value = row.get(column_name)
    
    # 🔥 Нормализуем значение
    if raw_value is None or (isinstance(raw_value, float) and np.isnan(raw_value)):
        column_value = 'null'
    elif isinstance(raw_value, (int, np.integer)):
        column_value = str(int(raw_value))
    elif isinstance(raw_value, (float, np.floating)):
        if np.isnan(raw_value):
            column_value = 'null'
        elif raw_value.is_integer():
            column_value = str(int(raw_value))
        else:
            column_value = str(raw_value).strip().lower()
    else:
        # 🔥 КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ: используем normalize_uuid_to_string
        column_value = normalize_uuid_to_string(raw_value)
    
    if is_system_type_data:
        inner_input = f"{prefix}|{column_value}"
    else:
        inner_input = f"{column_value}|{str(data_file_id).lower()}"
    
    inner_md5 = hashlib.md5(inner_input.encode('utf-8')).hexdigest()
    return md5_hex_to_uuid(inner_md5)


def calc_link_hash_FIXED(
    row: dict,
    link_column: str,
    is_system_type_data: bool,
    prefix: str,
    data_file_id: uuid.UUID,
    hash_column_name: str = 'hash_sk'
) -> Optional[uuid.UUID]:
    """
    🔥 ИСПРАВЛЕННАЯ версия calc_link_hash.
    """
    raw_value = row.get(link_column)
    link_value = normalize_uuid_to_string(raw_value) if raw_value is not None else ''
    
    hash_sk = str(row.get(hash_column_name, ''))
    
    if is_system_type_data:
        inner_input = f"{prefix}|{link_value}"
    else:
        inner_input = f"{link_value}|{str(data_file_id).lower()}"
    
    inner_md5 = hashlib.md5(inner_input.encode('utf-8')).hexdigest()
    final_input = f"{hash_sk}|{inner_md5}"
    md5_hex = hashlib.md5(final_input.encode('utf-8')).hexdigest()
    return md5_hex_to_uuid(md5_hex)


# =============================================================================
# ФУНКЦИИ ДЛЯ РАБОТЫ С HDFS
# =============================================================================

def _rewrite_hdfs_url(url: str) -> str:
    parsed = urlparse(url)
    hostname = parsed.hostname
    if hostname in HDFS_HOSTNAME_MAP:
        new_hostname = HDFS_HOSTNAME_MAP[hostname]
    else:
        new_hostname = hostname
    port = parsed.port
    if new_hostname != hostname:
        new_netloc = f"{new_hostname}:{port}" if port else new_hostname
        new_parsed = parsed._replace(netloc=new_netloc)
        return urlunparse(new_parsed)
    return url


def download_file_from_hdfs(hdfs_path: str) -> str:
    log.info(f"📥 Скачивание файла из HDFS: {hdfs_path}")
    
    status_url = f"{HDFS_BASE_URL}{hdfs_path}?op=GETFILESTATUS&user.name={HDFS_USER}"
    response = requests.get(status_url, timeout=30)
    response.raise_for_status()
    file_size = response.json()['FileStatus']['length']
    
    probe_url = f"{HDFS_BASE_URL}{hdfs_path}?op=OPEN&user.name={HDFS_USER}&offset=0&length=1"
    session = requests.Session()
    response = session.get(probe_url, stream=True, allow_redirects=False, timeout=30)
    
    datanode_base_url = None
    for _ in range(5):
        if response.status_code not in (301, 302, 303, 307, 308):
            datanode_base_url = response.url
            response.close()
            break
        location = response.headers.get('Location')
        rewritten_url = _rewrite_hdfs_url(location)
        response.close()
        response = session.get(rewritten_url, stream=True, allow_redirects=False, timeout=30)
    else:
        datanode_base_url = response.url
        response.close()
    session.close()
    
    parsed = urlparse(datanode_base_url)
    query_params = parse_qs(parsed.query)
    query_params.pop('offset', None)
    query_params.pop('length', None)
    clean_query = urlencode({k: v[0] for k, v in query_params.items()})
    datanode_clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{clean_query}"
    
    suffix = Path(hdfs_path).suffix or '.db'
    temp_file = tempfile.NamedTemporaryFile(mode='wb', suffix=suffix, delete=False)
    temp_path = temp_file.name
    
    HDFS_DOWNLOAD_CHUNK_SIZE = 500 * 1024 * 1024
    offset = 0
    chunk_num = 0
    total_chunks = (file_size + HDFS_DOWNLOAD_CHUNK_SIZE - 1) // HDFS_DOWNLOAD_CHUNK_SIZE
    
    while offset < file_size:
        chunk_num += 1
        current_chunk_size = min(HDFS_DOWNLOAD_CHUNK_SIZE, file_size - offset)
        chunk_url = f"{datanode_clean_url}&offset={offset}&length={current_chunk_size}"
        log.info(f"   📦 Чанк {chunk_num}/{total_chunks}")
        
        response = requests.get(chunk_url, stream=False, timeout=(30, 600))
        response.raise_for_status()
        temp_file.write(response.content)
        offset += len(response.content)
    
    temp_file.flush()
    os.fsync(temp_file.fileno())
    temp_file.close()
    
    log.info(f"✅ Файл скачан: {temp_path}")
    return temp_path


# =============================================================================
# ФУНКЦИИ ДЛЯ РАБОТЫ С DWH
# =============================================================================

def get_dwh_engine() -> Engine:
    db_url = f"postgresql+psycopg2://{DWH_USER}:{DWH_PASSWORD}@{DWH_HOST}:{DWH_PORT}/{DWH_DB}"
    return create_engine(db_url, echo=False, pool_pre_ping=True, pool_size=5, max_overflow=10)


def get_data_file_id_from_sqlite(sqlite_path: str) -> uuid.UUID:
    """Получает data_file_id из таблицы DatabaseIdent в SQLite."""
    conn = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()
    cursor.execute("SELECT ID FROM DatabaseIdent LIMIT 1")
    row = cursor.fetchone()
    conn.close()
    
    if row is None:
        raise ValueError("Таблица DatabaseIdent пуста")
    
    raw_id = row[0]
    # 🔥 Используем normalize_uuid_to_string
    return uuid.UUID(normalize_uuid_to_string(raw_id))


# =============================================================================
# 🔥 ДИАГНОСТИКА С ИСПРАВЛЕННЫМИ ФУНКЦИЯМИ
# =============================================================================

def diagnose_with_fixes(sqlite_path: str, data_file_id: uuid.UUID):
    """Диагностика с использованием исправленных функций."""
    print("\n" + "="*100)
    print("🔧 ДИАГНОСТИКА С ИСПРАВЛЕННЫМИ ФУНКЦИЯМИ")
    print("="*100)
    
    conn = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()
    
    # 🔥 Читаем с конвертацией bytes → UUID
    print("\n📋 Анализ таблицы ObjectProperties (с конвертацией bytes → UUID):")
    print("-"*100)
    
    cursor.execute("""
        SELECT PropertyID, ObjectID, PropertyDescriptorID
        FROM ObjectProperties
        LIMIT 5
    """)
    
    rows = cursor.fetchall()
    
    for idx, row in enumerate(rows):
        prop_id_raw = row[0]
        
        # 🔥 Показываем как нормализуется UUID
        prop_id_normalized = normalize_uuid_to_string(prop_id_raw)
        
        print(f"\n   [{idx}] PropertyID (raw): {repr(prop_id_raw)[:60]}...")
        print(f"       PropertyID (normalized): {prop_id_normalized}")
        
        # 🔥 Хешируем с исправленной функцией
        values = {
            'propertyid': prop_id_raw,
            'data_file_id': data_file_id
        }
        hash_sk_fixed = calc_hash_sk_FIXED(values)
        
        # Показываем raw_string для отладки
        normalized_values = [normalize_uuid_to_string(v) for v in values.values()]
        raw_string = '|'.join(normalized_values)
        
        print(f"       raw_string (FIXED): '{raw_string}'")
        print(f"       hash_sk (FIXED):    {hash_sk_fixed}")
    
    # 🔥 Анализ DiagnosticAlarms
    print("\n📋 Анализ таблицы DiagnosticAlarms_v2 (с конвертацией bytes → UUID):")
    print("-"*100)
    
    cursor.execute("""
        SELECT PropertyID, DiagAlarmID, DiagID
        FROM DiagnosticAlarmHistory_v2
        LIMIT 5
    """)
    
    rows = cursor.fetchall()
    
    for idx, row in enumerate(rows):
        prop_id_raw = row[0]
        prop_id_normalized = normalize_uuid_to_string(prop_id_raw)
        
        print(f"\n   [{idx}] PropertyID (raw): {repr(prop_id_raw)[:60]}...")
        print(f"       PropertyID (normalized): {prop_id_normalized}")
        
        # 🔥 Хешируем для Link с исправленной функцией
        row_dict = {'propertyid': prop_id_raw}
        propertyid_sk_fixed = calc_hub_hash_for_column_FIXED(
            row_dict, 'propertyid', False, '', data_file_id
        )
        
        # Показываем inner_input
        column_value = normalize_uuid_to_string(prop_id_raw)
        inner_input = f"{column_value}|{str(data_file_id).lower()}"
        
        print(f"       inner_input (FIXED): '{inner_input}'")
        print(f"       propertyid_sk (FIXED): {propertyid_sk_fixed}")
    
    # 🔥 Сравнение с DWH
    print("\n📋 Сравнение с DWH (проверка осиротевших записей):")
    print("-"*100)
    
    dwh_engine = get_dwh_engine()
    
    with dwh_engine.begin() as conn_dwh:
        # Получаем примеры SK из хаба
        result = conn_dwh.execute(text("""
            SELECT h_object_property_sk 
            FROM h_object_properties 
            LIMIT 5
        """))
        hub_sks = [str(row[0]) for row in result.fetchall()]
        
        print(f"\n   Примеры h_object_property_sk из DWH:")
        for sk in hub_sks:
            print(f"      {sk}")
        
        # Проверяем, сколько записей в линке имеют SK из хаба
        result = conn_dwh.execute(text("""
            SELECT COUNT(*) 
            FROM l_object_properties_diagnostic_alarms l
            WHERE h_object_property_sk IN (
                SELECT h_object_property_sk FROM h_object_properties
            )
        """))
        matched = result.fetchone()[0]
        
        result = conn_dwh.execute(text("""
            SELECT COUNT(*) 
            FROM l_object_properties_diagnostic_alarms
        """))
        total_in_link = result.fetchone()[0]
        
        print(f"\n   Всего записей в линке: {total_in_link}")
        print(f"   Записей с SK из хаба: {matched}")
        print(f"   Осиротевших записей: {total_in_link - matched}")
    
    conn.close()
    print("\n" + "="*100)


# =============================================================================
# ОСНОВНАЯ ФУНКЦИЯ
# =============================================================================

def main():
    print("🔍 Диагностика проблемы с хешами (ИСПРАВЛЕННАЯ ВЕРСИЯ)")
    print(f"📊 Подключение к DWH: {DWH_HOST}:{DWH_PORT}/{DWH_DB}")
    
    # Скачиваем файл
    hdfs_path = "/raw_data/Kriogen/MainDb/Kriogen_2026-06-16_15-40.db"
    temp_path = download_file_from_hdfs(hdfs_path)
    
    try:
        # Получаем data_file_id
        data_file_id = get_data_file_id_from_sqlite(temp_path)
        print(f"\n📁 data_file_id: {data_file_id}")
        
        # 🔥 Запускаем диагностику с исправленными функциями
        diagnose_with_fixes(temp_path, data_file_id)
        
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)
    
    print("\n🎉 Диагностика завершена!")


if __name__ == "__main__":
    main()