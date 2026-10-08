# airflow/dags/validate_data/common_tasks.py

# === Стандартная библиотека ===
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import StringIO
from typing import Dict, Any, List, Optional
import csv
import json
import ast
import logging
import sqlite3
import uuid
import struct
import binascii

# === Сторонние библиотеки ===
import numpy as np
import pandas as pd
import psycopg2
from clickhouse_driver import Client
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert

# === Airflow ===
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

# === Локальные модули ===
from validate_data.hdfs_utils import hdfs_tempfile
from validate_data.check_system_type_data import (
    check_table_exist,
    check_system_type_data_tables,
)
from functions.common_functions import (
    get_dwh_engine,
    decode_creyt_timeseries,
)

log = logging.getLogger(__name__)
DWH_CONN_ID = 'cloudberry_test_dwh'
cloudberry_engine = get_dwh_engine(DWH_CONN_ID)


# ═══════════════════════════════════════════════════════════════════
#  Константы формата .fh
# ═══════════════════════════════════════════════════════════════════

FH_MAGIC_HEX = 0xBEBAADDE
FH_MIN_HEADER_LENGTH = 30
FH_HEADER_DATA_OFFSET = 26
FH_FROM_DATE_OFFSET = 8
FH_TO_DATE_OFFSET = 16
FH_VERSION_OFFSET = 24
FH_EXTRA_MAGIC = 0x4B52494F  # "KRIO"

FH_QUALITY_LENGTH = 1
FH_VALUE_LENGTH = 8
FH_FIELD_LENGTH = FH_QUALITY_LENGTH + FH_VALUE_LENGTH  # 9 байт на точку
FH_ROW_ID_LENGTH = 16  # GUID

FH_SUPPORTED_VERSIONS = (1,)

# 🔑 .fh файлы всегда содержат данные типа Double
FH_DATA_TYPE = 2
FH_DATA_TYPE_NAME = "Double"


@task
def get_records_from_conf(**context) -> list[dict]:
    """Универсальная задача: получение и парсинг записей из conf"""
    
    # Получаем records из conf, по умолчанию — пустой список
    records = context['dag_run'].conf.get('records', [])
    
    # 🔥 Критически важно: обработка None и пустой строки ДО парсинга
    if records is None or (isinstance(records, str) and records.strip() == ""):
        log.info("⚠️ Записи не переданы (conf['records'] пустой), возвращаем пустой список")
        return []
    
    # Если строка — пытаемся распарсить
    if isinstance(records, str):
        try:
            records = json.loads(records)
            log.info(f"✅ Распарсено {len(records)} записей из JSON")
        except json.JSONDecodeError:
            try:
                records = ast.literal_eval(records)
                # ast.literal_eval("") возвращает None — проверяем!
                if records is None:
                    log.warning("⚠️ ast.literal_eval вернул None, возвращаем пустой список")
                    return []
                log.info(f"✅ Распарсено {len(records)} записей из Python-литерала")
            except (ValueError, SyntaxError) as e:
                log.error(f"❌ Ошибка парсинга: {e}")
                return []  # Возвращаем пустой список вместо падения
    
    # Гарантируем, что результат — список
    if not isinstance(records, list):
        log.warning(f"⚠️ Ожидался list[dict], получено {type(records).__name__}. Возвращаем пустой список.")
        return []
    
    # Валидация: первые 3 записи должны быть dict (для отладки)
    for i, rec in enumerate(records[:3]):
        if not isinstance(rec, dict):
            log.error(f"❌ Запись #{i} должна быть dict, получено {type(rec).__name__}")
            return []
    
    log.info(f"📦 Получено {len(records)} записей для обработки")
    return records


@task.short_circuit
def check_records(records: list, record_type: str = "записей") -> bool:
    """Универсальная проверка: есть ли записи для обработки"""
    if not records:
        log.info(f"✅ Нет {record_type} для обработки")
        return False
    return True


@task(task_id='get_record_info_from_conf')
def get_record_info_from_conf(**context) -> dict:
    """
    Извлекает информацию о файле из conf запуска DAG.
    Приоритет источников:
      1. conf['records'][0] — формат оркестратора
      2. conf['record']     — старый формат
      3. conf (прямой)      — conf['hdfs_full_path']
      4. params             — ручной запуск из UI
    """
    dag_run = context.get('dag_run')
    conf = dag_run.conf if dag_run else {}
    params = context.get('params', {}) or {}

    record = None
    source = "unknown"

    # 1. Оркестратор: conf = {'records': [...], 'record_type': '...'}
    records = conf.get('records', []) if conf else []
    if records and isinstance(records, list) and len(records) > 0:
        record = records[0]
        source = "conf['records'][0] (оркестратор)"

    # 2. Старый формат: conf = {'record': {...}}
    if not record and conf and isinstance(conf.get('record'), dict):
        record = conf['record']
        source = "conf['record'] (старый формат)"

    # 3. Прямой формат: conf = {'hdfs_full_path': '...', ...}
    if not record and conf and ('hdfs_full_path' in conf or 'hdfs_path' in conf):
        record = conf
        source = "conf (прямой JSON)"

    # 4. Ручной запуск из UI Airflow
    if not record and 'hdfs_path' in params:
        record = params
        source = "params (UI Airflow)"

    if not record:
        raise AirflowException(
            "Не удалось извлечь информацию о файле.\n"
            "Поддерживаемые форматы:\n"
            "  1. conf['records'][0] (оркестратор)\n"
            "  2. conf['record'] (старый формат)\n"
            "  3. conf['hdfs_full_path'] (прямой JSON)\n"
            "  4. params['hdfs_path'] (UI Airflow)"
        )

    # Извлекаем поля с fallback'ами
    hdfs_path = (
        record.get('hdfs_full_path')
        or record.get('hdfs_path')
        or params.get('hdfs_path')
    )

    data_file_id = record.get('data_file_id') or str(uuid.uuid4())
    data_source_id = record.get('data_source_id') or str(uuid.uuid4())
    main_db_id = record.get('main_db_id') or str(uuid.uuid4())
    main_db_name = record.get('main_db_name') or 'Kriogen'

    if not hdfs_path:
        raise AirflowException(
            "Не указан путь к файлу в HDFS.\n"
            "Передайте через:\n"
            "  - conf['records'][0]['hdfs_full_path'] (оркестратор)\n"
            "  - conf['record']['hdfs_full_path'] (старый формат)\n"
            "  - conf['hdfs_full_path'] (прямой JSON)\n"
            "  - params['hdfs_path'] (UI Airflow)"
        )

    if not hdfs_path.startswith('/'):
        raise AirflowException(f"Некорректный путь HDFS: {hdfs_path}")

    log.info(f"✓ Источник данных: {source}")
    log.info(f"✓ Файл HDFS: {hdfs_path}")
    log.info(f"  data_file_id:   {data_file_id}")
    log.info(f"  data_source_id: {data_source_id}")
    log.info(f"  main_db_id:     {main_db_id}")
    log.info(f"  main_db_name:   {main_db_name}")

    return {
        'hdfs_path': hdfs_path,
        'data_file_id': data_file_id,
        'data_source_id': data_source_id,
        'main_db_id': main_db_id,
        'main_db_name': main_db_name,
    }


@task(task_id='validate_sqlite_file')
def validate_sqlite_file(hdfs_path: str, tables_list: List[str]) -> Dict[str, Any]:
    """
    Универсальная валидация SQLite файла.
    :param tables_list: список обязательных таблиц для проверки
    """
    hook = WebHDFSHook(webhdfs_conn_id='hdfs_default')
    client = hook.get_conn()
    
    file_status = client.status(hdfs_path, strict=True)
    file_size = file_status.get('length', 0)
    
    if file_size == 0:
        raise ValueError(f"Файл {hdfs_path} пустой (размер 0 байт)")
    
    with client.read(hdfs_path, length=100) as reader:
        header = reader.read(100)
    is_valid_header = header.startswith(b'SQLite format 3\x00')
    
    is_valid_sqlite = False
    error_detail = None
    missing_tables = []
    
    with hdfs_tempfile(hdfs_path) as local_path:
        try:
            conn = sqlite3.connect(local_path)
            cursor = conn.cursor()
            cursor.execute("PRAGMA quick_check")
            integrity_result = cursor.fetchone()
            
            if integrity_result and integrity_result[0] == 'ok':
                is_valid_sqlite = True
            else:
                error_detail = f"PRAGMA quick_check failed: {integrity_result}"
            conn.close()
        except sqlite3.DatabaseError as e:
            error_detail = f"SQLite error: {str(e)}"
        except Exception as e:
            error_detail = f"Unexpected error: {str(e)}"
        
        if is_valid_sqlite:
            for table_name in tables_list:
                if not check_table_exist(table_name, local_path):
                    log.info(f"{table_name} doesn't exist in sqlite file!")
                    missing_tables.append(table_name)
    
    result = {
        'hdfs_path': hdfs_path,
        'file_size_bytes': file_size,
        'file_size_mb': round(file_size / 1024**2, 2),
        'header_valid': is_valid_header,
        'sqlite_valid': is_valid_sqlite,
        'error_detail': error_detail,
        'status': 'valid' if (is_valid_header and is_valid_sqlite) else 'invalid',
        'missing_tables': missing_tables
    }
    
    if not (is_valid_header and is_valid_sqlite):
        print(f"✗ Файл повреждён или не является SQLite: {hdfs_path}")
        raise AirflowException(f"Валидация файла не пройдена: {hdfs_path}")
    
    print(f"✓ Файл валиден: {hdfs_path} ({file_size / 1024**2:.2f} MB)")
    return result


# ═══════════════════════════════════════════════════════════════════
#  Вспомогательные функции для .fh
# ═══════════════════════════════════════════════════════════════════

def fh_ts_to_datetime(ts_ms: int) -> datetime:
    """Конвертирует Unix timestamp (мс) в datetime UTC."""
    return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=ts_ms)


def fh_compute_crc32(data: bytes) -> int:
    """Вычисляет CRC32 (совместимый с C# Crc32.Compute)."""
    return binascii.crc32(data) & 0xFFFFFFFF


def fh_guid_from_bytes_le(b: bytes) -> uuid.UUID:
    """Читает GUID в формате .NET (little-endian)."""
    return uuid.UUID(bytes_le=b)


@dataclass
class FhValidationResult:
    """Результат валидации .fh файла."""
    hdfs_path: str
    file_size_bytes: int = 0
    file_size_mb: float = 0.0
    
    # Проверки заголовка
    magic_valid: bool = False
    magic_value: str = ""
    header_length: int = 0
    header_length_valid: bool = False
    file_version: int = 0
    version_valid: bool = False
    
    # Даты
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None
    duration_seconds: int = 0
    dates_valid: bool = False
    
    # CRC32
    stored_crc32: str = ""
    calculated_crc32: str = ""
    crc32_valid: bool = False
    
    # Структура данных
    data_row_values_count: int = 0
    row_length: int = 0
    data_section_size: int = 0
    data_section_valid: bool = False
    rows_count: int = 0
    total_values_count: int = 0
    
    # Дополнительные метаданные (Id, MainDbId, MainDbName)
    extra_metadata_present: bool = False
    file_id: Optional[str] = None
    main_db_id: Optional[str] = None
    main_db_name: Optional[str] = None
    
    # Тип данных (всегда Double для .fh)
    data_type: int = FH_DATA_TYPE
    data_type_name: str = FH_DATA_TYPE_NAME
    
    # Общий статус
    error_detail: Optional[str] = None
    status: str = "invalid"


# ═══════════════════════════════════════════════════════════════════
#  Основная функция валидации .fh
# ═══════════════════════════════════════════════════════════════════

@task(task_id='validate_fh_file')
def validate_fh_file(hdfs_path: str) -> Dict[str, Any]:
    """
    Валидация .fh файла (бинарный формат исторических данных типа Double).
    
    Проверяет:
    1. Размер файла (не пустой, не меньше минимального заголовка)
    2. Магическое слово (0xBEBAADDE)
    3. Длину заголовка
    4. Версию файла (поддерживается версия 1)
    5. CRC32 заголовка
    6. Корректность дат (FromDate < ToDate)
    7. Наличие дополнительных метаданных (сигнатура KRIO)
    8. Структуру секции данных (размер кратен длине строки)
    
    Примечание: .fh файлы всегда содержат данные типа Double
    
    :param hdfs_path: путь к .fh файлу в HDFS
    :return: словарь с результатами валидации
    :raises AirflowException: если валидация не пройдена
    """
    result = FhValidationResult(hdfs_path=hdfs_path)
    errors = []
    
    # ═══════════════════════════════════════════════════════════════
    #  1. Получение информации о файле из HDFS
    # ═══════════════════════════════════════════════════════════════
    try:
        hook = WebHDFSHook(webhdfs_conn_id='hdfs_default')
        client = hook.get_conn()
        
        file_status = client.status(hdfs_path, strict=True)
        file_size = file_status.get('length', 0)
        
        result.file_size_bytes = file_size
        result.file_size_mb = round(file_size / 1024**2, 2)
        
        log.info(f"📄 Файл {hdfs_path}: {result.file_size_mb} MB")
        
        if file_size == 0:
            raise ValueError(f"Файл пустой (размер 0 байт)")
        
        if file_size < FH_MIN_HEADER_LENGTH:
            raise ValueError(
                f"Файл слишком мал: {file_size} байт "
                f"(минимум {FH_MIN_HEADER_LENGTH} байт для заголовка)"
            )
    
    except Exception as e:
        raise AirflowException(f"Ошибка доступа к файлу в HDFS: {e}")
    
    # ═══════════════════════════════════════════════════════════════
    #  2. Скачивание файла во временный локальный файл
    # ═══════════════════════════════════════════════════════════════
    try:
        with hdfs_tempfile(hdfs_path) as local_path:
            with open(local_path, 'rb') as f:
                file_data = f.read()
            
            actual_size = len(file_data)
            if actual_size != file_size:
                raise ValueError(
                    f"Размер скачанного файла ({actual_size}) "
                    f"не совпадает с размером в HDFS ({file_size})"
                )
            
            # ═══════════════════════════════════════════════════════════
            #  3. Проверка магического слова
            # ═══════════════════════════════════════════════════════════
            magic = struct.unpack_from('<I', file_data, 0)[0]
            result.magic_value = f"0x{magic:08X}"
            result.magic_valid = (magic == FH_MAGIC_HEX)
            
            if not result.magic_valid:
                errors.append(
                    f"Неверное магическое слово: {result.magic_value} "
                    f"(ожидается 0x{FH_MAGIC_HEX:08X})"
                )
                # Критическая ошибка — дальше проверять нечего
                result.error_detail = "; ".join(errors)
                result.status = "invalid"
                log.error(f"✗ {result.error_detail}")
                raise AirflowException(f"Валидация файла не пройдена: {hdfs_path} — {result.error_detail}")
            
            # ═══════════════════════════════════════════════════════════
            #  4. Проверка длины заголовка
            # ═══════════════════════════════════════════════════════════
            header_length = struct.unpack_from('<I', file_data, 4)[0]
            result.header_length = header_length
            
            result.header_length_valid = (
                header_length >= FH_MIN_HEADER_LENGTH and 
                header_length <= actual_size
            )
            
            if not result.header_length_valid:
                errors.append(
                    f"Некорректная длина заголовка: {header_length} "
                    f"(размер файла: {actual_size}, минимум: {FH_MIN_HEADER_LENGTH})"
                )
                result.error_detail = "; ".join(errors)
                result.status = "invalid"
                raise AirflowException(f"Валидация файла не пройдена: {hdfs_path} — {result.error_detail}")
            
            # ═══════════════════════════════════════════════════════════
            #  5. Проверка версии файла
            # ═══════════════════════════════════════════════════════════
            file_version = struct.unpack_from('<H', file_data, FH_VERSION_OFFSET)[0]
            result.file_version = file_version
            result.version_valid = (file_version in FH_SUPPORTED_VERSIONS)
            
            if not result.version_valid:
                errors.append(
                    f"Неподдерживаемая версия файла: {file_version} "
                    f"(поддерживаются: {FH_SUPPORTED_VERSIONS})"
                )
            
            # ═══════════════════════════════════════════════════════════
            #  6. Чтение дат
            # ═══════════════════════════════════════════════════════════
            from_ts = struct.unpack_from('<Q', file_data, FH_FROM_DATE_OFFSET)[0]
            to_ts = struct.unpack_from('<Q', file_data, FH_TO_DATE_OFFSET)[0]
            
            result.from_date = fh_ts_to_datetime(from_ts)
            result.to_date = fh_ts_to_datetime(to_ts)
            result.duration_seconds = int((result.to_date - result.from_date).total_seconds())
            
            result.dates_valid = (
                result.from_date < result.to_date and 
                result.from_date.year >= 1970 and 
                result.to_date.year <= 2100
            )
            
            if not result.dates_valid:
                errors.append(
                    f"Некорректные даты: FromDate={result.from_date}, "
                    f"ToDate={result.to_date}, Duration={result.duration_seconds}s"
                )
            
            # ═══════════════════════════════════════════════════════════
            #  7. Проверка CRC32 заголовка
            # ═══════════════════════════════════════════════════════════
            header_data_end = header_length - 4
            stored_crc32 = struct.unpack_from('<I', file_data, header_data_end)[0]
            calculated_crc32 = fh_compute_crc32(file_data[:header_data_end])
            
            result.stored_crc32 = f"0x{stored_crc32:08X}"
            result.calculated_crc32 = f"0x{calculated_crc32:08X}"
            result.crc32_valid = (stored_crc32 == calculated_crc32)
            
            if not result.crc32_valid:
                errors.append(
                    f"CRC32 заголовка не совпадает: "
                    f"файл={result.stored_crc32}, вычислено={result.calculated_crc32}"
                )
            
            # ═══════════════════════════════════════════════════════════
            #  8. Разбор HeaderData (зависит от версии)
            # ═══════════════════════════════════════════════════════════
            header_data = file_data[FH_HEADER_DATA_OFFSET:header_data_end]
            
            if file_version == 1:
                # 8.1. DataRowValuesCount (4 байта)
                if len(header_data) >= 4:
                    result.data_row_values_count = struct.unpack_from('<I', header_data, 0)[0]
                else:
                    errors.append("HeaderData слишком короткий: нет DataRowValuesCount")
                
                # 8.2. Проверка дополнительных метаданных (сигнатура KRIO)
                if len(header_data) >= 8:
                    extra_sig = struct.unpack_from('<I', header_data, 4)[0]
                    result.extra_metadata_present = (extra_sig == FH_EXTRA_MAGIC)
                    
                    if not result.extra_metadata_present:
                        errors.append(
                            f"Отсутствует сигнатура дополнительных метаданных: "
                            f"0x{extra_sig:08X} (ожидается 0x{FH_EXTRA_MAGIC:08X})"
                        )
                    else:
                        # Чтение метаданных
                        try:
                            offset = 8
                            
                            # Id (16 байт)
                            if len(header_data) >= offset + 16:
                                file_id = fh_guid_from_bytes_le(header_data[offset:offset + 16])
                                result.file_id = str(file_id)
                                offset += 16
                            else:
                                errors.append("Недостаточно данных для чтения Id")
                            
                            # MainDbId (16 байт)
                            if len(header_data) >= offset + 16:
                                main_db_id = fh_guid_from_bytes_le(header_data[offset:offset + 16])
                                result.main_db_id = str(main_db_id)
                                offset += 16
                            else:
                                errors.append("Недостаточно данных для чтения MainDbId")
                            
                            # MainDbName length (4 байта)
                            if len(header_data) >= offset + 4:
                                name_len = struct.unpack_from('<I', header_data, offset)[0]
                                offset += 4
                                
                                if name_len > 1000:  # Защита от некорректных данных
                                    errors.append(f"Некорректная длина MainDbName: {name_len}")
                                elif len(header_data) >= offset + name_len:
                                    result.main_db_name = header_data[offset:offset + name_len].decode('utf-8')
                                    offset += name_len
                                else:
                                    errors.append("Недостаточно данных для чтения MainDbName")
                            else:
                                errors.append("Недостаточно данных для чтения длины MainDbName")
                            
                            # DataType (4 байта) — читаем, но не проверяем (всегда Double)
                            if len(header_data) >= offset + 4:
                                raw_data_type = struct.unpack_from('<I', header_data, offset)[0]
                                # Для .fh файлов всегда ожидаем Double (2)
                                if raw_data_type != FH_DATA_TYPE:
                                    log.warning(
                                        f"DataType в файле = {raw_data_type}, "
                                        f"но .fh файлы всегда содержат Double данные"
                                    )
                            else:
                                errors.append("Недостаточно данных для чтения DataType")
                        
                        except UnicodeDecodeError as e:
                            errors.append(f"Ошибка декодирования MainDbName: {e}")
                        except Exception as e:
                            errors.append(f"Ошибка чтения метаданных: {e}")
                else:
                    errors.append("HeaderData слишком короткий: нет сигнатуры KRIO")
                    result.extra_metadata_present = False
            
            # ═══════════════════════════════════════════════════════════
            #  9. Проверка структуры секции данных
            # ═══════════════════════════════════════════════════════════
            if result.data_row_values_count > 0:
                result.row_length = (
                    FH_ROW_ID_LENGTH + 
                    (FH_FIELD_LENGTH * result.data_row_values_count)
                )
                
                result.data_section_size = actual_size - header_length
                result.rows_count = result.data_section_size // result.row_length
                
                # Проверка: размер данных должен быть кратен длине строки
                result.data_section_valid = (
                    result.data_section_size > 0 and 
                    result.data_section_size % result.row_length == 0
                )
                
                if not result.data_section_valid:
                    remainder = result.data_section_size % result.row_length
                    errors.append(
                        f"Размер секции данных ({result.data_section_size}) "
                        f"не кратен длине строки ({result.row_length}), "
                        f"остаток: {remainder} байт"
                    )
                
                result.total_values_count = result.rows_count * result.data_row_values_count
                
                # sanity check: должно быть хотя бы одна строка
                if result.rows_count == 0:
                    errors.append("Секция данных пуста: нет ни одной строки")
                    result.data_section_valid = False
            else:
                errors.append("DataRowValuesCount = 0: файл не содержит данных")
                result.data_section_valid = False
            
            # ═══════════════════════════════════════════════════════════
            #  10. Формирование итогового статуса
            # ═══════════════════════════════════════════════════════════
            if errors:
                result.error_detail = "; ".join(errors)
                result.status = "invalid"
                
                log.error(f"✗ Валидация .fh файла НЕ пройдена: {hdfs_path}")
                for err in errors:
                    log.error(f"   • {err}")
                
                raise AirflowException(
                    f"Валидация .fh файла не пройдена: {hdfs_path}\n"
                    f"Ошибки:\n" + "\n".join(f"  • {e}" for e in errors)
                )
            else:
                result.status = "valid"
                
                log.info(
                    f"✓ Файл валиден: {hdfs_path} "
                    f"({result.file_size_mb:.2f} MB, "
                    f"версия={result.file_version}, "
                    f"строк={result.rows_count}, "
                    f"значений={result.total_values_count}, "
                    f"период=[{result.from_date:%Y-%m-%d %H:%M:%S} — "
                    f"{result.to_date:%Y-%m-%d %H:%M:%S}])"
                )
                log.info(
                    f"   Метаданные: Id={result.file_id}, "
                    f"MainDb={result.main_db_name} ({result.main_db_id}), "
                    f"DataType={result.data_type_name}"
                )
    
    except AirflowException:
        raise
    except Exception as e:
        result.error_detail = f"Непредвиденная ошибка: {str(e)}"
        result.status = "invalid"
        log.exception(f"✗ Критическая ошибка при валидации {hdfs_path}")
        raise AirflowException(f"Ошибка валидации .fh файла: {hdfs_path} — {e}")
    
    # ═══════════════════════════════════════════════════════════════
    #  11. Формирование ответа
    # ═══════════════════════════════════════════════════════════════
    return {
        'hdfs_path': result.hdfs_path,
        'file_size_bytes': result.file_size_bytes,
        'file_size_mb': result.file_size_mb,
        
        # Заголовок
        'magic_valid': result.magic_valid,
        'magic_value': result.magic_value,
        'header_length': result.header_length,
        'header_length_valid': result.header_length_valid,
        'file_version': result.file_version,
        'version_valid': result.version_valid,
        
        # Даты
        'from_date': result.from_date.isoformat() if result.from_date else None,
        'to_date': result.to_date.isoformat() if result.to_date else None,
        'duration_seconds': result.duration_seconds,
        'dates_valid': result.dates_valid,
        
        # CRC32
        'stored_crc32': result.stored_crc32,
        'calculated_crc32': result.calculated_crc32,
        'crc32_valid': result.crc32_valid,
        
        # Структура данных
        'data_row_values_count': result.data_row_values_count,
        'row_length': result.row_length,
        'data_section_size': result.data_section_size,
        'data_section_valid': result.data_section_valid,
        'rows_count': result.rows_count,
        'total_values_count': result.total_values_count,
        
        # Метаданные
        'extra_metadata_present': result.extra_metadata_present,
        'file_id': result.file_id,
        'main_db_id': result.main_db_id,
        'main_db_name': result.main_db_name,
        
        # Тип данных (всегда Double для .fh)
        'data_type': result.data_type,
        'data_type_name': result.data_type_name,
        
        # Общий статус
        'error_detail': result.error_detail,
        'status': result.status,
    }


@task(task_id='validate_system_type_data')
def validate_system_type_data(hdfs_path: str, source_system_type_data_info: dict) -> bool:
    with hdfs_tempfile(hdfs_path=hdfs_path) as local_sqlite_path:
        for system_type_data_set in source_system_type_data_info:
            data_matches=check_system_type_data_tables(sqlite_path=local_sqlite_path, 
                                        source_table_name=source_system_type_data_info[system_type_data_set]['source_adress']['table_name'],
                                        source_column_name=source_system_type_data_info[system_type_data_set]['source_adress']['column_name'],
                                        dwh_table_name=source_system_type_data_info[system_type_data_set]['dwh_adress']['table_name'],
                                        dwh_column_name=source_system_type_data_info[system_type_data_set]['dwh_adress']['column_name'])
            if data_matches:
                print(f"✓ Проверка пройдена, все элементы справочных данных {system_type_data_set} из SQLite найдены в DWH.")
            else:
                raise AirflowException(f"❌ Проверка НЕ пройдена: в DWH отсутствуют элементы справочных данных {system_type_data_set}!/n" 
                                    f"Необходимо актуализировать справочные данные {system_type_data_set}!")
    return True


@task
def has_records_task(records: list) -> bool:
    """Возвращает True, если есть записи для обработки"""
    return len(records) > 0


# [Остальные функции convert_and_load_creyt_timeseries, export_creyt_timeseries_to_csv и т.д. остаются без изменений]