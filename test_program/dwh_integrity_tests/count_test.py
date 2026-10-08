import sqlite3
import tempfile
import os
import logging
import requests
import socket
import time
from typing import Dict, List, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from pathlib import Path
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# =============================================================================
# КОНФИГУРАЦИЯ ПОДКЛЮЧЕНИЙ
# =============================================================================

HDFS_HOST = 'localhost'
HDFS_PORT = 9870
HDFS_USER = 'hdfs'
HDFS_BASE_URL = f'http://{HDFS_HOST}:{HDFS_PORT}/webhdfs/v1'

HDFS_HOSTNAME_MAP = {
    'datanode1': 'localhost', 'datanode2': 'localhost', 'datanode3': 'localhost',
    'namenode': 'localhost',
    '172.21.0.31': 'localhost', '172.21.0.32': 'localhost',
    '172.21.0.33': 'localhost', '172.21.0.30': 'localhost',
}

DWH_HOST = 'localhost'
DWH_PORT = 7000
DWH_DB = 'dwh'
DWH_USER = 'gpadmin'
DWH_PASSWORD = ''

HDFS_DOWNLOAD_CHUNK_SIZE = 500 * 1024 * 1024  # 500 MB

# =============================================================================
# КОНФИГ: МАППИНГ SQLITE → HUB + SATELLITE (Data Vault 2.0)
# =============================================================================

SQLITE_TO_DWH_MAPPING = {
    # Основные бизнес-сущности
    'Objects': {'hub': 'h_objects', 'satellite': 's_objects'},
    'ObjectProperties': {'hub': 'h_object_properties', 'satellite': 's_object_properties'},
    'ObjectPropertyDescriptorNodes': {'hub': 'h_object_property_descriptor_nodes', 'satellite': 's_object_property_descriptor_nodes'},
    'ObjectPropertyDescriptors': {'hub': 'h_object_property_descriptors', 'satellite': 's_object_property_descriptors'},
    'ObjectRules': {'hub': 'h_object_rules', 'satellite': 's_object_rules'},
    'ObjectTemplates': {'hub': 'h_object_templates', 'satellite': 's_object_templates'},
    'ObjectTypeDescriptors': {'hub': 'h_object_type_descriptors', 'satellite': 's_object_type_descriptors'},
    'ObjectGroups': {'hub': 'h_object_groups', 'satellite': None},
    'ObjectGroupDescriptors': {'hub': 'h_object_group_descriptors', 'satellite': 's_object_group_descriptors'},
    
    # Справочники
    'Bearings': {'hub': 'h_bearings', 'satellite': 's_bearings'},
    'Images': {'hub': 'h_images', 'satellite': 's_images'},
    'Annotation': {'hub': 'h_annotations', 'satellite': 's_annotations'},
    'PresetChartSettings': {'hub': 'h_preset_chart_settings', 'satellite': 's_preset_chart_settings'},
    
    # Диагностика
    'DiagnosticAlarmHistory_v2': {'hub': 'h_diagnostic_data_records', 'satellite': 's_diagnostic_data_records'},
    
    # Логи и действия
    'UserLog': {'hub': 'h_user_logs', 'satellite': 's_user_logs'},
    'TikScadaLog': {'hub': 'h_tik_scada_logs', 'satellite': 's_tik_scada_logs'},
    'TikExpertSlices': {'hub': 'h_tik_expert_slices', 'satellite': 's_tik_expert_slices'},
    
    # POU
    'PouUserDefinedItems': {'hub': 'h_pou_user_defined_items', 'satellite': 's_pou_user_defined_items'},
    'PouUserItemsTree': {'hub': 'h_pou_user_tree_items', 'satellite': 's_pou_user_tree_items'},
    
    # Маршруты и списки
    'PionRouteObjectsLists': {'hub': 'h_pion_route_objects_lists', 'satellite': 's_pion_route_objects_lists'},
    'UserDefinedPropertyLists': {'hub': 'h_user_defined_property_lists', 'satellite': 's_user_defined_property_lists'},
    'UserDefinedPropertyListsTypes': {'hub': 'h_user_defined_property_lists_types', 'satellite': 's_user_defined_property_lists_types'},
    'UserDefinedTilesPropertiesConfigs': {'hub': 'h_user_defined_tiles_property_configs', 'satellite': 's_user_defined_tiles_property_configs'},
    
    # Единицы измерения
    'MeasureConvert': {'hub': 'h_measure_converts', 'satellite': 's_measure_converts'},
    'MeasureGroups': {'hub': 'h_measure_groups', 'satellite': 's_measure_groups'},
    'MeasureUnits': {'hub': 'h_measure_units', 'satellite': 's_measure_units'},
    
    # Шаблоны моделей
    'ModelTemplates': {'hub': 'h_model_templates', 'satellite': 's_model_templates'},
    'ModelTemplateTreeNodes': {'hub': 'h_model_template_tree_nodes', 'satellite': 's_model_template_tree_nodes'},
    
    # IO Device Configs
    'IODeviceConfigs': {'hub': 'h_io_device_configs', 'satellite': 's_io_device_configs'},
    'IODeviceConfigNodes': {'hub': 'h_io_device_config_nodes', 'satellite': 's_io_device_config_nodes'},
    
    # IO Creyt
    'IOCreytConfigs': {'hub': 'h_io_creyt_configs', 'satellite': 's_io_creyt_configs'},
    'IOCreytChannelConfigs': {'hub': 'h_io_creyt_channel_configs', 'satellite': 's_io_creyt_channel_configs'},
    'IOCreytChannelStates': {'hub': 'h_io_creyt_channel_states', 'satellite': 's_io_creyt_channel_states'},
    'IOCreytControllerStates': {'hub': 'h_io_creyt_controller_states', 'satellite': 's_io_creyt_controller_states'},
    'IOCreytIMOperTimes': {'hub': 'h_io_creyt_im_oper_times', 'satellite': 's_io_creyt_im_oper_times'},
    
    # IO LCard
    'IOLCardConfigs': {'hub': 'h_io_lcard_configs', 'satellite': 's_io_lcard_configs'},
    'IOLCardChannelConfigs': {'hub': 'h_io_lcard_channel_configs', 'satellite': 's_io_lcard_channel_configs'},
    'IOLCardCrateConfigs': {'hub': 'h_io_lcard_crate_configs', 'satellite': 's_io_lcard_crate_configs'},
    'IOLCardCrateSync': {'hub': 'h_io_lcard_sync_crates', 'satellite': 's_io_lcard_sync_crates'},
    'IOLCardInputConfigs': {'hub': 'h_io_lcard_input_configs', 'satellite': 's_io_lcard_input_configs'},
    'IOLCardLogicInputConfigs': {'hub': 'h_io_lcard_logic_input_configs', 'satellite': 's_io_lcard_logic_input_configs'},
    'IOLCardModuleConfigs': {'hub': 'h_io_lcard_module_configs', 'satellite': 's_io_lcard_module_configs'},
    
    # IO Modbus TCP
    'IOModbusTcpConfigs': {'hub': 'h_io_modbus_tcp_configs', 'satellite': 's_io_modbus_tcp_configs'},
    'IOModbusTcpRegisterConfigs': {'hub': 'h_io_modbus_tcp_register_configs', 'satellite': 's_io_modbus_tcp_register_configs'},
    'IOModbustcpRegisterBitDecompressionConfigs': {'hub': 'h_io_modbus_tcp_bit_decompression_configs', 'satellite': 's_io_modbus_tcp_bit_decompression_configs'},
    
    # IO OPC DA
    'IOOpcDaClientConfigs': {'hub': 'h_io_opc_da_client_configs', 'satellite': 's_io_opc_da_client_configs'},
    'IOOpcDaClientGroupConfigs': {'hub': 'h_io_opc_da_client_group_configs', 'satellite': 's_io_opc_da_client_group_configs'},
    'IOOpcDaClientItemConfigs': {'hub': 'h_io_opc_da_client_item_configs', 'satellite': 's_io_opc_da_client_item_configs'},
    
    # IO OPC UA
    'IOOpcUaClientConfigs': {'hub': 'h_io_opc_ua_client_configs', 'satellite': 's_io_opc_ua_client_configs'},
    'IOOpcUaClientGroupConfigs': {'hub': 'h_io_opc_ua_client_group_configs', 'satellite': 's_io_opc_ua_client_group_configs'},
    'IOOpcUaClientItemConfigs': {'hub': 'h_io_opc_ua_client_item_configs', 'satellite': 's_io_opc_ua_client_item_configs'},
    'IOOpcUaClientTransformItemConfigs': {'hub': 'h_io_opc_ua_client_transform_item_configs', 'satellite': 's_io_opc_ua_client_transform_item_configs'},
    
    # Aggregates
    'Aggregates': {'hub': 'h_aggregates', 'satellite': 's_aggregates'},
    'AggregateNotificationConfigs': {'hub': 'h_aggregate_notification_configs', 'satellite': 's_aggregate_notification_configs'},
    'EmailAddresses': {'hub': 'h_email_addresses', 'satellite': None},
}

# =============================================================================
# ПРОВЕРКА ДОСТУПНОСТИ
# =============================================================================

def check_port(host: str, port: int, timeout: int = 3) -> bool:
    try:
        sock = socket.create_connection((host, port), timeout)
        sock.close()
        return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False

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


def get_file_size_from_hdfs(hdfs_path: str) -> int:
    status_url = f"{HDFS_BASE_URL}{hdfs_path}?op=GETFILESTATUS&user.name={HDFS_USER}"
    response = requests.get(status_url, timeout=30)
    response.raise_for_status()
    return response.json()['FileStatus']['length']


def _download_chunk_with_retry(chunk_url: str, chunk_num: int, total_chunks: int, max_retries: int = 3) -> bytes:
    timeout = (30, 600)
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(chunk_url, stream=False, timeout=timeout)
            response.raise_for_status()
            data = response.content
            response.close()
            return data
        except (requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError,
                ConnectionResetError, requests.exceptions.Timeout) as e:
            log.warning(f"   ⚠️ Чанк {chunk_num}: попытка {attempt}/{max_retries} не удалась: {type(e).__name__}")
            if attempt == max_retries:
                raise
            time.sleep(3 * attempt)


def download_file_from_hdfs_chunked(hdfs_path: str) -> str:
    log.info(f"📥 Скачивание файла из HDFS (chunked): {hdfs_path}")
    file_size = get_file_size_from_hdfs(hdfs_path)
    log.info(f"   📊 Размер файла: {file_size:,} байт ({file_size/(1024*1024):.1f} MB)")
    if file_size == 0:
        raise Exception(f"Файл {hdfs_path} пустой")

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
    try:
        offset = 0
        chunk_num = 0
        total_chunks = (file_size + HDFS_DOWNLOAD_CHUNK_SIZE - 1) // HDFS_DOWNLOAD_CHUNK_SIZE
        while offset < file_size:
            chunk_num += 1
            current_chunk_size = min(HDFS_DOWNLOAD_CHUNK_SIZE, file_size - offset)
            chunk_url = f"{datanode_clean_url}&offset={offset}&length={current_chunk_size}"
            log.info(f"   📦 Чанк {chunk_num}/{total_chunks}: offset={offset:,}, size={current_chunk_size/(1024*1024):.0f} MB")
            chunk_data = _download_chunk_with_retry(chunk_url, chunk_num, total_chunks)
            temp_file.write(chunk_data)
            offset += len(chunk_data)
            del chunk_data
        temp_file.flush()
        os.fsync(temp_file.fileno())
        temp_file.close()
        actual_size = os.path.getsize(temp_path)
        if actual_size != file_size:
            raise Exception(f"Размер файла не совпадает: ожидалось {file_size}, получено {actual_size}")
        log.info(f"✅ Файл скачан: {temp_path} ({actual_size:,} байт)")
        return temp_path
    except Exception as e:
        temp_file.close()
        if os.path.exists(temp_path):
            try:
                os.unlink(temp_path)
            except OSError:
                pass
        raise


def _safe_delete_file(temp_path: str):
    if temp_path and os.path.exists(temp_path):
        for attempt in range(3):
            try:
                os.unlink(temp_path)
                return
            except PermissionError:
                if attempt < 2:
                    time.sleep(1)
            except OSError:
                break

# =============================================================================
# ФУНКЦИИ ДЛЯ РАБОТЫ С DWH
# =============================================================================

def get_dwh_engine() -> Engine:
    db_url = f"postgresql+psycopg2://{DWH_USER}:{DWH_PASSWORD}@{DWH_HOST}:{DWH_PORT}/{DWH_DB}"
    return create_engine(db_url, echo=False, pool_pre_ping=True, pool_size=5, max_overflow=10)


def get_maindb_files_from_catalogue(dwh_engine: Engine) -> List[Dict]:
    maindb_files = []
    try:
        with dwh_engine.begin() as conn:
            result = conn.execute(text("""
                SELECT data_source_id, data_file_id, main_db_id, main_db_name,
                       file_name, hdfs_full_path, data_type, data_format,
                       is_uploaded_to_dwh, dwh_upload_dttm
                FROM public.data_catalogue
                WHERE data_type = 'MainDb' AND data_format = 'sqlite'
                ORDER BY upload_date_time DESC
            """))
            for row in result.fetchall():
                maindb_files.append({
                    'data_source_id': str(row[0]), 'data_file_id': str(row[1]),
                    'main_db_id': str(row[2]), 'main_db_name': row[3],
                    'file_name': row[4], 'hdfs_full_path': row[5],
                    'data_type': row[6], 'data_format': row[7],
                    'is_uploaded_to_dwh': row[8], 'dwh_upload_dttm': row[9]
                })
    except Exception as e:
        log.error(f"❌ Ошибка получения списка файлов: {e}")
        raise
    return maindb_files

# =============================================================================
# 🔥 ПОДСЧЁТ СТРОК С УЧЁТОМ ДУБЛИКАТОВ (ТОЛЬКО ПО КОНФИГУ)
# =============================================================================

def _get_table_columns(cursor, table_name: str) -> List[str]:
    """Получает список столбцов таблицы через PRAGMA table_info."""
    cursor.execute(f'PRAGMA table_info("{table_name}")')
    return [row[1] for row in cursor.fetchall()]


def count_sqlite_tables_by_config(sqlite_path: str, config_tables: List[str]) -> Dict[str, Dict[str, int]]:
    """
    🔥 Подсчет строк в таблицах из конфига С УЧЁТОМ ДУБЛИКАТОВ.
    
    Для каждой таблицы возвращает:
    {
        'total': <общее количество строк>,
        'unique': <количество уникальных строк>,
        'duplicates': <количество дубликатов (total - unique)>
    }
    
    Если таблица отсутствует в SQLite, возвращает все нули.
    """
    table_counts = {}
    try:
        conn = sqlite3.connect(sqlite_path)
        cursor = conn.cursor()
        
        # Получаем список всех таблиц в SQLite
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
        """)
        existing_tables = set(row[0] for row in cursor.fetchall())
        
        for table_name in config_tables:
            if table_name in existing_tables:
                try:
                    # 1. Общее количество строк
                    cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                    total_count = cursor.fetchone()[0]
                    
                    # 2. Количество уникальных строк (через GROUP BY по всем столбцам)
                    columns = _get_table_columns(cursor, table_name)
                    
                    if columns:
                        # Экранируем имена столбцов
                        quoted_cols = ', '.join([f'"{col}"' for col in columns])
                        cursor.execute(f'''
                            SELECT COUNT(*) FROM (
                                SELECT {quoted_cols} 
                                FROM "{table_name}" 
                                GROUP BY {quoted_cols}
                            )
                        ''')
                        unique_count = cursor.fetchone()[0]
                    else:
                        unique_count = 0
                    
                    duplicates = total_count - unique_count
                    
                    table_counts[table_name] = {
                        'total': total_count,
                        'unique': unique_count,
                        'duplicates': duplicates
                    }
                    
                    if duplicates > 0:
                        log.info(f"   📋 {table_name}: {total_count:,} строк, "
                                f"{unique_count:,} уникальных, "
                                f"{duplicates:,} дубликатов")
                except Exception as e:
                    log.warning(f"⚠ Ошибка в таблице {table_name}: {e}")
                    table_counts[table_name] = {'total': 0, 'unique': 0, 'duplicates': 0}
            else:
                table_counts[table_name] = {'total': 0, 'unique': 0, 'duplicates': 0}
        
        conn.close()
    except Exception as e:
        log.error(f"❌ Ошибка SQLite: {e}")
        raise
    return table_counts


def count_dwh_hub_satellite(dwh_engine: Engine, data_file_id: str) -> Dict[str, Dict[str, int]]:
    """
    Подсчет строк ТОЛЬКО в Hub и Satellite таблицах DWH для конкретного data_file_id.
    Проверяет только таблицы из SQLITE_TO_DWH_MAPPING.
    """
    result = {}
    try:
        with dwh_engine.begin() as conn:
            for sqlite_table, dwh_mapping in SQLITE_TO_DWH_MAPPING.items():
                table_result = {'hub': 0, 'satellite': 0, 'hub_table': None, 'sat_table': None}
                
                # Hub
                hub_table = dwh_mapping.get('hub')
                if hub_table:
                    table_result['hub_table'] = hub_table
                    try:
                        count_result = conn.execute(text(f"""
                            SELECT COUNT(*) FROM public.{hub_table}
                            WHERE data_file_id = :data_file_id
                        """), {"data_file_id": data_file_id})
                        table_result['hub'] = count_result.fetchone()[0]
                    except Exception as e:
                        log.debug(f"⚠ Ошибка подсчета в hub {hub_table}: {e}")
                
                # Satellite
                sat_table = dwh_mapping.get('satellite')
                if sat_table:
                    table_result['sat_table'] = sat_table
                    try:
                        count_result = conn.execute(text(f"""
                            SELECT COUNT(*) FROM public.{sat_table}
                            WHERE data_file_id = :data_file_id
                        """), {"data_file_id": data_file_id})
                        table_result['satellite'] = count_result.fetchone()[0]
                    except Exception as e:
                        log.debug(f"⚠ Ошибка подсчета в satellite {sat_table}: {e}")
                
                result[sqlite_table] = table_result
    except Exception as e:
        log.error(f"❌ Ошибка подсчета в DWH: {e}")
        raise
    return result


def print_hub_sat_report(file_info: Dict, sqlite_counts: Dict[str, Dict[str, int]], 
                         dwh_counts: Dict[str, Dict[str, int]]):
    """
    🔥 Вывод отчета с учётом дубликатов.
    Сравниваем DWH с количеством УНИКАЛЬНЫХ строк в SQLite.
    """
    print("\n" + "="*150)
    print(f"ОТЧЕТ ПО ФАЙЛУ: {file_info['file_name']}")
    print("="*150)
    print(f"Data Source ID:  {file_info['data_source_id']}")
    print(f"Data File ID:    {file_info['data_file_id']}")
    print(f"Main DB Name:    {file_info['main_db_name']}")
    print(f"HDFS Path:       {file_info['hdfs_full_path']}")
    print(f"Загружен в DWH:  {'Да' if file_info['is_uploaded_to_dwh'] else 'Нет'}")
    print("="*150)
    
    # 🔥 Добавляем колонки для дубликатов
    print(f"\n{'SQLite таблица':<42} {'Всего':>8} {'Уник.':>8} {'Дубли':>8} {'Hub':>8} {'Satellite':>10} {'Hub таблица':>22} {'Sat таблица':>22} {'Статус':>8}")
    print("-"*150)
    
    total_sqlite_all = 0
    total_sqlite_unique = 0
    total_sqlite_dups = 0
    total_hub = 0
    total_sat = 0
    matched_tables = 0
    mismatched_tables = 0
    missing_tables = 0
    tables_with_data = 0
    tables_with_dups = 0
    
    # 🔥 Итерируемся ТОЛЬКО по таблицам из конфига
    sorted_tables = sorted(
        SQLITE_TO_DWH_MAPPING.keys(),
        key=lambda x: (sqlite_counts.get(x, {}).get('unique', 0) == 0, x)
    )
    
    for sqlite_table in sorted_tables:
        counts = sqlite_counts.get(sqlite_table, {'total': 0, 'unique': 0, 'duplicates': 0})
        sqlite_total = counts['total']
        sqlite_unique = counts['unique']
        sqlite_dups = counts['duplicates']
        
        dwh_data = dwh_counts.get(sqlite_table, {})
        hub_count = dwh_data.get('hub', 0)
        sat_count = dwh_data.get('satellite', 0)
        hub_table = dwh_data.get('hub_table') or '-'
        sat_table = dwh_data.get('sat_table') or '-'
        
        total_sqlite_all += sqlite_total
        total_sqlite_unique += sqlite_unique
        total_sqlite_dups += sqlite_dups
        total_hub += hub_count
        total_sat += sat_count
        
        if sqlite_unique > 0:
            tables_with_data += 1
        if sqlite_dups > 0:
            tables_with_dups += 1
        
        # 🔥 Определяем статус: сравниваем DWH с УНИКАЛЬНЫМИ строками SQLite
        if sqlite_unique > 0 and hub_count == 0 and sat_count == 0:
            status = "MISS"
            missing_tables += 1
            marker = " ❌"
        elif sqlite_unique == hub_count or sqlite_unique == sat_count:
            status = "OK"
            matched_tables += 1
            marker = " ✅"
        else:
            status = "DIFF"
            mismatched_tables += 1
            marker = " ⚠️"
        
        # Обрезаем длинные имена таблиц
        hub_display = hub_table[:22] if len(hub_table) <= 22 else hub_table[:19] + "..."
        sat_display = sat_table[:22] if len(sat_table) <= 22 else sat_table[:19] + "..."
        
        # 🔥 Форматирование дубликатов
        dups_str = f"{sqlite_dups:,}" if sqlite_dups > 0 else "-"
        
        print(f"{sqlite_table:<42} {sqlite_total:>8,} {sqlite_unique:>8,} {dups_str:>8} "
              f"{hub_count:>8,} {sat_count:>10,} {hub_display:>22} {sat_display:>22} {status:>6}{marker}")
    
    print("-"*150)
    print(f"{'ИТОГО':<42} {total_sqlite_all:>8,} {total_sqlite_unique:>8,} {total_sqlite_dups:>8,} "
          f"{total_hub:>8,} {total_sat:>10,}")
    print("="*150)
    
    # Статистика
    total_config_tables = len(SQLITE_TO_DWH_MAPPING)
    
    print(f"\n📊 СТАТИСТИКА:")
    print(f"   Всего таблиц в конфиге: {total_config_tables}")
    print(f"   Таблиц с данными (уникальные строки): {tables_with_data}")
    print(f"   Пустых таблиц: {total_config_tables - tables_with_data}")
    print(f"   Таблиц с дубликатами: {tables_with_dups}")
    print(f"\n   📈 Строки в SQLite:")
    print(f"      Всего строк:       {total_sqlite_all:>12,}")
    print(f"      Уникальных строк:  {total_sqlite_unique:>12,}")
    print(f"      Дубликатов:        {total_sqlite_dups:>12,}", end="")
    if total_sqlite_all > 0:
        dup_pct = 100 * total_sqlite_dups / total_sqlite_all
        print(f"  ({dup_pct:.1f}%)")
    else:
        print()
    print(f"\n   📈 Строки в DWH:")
    print(f"      Hub:               {total_hub:>12,}")
    print(f"      Satellite:         {total_sat:>12,}")
    print(f"\n   ✓ Совпало (уник. SQLite = Hub или Satellite): {matched_tables}")
    print(f"   ✗ Не совпало: {mismatched_tables}")
    print(f"   ⚠ Отсутствует в DWH: {missing_tables}")
    
    # 🔥 Детали по таблицам с дубликатами
    if tables_with_dups > 0:
        print(f"\n🔄 ТАБЛИЦЫ С ДУБЛИКАТАМИ В SQLITE ({tables_with_dups}):")
        print("-"*150)
        dup_tables = [
            (t, sqlite_counts.get(t, {}))
            for t in sorted_tables
            if sqlite_counts.get(t, {}).get('duplicates', 0) > 0
        ]
        # Сортируем по убыванию количества дубликатов
        dup_tables.sort(key=lambda x: x[1].get('duplicates', 0), reverse=True)
        for table_name, counts in dup_tables[:20]:  # Показываем топ-20
            total = counts['total']
            unique = counts['unique']
            dups = counts['duplicates']
            pct = 100 * dups / total if total > 0 else 0
            print(f"   {table_name:<42} {total:>10,} всего, {unique:>10,} уник., "
                  f"{dups:>10,} дубл. ({pct:.1f}%)")
    
    # Детали по пропущенным таблицам
    if missing_tables > 0:
        print(f"\n⚠️ ТАБЛИЦЫ С ДАННЫМИ, ОТСУТСТВУЮЩИЕ В DWH:")
        print("-"*150)
        for sqlite_table in sorted_tables:
            counts = sqlite_counts.get(sqlite_table, {'total': 0, 'unique': 0, 'duplicates': 0})
            sqlite_unique = counts['unique']
            dwh_data = dwh_counts.get(sqlite_table, {})
            hub_count = dwh_data.get('hub', 0)
            sat_count = dwh_data.get('satellite', 0)
            
            if sqlite_unique > 0 and hub_count == 0 and sat_count == 0:
                dwh_mapping = SQLITE_TO_DWH_MAPPING.get(sqlite_table, {})
                hub_table = dwh_mapping.get('hub', 'N/A')
                sat_table = dwh_mapping.get('satellite', 'N/A')
                note = dwh_mapping.get('note', '')
                
                print(f"   {sqlite_table:<42} ({sqlite_unique:>8,} уник. строк)")
                print(f"      → Hub: {hub_table}, Satellite: {sat_table}")
                if note:
                    print(f"      💡 {note}")
    
    print("="*150 + "\n")

# =============================================================================
# ОСНОВНАЯ ФУНКЦИЯ
# =============================================================================

def main():
    print("🚀 Запуск проверки загрузки MainDb файлов (с учётом дубликатов)...")
    print(f"📊 Подключение к HDFS: {HDFS_HOST}:{HDFS_PORT}")
    print(f"📊 Подключение к DWH: {DWH_HOST}:{DWH_PORT}/{DWH_DB}")
    print(f"📋 Таблиц в конфиге: {len(SQLITE_TO_DWH_MAPPING)}")
    print()
    
    if not check_port(DWH_HOST, DWH_PORT):
        log.error(f"❌ Порт DWH {DWH_HOST}:{DWH_PORT} недоступен!")
        return
    if not check_port(HDFS_HOST, HDFS_PORT):
        log.error(f"❌ Порт NameNode {HDFS_HOST}:{HDFS_PORT} недоступен!")
        return
    if not check_port(HDFS_HOST, 9864):
        log.error(f"❌ Порт DataNode {HDFS_HOST}:9864 недоступен!")
        log.error("💡 Добавьте в docker-compose.yml для hadoop-datanode1: ports: - \"9864:9864\"")
        return
    
    log.info("✅ Все порты доступны")
    
    try:
        dwh_engine = get_dwh_engine()
        with dwh_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log.info("✅ Подключение к DWH установлено")
    except Exception as e:
        log.error(f"❌ Не удалось подключиться к DWH: {e}")
        return
    
    try:
        maindb_files = get_maindb_files_from_catalogue(dwh_engine)
        log.info(f"✅ Найдено {len(maindb_files)} MainDb файлов")
    except Exception as e:
        log.error(f"❌ Ошибка: {e}")
        return
    
    if not maindb_files:
        print("❌ Нет MainDb файлов")
        return
    
    print(f"\n{'='*150}")
    print(f"НАЙДЕНО {len(maindb_files)} MAINDB ФАЙЛОВ")
    print(f"{'='*150}\n")
    
    for idx, file_info in enumerate(maindb_files, 1):
        print(f"\n[{idx}/{len(maindb_files)}] {file_info['file_name']}")
        print("-"*150)
        
        temp_sqlite_path = None
        try:
            temp_sqlite_path = download_file_from_hdfs_chunked(file_info['hdfs_full_path'])
            
            print(f"   🔍 Подсчет строк в SQLite (с учётом дубликатов, {len(SQLITE_TO_DWH_MAPPING)} таблиц)...")
            sqlite_counts = count_sqlite_tables_by_config(
                temp_sqlite_path, 
                list(SQLITE_TO_DWH_MAPPING.keys())
            )
            total_rows = sum(c['total'] for c in sqlite_counts.values())
            unique_rows = sum(c['unique'] for c in sqlite_counts.values())
            dup_rows = sum(c['duplicates'] for c in sqlite_counts.values())
            tables_with_data = sum(1 for c in sqlite_counts.values() if c['unique'] > 0)
            print(f"   ✅ Найдено {tables_with_data} таблиц с данными")
            print(f"   📊 Всего: {total_rows:,} строк, {unique_rows:,} уникальных, {dup_rows:,} дубликатов")
            
            print("   🔍 Подсчет строк в DWH (Hub + Satellite)...")
            dwh_counts = count_dwh_hub_satellite(dwh_engine, file_info['data_file_id'])
            print(f"   ✅ Проверено {len(dwh_counts)} маппингов")
            
            print_hub_sat_report(file_info, sqlite_counts, dwh_counts)
            
        except Exception as e:
            print(f"   ❌ Ошибка: {e}")
            log.error(f"   {e}", exc_info=True)
        finally:
            if temp_sqlite_path and os.path.exists(temp_sqlite_path):
                _safe_delete_file(temp_sqlite_path)
    
    print("\n🎉 Проверка завершена!")
    dwh_engine.dispose()


if __name__ == "__main__":
    main()