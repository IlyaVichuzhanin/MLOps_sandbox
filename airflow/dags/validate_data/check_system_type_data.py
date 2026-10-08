from pathlib import Path
from datetime import datetime, timedelta
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
import sqlite3
import tempfile
import os
import json
import logging
from contextlib import contextmanager
from typing import Dict, Any, Optional, List
import uuid
from db_requests.db_request_methods import get_data_from_dwh, get_data_from_sqlite

log = logging.getLogger(__name__)



def check_system_type_data_tables(
    sqlite_path: str, 
    source_table_name: str, 
    source_column_name: str, 
    dwh_table_name: str, 
    dwh_column_name: str
) -> bool:
    query_to_sqlite = f"SELECT DISTINCT {source_column_name} FROM {source_table_name}"
    query_to_dwh = f"SELECT {dwh_column_name} FROM {dwh_table_name}"

    system_types_source = get_data_from_sqlite(sqlite_path=sqlite_path, sql_query=query_to_sqlite)
    system_types_dwh = get_data_from_dwh(sql_query=query_to_dwh)
    
    # Извлекаем значения с нормализацией типов
    sqlite_values = []
    for row in system_types_source:
        val = row[source_column_name]
        # Нормализуем: приводим к строке и убираем лишние пробелы
        if val is not None:
            sqlite_values.append(str(val).strip().lower() if isinstance(val, str) else str(val))
        else:
            sqlite_values.append(None)
    
    dwh_values = []
    for row in system_types_dwh:
        val = row[dwh_column_name]
        if val is not None:
            dwh_values.append(str(val).strip().lower() if isinstance(val, str) else str(val))
        else:
            dwh_values.append(None)
    
    sqlite_set = set(sqlite_values)
    dwh_set = set(dwh_values)
    
    # 🔍 ДЕТАЛЬНОЕ ЛОГИРОВАНИЕ ДЛЯ ОТЛАДКИ
    print(f"\n{'='*70}")
    print(f"🔍 ПРОВЕРКА СПРАВОЧНИКА: {source_table_name}.{source_column_name} → {dwh_table_name}.{dwh_column_name}")
    print(f"{'='*70}")
    
    print(f"\n🔍 Анализ расхождений:")
    print(f"   SQLite уникальные значения: {sqlite_set}")
    print(f"   DWH уникальные значения:    {dwh_set}")
    
    missing_in_dwh = sqlite_set - dwh_set
    
    if missing_in_dwh:
        print(f"\n❌ КРИТИЧНО: Значения из SQLite ОТСУТСТВУЮТ в DWH ({len(missing_in_dwh)}):")
        for val in missing_in_dwh:
            print(f"   → '{val}'")
    
    print(f"{'='*70}\n")
    
    return len(missing_in_dwh) == 0

def check_table_exist(table_name: str, sqlite_path: str) -> bool:
    # ✅ Используем параметризованный запрос для защиты от SQL Injection
    sql_query = """SELECT name 
                   FROM sqlite_master 
                   WHERE type = 'table' 
                   AND name = ?;"""
    
    try:
        conn = sqlite3.connect(sqlite_path)
        cursor = conn.cursor()
        
        # ✅ Передаем table_name как параметр (кортеж)
        cursor.execute(sql_query, (table_name,))
        
        # ✅ Безопасно получаем результат
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        # ✅ Если строка найдена - таблица существует (True), иначе - False
        return result is not None
        
    except sqlite3.Error as e:
        # Логируем ошибку БД для отладки
        logging.error(f"SQLite error checking table {table_name}: {e}")
        raise AirflowException(f"Ошибка выполнения SQL-запроса к SQLite: {str(e)}")
    except Exception as e:
        logging.error(f"Unexpected error checking table {table_name}: {e}")
        raise AirflowException(f"Неожиданная ошибка при выполнении запроса: {str(e)}")
        


        
        

    



