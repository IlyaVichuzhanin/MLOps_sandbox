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
from contextlib import contextmanager
from typing import Dict, Any, Optional, List
import uuid
from sqlalchemy import create_engine, Column, String, DateTime, text
from sqlalchemy.dialects.postgresql import UUID, insert
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from uuid import uuid4
from datetime import datetime, timezone
from typing import Optional



def get_data_from_sqlite(sqlite_path: str, sql_query: str) -> Dict[str, Any]:
    if not sql_query:
        raise AirflowException(
            "Не указан SQL-запрос. Передайте параметр 'sql_query' при запуске DAG:\n"
            "  airflow dags trigger read_sqlite_file --conf '{\"sqlite_path\": \"/path/to/file.db\", \"sql_query\": \"SELECT * FROM users\"}'"
        )
    
    try:
        print(f"Выполнение запроса к {sqlite_path}:")
        print(f"  {sql_query}")

        conn = sqlite3.connect(sqlite_path)
        cursor = conn.cursor()
        cursor.execute(sql_query)
        
        # Для запросов без результата (INSERT/UPDATE/DELETE)
        if cursor.description is None:
            conn.commit()
            cursor.close()
            conn.close()
            print("✓ Запрос выполнен (без возвращаемых данных)")
            return []
        
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        # Возвращаем данные с обработкой BLOB-данных
        results = []
        for row in rows:
            formatted_row = {}
            for col, val in zip(columns, row):
                # Обработка BLOB-данных
                if isinstance(val, bytes):
                    # Попытка преобразовать в UUID (16-байтовые данные)
                    if len(val) == 16:
                        try:
                            # Используем bytes_le для правильного преобразования
                            # Это ключевое изменение для корректного преобразования
                            formatted_row[col] = str(uuid.UUID(bytes_le=val)).upper()
                        except:
                            # Если не получается, попробуем как строку
                            try:
                                formatted_row[col] = val.decode('utf-8')
                            except:
                                formatted_row[col] = f"<BLOB:{len(val)} bytes>"
                    else:
                        # Для других байтовых данных
                        try:
                            formatted_row[col] = val.decode('utf-8')
                        except:
                            formatted_row[col] = f"<BLOB:{len(val)} bytes>"
                else:
                    formatted_row[col] = val
            results.append(formatted_row)
            
        cursor.close()
        conn.close()
        
        print(f"✓ Запрос выполнен успешно. Получено строк: {len(results)}")
        return results
            
    except sqlite3.Error as e:
        raise AirflowException(f"Ошибка выполнения SQL-запроса к SQLite: {str(e)}")
    except Exception as e:
        raise AirflowException(f"Неожиданная ошибка при выполнении запроса: {str(e)}")
    

def get_data_from_dwh(sql_query: str) -> Dict[str, Any]:
    """
    Выполняет SQL-запрос к базе данных Cloudberry через подключение Airflow.
    """

    if not sql_query or not sql_query.strip():
        raise AirflowException(
            "Не указан SQL-запрос. Передайте параметр 'sql_query' при запуске DAG:\n"
            "  airflow dags trigger query_cloudberry --conf '{\"sql_query\": \"SELECT * FROM schema.table\"}'"
        )
    
    try:
        # print(f"Выполнение запроса к Cloudberry (cloudberry_test_dwh):")
        # print(f"  {sql_query}")
        
        hook = PostgresHook(postgres_conn_id='cloudberry_test_dwh')
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(sql_query)
        # Для запросов без результата (INSERT/UPDATE/DELETE)
        if cursor.description is None:
            conn.commit()
            cursor.close()
            conn.close()
            # print("✓ Запрос выполнен (без возвращаемых данных)")
            return []
        
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        # Возвращаем данные "как есть" из БД — без форматирования
        results = [dict(zip(columns, row)) for row in rows]
        cursor.close()
        conn.close()
        
        # print(f"✓ Запрос выполнен успешно. Получено строк: {len(results)}")
        return results
            
    except Exception as e:
        raise AirflowException(f"Ошибка выполнения SQL-запроса к Cloudberry: {str(e)}")
    

def get_data_from_sqlite(sqlite_path: str, sql_query: str) -> List[Any]:
    """
    Вспомогательный метод для выполнения SQL запросов к SQLite.
    """
    if not sql_query:
        raise AirflowException(
            "Не указан SQL-запрос. Передайте параметр 'sql_query' при запуске DAG:\n"
            "  airflow dags trigger read_sqlite_file --conf '{\"sqlite_path\": \"/path/to/file.db\", \"sql_query\": \"SELECT * FROM users\"}'"
        )
    
    try:
        print(f"Выполнение запроса к {sqlite_path}:")
        print(f"  {sql_query}")

        conn = sqlite3.connect(sqlite_path)
        cursor = conn.cursor()  
        cursor.execute(sql_query)
        
        # Для запросов без результата (INSERT/UPDATE/DELETE)
        if cursor.description is None:
            conn.commit()
            cursor.close()
            conn.close()
            print("✓ Запрос выполнен (без возвращаемых данных)")
            return []
        
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        # Возвращаем данные с обработкой BLOB-данных
        results = []
        for row in rows:
            formatted_row = {}
            for col, val in zip(columns, row):
                # Обработка BLOB-данных
                if isinstance(val, bytes):
                    # Попытка преобразовать в UUID (16-байтовые данные)
                    if len(val) == 16:
                        try:
                            # Используем bytes_le для правильного преобразования
                            formatted_row[col] = str(uuid.UUID(bytes_le=val)).upper()
                        except:
                            # Если не получается, попробуем как строку
                            try:
                                formatted_row[col] = val.decode('utf-8')
                            except:
                                formatted_row[col] = f"<BLOB:{len(val)} bytes>"
                    else:
                        # Для других байтовых данных
                        try:
                            formatted_row[col] = val.decode('utf-8')
                        except:
                            formatted_row[col] = f"<BLOB:{len(val)} bytes>"
                else:
                    formatted_row[col] = val
            results.append(formatted_row)
            
        cursor.close()
        conn.close()
        
        print(f"✓ Запрос выполнен успешно. Получено строк: {len(results)}")
        return results
            
    except sqlite3.Error as e:
        raise AirflowException(f"Ошибка выполнения SQL-запроса к SQLite: {str(e)}")
    except Exception as e:
        raise AirflowException(f"Неожиданная ошибка при выполнении запроса: {str(e)}")
    
def insert_row_in_dwh(sql_query: str)->bool:

    hook = PostgresHook(postgres_conn_id='cloudberry_test_dwh')
    engine = hook.get_sqlalchemy_engine()
    
    # Создаём сессию
    with Session(engine) as session:
        try:
            # Генерация данных
            user_id = str(uuid4())
            user_name = f"User_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Выполнение UPSERT
            result = upsert_user_sqlalchemy(session, user_id, user_name)
            session.commit()
            
            # Логирование и XCom
            context['ti'].xcom_push(key='user_id', value=user_id)
            context['ti'].xcom_push(key='status', value=result)
            
            print(f"✅ UPSERT выполнен: {user_id} -> {user_name}")
            return {'status': result, 'user_id': user_id}
            
        except Exception as e:
            session.rollback()
            print(f"❌ Ошибка UPSERT: {e}")
            raise



    return True


