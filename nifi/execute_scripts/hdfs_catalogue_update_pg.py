#!/usr/bin/env python3
import sys
import json
import os
import argparse
import psycopg2
import psycopg2.extras  # 🔑 ДОБАВЛЕНО: поддержка UUID
psycopg2.extras.register_uuid()  # 🔑 КРИТИЧЕСКИ ВАЖНО: регистрация до подключения к БД!
import uuid  # 🔑 ДОБАВЛЕНО: работа с UUID
from pathlib import Path
from datetime import datetime
import traceback
import binascii

# === НАСТРОЙКИ ПОДКЛЮЧЕНИЯ К Cloudberry ===
CB_CONFIG = {
    "host": "172.21.0.50",
    "port": 7000,
    "dbname": "dwh",
    "user": "gpadmin",
    "password": "gpadmin_password"
}

CATALOGUE_TABLE = "data_catalogue"

# === СТРОКОВЫЕ ТИПЫ ДАННЫХ ===
class DataType:
    MainDb = "MainDb"
    Double = "Double"
    Any = "Any"

class DataFormat:
    sqlite = "sqlite"
    fh = "fh"
    other = "other"

# Маппинг числовых значений для обратной совместимости
NUMERIC_DATATYPE_MAPPING = {
    1: DataType.MainDb,
    2: DataType.Double,
    3: DataType.Any
}

NUMERIC_DATAFORMAT_MAPPING = {
    1: DataFormat.sqlite,
    2: DataFormat.fh,
    3: DataFormat.other
}

def to_uuid(value) -> uuid.UUID:
    """
    Преобразует входное значение (строка, bytes) в объект UUID.
    Поддерживает:
      - Строки в формате UUID (с дефисами и без)
      - 16-байтные последовательности (bytes)
      - Hex-строки длиной 32 символа
    """
    if value is None:
        raise ValueError("UUID value cannot be None")
    
    if isinstance(value, uuid.UUID):
        return value
    
    if isinstance(value, bytes):
        if len(value) == 16:
            return uuid.UUID(bytes=value)
        else:
            raise ValueError(f"Expected 16 bytes for UUID, got {len(value)} bytes")
    
    if isinstance(value, str):
        s = value.strip()
        # Убираем префикс 0x если есть
        if s.startswith(("0x", "0X")):
            s = s[2:]
        # Пробуем распарсить как стандартный UUID
        try:
            return uuid.UUID(s)
        except ValueError:
            # Убираем дефисы и пробелы для обработки "чистого" hex
            s_clean = s.replace('-', '').replace(' ', '')
            if len(s_clean) == 32:
                try:
                    return uuid.UUID(hex=s_clean)
                except ValueError:
                    pass
            raise ValueError(f"Cannot convert string to UUID: {value}")
    
    raise TypeError(f"Cannot convert {type(value)} to UUID")

def to_hex_string(value) -> str:
    """
    Преобразует входное значение (bytes, hex-строка) в нормализованную hex-строку (нижний регистр, без префиксов).
    Примеры:
      - b'\xde\xad\xbe\xef' → "deadbeef"
      - "DEADBEEF" → "deadbeef"
      - "0xdeadbeef" → "deadbeef"
      - "dead-beef" → "deadbeef"
    """
    if value is None:
        raise ValueError("Hash value cannot be None")
    
    if isinstance(value, bytes):
        return binascii.hexlify(value).decode('ascii').lower()
    
    if isinstance(value, str):
        s = value.strip()
        # Убираем префикс 0x/0X
        if s.startswith(("0x", "0X")):
            s = s[2:]
        # Убираем дефисы, двоеточия, пробелы (для форматов вроде "de:ad:be:ef")
        s = s.replace('-', '').replace(':', '').replace(' ', '').lower()
        # Валидация: должна быть четной длины и содержать только hex-символы
        if len(s) % 2 != 0 or not all(c in '0123456789abcdef' for c in s):
            raise ValueError(f"Invalid hex string: {value}")
        return s
    
    raise TypeError(f"Cannot convert {type(value)} to hex string")

def normalize_data_type(value) -> str:
    """Нормализует значение типа данных в строку."""
    if isinstance(value, str):
        return value.strip() or DataType.Any
    elif isinstance(value, (int, float)):
        return NUMERIC_DATATYPE_MAPPING.get(int(value), DataType.Any)
    else:
        return DataType.Any

def normalize_data_format(value) -> str:
    """Нормализует значение формата данных в строку."""
    if isinstance(value, str):
        return value.strip() or DataFormat.other
    elif isinstance(value, (int, float)):
        return NUMERIC_DATAFORMAT_MAPPING.get(int(value), DataFormat.other)
    else:
        return DataFormat.other

def get_postgres_connection():
    dsn = " ".join(f"{k}={v}" for k, v in CB_CONFIG.items())
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    return conn

def migrate_bytea_to_text(conn) -> None:
    """Миграция существующих данных из BYTEA в TEXT (выполняется один раз при обнаружении старой схемы)."""
    cur = conn.cursor()
    try:
        # Проверяем тип колонки file_hash
        cur.execute("""
            SELECT data_type 
            FROM information_schema.columns 
            WHERE table_name = %s AND column_name = 'file_hash'
        """, (CATALOGUE_TABLE,))
        result = cur.fetchone()
        
        if result and result[0].lower() == 'bytea':
            print("[INFO] Migrating file_hash from BYTEA to TEXT...", file=sys.stderr)
            # Создаём временную колонку
            cur.execute(f'ALTER TABLE "{CATALOGUE_TABLE}" ADD COLUMN IF NOT EXISTS "file_hash_tmp" TEXT')
            # Конвертируем данные
            cur.execute(f'UPDATE "{CATALOGUE_TABLE}" SET "file_hash_tmp" = encode("file_hash", \'hex\')')
            # Удаляем старую колонку и переименовываем новую
            cur.execute(f'ALTER TABLE "{CATALOGUE_TABLE}" DROP COLUMN "file_hash"')
            cur.execute(f'ALTER TABLE "{CATALOGUE_TABLE}" RENAME COLUMN "file_hash_tmp" TO "file_hash"')
            # Пересоздаём индекс
            cur.execute(f'DROP INDEX IF EXISTS idx_{CATALOGUE_TABLE}_file_hash')
            cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{CATALOGUE_TABLE}_file_hash ON "{CATALOGUE_TABLE}"("file_hash")')
            print("[INFO] Migration completed successfully", file=sys.stderr)
    finally:
        cur.close()

def ensure_catalogue_schema(conn) -> None:
    cur = conn.cursor()
    try:
        # ИСПРАВЛЕНО: колонка первичного ключа называется "id" (строчная буква), а не "Id"
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS "{CATALOGUE_TABLE}" (
                "id" SERIAL PRIMARY KEY,  
                "data_source_id" UUID NOT NULL,
                "main_db_id" UUID NOT NULL,
                "main_db_name" TEXT NOT NULL,
                "file_name" TEXT,
                "file_size" INTEGER,
                "file_hash" TEXT NOT NULL, 
                "last_update" TIMESTAMP NOT NULL,
                "upload_date_time" TIMESTAMP NOT NULL,
                "hdfs_storage_path" TEXT NOT NULL,
                "data_type" TEXT NOT NULL,
                "data_format" TEXT NOT NULL
            );
        """)
        cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{CATALOGUE_TABLE}_data_source_id ON "{CATALOGUE_TABLE}"("data_source_id");')
        cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{CATALOGUE_TABLE}_file_hash ON "{CATALOGUE_TABLE}"("file_hash");')
        
        # Выполняем миграцию если нужно
        migrate_bytea_to_text(conn)
    finally:
        cur.close()

def upsert_catalogue(conn, row: dict) -> int:
    cur = conn.cursor()
    try:
        # 🔑 ИСПРАВЛЕНО: "id" вместо "Id" в SELECT
        cur.execute(f'SELECT "id", "file_hash" FROM "{CATALOGUE_TABLE}" WHERE "data_source_id" = %s LIMIT 1;', (row["data_source_id"],))
        existing = cur.fetchone()

        if existing:
            existing_id, existing_hash = existing
            # Сравниваем хеш как строки (в нижнем регистре для надёжности)
            if existing_hash.lower() == row["file_hash"].lower():
                # Файл не изменился — ничего не делаем
                print("[DEBUG] File hash unchanged — skipping update", file=sys.stderr)
                return existing_id
            else:
                # Хеш изменился — обновляем
                print("[DEBUG] File hash changed — updating record", file=sys.stderr)
                # 🔑 ИСПРАВЛЕНО: "id" вместо "Id" в WHERE
                cur.execute(f"""
                    UPDATE "{CATALOGUE_TABLE}"
                    SET "main_db_id" = %s, "main_db_name" = %s, "file_name" = %s, "file_size" = %s,
                        "file_hash" = %s, "last_update" = %s, "upload_date_time" = %s, "hdfs_storage_path" = %s,
                        "data_type" = %s, "data_format" = %s
                    WHERE "id" = %s;
                """, (
                    row["main_db_id"], row["main_db_name"], row["file_name"], row["file_size"],
                    row["file_hash"], row["last_update"], row["upload_date_time"], row["hdfs_storage_path"],
                    row["data_type"], row["data_format"], existing_id
                ))
                return existing_id
        else:
            # 🔑 ВСТАВКА С ОБЪЕКТАМИ UUID
            print("[DEBUG] Inserting new record", file=sys.stderr)
            # 🔑 ИСПРАВЛЕНО: "id" вместо "Id" в RETURNING
            cur.execute(f"""
                INSERT INTO "{CATALOGUE_TABLE}"
                ("data_source_id", "main_db_id", "main_db_name", "file_name", "file_size", "file_hash",
                 "last_update", "upload_date_time", "hdfs_storage_path", "data_type", "data_format")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING "id";
            """, (
                row["data_source_id"],
                row["main_db_id"],
                row["main_db_name"], row["file_name"], row["file_size"], row["file_hash"],
                row["last_update"], row["upload_date_time"], row["hdfs_storage_path"],
                row["data_type"], row["data_format"]
            ))
            result = cur.fetchone()
            return result[0] if result else -1
    finally:
        cur.close()

def main():
    # === Определяем режим: stdin или --json ===
    use_stdin = True
    raw_input = ""

    if len(sys.argv) > 1 and '--json' in sys.argv:
        use_stdin = False
        parser = argparse.ArgumentParser()
        parser.add_argument("--json", required=True)
        args = parser.parse_args()
        raw_input = args.json
        print(f"[DEBUG] Mode: command-line argument", file=sys.stderr)
    else:
        print(f"[DEBUG] Mode: reading from stdin", file=sys.stderr)
        raw_input = sys.stdin.read()

    print(f"[DEBUG] Raw input length: {len(raw_input)} chars", file=sys.stderr)
    print(f"[DEBUG] First 300 chars of input:\n{repr(raw_input[:300])}", file=sys.stderr)

    if not raw_input.strip():
        print(json.dumps({"ok": True, "skipped": True, "reason": "empty_input"}, ensure_ascii=False))
        return

    # === Парсинг JSON ===
    attr = None
    try:
        attr = json.loads(raw_input)
        print("[DEBUG] JSON parsed successfully", file=sys.stderr)
    except json.JSONDecodeError as e:
        error_msg = f"JSON decode error: {str(e)}"
        print(f"[ERROR] {error_msg}", file=sys.stderr)
        print(json.dumps({"ok": False, "error": error_msg}, ensure_ascii=False), file=sys.stderr)
        return

    # === Логика обработки ===
    try:
        file_operation = str(attr.get("file_operation", "")).strip()
        file_path = str(attr.get("file_path", "")).strip()

        if not file_path or file_operation.lower() == "skip":
            print(json.dumps({"ok": True, "skipped": True}, ensure_ascii=False))
            return

        file_name = Path(file_path).name
        hdfs_storage_path = str(attr.get("hdfs_storage_path") or Path(file_path).parent).strip()
        upload_dt_str = str(attr.get("upload_date_time", "")).strip() or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        last_update_str = str(attr.get("last_update", "")).strip() or datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # === Обязательно: file_hash должен быть передан как hex-строка или bytes ===
        file_hash_raw = attr.get("file_hash")
        if not file_hash_raw:
            print(json.dumps({
                "ok": False,
                "error": "Missing 'file_hash' in input JSON"
            }, ensure_ascii=False), file=sys.stderr)
            return

        # 🔑 КОНВЕРТАЦИЯ В UUID
        try:
            data_source_id = to_uuid(attr.get("data_id"))
            main_db_id = to_uuid(attr.get("main_db_id"))
        except (ValueError, TypeError) as e:
            print(json.dumps({
                "ok": False,
                "error": f"Invalid UUID format: {str(e)}",
                "field": "data_id or main_db_id"
            }, ensure_ascii=False), file=sys.stderr)
            return

        # 🔑 КОНВЕРТАЦИЯ ХЕША В СТРОКУ
        try:
            file_hash_str = to_hex_string(file_hash_raw)
        except (ValueError, TypeError) as e:
            print(json.dumps({
                "ok": False,
                "error": f"Invalid hash format: {str(e)}",
                "field": "file_hash"
            }, ensure_ascii=False), file=sys.stderr)
            return

        # Нормализуем типы данных в строки (с поддержкой обратной совместимости)
        data_type_str = normalize_data_type(attr.get("data_type"))
        data_format_str = normalize_data_format(attr.get("data_format"))

        row = {
            "id": int(attr.get("id", 0) or 0),  # 🔑 ИСПРАВЛЕНО: "id" вместо "Id" для внутренней согласованности
            "data_source_id": data_source_id,
            "main_db_id": main_db_id,
            "main_db_name": str(attr.get("main_db_name", "")).strip(),
            "file_name": file_name,
            "file_size": int(attr.get("file_size", 0) or 0),
            "file_hash": file_hash_str,  # 🔑 ИЗМЕНЕНО: строка вместо bytes
            "last_update": last_update_str,
            "upload_date_time": upload_dt_str,
            "hdfs_storage_path": hdfs_storage_path,
            "data_type": data_type_str,
            "data_format": data_format_str
        }

        # Валидация обязательных полей (нулевой UUID считается недопустимым)
        if data_source_id.int == 0 or main_db_id.int == 0 or not row["main_db_name"] or not row["hdfs_storage_path"]:
            print(json.dumps({"ok": True, "skipped": True, "reason": "missing_required_fields"}, ensure_ascii=False))
            return

        conn = get_postgres_connection()
        try:
            ensure_catalogue_schema(conn)
            row_id = upsert_catalogue(conn, row)
        finally:
            conn.close()

        # Определяем, был ли пропущен файл из-за совпадения хеша
        resp = {
            "ok": True,
            "updated": True,
            "file_operation": file_operation,
            "file_path": file_path,
            "id": row_id,
            "data_type": row["data_type"],
            "data_format": row["data_format"]
        }
        print(json.dumps(resp, ensure_ascii=False))

    except Exception as e:
        print(json.dumps({
            "ok": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }, ensure_ascii=False), file=sys.stderr)

if __name__ == "__main__":
    main()