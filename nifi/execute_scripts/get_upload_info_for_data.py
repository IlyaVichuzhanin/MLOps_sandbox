#!/usr/bin/env python3
import sys
import os
import hashlib
import psycopg2
import psycopg2.extras  # 🔑 ДОБАВЛЕНО
psycopg2.extras.register_uuid()  # 🔑 КРИТИЧЕСКИ ВАЖНО: до любого подключения к БД!
from psycopg2.extras import RealDictCursor
from pathlib import Path
from datetime import datetime
import shutil
import json
import sqlite3
import uuid


# === НАСТРОЙКИ ПОДКЛЮЧЕНИЯ К Cloudberry ===
CB_CONFIG = {
    "host": "172.21.0.50",
    "port": 7000,
    "dbname": "dwh",
    "user": "gpadmin",
    "password": "gpadmin_password"
}

TEMP_FILES_PATH_DIR = Path("/home/temp_data")

DB_IDENT_TABLE_NAME = "DatabaseIdent"
CATALOGUE_TABLE = "data_catalogue"


# === СТРОКОВЫЕ ТИПЫ ДАННЫХ (ВМЕСТО ЧИСЕЛ) ===
class DataType:
    MainDb = "MainDb"
    Double = "Double"
    Any = "Any"

class DataFormat:
    sqlite = "sqlite"
    fh = "fh"
    other = "other"

class FileOperation:
    Replace = "Replace"
    Add = "Add"
    Skip = "Skip"


# Маппинг старых числовых значений из SQLite на новые строковые типы
SQLITE_DATATYPE_MAPPING = {
    1: DataType.MainDb,
    2: DataType.Double,
    3: DataType.Any
}


def calculate_sha256(filepath: str) -> str:
    """Возвращает SHA-256 хеш файла как шестнадцатеричную строку (64 символа)."""
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()  # 🔑 СТРОКА вместо bytes


class DataIdentInfo:
    def __init__(self, file_path: str, data_id: uuid.UUID, main_db_name: str, main_db_id: uuid.UUID, data_type: str, data_format: str, size: int,
                 last_date_time_update: datetime, file_hash: str):  # 🔑 str вместо bytes
        if not isinstance(data_id, uuid.UUID):
            raise TypeError(f"data_id must be uuid.UUID, got {type(data_id)}")
        if not isinstance(main_db_id, uuid.UUID):
            raise TypeError(f"main_db_id must be uuid.UUID, got {type(main_db_id)}")
        if not isinstance(file_hash, str) or len(file_hash) != 64:
            raise TypeError(f"file_hash must be 64-char hex string, got {type(file_hash)} len={len(file_hash) if hasattr(file_hash, '__len__') else 'N/A'}")

        self.file_path = Path(file_path)
        self.data_id = data_id
        self.main_db_name = str(main_db_name)
        self.main_db_id = main_db_id
        self.data_type = str(data_type)
        self.data_format = str(data_format)
        self.size = int(size)
        self.last_date_time_update = last_date_time_update
        self.file_hash = file_hash  # 🔑 СТРОКА


class UploadInfo:
    def __init__(self,
                 file_operation: str,
                 file_path: Path,
                 id: int = None,
                 data_id: uuid.UUID = None,
                 main_db_id: uuid.UUID = None,
                 main_db_name: str = None,
                 file_size: int = None,
                 last_update: datetime = None,
                 upload_date_time: str = None,
                 hdfs_storage_path: Path = None,
                 hdfs_full_path: Path = None,
                 data_type: str = None,
                 data_format: str = None,
                 file_hash: str = None):  # 🔑 str вместо bytes
        self.file_operation = str(file_operation)
        self.file_path = file_path
        self.id = int(id) if id is not None else None
        self.data_id = data_id
        self.main_db_id = main_db_id
        self.main_db_name = str(main_db_name) if main_db_name else None
        self.file_size = int(file_size) if file_size is not None else None
        self.last_update = last_update
        self.upload_date_time = str(upload_date_time) if upload_date_time else None
        self.hdfs_storage_path = hdfs_storage_path
        self.hdfs_full_path = hdfs_full_path
        self.data_type = str(data_type) if data_type is not None else None
        self.data_format = str(data_format) if data_format is not None else None
        self.file_hash = str(file_hash) if file_hash is not None else None  # 🔑 СТРОКА


def get_data_format(file_path: Path) -> str:
    """Определяет формат данных по расширению файла (возвращает строку)."""
    ext = file_path.suffix.lower()
    if ext == ".db":
        return DataFormat.sqlite
    elif ext == ".fh":
        return DataFormat.fh
    else:
        return DataFormat.other


def get_cloudberry_connection():
    try:
        dsn = " ".join(f"{k}={v}" for k, v in CB_CONFIG.items())
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        return conn
    except Exception as e:
        raise ConnectionError(f"Cloudberry error: {e}")


def json_default(obj):
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, uuid.UUID):
        return str(obj)
    return str(obj)


def ensure_catalogue_schema(conn):
    cur = conn.cursor()
    try:
        # 🔑 ИЗМЕНЕНО: поле "file_hash" теперь TEXT (64 символа) вместо BYTEA
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS "{CATALOGUE_TABLE}" (
                "id" SERIAL PRIMARY KEY,
                "data_source_id" UUID NOT NULL,
                "main_db_id" UUID NOT NULL,
                "main_db_name" TEXT NOT NULL,
                "file_name" TEXT,
                "file_size" INTEGER,
                "file_hash" TEXT CHECK (LENGTH("file_hash") = 64),  -- 🔑 СТРОКОВЫЙ ХЕШ (64 символа)
                "last_update" TIMESTAMP,
                "upload_date_time" TIMESTAMP,
                "hdfs_storage_path" TEXT,
                "hdfs_full_path" TEXT,
                "data_type" TEXT,
                "data_format" TEXT,
                "is_uploaded_to_dwh" BOOL DEFAULT false
            );
        """)
        cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{CATALOGUE_TABLE}_data_source_id ON "{CATALOGUE_TABLE}"("data_source_id");')
        cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{CATALOGUE_TABLE}_file_hash ON "{CATALOGUE_TABLE}"("file_hash");')  # 🔑 Индекс работает со строками
    finally:
        cur.close()


def ensure_local_catalogue_exists():
    conn = get_cloudberry_connection()
    try:
        ensure_catalogue_schema(conn)
    finally:
        conn.close()


def save_to_catalogue(upload_info: UploadInfo):
    """
    Сохраняет информацию о файле в каталог данных.
    Обновляет запись при замене, вставляет новую при добавлении.
    """
    conn = get_cloudberry_connection()
    cursor = conn.cursor()
    try:
        if upload_info.file_operation == FileOperation.Replace and upload_info.id is not None:
            # 🔑 ОБНОВЛЕНИЕ с хешем как строкой
            cursor.execute(f"""
                UPDATE "{CATALOGUE_TABLE}"
                SET "file_name" = %s,
                    "file_size" = %s,
                    "file_hash" = %s,  -- 🔑 СТРОКА
                    "last_update" = %s,
                    "upload_date_time" = %s,
                    "hdfs_storage_path" = %s,
                    "hdfs_full_path" = %s,
                    "data_type" = %s,
                    "data_format" = %s,
                    "is_uploaded_to_dwh" = false
                WHERE "id" = %s
            """, (
                upload_info.file_path.name,
                upload_info.file_size,
                upload_info.file_hash,  # 🔑 СТРОКА
                upload_info.last_update,
                upload_info.upload_date_time,
                str(upload_info.hdfs_storage_path),
                str(upload_info.hdfs_full_path),
                upload_info.data_type,
                upload_info.data_format,
                upload_info.id
            ))
            sys.stderr.write(f"INFO: Updated catalogue record id={upload_info.id}\n")
        
        elif upload_info.file_operation == FileOperation.Add:
            # 🔑 ВСТАВКА с хешем как строкой
            cursor.execute(f"""
                INSERT INTO "{CATALOGUE_TABLE}" (
                    "data_source_id", "main_db_id", "main_db_name", "file_name",
                    "file_size", "file_hash", "last_update", "upload_date_time",
                    "hdfs_storage_path", "hdfs_full_path",
                    "data_type", "data_format", "is_uploaded_to_dwh"
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING "id"
            """, (
                upload_info.data_id,
                upload_info.main_db_id,
                upload_info.main_db_name,
                upload_info.file_path.name,
                upload_info.file_size,
                upload_info.file_hash,  # 🔑 СТРОКА
                upload_info.last_update,
                upload_info.upload_date_time,
                str(upload_info.hdfs_storage_path),
                str(upload_info.hdfs_full_path),
                upload_info.data_type,
                upload_info.data_format,
                False
            ))
            new_id = cursor.fetchone()[0]
            sys.stderr.write(f"INFO: Inserted new catalogue record id={new_id}\n")
    finally:
        cursor.close()
        conn.close()


def get_data_ident_info(path_to_file: str) -> DataIdentInfo:
    stat = os.stat(path_to_file)
    file_hash = calculate_sha256(path_to_file)  # 🔑 ВОЗВРАЩАЕТ СТРОКУ

    conn = sqlite3.connect(path_to_file)
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT Id, MainDbName, MainDbId, DataType FROM {DB_IDENT_TABLE_NAME} LIMIT 1;")
        row = cursor.fetchone()
        if not row:
            raise ValueError("No data in DatabaseIdent table")

        data_id_blob = row[0]
        main_db_id_blob = row[2]
        
        if not isinstance(data_id_blob, (bytes, bytearray)) or len(data_id_blob) != 16:
            raise ValueError(f"data_source_id must be 16-byte UUID BLOB, got {type(data_id_blob)} len={len(data_id_blob) if hasattr(data_id_blob, '__len__') else 'N/A'}")
        if not isinstance(main_db_id_blob, (bytes, bytearray)) or len(main_db_id_blob) != 16:
            raise ValueError(f"main_db_id must be 16-byte UUID BLOB, got {type(main_db_id_blob)} len={len(main_db_id_blob) if hasattr(main_db_id_blob, '__len__') else 'N/A'}")
        
        data_id = uuid.UUID(bytes=data_id_blob)
        main_db_id = uuid.UUID(bytes=main_db_id_blob)

        raw_data_type = row[3]
        data_type_str = SQLITE_DATATYPE_MAPPING.get(raw_data_type, DataType.Any)

        return DataIdentInfo(
            file_path=path_to_file,
            data_id=data_id,
            main_db_name=row[1],
            main_db_id=main_db_id,
            data_type=data_type_str,
            data_format=get_data_format(Path(path_to_file)),
            size=stat.st_size,
            last_date_time_update=datetime.fromtimestamp(stat.st_mtime),
            file_hash=file_hash  # 🔑 СТРОКА
        )
    finally:
        conn.close()


def get_upload_file_path(data_ident_info: DataIdentInfo, with_date: bool = False) -> Path:
    base_dir = Path("/test_raw_data/data")
    data_type_name = data_ident_info.data_type
    subdir = base_dir / data_ident_info.main_db_name / data_type_name
    if with_date:
        dt_string = data_ident_info.last_date_time_update.strftime("%Y-%m-%d %H-%M")
        file_name = f"{data_ident_info.file_path.stem}_{dt_string}{data_ident_info.file_path.suffix}"
        return subdir / file_name
    else:
        return subdir / data_ident_info.file_path.name


def get_hdfs_upload_info(data_ident_info: DataIdentInfo) -> UploadInfo:
    data_id = data_ident_info.data_id
    main_db_type = data_ident_info.data_type == DataType.MainDb
    file_path = get_upload_file_path(data_ident_info, with_date=False)

    conn = get_cloudberry_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        if main_db_type:
            return UploadInfo(
                file_operation=FileOperation.Add,
                file_path=file_path,
                data_id=data_id,
                main_db_id=data_ident_info.main_db_id,
                main_db_name=data_ident_info.main_db_name,
                file_size=data_ident_info.size,
                last_update=data_ident_info.last_date_time_update,
                upload_date_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                hdfs_storage_path=file_path.parent,
                hdfs_full_path=file_path,
                data_type=data_ident_info.data_type,
                data_format=data_ident_info.data_format,
                file_hash=data_ident_info.file_hash  # 🔑 СТРОКА
            )

        # Ищем запись по data_source_id
        cursor.execute(f'SELECT * FROM "{CATALOGUE_TABLE}" WHERE "data_source_id" = %s LIMIT 1', (data_id,))
        rows = cursor.fetchall()
        if not rows:
            return UploadInfo(
                file_operation=FileOperation.Add,
                file_path=file_path,
                data_id=data_id,
                main_db_id=data_ident_info.main_db_id,
                main_db_name=data_ident_info.main_db_name,
                file_size=data_ident_info.size,
                last_update=data_ident_info.last_date_time_update,
                upload_date_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                hdfs_storage_path=file_path.parent,
                hdfs_full_path=file_path,
                data_type=data_ident_info.data_type,
                data_format=data_ident_info.data_format,
                file_hash=data_ident_info.file_hash  # 🔑 СТРОКА
            )

        row = rows[0]
        existing_hash = row["file_hash"]  # 🔑 СТРОКА из БД

        # 🔑 Сравнение строковых хешей
        if existing_hash == data_ident_info.file_hash:
            return UploadInfo(
                file_operation=FileOperation.Skip,
                file_path=file_path,
                id=row["id"],
                data_id=data_id,
                main_db_id=data_ident_info.main_db_id,
                main_db_name=data_ident_info.main_db_name,
                file_size=data_ident_info.size,
                last_update=data_ident_info.last_date_time_update,
                upload_date_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                hdfs_storage_path=Path(row["hdfs_storage_path"]) if row.get("hdfs_storage_path") else file_path.parent,
                hdfs_full_path=Path(row["hdfs_full_path"]) if row.get("hdfs_full_path") else file_path,
                data_type=row["data_type"],
                data_format=row["data_format"],
                file_hash=data_ident_info.file_hash  # 🔑 СТРОКА
            )
        else:
            return UploadInfo(
                file_operation=FileOperation.Replace,
                file_path=file_path,
                id=row["id"],
                data_id=data_id,
                main_db_id=data_ident_info.main_db_id,
                main_db_name=data_ident_info.main_db_name,
                file_size=data_ident_info.size,
                last_update=data_ident_info.last_date_time_update,
                upload_date_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                hdfs_storage_path=file_path.parent,
                hdfs_full_path=file_path,
                data_type=row["data_type"],
                data_format=row["data_format"],
                file_hash=data_ident_info.file_hash  # 🔑 СТРОКА
            )
    finally:
        cursor.close()
        conn.close()


def save_flowfile_to_temp() -> str:
    TEMP_FILES_PATH_DIR.mkdir(parents=True, exist_ok=True)
    if len(sys.argv) < 2:
        raise RuntimeError("file_name not provided")
    file_name = sys.argv[1]
    if not file_name or file_name == "unknown":
        raise RuntimeError("invalid file_name")
    try:
        data = sys.stdin.buffer.read()
        if not data:
            return ""
    except Exception as e:
        raise RuntimeError(f"Error reading stdin: {e}")
    temp_file_path = TEMP_FILES_PATH_DIR / file_name
    with open(temp_file_path, 'wb') as f:
        f.write(data)
    return str(temp_file_path)


def main():
    try:
        ensure_local_catalogue_exists()
        temp_file_path_str = save_flowfile_to_temp()

        if not temp_file_path_str:
            print(json.dumps({
                "file_operation": FileOperation.Skip,
                "file_path": "",
                "temp_file_path": "",
                "upload_date_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }, ensure_ascii=True, separators=(',', ':')), flush=True)
            return

        db_info = get_data_ident_info(temp_file_path_str)
        sys.stderr.write(f"INFO: Processed file: data_source_id={db_info.data_id}, Size={db_info.size}, Hash={db_info.file_hash}, "
                         f"DataType={db_info.data_type}, DataFormat={db_info.data_format}\n")  # 🔑 Хеш как строка

        upload_info = get_hdfs_upload_info(db_info)

        if upload_info.file_operation in [FileOperation.Add, FileOperation.Replace]:
            save_to_catalogue(upload_info)

        response = {
            "file_operation": upload_info.file_operation,
            "file_path": str(upload_info.file_path) if upload_info.file_path else "",
            "temp_file_path": temp_file_path_str,
            "data_id": upload_info.data_id or uuid.UUID(int=0),
            "main_db_id": upload_info.main_db_id or uuid.UUID(int=0),
            "main_db_name": upload_info.main_db_name or "",
            "file_size": upload_info.file_size or 0,
            "file_hash": upload_info.file_hash or "",  # 🔑 СТРОКА
            "last_update": upload_info.last_update.strftime("%Y-%m-%d %H:%M:%S") if upload_info.last_update else "",
            "upload_date_time": upload_info.upload_date_time or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "hdfs_storage_path": str(upload_info.hdfs_storage_path) if upload_info.hdfs_storage_path else "",
            "hdfs_full_path": str(upload_info.hdfs_full_path) if upload_info.hdfs_full_path else "",
            "data_type": upload_info.data_type or "",
            "data_format": upload_info.data_format or ""
        }
        if upload_info.file_operation == FileOperation.Replace and upload_info.id is not None:
            response["id"] = upload_info.id

        print(json.dumps(response, ensure_ascii=True, separators=(',', ':'), default=json_default), flush=True)

    except Exception as e:
        sys.stderr.write(f"ERROR: {e}\n")
        print(json.dumps({
            "file_operation": FileOperation.Skip,
            "file_path": "",
            "temp_file_path": "",
            "upload_date_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }, ensure_ascii=True, separators=(',', ':'), default=json_default), flush=True)

    finally:
        try:
            if TEMP_FILES_PATH_DIR.exists():
                for f in TEMP_FILES_PATH_DIR.iterdir():
                    try:
                        os.chmod(f, 0o777)
                        if f.is_file() or f.is_symlink():
                            f.unlink()
                        elif f.is_dir():
                            shutil.rmtree(f)
                    except Exception as e:
                        sys.stderr.write(f"WARNING: Failed to delete {f}: {e}\n")
                sys.stderr.write("INFO: Temp directory cleaned successfully\n")
        except Exception as e:
            sys.stderr.write(f"WARNING: Failed to cleanup temp dir: {e}\n")


if __name__ == "__main__":
    main()
    try:
        import subprocess
        if Path("/usr/local/bin/cleanup_temp.sh").exists():
            subprocess.run(["/usr/local/bin/cleanup_temp.sh"], check=False, capture_output=True)
    except Exception:
        pass