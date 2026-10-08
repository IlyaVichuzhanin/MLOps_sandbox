#!/usr/bin/env python3
import sys
import os
import hashlib
import re
import struct
import binascii
import psycopg2
import psycopg2.extras
psycopg2.extras.register_uuid()
from psycopg2.extras import RealDictCursor
from pathlib import Path
from datetime import datetime, timezone, timedelta
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
    "password": "123"
}

TEMP_FILES_PATH_DIR = Path("/home/temp_data")

DB_IDENT_TABLE_NAME = "DatabaseIdent"
CATALOGUE_TABLE = "data_catalogue"


# === КОНСТАНТЫ ФОРМАТА .fh ===
FH_MAGIC = 0xBEBAADDE
FH_MIN_HEADER_LENGTH = 30
FH_HEADER_DATA_OFFSET = 26
FH_FROM_DATE_OFFSET = 8
FH_TO_DATE_OFFSET = 16
FH_VERSION_OFFSET = 24
FH_EXTRA_MAGIC = 0x4B52494F  # "KRIO"

# === СТРОКОВЫЕ ТИПЫ ДАННЫХ ===
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


FH_DATATYPE_MAPPING = {
    1: DataType.MainDb,
    2: DataType.Double,
    3: DataType.Any
}

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
    return sha256_hash.hexdigest()


class DataIdentInfo:
    def __init__(self, file_path: str,
                 data_source_id: uuid.UUID,
                 data_file_id: uuid.UUID,
                 main_db_name: str,
                 main_db_id: uuid.UUID,
                 data_type: str,
                 data_format: str,
                 size: int,
                 last_date_time_update: datetime,
                 file_hash: str,
                 interval_date: datetime = None):
        if not isinstance(data_source_id, uuid.UUID):
            raise TypeError(f"data_source_id must be uuid.UUID, got {type(data_source_id)}")
        if not isinstance(data_file_id, uuid.UUID):
            raise TypeError(f"data_file_id must be uuid.UUID, got {type(data_file_id)}")
        if not isinstance(main_db_id, uuid.UUID):
            raise TypeError(f"main_db_id must be uuid.UUID, got {type(main_db_id)}")
        if not isinstance(file_hash, str) or len(file_hash) != 64:
            raise TypeError(f"file_hash must be 64-char hex string")

        self.file_path = Path(file_path)
        self.data_source_id = data_source_id
        self.data_file_id = data_file_id
        self.main_db_name = str(main_db_name)
        self.main_db_id = main_db_id
        self.data_type = str(data_type)
        self.data_format = str(data_format)
        self.size = int(size)
        self.last_date_time_update = last_date_time_update
        self.file_hash = file_hash
        self.interval_date = interval_date


class UploadInfo:
    def __init__(self,
                 file_operation: str,
                 file_path: Path,
                 data_source_id: uuid.UUID = None,
                 data_file_id: uuid.UUID = None,
                 main_db_id: uuid.UUID = None,
                 main_db_name: str = None,
                 file_size: int = None,
                 last_update: datetime = None,
                 upload_date_time: str = None,
                 hdfs_storage_path: Path = None,
                 hdfs_full_path: Path = None,
                 data_type: str = None,
                 data_format: str = None,
                 file_hash: str = None):
        self.file_operation = str(file_operation)
        self.file_path = file_path
        self.data_source_id = data_source_id
        self.data_file_id = data_file_id
        self.main_db_id = main_db_id
        self.main_db_name = str(main_db_name) if main_db_name else None
        self.file_size = int(file_size) if file_size is not None else None
        self.last_update = last_update
        self.upload_date_time = str(upload_date_time) if upload_date_time else None
        self.hdfs_storage_path = hdfs_storage_path
        self.hdfs_full_path = hdfs_full_path
        self.data_type = str(data_type) if data_type is not None else None
        self.data_format = str(data_format) if data_format is not None else None
        self.file_hash = str(file_hash) if file_hash is not None else None


def get_data_format(file_path: Path) -> str:
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
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = %s
            )
        """, (CATALOGUE_TABLE,))
        table_exists = cur.fetchone()[0]

        if not table_exists:
            cur.execute(f"""
                CREATE TABLE "{CATALOGUE_TABLE}" (
                    "data_source_id" UUID NOT NULL,
                    "data_file_id" UUID NOT NULL,
                    "main_db_id" UUID NOT NULL,
                    "main_db_name" TEXT NOT NULL,
                    "file_name" TEXT,
                    "file_size" BIGINT,
                    "file_hash" TEXT,
                    "last_update" TIMESTAMP,
                    "upload_date_time" TIMESTAMP,
                    "created_dttm" TIMESTAMP DEFAULT NOW() NOT NULL,
                    "hdfs_storage_path" TEXT,
                    "hdfs_full_path" TEXT,
                    "data_type" TEXT,
                    "data_format" TEXT,
                    "is_uploaded_to_dwh" BOOL DEFAULT FALSE NOT NULL,
                    "dwh_upload_dttm" TIMESTAMPTZ,
                    CONSTRAINT data_catalogue_pk PRIMARY KEY (data_source_id)
                );
            """)
            cur.execute(f'CREATE INDEX idx_{CATALOGUE_TABLE}_data_file_id ON "{CATALOGUE_TABLE}"("data_file_id");')
            cur.execute(f'CREATE INDEX idx_{CATALOGUE_TABLE}_file_hash ON "{CATALOGUE_TABLE}"("file_hash");')
            sys.stderr.write("INFO: Created data_catalogue with data_source_id as PK\n")
        else:
            for col_name, col_def in [
                ("data_file_id", "UUID"),
                ("hdfs_full_path", "TEXT"),
                ("created_dttm", "TIMESTAMP DEFAULT NOW()"),
                ("is_uploaded_to_dwh", "BOOL DEFAULT FALSE"),
                ("dwh_upload_dttm", "TIMESTAMPTZ"),
            ]:
                cur.execute(f"""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name = '{CATALOGUE_TABLE}' AND column_name = '{col_name}'
                """)
                if not cur.fetchone():
                    cur.execute(f'ALTER TABLE "{CATALOGUE_TABLE}" ADD COLUMN "{col_name}" {col_def}')
                    sys.stderr.write(f"INFO: Added column {col_name} to data_catalogue\n")

            cur.execute(f"""
                SELECT indexname FROM pg_indexes
                WHERE tablename = '{CATALOGUE_TABLE}' AND indexname = 'idx_{CATALOGUE_TABLE}_data_file_id'
            """)
            if not cur.fetchone():
                cur.execute(f'CREATE INDEX idx_{CATALOGUE_TABLE}_data_file_id ON "{CATALOGUE_TABLE}"("data_file_id");')
    finally:
        cur.close()


def ensure_local_catalogue_exists():
    conn = get_cloudberry_connection()
    try:
        ensure_catalogue_schema(conn)
    finally:
        conn.close()


def save_to_catalogue(upload_info: UploadInfo):
    conn = get_cloudberry_connection()
    cursor = conn.cursor()
    try:
        if upload_info.file_operation == FileOperation.Add:
            cursor.execute(f"""
                INSERT INTO "{CATALOGUE_TABLE}" (
                    "data_source_id", "data_file_id", "main_db_id", "main_db_name",
                    "file_name", "file_size", "file_hash",
                    "last_update", "upload_date_time",
                    "hdfs_storage_path", "hdfs_full_path",
                    "data_type", "data_format", "is_uploaded_to_dwh"
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING "data_source_id"
            """, (
                upload_info.data_source_id,
                upload_info.data_file_id,
                upload_info.main_db_id,
                upload_info.main_db_name,
                upload_info.file_path.name,
                upload_info.file_size,
                upload_info.file_hash,
                upload_info.last_update,
                upload_info.upload_date_time,
                str(upload_info.hdfs_storage_path),
                str(upload_info.hdfs_full_path),
                upload_info.data_type,
                upload_info.data_format,
                False
            ))
            result = cursor.fetchone()
            returned_id = result[0] if result else upload_info.data_source_id
            sys.stderr.write(f"INFO: Inserted new catalogue record "
                             f"data_source_id={returned_id}, "
                             f"data_file_id={upload_info.data_file_id}\n")
    finally:
        cursor.close()
        conn.close()


def get_date_from_timeinterval(db_path: str) -> datetime:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='TimeInterval'")
        if not cursor.fetchone():
            return None

        cursor.execute("SELECT SegmentFromDate FROM TimeInterval LIMIT 1")
        row = cursor.fetchone()
        if row and row[0]:
            date_value = row[0]
            for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d.%m.%Y %H:%M:%S", "%d.%m.%Y"]:
                try:
                    parsed_date = datetime.strptime(str(date_value), fmt)
                    sys.stderr.write(f"INFO: Extracted date from TimeInterval: {parsed_date}\n")
                    return parsed_date
                except ValueError:
                    continue
            sys.stderr.write(f"WARNING: Could not parse date '{date_value}' from TimeInterval\n")
            return None
        return None
    except Exception as e:
        sys.stderr.write(f"WARNING: Error reading TimeInterval: {e}\n")
        return None
    finally:
        conn.close()


def fh_timestamp_to_datetime(ts_ms: int) -> datetime:
    """Конвертирует Unix timestamp (мс) в datetime UTC."""
    return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=ts_ms)


def get_fh_data_ident_info(path_to_file: str) -> DataIdentInfo:
    """
    Читает метаданные из заголовка .fh файла.
    Ожидает модифицированный заголовок с дополнительными полями (Id, MainDbId, MainDbName, DataType).
    """
    stat = os.stat(path_to_file)
    file_hash = calculate_sha256(path_to_file)
    data_source_id = uuid.uuid4()

    with open(path_to_file, 'rb') as f:
        file_data = f.read()

    file_size = len(file_data)

    # Проверка минимального размера
    if file_size < FH_MIN_HEADER_LENGTH:
        raise ValueError(f"Файл слишком мал: {file_size} байт (минимум {FH_MIN_HEADER_LENGTH})")

    # Проверка магического слова
    magic = struct.unpack_from('<I', file_data, 0)[0]
    if magic != FH_MAGIC:
        raise ValueError(f"Неверное магическое слово: 0x{magic:08X} (ожидается 0x{FH_MAGIC:08X})")

    # Чтение длины заголовка
    header_length = struct.unpack_from('<I', file_data, 4)[0]
    if header_length < FH_MIN_HEADER_LENGTH or header_length > file_size:
        raise ValueError(f"Некорректная длина заголовка: {header_length}")

    # Чтение дат
    from_ts = struct.unpack_from('<Q', file_data, FH_FROM_DATE_OFFSET)[0]
    to_ts = struct.unpack_from('<Q', file_data, FH_TO_DATE_OFFSET)[0]
    from_date = fh_timestamp_to_datetime(from_ts)
    to_date = fh_timestamp_to_datetime(to_ts)

    # Чтение версии
    file_version = struct.unpack_from('<H', file_data, FH_VERSION_OFFSET)[0]

    # Чтение HeaderData (без CRC32)
    header_data_end = header_length - 4
    header_data = file_data[FH_HEADER_DATA_OFFSET:header_data_end]

    # Проверка CRC32
    stored_crc32 = struct.unpack_from('<I', file_data, header_data_end)[0]
    calculated_crc32 = binascii.crc32(file_data[:header_data_end]) & 0xFFFFFFFF
    if stored_crc32 != calculated_crc32:
        raise ValueError(f"CRC32 не совпадает: файл=0x{stored_crc32:08X}, вычислено=0x{calculated_crc32:08X}")

    # Разбор HeaderData для версии 1
    if file_version != 1:
        raise ValueError(f"Неподдерживаемая версия файла: {file_version}")

    if len(header_data) < 4:
        raise ValueError("HeaderData слишком короткий для версии 1")

    # DataRowValuesCount (4 байта)
    data_row_values_count = struct.unpack_from('<I', header_data, 0)[0]

    # Проверка наличия дополнительных метаданных (сигнатура KRIO)
    if len(header_data) < 8:
        raise ValueError("Файл не содержит дополнительных метаданных (Id, MainDbId, MainDbName, DataType)")

    extra_magic = struct.unpack_from('<I', header_data, 4)[0]
    if extra_magic != FH_EXTRA_MAGIC:
        raise ValueError(f"Отсутствует сигнатура дополнительных метаданных: 0x{extra_magic:08X} (ожидается 0x{FH_EXTRA_MAGIC:08X})")

    # Чтение дополнительных полей
    offset = 8
    
    # Id (16 байт, GUID LE)
    if len(header_data) < offset + 16:
        raise ValueError("Недостаточно данных для чтения Id")
    data_file_id = uuid.UUID(bytes_le=header_data[offset:offset + 16])
    offset += 16

    # MainDbId (16 байт, GUID LE)
    if len(header_data) < offset + 16:
        raise ValueError("Недостаточно данных для чтения MainDbId")
    main_db_id = uuid.UUID(bytes_le=header_data[offset:offset + 16])
    offset += 16

    # MainDbName length (4 байта)
    if len(header_data) < offset + 4:
        raise ValueError("Недостаточно данных для чтения длины MainDbName")
    main_db_name_length = struct.unpack_from('<I', header_data, offset)[0]
    offset += 4

    # MainDbName (UTF-8)
    if len(header_data) < offset + main_db_name_length:
        raise ValueError("Недостаточно данных для чтения MainDbName")
    main_db_name = header_data[offset:offset + main_db_name_length].decode('utf-8')
    offset += main_db_name_length

    # DataType (4 байта)
    if len(header_data) < offset + 4:
        raise ValueError("Недостаточно данных для чтения DataType")
    raw_data_type = struct.unpack_from('<I', header_data, offset)[0]
    data_type_str = FH_DATATYPE_MAPPING.get(raw_data_type, DataType.Any)

    sys.stderr.write(f"INFO: FH file metadata: data_file_id={data_file_id}, main_db_id={main_db_id}, "
                     f"main_db_name={main_db_name}, data_type={data_type_str}, "
                     f"from_date={from_date}, to_date={to_date}, values_count={data_row_values_count}\n")

    return DataIdentInfo(
        file_path=path_to_file,
        data_source_id=data_source_id,
        data_file_id=data_file_id,
        main_db_name=main_db_name,
        main_db_id=main_db_id,
        data_type=data_type_str,
        data_format=get_data_format(Path(path_to_file)),
        size=stat.st_size,
        last_date_time_update=datetime.fromtimestamp(stat.st_mtime),
        file_hash=file_hash,
        interval_date=from_date  # Для .fh файлов используем FromDate из заголовка
    )


def get_data_ident_info(path_to_file: str) -> DataIdentInfo:
    """Определяет формат файла и вызывает соответствующую функцию для чтения метаданных."""
    file_path = Path(path_to_file)
    data_format = get_data_format(file_path)

    if data_format == DataFormat.sqlite:
        # Обработка SQLite файла
        stat = os.stat(path_to_file)
        file_hash = calculate_sha256(path_to_file)
        data_source_id = uuid.uuid4()

        conn = sqlite3.connect(path_to_file)
        cursor = conn.cursor()
        try:
            cursor.execute(f"SELECT Id, MainDbName, MainDbId, DataType FROM {DB_IDENT_TABLE_NAME} LIMIT 1;")
            row = cursor.fetchone()
            if not row:
                raise ValueError("No data in DatabaseIdent table")

            data_file_id_blob = row[0]
            main_db_id_blob = row[2]

            if not isinstance(data_file_id_blob, (bytes, bytearray)) or len(data_file_id_blob) != 16:
                raise ValueError(f"data_file_id must be 16-byte UUID BLOB, got {type(data_file_id_blob)}")
            if not isinstance(main_db_id_blob, (bytes, bytearray)) or len(main_db_id_blob) != 16:
                raise ValueError(f"main_db_id must be 16-byte UUID BLOB, got {type(main_db_id_blob)}")

            data_file_id = uuid.UUID(bytes=data_file_id_blob)
            main_db_id = uuid.UUID(bytes=main_db_id_blob)

            raw_data_type = row[3]
            data_type_str = SQLITE_DATATYPE_MAPPING.get(raw_data_type, DataType.Any)

            interval_date = None
            if data_type_str in (DataType.Any, DataType.Double):
                interval_date = get_date_from_timeinterval(path_to_file)

            return DataIdentInfo(
                file_path=path_to_file,
                data_source_id=data_source_id,
                data_file_id=data_file_id,
                main_db_name=row[1],
                main_db_id=main_db_id,
                data_type=data_type_str,
                data_format=data_format,
                size=stat.st_size,
                last_date_time_update=datetime.fromtimestamp(stat.st_mtime),
                file_hash=file_hash,
                interval_date=interval_date
            )
        finally:
            conn.close()

    elif data_format == DataFormat.fh:
        # Обработка .fh файла
        return get_fh_data_ident_info(path_to_file)

    else:
        raise ValueError(f"Неподдерживаемый формат файла: {data_format}")


def get_date_from_filename(filename: str) -> datetime:
    """Извлекает дату (YYYY-MM-DD) из имени файла."""
    match = re.search(r'(\d{4})-(\d{2})-(\d{2})', filename)
    if match:
        try:
            year = int(match.group(1))
            month = int(match.group(2))
            day = int(match.group(3))
            return datetime(year, month, day)
        except ValueError:
            return None
    return None


def get_upload_file_path(data_ident_info: DataIdentInfo, with_date: bool = False) -> Path:
    base_dir = Path("/raw_data")
    
    # 🔑 .fh файлы всегда сохраняем в каталоге Double
    if data_ident_info.data_format == DataFormat.fh:
        data_type_name = DataType.Double
    else:
        data_type_name = data_ident_info.data_type
    
    subdir = base_dir / data_ident_info.main_db_name / data_type_name

    # Для Double, Any и .fh файлов используем партиционирование по дате
    if data_ident_info.data_type in [DataType.Any, DataType.Double] or data_ident_info.data_format == DataFormat.fh:
        # 🔑 Пытаемся извлечь дату из имени файла
        file_date = get_date_from_filename(data_ident_info.file_path.name)
        date_to_use = file_date if file_date else data_ident_info.last_date_time_update
        
        year_dir = f"year={date_to_use.year:04d}"
        month_dir = f"month={date_to_use.month:02d}"
        day_dir = f"day={date_to_use.day:02d}"

        date_path = Path(year_dir) / month_dir / day_dir
        full_path = subdir / date_path

        # 🔑 НЕ переименовываем файл, используем оригинальное имя
        return full_path / data_ident_info.file_path.name
    else:
        # 🔑 MainDb: ВСЕГДА с датой/временем в имени
        date_to_use = data_ident_info.last_date_time_update
        dt_string = date_to_use.strftime("%Y-%m-%d_%H-%M")
        file_name = f"{data_ident_info.main_db_name}_{dt_string}{data_ident_info.file_path.suffix}"
        return subdir / file_name


def get_hdfs_upload_info(data_ident_info: DataIdentInfo) -> UploadInfo:
    data_file_id = data_ident_info.data_file_id
    data_source_id = data_ident_info.data_source_id
    is_main_db = data_ident_info.data_type == DataType.MainDb
    
    # 🔑 .fh файлы обрабатываем как Double/Any (не MainDb)
    is_fh_file = data_ident_info.data_format == DataFormat.fh

    conn = get_cloudberry_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            f'SELECT * FROM "{CATALOGUE_TABLE}" WHERE "data_file_id" = %s LIMIT 1',
            (data_file_id,)
        )
        rows = cursor.fetchall()

        if not rows:
            # Записи нет → Add
            file_path = get_upload_file_path(data_ident_info, with_date=True)
            return UploadInfo(
                file_operation=FileOperation.Add,
                file_path=file_path,
                data_source_id=data_source_id,
                data_file_id=data_file_id,
                main_db_id=data_ident_info.main_db_id,
                main_db_name=data_ident_info.main_db_name,
                file_size=data_ident_info.size,
                last_update=data_ident_info.last_date_time_update,
                upload_date_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                hdfs_storage_path=file_path.parent,
                hdfs_full_path=file_path,
                data_type=data_ident_info.data_type,
                data_format=data_ident_info.data_format,
                file_hash=data_ident_info.file_hash
            )

        row = rows[0]
        existing_hash = row.get("file_hash")

        # 🔑 MainDb файлы (но не .fh) проверяем по хешу
        if is_main_db and not is_fh_file:
            if existing_hash == data_ident_info.file_hash:
                sys.stderr.write(f"INFO: MainDb file already exists with same hash, skipping. "
                                 f"data_file_id={data_file_id}\n")
                file_path = get_upload_file_path(data_ident_info, with_date=True)
                return UploadInfo(
                    file_operation=FileOperation.Skip,
                    file_path=file_path,
                    data_source_id=uuid.UUID(row["data_source_id"]),
                    data_file_id=data_file_id,
                    main_db_id=data_ident_info.main_db_id,
                    main_db_name=data_ident_info.main_db_name,
                    file_size=data_ident_info.size,
                    last_update=data_ident_info.last_date_time_update,
                    upload_date_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    hdfs_storage_path=file_path.parent,
                    hdfs_full_path=file_path,
                    data_type=row["data_type"],
                    data_format=row["data_format"],
                    file_hash=data_ident_info.file_hash
                )
            else:
                # Хеш не совпадает → Add с новым именем
                file_path = get_upload_file_path(data_ident_info, with_date=True)
                sys.stderr.write(f"INFO: MainDb file hash changed, adding new version. "
                                 f"data_file_id={data_file_id}, new_path={file_path}\n")
                return UploadInfo(
                    file_operation=FileOperation.Add,
                    file_path=file_path,
                    data_source_id=data_source_id,
                    data_file_id=data_file_id,
                    main_db_id=data_ident_info.main_db_id,
                    main_db_name=data_ident_info.main_db_name,
                    file_size=data_ident_info.size,
                    last_update=data_ident_info.last_date_time_update,
                    upload_date_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    hdfs_storage_path=file_path.parent,
                    hdfs_full_path=file_path,
                    data_type=data_ident_info.data_type,
                    data_format=data_ident_info.data_format,
                    file_hash=data_ident_info.file_hash
                )
        else:
            # 🔑 Double, Any и .fh файлы: если запись есть → Skip
            sys.stderr.write(f"INFO: {'FH' if is_fh_file else 'Any/Double'} file already exists, skipping. "
                             f"data_file_id={data_file_id}\n")
            return UploadInfo(
                file_operation=FileOperation.Skip,
                file_path=Path(row["hdfs_full_path"]) if row.get("hdfs_full_path") else get_upload_file_path(data_ident_info, with_date=False),
                data_source_id=uuid.UUID(row["data_source_id"]),
                data_file_id=data_file_id,
                main_db_id=data_ident_info.main_db_id,
                main_db_name=data_ident_info.main_db_name,
                file_size=data_ident_info.size,
                last_update=data_ident_info.last_date_time_update,
                upload_date_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                hdfs_storage_path=Path(row["hdfs_storage_path"]) if row.get("hdfs_storage_path") else get_upload_file_path(data_ident_info, with_date=False).parent,
                hdfs_full_path=Path(row["hdfs_full_path"]) if row.get("hdfs_full_path") else get_upload_file_path(data_ident_info, with_date=False),
                data_type=row["data_type"],
                data_format=row["data_format"],
                file_hash=data_ident_info.file_hash
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

        interval_date_str = db_info.interval_date.strftime("%Y-%m-%d") if db_info.interval_date else "None"
        sys.stderr.write(f"INFO: Processed file: data_source_id={db_info.data_source_id}, "
                         f"data_file_id={db_info.data_file_id}, Size={db_info.size}, "
                         f"Hash={db_info.file_hash}, DataType={db_info.data_type}, "
                         f"DataFormat={db_info.data_format}, IntervalDate={interval_date_str}\n")

        upload_info = get_hdfs_upload_info(db_info)

        if upload_info.file_operation == FileOperation.Add:
            save_to_catalogue(upload_info)

        response = {
            "file_operation": upload_info.file_operation,
            "file_path": str(upload_info.file_path) if upload_info.file_path else "",
            "temp_file_path": temp_file_path_str,
            "data_source_id": upload_info.data_source_id or uuid.UUID(int=0),
            "data_file_id": upload_info.data_file_id or uuid.UUID(int=0),
            "main_db_id": upload_info.main_db_id or uuid.UUID(int=0),
            "main_db_name": upload_info.main_db_name or "",
            "file_size": upload_info.file_size or 0,
            "file_hash": upload_info.file_hash or "",
            "last_update": upload_info.last_update.strftime("%Y-%m-%d %H:%M:%S") if upload_info.last_update else "",
            "upload_date_time": upload_info.upload_date_time or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "hdfs_storage_path": str(upload_info.hdfs_storage_path) if upload_info.hdfs_storage_path else "",
            "hdfs_full_path": str(upload_info.hdfs_full_path) if upload_info.hdfs_full_path else "",
            "data_type": upload_info.data_type or "",
            "data_format": upload_info.data_format or ""
        }

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