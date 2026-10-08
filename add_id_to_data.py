"""
Добавляет идентификационную информацию во все файлы в каталоге data_to_transfer:
- .db файлы (SQLite): создаёт таблицу DatabaseIdent
- .fh файлы (бинарный): добавляет метаданные в заголовок файла

DataType: Double=2, FH=2, Any=3

Доработка:
- Добавлен параметр MAIN_DB_FILE_NAME.
- Для файла главной БД ID берётся не случайно, а из MAIN_DB_ID.
"""

import sqlite3
import uuid
import struct
import binascii
import os
import shutil
from pathlib import Path


# ═══════════════════════════════════════════════════════════════════
#  НАСТРОЙКИ
# ═══════════════════════════════════════════════════════════════════

# Директория с файлами
DIRECTORY = r".\test_data\LukoilUNP-3"

# Имя главной базы данных
MAIN_DB_NAME = "Lukoil_UNP_3"

# ID главной базы данных (GUID)
MAIN_DB_ID = "c1b2c3d5-e5f6-7890-abcd-ef1234567890"

# a1b2c3d4-e5f6-7890-abcd-ef1234567890 - kriogen
# b1b2c3d4-e5f6-7890-abcd-ef1234567890 - anomaly_detection
# c1b2c3d4-e5f6-7890-abcd-ef1234567890 - Polygon
# c1b2c3d5-e5f6-7890-abcd-ef1234567890 - LukoilUNP3

# Имя файла главной БД.
# Например: "Kriogen_anomaly_detection.db"
# Если расширение не указано, главным будет считаться .db-файл с таким же stem.
MAIN_DB_FILE_NAME = "LukoilUNP-3.db"

# Если True: для главного .db файла старые записи DatabaseIdent будут удалены
# и будет вставлена одна корректная запись с ID = MAIN_DB_ID.
# Если False: скрипт просто добавит запись с ID = MAIN_DB_ID, не удаляя старые.
REPLACE_MAIN_DB_IDENT = True

# Создавать .bak копию перед изменением?
BACKUP = False

# Тестовый прогон без реальной записи?
DRY_RUN = False

# Перезаписывать метаданные, если они уже есть? (для .fh)
FORCE_FH = False

# Выводить подробную информацию
VERBOSE = True


# ═══════════════════════════════════════════════════════════════════
#  Формат .fh — константы
# ═══════════════════════════════════════════════════════════════════

MAGIC_HEX = 0xBEBAADDE
MINIMUM_HEADER_LENGTH = 30
HEADER_DATA_OFFSET = 26
EXTRA_MAGIC = 0x4B52494F  # "KRIO"
EXTRA_MAGIC_BYTES = struct.pack('<I', EXTRA_MAGIC)


# ═══════════════════════════════════════════════════════════════════
#  ОБЩИЕ ФУНКЦИИ
# ═══════════════════════════════════════════════════════════════════

def validate_uuid(value: str) -> bool:
    """Проверяет корректность GUID/UUID."""
    try:
        uuid.UUID(value)
        return True
    except Exception:
        return False


def parse_filename_for_datatype(filename: str) -> int:
    """
    Определяет DataType из имени файла или пути.
    Double/FH = 2, Any = 3
    """
    name_upper = filename.upper()

    if 'ANY' in name_upper:
        return 3
    elif 'DOUBLE' in name_upper or 'FH' in name_upper:
        return 2
    else:
        # По умолчанию 2 (Double)
        return 2


def backup_file(file_path: str):
    """Создаёт резервную копию файла."""
    if BACKUP and not DRY_RUN:
        backup_path = file_path + '.bak'
        shutil.copy2(file_path, backup_path)
        if VERBOSE:
            print(f"    💾 Копия: {os.path.basename(backup_path)}")


def find_all_files(directory: str) -> tuple:
    """Находит все .db и .fh файлы рекурсивно."""
    dir_path = Path(directory)
    db_files = sorted(dir_path.rglob('*.db'))
    fh_files = sorted(dir_path.rglob('*.fh'))

    # Исключаем .bak файлы
    db_files = [f for f in db_files if not str(f).endswith('.bak')]
    fh_files = [f for f in fh_files if not str(f).endswith('.bak')]

    return db_files, fh_files


def is_main_db_file(file_path: Path) -> bool:
    """
    Проверяет, является ли файл главным .db файлом.

    Логика:
    - если MAIN_DB_FILE_NAME задан с расширением, сравниваем полное имя файла;
    - если MAIN_DB_FILE_NAME задан без расширения, сравниваем stem и расширяем
      только для .db файлов.
    """
    if not MAIN_DB_FILE_NAME:
        return False

    main_path = Path(MAIN_DB_FILE_NAME)

    if main_path.suffix:
        return file_path.name.lower() == main_path.name.lower()

    return (
        file_path.suffix.lower() == '.db' and
        file_path.stem.lower() == main_path.name.lower()
    )


def is_main_fh_file(file_path: Path) -> bool:
    """
    Проверяет, является ли файл главным .fh файлом.

    Для .fh файлов главный файл учитывается только если в MAIN_DB_FILE_NAME
    явно указано расширение .fh.
    """
    if not MAIN_DB_FILE_NAME:
        return False

    main_path = Path(MAIN_DB_FILE_NAME)

    if main_path.suffix.lower() != '.fh':
        return False

    return file_path.name.lower() == main_path.name.lower()


# ═══════════════════════════════════════════════════════════════════
#  ОБРАБОТКА .db ФАЙЛОВ (SQLite)
# ═══════════════════════════════════════════════════════════════════

def check_db_ident_exists(db_path: str) -> bool:
    """Проверяет, существует ли таблица DatabaseIdent с данными."""
    conn = None

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='DatabaseIdent'
        """)

        if not cursor.fetchone():
            return False

        cursor.execute("SELECT COUNT(*) FROM DatabaseIdent")
        count = cursor.fetchone()[0]

        return count > 0

    except Exception as e:
        if VERBOSE:
            print(f"    ⚠ Ошибка проверки: {e}")
        return False

    finally:
        if conn:
            conn.close()


def check_main_db_ident_correct(
    db_path: str,
    main_db_name: str,
    main_db_id: str,
    data_type: int,
    require_single_row: bool
) -> bool:
    """
    Проверяет, что в главном .db файле уже есть корректная запись:
    ID = MAIN_DB_ID, MainDbId = MAIN_DB_ID.

    Если require_single_row=True, дополнительно требует, чтобы в таблице
    была ровно одна запись.
    """
    conn = None

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='DatabaseIdent'
        """)

        if not cursor.fetchone():
            return False

        main_db_id_bytes = uuid.UUID(main_db_id).bytes

        cursor.execute("SELECT COUNT(*) FROM DatabaseIdent")
        total_rows = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COUNT(*)
            FROM DatabaseIdent
            WHERE ID = ? AND MainDbId = ? AND MainDbName = ? AND DataType = ?
        """, (
            main_db_id_bytes,
            main_db_id_bytes,
            main_db_name,
            data_type
        ))

        correct_rows = cursor.fetchone()[0]

        if require_single_row:
            return total_rows == 1 and correct_rows == 1

        return correct_rows > 0

    except Exception as e:
        if VERBOSE:
            print(f"    ⚠ Ошибка проверки maindb: {e}")
        return False

    finally:
        if conn:
            conn.close()


def add_db_identification(
    db_path: str,
    main_db_name: str,
    main_db_id: str,
    data_type: int,
    is_main_db: bool = False,
    clear_existing: bool = False
) -> dict:
    """Добавляет идентификацию в SQLite базу."""
    result = {
        'file': db_path,
        'status': 'ok',
        'message': '',
        'new_id': None
    }

    conn = None

    try:
        main_db_id_bytes = uuid.UUID(main_db_id).bytes

        # Для главного файла ID берём из MAIN_DB_ID, для остальных — случайный.
        if is_main_db:
            record_id = main_db_id_bytes
        else:
            record_id = uuid.uuid4().bytes

        if DRY_RUN:
            result['new_id'] = record_id.hex().upper()

            message = (
                f"DRY RUN: DataType={data_type}, "
                f"MainDbName={main_db_name}"
            )

            if is_main_db:
                message += ", ID=MAIN_DB_ID"

            if clear_existing:
                message += ", replace existing DatabaseIdent"

            result['message'] = message
            return result

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS "DatabaseIdent" (
                "ID" BLOB NOT NULL,
                "MainDbName" TEXT NOT NULL,
                "MainDbId" BLOB NOT NULL,
                "DataType" INTEGER NOT NULL,
                CONSTRAINT "PK_DatabaseIdent" PRIMARY KEY ("ID")
            )
        """)

        if clear_existing:
            cursor.execute('DELETE FROM "DatabaseIdent"')

        cursor.execute("""
            INSERT OR REPLACE INTO "DatabaseIdent" (
                "ID",
                "MainDbName",
                "MainDbId",
                "DataType"
            )
            VALUES (?, ?, ?, ?)
        """, (
            record_id,
            main_db_name,
            main_db_id_bytes,
            data_type
        ))

        conn.commit()

        result['new_id'] = record_id.hex().upper()

        message = (
            f"DataType={data_type}, "
            f"MainDbName={main_db_name}"
        )

        if is_main_db:
            message += ", ID=MAIN_DB_ID"

        if clear_existing:
            message += ", replace existing DatabaseIdent"

        result['message'] = message

    except Exception as e:
        result['status'] = 'error'
        result['message'] = str(e)

    finally:
        if conn:
            conn.close()

    return result


def process_db_file(db_path: Path) -> dict:
    """Обрабатывает один .db файл."""
    result = {
        'file': str(db_path),
        'status': 'skipped',
        'message': '',
        'new_id': None,
        'data_type': None,
        'file_type': 'db'
    }

    filename = db_path.name
    data_type = parse_filename_for_datatype(str(db_path))
    result['data_type'] = data_type

    is_main = is_main_db_file(db_path)

    if VERBOSE:
        main_label = " [MAIN DB]" if is_main else ""
        print(f"\n  📄 [DB]{main_label} {filename}")

    # Для главного файла проверяем не просто наличие таблицы,
    # а корректность ID = MAIN_DB_ID.
    if is_main:
        already_correct = check_main_db_ident_correct(
            str(db_path),
            MAIN_DB_NAME,
            MAIN_DB_ID,
            data_type,
            require_single_row=REPLACE_MAIN_DB_IDENT
        )

        if already_correct:
            result['status'] = 'skipped'
            result['message'] = 'Уже содержит корректный MAIN_DB_ID'

            if VERBOSE:
                print(f"    ⏭ Пропущен: {result['message']}")

            return result

    else:
        if check_db_ident_exists(str(db_path)):
            result['status'] = 'skipped'
            result['message'] = 'Уже содержит DatabaseIdent'

            if VERBOSE:
                print(f"    ⏭ Пропущен: {result['message']}")

            return result

    if BACKUP and not DRY_RUN:
        backup_file(str(db_path))

    ident_result = add_db_identification(
        str(db_path),
        MAIN_DB_NAME,
        MAIN_DB_ID,
        data_type,
        is_main_db=is_main,
        clear_existing=(is_main and REPLACE_MAIN_DB_IDENT)
    )

    result['status'] = ident_result['status']
    result['message'] = ident_result['message']
    result['new_id'] = ident_result.get('new_id')

    if VERBOSE:
        if result['status'] == 'ok':
            print(f"    ✓ Добавлено: {result['message']}")
        else:
            print(f"    ✗ Ошибка: {result['message']}")

    return result


# ═══════════════════════════════════════════════════════════════════
#  ОБРАБОТКА .fh ФАЙЛОВ (бинарный формат)
# ═══════════════════════════════════════════════════════════════════

def compute_crc32(data: bytes) -> int:
    return binascii.crc32(data) & 0xFFFFFFFF


def build_fh_extra_payload(
    main_db_name: str,
    main_db_id_guid: str,
    data_type: int,
    file_id_guid: str = None
) -> tuple:
    """
    Собирает блок дополнительных метаданных для .fh файла.

    Если file_id_guid=None, создаётся случайный file_id.
    Если file_id_guid задан, используется он.
    """
    if file_id_guid:
        file_id = uuid.UUID(file_id_guid)
    else:
        file_id = uuid.uuid4()

    main_db_id = uuid.UUID(main_db_id_guid)

    payload = bytearray()
    payload.extend(EXTRA_MAGIC_BYTES)
    payload.extend(file_id.bytes_le)
    payload.extend(main_db_id.bytes_le)

    name_bytes = main_db_name.encode('utf-8')
    payload.extend(struct.pack('<I', len(name_bytes)))
    payload.extend(name_bytes)
    payload.extend(struct.pack('<I', data_type))

    return bytes(payload), file_id


def process_fh_file(file_path: Path) -> dict:
    """Обрабатывает один .fh файл."""
    result = {
        'file': str(file_path),
        'status': 'ok',
        'message': '',
        'new_id': None,
        'data_type': None,
        'file_type': 'fh'
    }

    filename = file_path.name
    data_type = parse_filename_for_datatype(str(file_path))
    result['data_type'] = data_type

    is_main = is_main_fh_file(file_path)

    if VERBOSE:
        main_label = " [MAIN FILE]" if is_main else ""
        print(f"\n  📄 [FH]{main_label} {filename}")

    if not os.path.exists(file_path):
        result['status'] = 'error'
        result['message'] = 'Файл не найден'
        return result

    try:
        with open(file_path, 'rb') as f:
            file_data = bytearray(f.read())
    except Exception as e:
        result['status'] = 'error'
        result['message'] = f'Ошибка чтения: {e}'

        if VERBOSE:
            print(f"    ✗ Ошибка: {result['message']}")

        return result

    file_size = len(file_data)

    if file_size < 4:
        result['status'] = 'error'
        result['message'] = 'Файл слишком мал'

        if VERBOSE:
            print(f"    ✗ Ошибка: {result['message']}")

        return result

    magic = struct.unpack_from('<I', file_data, 0)[0]
    if magic != MAGIC_HEX:
        result['status'] = 'error'
        result['message'] = f'Неверное магическое слово: 0x{magic:08X}'

        if VERBOSE:
            print(f"    ✗ Ошибка: {result['message']}")

        return result

    old_header_length = struct.unpack_from('<I', file_data, 4)[0]

    if old_header_length < MINIMUM_HEADER_LENGTH or old_header_length > file_size:
        result['status'] = 'error'
        result['message'] = f'Некорректная длина заголовка: {old_header_length}'

        if VERBOSE:
            print(f"    ✗ Ошибка: {result['message']}")

        return result

    old_header = file_data[:old_header_length]
    data_section = file_data[old_header_length:]
    old_header_data = old_header[HEADER_DATA_OFFSET: old_header_length - 4]

    # Проверка: уже модифицирован?
    already_modified = False

    if len(old_header_data) >= 8:
        if old_header_data[4:8] == EXTRA_MAGIC_BYTES:
            already_modified = True

    if already_modified:
        if is_main:
            # Для главного .fh файла можем дополнительно проверить,
            # что текущий file_id уже равен MAIN_DB_ID.
            main_file_id_bytes_le = uuid.UUID(MAIN_DB_ID).bytes_le

            if len(old_header_data) >= 24:
                existing_file_id_bytes_le = old_header_data[8:24]
            else:
                existing_file_id_bytes_le = b''

            if existing_file_id_bytes_le == main_file_id_bytes_le and not FORCE_FH:
                result['status'] = 'skipped'
                result['message'] = 'Уже содержит MAIN_DB_ID'

                if VERBOSE:
                    print(f"    ⏭ Пропущен: {result['message']}")

                return result

        elif not FORCE_FH:
            result['status'] = 'skipped'
            result['message'] = 'Уже содержит метаданные (FORCE_FH=False)'

            if VERBOSE:
                print(f"    ⏭ Пропущен: {result['message']}")

            return result

    # Собираем новый заголовок.
    # Если это главный .fh файл и для него явно задан MAIN_DB_FILE_NAME,
    # используем MAIN_DB_ID как file_id.
    file_id_override = MAIN_DB_ID if is_main else None

    base_header_data = old_header_data[:4] if len(old_header_data) >= 4 else old_header_data
    extra_payload, new_file_id = build_fh_extra_payload(
        MAIN_DB_NAME,
        MAIN_DB_ID,
        data_type,
        file_id_override
    )

    new_header_data = base_header_data + extra_payload
    new_header_length = HEADER_DATA_OFFSET + len(new_header_data) + 4

    new_header = bytearray()
    new_header.extend(old_header[0:4])
    new_header.extend(struct.pack('<I', new_header_length))
    new_header.extend(old_header[8:HEADER_DATA_OFFSET])
    new_header.extend(new_header_data)

    crc32 = compute_crc32(bytes(new_header))
    new_header.extend(struct.pack('<I', crc32))

    new_file = bytes(new_header) + bytes(data_section)

    if DRY_RUN:
        result['message'] = (
            f"DRY RUN: +{new_header_length - old_header_length} байт, "
            f"DataType={data_type}"
        )

        if is_main:
            result['message'] += ", ID=MAIN_DB_ID"

        result['new_id'] = new_file_id

        if VERBOSE:
            print(f"    ✓ {result['message']}")

        return result

    if BACKUP:
        backup_file(str(file_path))

    with open(file_path, 'wb') as f:
        f.write(new_file)

    result['new_id'] = new_file_id
    result['message'] = (
        f"+{new_header_length - old_header_length} байт, "
        f"DataType={data_type}"
    )

    if is_main:
        result['message'] += ", ID=MAIN_DB_ID"

    if VERBOSE:
        print(f"    ✓ Добавлено: Id={new_file_id}, {result['message']}")

    return result


# ═══════════════════════════════════════════════════════════════════
#  ОСНОВНАЯ ФУНКЦИЯ
# ═══════════════════════════════════════════════════════════════════

def main():
    """Основная функция."""
    print("=" * 80)
    print("  ДОБАВЛЕНИЕ ИДЕНТИФИКАЦИОННЫХ ДАННЫХ В ФАЙЛЫ")
    print("  (.db — SQLite таблица DatabaseIdent, .fh — бинарный заголовок)")
    print("=" * 80)
    print(f"  Директория    : {Path(DIRECTORY).resolve()}")
    print(f"  MainDbName    : {MAIN_DB_NAME}")
    print(f"  MainDbId      : {MAIN_DB_ID}")
    print(f"  MainDbFile    : {MAIN_DB_FILE_NAME or '(не задан)'}")
    print(f"  Replace main  : {'да' if REPLACE_MAIN_DB_IDENT else 'нет'}")
    print(f"  Backup        : {'да' if BACKUP else 'нет'}")
    print(f"  Dry run       : {'да' if DRY_RUN else 'нет'}")
    print(f"  Force FH      : {'да' if FORCE_FH else 'нет'}")
    print("=" * 80)

    if not validate_uuid(MAIN_DB_ID):
        print(f"\n❌ Некорректный MAIN_DB_ID: {MAIN_DB_ID}")
        return

    if not os.path.exists(DIRECTORY):
        print(f"\n❌ Директория не найдена: {DIRECTORY}")
        return

    db_files, fh_files = find_all_files(DIRECTORY)

    print(f"\n📁 Найдено файлов:")
    print(f"   .db : {len(db_files)}")
    print(f"   .fh : {len(fh_files)}")
    print(f"   Всего: {len(db_files) + len(fh_files)}")
    print("-" * 80)

    if MAIN_DB_FILE_NAME:
        main_db_found = any(is_main_db_file(f) for f in db_files)
        main_fh_found = any(is_main_fh_file(f) for f in fh_files)

        if not main_db_found and not main_fh_found:
            print(f"⚠ Файл, указанный в MAIN_DB_FILE_NAME, не найден: {MAIN_DB_FILE_NAME}")

    if not db_files and not fh_files:
        print(f"\n⚠ Файлы .db и .fh не найдены в {DIRECTORY}")
        return

    results = []

    # Обработка .db файлов
    if db_files:
        print(f"\n{'─' * 80}")
        print(f"  ОБРАБОТКА .db ФАЙЛОВ ({len(db_files)} шт.)")
        print(f"{'─' * 80}")

        for idx, db_path in enumerate(db_files, 1):
            if VERBOSE:
                print(f"\n  [{idx}/{len(db_files)}]")

            result = process_db_file(db_path)
            results.append(result)

    # Обработка .fh файлов
    if fh_files:
        print(f"\n{'─' * 80}")
        print(f"  ОБРАБОТКА .fh ФАЙЛОВ ({len(fh_files)} шт.)")
        print(f"{'─' * 80}")

        for idx, fh_path in enumerate(fh_files, 1):
            if VERBOSE:
                print(f"\n  [{idx}/{len(fh_files)}]")

            result = process_fh_file(fh_path)
            results.append(result)

    # Статистика
    print("\n" + "=" * 80)
    print("  СТАТИСТИКА")
    print("=" * 80)

    processed = sum(1 for r in results if r['status'] == 'ok')
    skipped = sum(1 for r in results if r['status'] == 'skipped')
    errors = sum(1 for r in results if r['status'] == 'error')

    db_processed = sum(
        1 for r in results
        if r['status'] == 'ok' and r['file_type'] == 'db'
    )

    fh_processed = sum(
        1 for r in results
        if r['status'] == 'ok' and r['file_type'] == 'fh'
    )

    db_skipped = sum(
        1 for r in results
        if r['status'] == 'skipped' and r['file_type'] == 'db'
    )

    fh_skipped = sum(
        1 for r in results
        if r['status'] == 'skipped' and r['file_type'] == 'fh'
    )

    type_2 = sum(1 for r in results if r.get('data_type') == 2)
    type_3 = sum(1 for r in results if r.get('data_type') == 3)

    print(f"  Обработано всего : {processed}")
    print(f"    ├─ .db         : {db_processed}")
    print(f"    └─ .fh         : {fh_processed}")
    print(f"  Пропущено всего  : {skipped}")
    print(f"    ├─ .db         : {db_skipped}")
    print(f"    └─ .fh         : {fh_skipped}")
    print(f"  Ошибок           : {errors}")
    print(f"  DataType=2 (Double/FH): {type_2}")
    print(f"  DataType=3 (Any)      : {type_3}")
    print(f"  Всего файлов          : {len(results)}")
    print("=" * 80)

    if errors > 0:
        print("\n❌ Файлы с ошибками:")

        for r in results:
            if r['status'] == 'error':
                print(
                    f"  - [{r['file_type'].upper()}] "
                    f"{os.path.basename(r['file'])}: {r['message']}"
                )

    if DRY_RUN:
        print("\n⚠️  Это был тестовый прогон (DRY_RUN=True). Файлы не изменены.")


if __name__ == "__main__":
    main()