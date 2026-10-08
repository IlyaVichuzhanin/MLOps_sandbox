"""
Добавляет метаданные (Id, MainDbId, MainDbName, DataType) в заголовок всех .fh файлов.
Все параметры задаются константами ниже.
"""
import struct
import uuid
import binascii
import os
import shutil
from datetime import datetime
from pathlib import Path

# ═══════════════════════════════════════════════════════════════════
#  НАСТРОЙКИ — измените под свои задачи
# ══════════════════════════════════════════════════════════════════

# Директория с .fh файлами (абсолютный или относительный путь)
DIRECTORY = r".\test_data\data\rotorTik\data\double"

# Искать файлы рекурсивно во вложенных папках?
RECURSIVE = True

# Имя базы данных
MAIN_DB_NAME = "Kriogen"

# ID базы данных (GUID)
MAIN_DB_ID = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"

# Тип данных
DATA_TYPE = 2

# Создавать .bak копию перед изменением?
BACKUP = True

# Перезаписывать метаданные, если они уже есть?
FORCE = False

# Тестовый прогон без реальной записи файлов?
DRY_RUN = False

# ═══════════════════════════════════════════════════════════════════
#  Формат .fh — не менять
# ═══════════════════════════════════════════════════════════════════

MAGIC_HEX = 0xBEBAADDE
MINIMUM_HEADER_LENGTH = 30
HEADER_DATA_OFFSET = 26
EXTRA_MAGIC = 0x4B52494F  # "KRIO"
EXTRA_MAGIC_BYTES = struct.pack('<I', EXTRA_MAGIC)


def compute_crc32(data: bytes) -> int:
    return binascii.crc32(data) & 0xFFFFFFFF


def build_extra_payload(main_db_name: str, main_db_id_guid: str) -> tuple:
    """Собирает блок дополнительных метаданных. Возвращает (payload_bytes, file_id)."""
    file_id = uuid.uuid4()
    main_db_id = uuid.UUID(main_db_id_guid)

    payload = bytearray()
    payload.extend(EXTRA_MAGIC_BYTES)
    payload.extend(file_id.bytes_le)
    payload.extend(main_db_id.bytes_le)

    name_bytes = main_db_name.encode('utf-8')
    payload.extend(struct.pack('<I', len(name_bytes)))
    payload.extend(name_bytes)
    payload.extend(struct.pack('<I', DATA_TYPE))

    return bytes(payload), file_id


def process_single_file(file_path: str, main_db_name: str, main_db_id: str) -> dict:
    """Обрабатывает один .fh файл. Возвращает результат."""
    result = {'file': file_path, 'status': 'ok', 'message': '', 'new_id': None, 'skipped': False}

    if not os.path.exists(file_path):
        result['status'] = 'error'
        result['message'] = 'Файл не найден'
        return result

    with open(file_path, 'rb') as f:
        file_data = bytearray(f.read())

    file_size = len(file_data)

    magic = struct.unpack_from('<I', file_data, 0)[0]
    if magic != MAGIC_HEX:
        result['status'] = 'error'
        result['message'] = f'Неверное магическое слово: 0x{magic:08X}'
        return result

    old_header_length = struct.unpack_from('<I', file_data, 4)[0]
    if old_header_length < MINIMUM_HEADER_LENGTH or old_header_length > file_size:
        result['status'] = 'error'
        result['message'] = f'Некорректная длина заголовка: {old_header_length}'
        return result

    old_header = file_data[:old_header_length]
    data_section = file_data[old_header_length:]
    old_header_data = old_header[HEADER_DATA_OFFSET : old_header_length - 4]

    # Проверка: уже модифицирован?
    already_modified = False
    if len(old_header_data) >= 8:
        if old_header_data[4:8] == EXTRA_MAGIC_BYTES:
            already_modified = True

    if already_modified and not FORCE:
        result['skipped'] = True
        result['message'] = 'Файл уже содержит метаданные (FORCE=False)'
        return result

    # Собираем новый заголовок
    base_header_data = old_header_data[:4] if len(old_header_data) >= 4 else old_header_data
    extra_payload, new_file_id = build_extra_payload(main_db_name, main_db_id)
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
        result['message'] = f'DRY RUN: +{new_header_length - old_header_length} байт, Id={new_file_id}'
        result['new_id'] = new_file_id
        return result

    if BACKUP:
        shutil.copy2(file_path, file_path + '.bak')

    with open(file_path, 'wb') as f:
        f.write(new_file)

    result['new_id'] = new_file_id
    result['message'] = f'+{new_header_length - old_header_length} байт'
    return result


def find_fh_files(directory: str, recursive: bool) -> list:
    dir_path = Path(directory)
    if recursive:
        return sorted(dir_path.rglob('*.fh'))
    else:
        return sorted(dir_path.glob('*.fh'))


def main():
    fh_files = find_fh_files(DIRECTORY, RECURSIVE)
    if not fh_files:
        print(f"В директории '{DIRECTORY}' .fh файлы не найдены.")
        return

    print("=" * 70)
    print(f"  Директория : {Path(DIRECTORY).resolve()}")
    print(f"  Файлов     : {len(fh_files)}")
    print(f"  Рекурсия   : {'да' if RECURSIVE else 'нет'}")
    print(f"  MainDbName : {MAIN_DB_NAME}")
    print(f"  MainDbId   : {MAIN_DB_ID}")
    print(f"  DataType   : {DATA_TYPE}")
    print(f"  Id         : случайный UUID4")
    print(f"  Backup     : {'да' if BACKUP else 'нет'}")
    print(f"  Force      : {'да' if FORCE else 'нет'}")
    print(f"  Dry run    : {'да' if DRY_RUN else 'нет'}")
    print("=" * 70)

    results = []
    for idx, fpath in enumerate(fh_files, 1):
        print(f"\n  [{idx}/{len(fh_files)}] {fpath.name}")
        res = process_single_file(str(fpath), MAIN_DB_NAME, MAIN_DB_ID)
        results.append(res)

        if res['skipped']:
            print(f"      ⏭  Пропущен: {res['message']}")
        elif res['status'] == 'error':
            print(f"      ✗  Ошибка: {res['message']}")
        else:
            print(f"      ✓  Id={res['new_id']}, {res['message']}")

    ok = sum(1 for r in results if r['status'] == 'ok' and not r['skipped'])
    skip = sum(1 for r in results if r['skipped'])
    err = sum(1 for r in results if r['status'] == 'error')

    print(f"\n{'=' * 70}")
    print(f"  Итого: обработано={ok}, пропущено={skip}, ошибок={err}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()