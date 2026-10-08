"""
Читает все .fh файлы из заданной директории и выводит полную информацию.
Параметры задаются константами ниже.
"""
import struct
import uuid
import binascii
import os
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import List, Optional
from pathlib import Path

# ═══════════════════════════════════════════════════════════════════
#  НАСТРОЙКИ
# ═══════════════════════════════════════════════════════════════════

# Директория с .fh файлами
DIRECTORY = r".\test_data\data\rotorTik\data\double"

# Искать рекурсивно?
RECURSIVE = True

# Показать все значения (True) или только превью (False)?
SHOW_ALL_VALUES = False

# Максимум значений для превью (если SHOW_ALL_VALUES=False)
MAX_PREVIEW = 10

# ═══════════════════════════════════════════════════════════════════
#  Формат .fh
# ═══════════════════════════════════════════════════════════════════

MAGIC_HEX = 0xBEBAADDE
MINIMUM_HEADER_LENGTH = 30
HEADER_DATA_OFFSET = 26
FROM_DATE_OFFSET = 8
TO_DATE_OFFSET = 16
VERSION_OFFSET = 24
EXTRA_MAGIC = 0x4B52494F

QUALITY_NAMES = {0: "UNKNOWN", 1: "NO_DATA", 2: "GOOD", 3: "BAD"}
QUALITY_LENGTH = 1
VALUE_LENGTH = 8
FIELD_LENGTH = QUALITY_LENGTH + VALUE_LENGTH
ROW_ID_LENGTH = 16


def ts_to_datetime(ts_ms: int) -> datetime:
    return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=ts_ms)


def compute_crc32(data: bytes) -> int:
    return binascii.crc32(data) & 0xFFFFFFFF


def guid_from_bytes_le(b: bytes) -> uuid.UUID:
    return uuid.UUID(bytes_le=b)


@dataclass
class DataValue:
    quality: int
    raw_value: bytes
    timestamp: datetime

    @property
    def quality_name(self) -> str:
        return QUALITY_NAMES.get(self.quality, f"UNK({self.quality})")

    @property
    def as_double(self) -> float:
        return struct.unpack('<d', self.raw_value)[0]

    @property
    def as_int64(self) -> int:
        return struct.unpack('<q', self.raw_value)[0]


@dataclass
class DataRow:
    property_id: uuid.UUID
    values: List[DataValue] = field(default_factory=list)


@dataclass
class ExtraMetadata:
    file_id: Optional[uuid.UUID] = None
    main_db_id: Optional[uuid.UUID] = None
    main_db_name: Optional[str] = None
    data_type: Optional[int] = None


@dataclass
class FhFile:
    file_path: str
    magic: int = 0
    header_length: int = 0
    from_date: datetime = None
    to_date: datetime = None
    file_version: int = 0
    header_data: bytes = b''
    stored_crc32: int = 0
    calculated_crc32: int = 0
    crc_valid: bool = False
    data_row_values_count: int = 0
    extra: ExtraMetadata = field(default_factory=ExtraMetadata)
    rows: List[DataRow] = field(default_factory=list)


def read_fh_file(file_path: str) -> Optional[FhFile]:
    if not os.path.exists(file_path):
        print(f"[ОШИБКА] Файл не найден: {file_path}")
        return None

    with open(file_path, 'rb') as f:
        file_data = f.read()

    file_size = len(file_data)
    result = FhFile(file_path=file_path)

    if file_size < MINIMUM_HEADER_LENGTH:
        print(f"[ОШИБКА] Файл слишком мал: {file_size} байт")
        return None

    result.magic = struct.unpack_from('<I', file_data, 0)[0]
    if result.magic != MAGIC_HEX:
        print(f"[ОШИБКА] Неверное магическое слово: 0x{result.magic:08X}")
        return None

    result.header_length = struct.unpack_from('<I', file_data, 4)[0]
    if result.header_length < MINIMUM_HEADER_LENGTH or result.header_length > file_size:
        print(f"[ОШИБКА] Некорректная длина заголовка: {result.header_length}")
        return None

    from_ts = struct.unpack_from('<Q', file_data, FROM_DATE_OFFSET)[0]
    to_ts = struct.unpack_from('<Q', file_data, TO_DATE_OFFSET)[0]
    result.from_date = ts_to_datetime(from_ts)
    result.to_date = ts_to_datetime(to_ts)

    result.file_version = struct.unpack_from('<H', file_data, VERSION_OFFSET)[0]

    header_data_end = result.header_length - 4
    result.header_data = file_data[HEADER_DATA_OFFSET:header_data_end]

    result.stored_crc32 = struct.unpack_from('<I', file_data, header_data_end)[0]
    result.calculated_crc32 = compute_crc32(file_data[:header_data_end])
    result.crc_valid = (result.stored_crc32 == result.calculated_crc32)

    hd = result.header_data
    if result.file_version == 1:
        if len(hd) >= 4:
            result.data_row_values_count = struct.unpack_from('<I', hd, 0)[0]

        if len(hd) >= 8:
            sig = struct.unpack_from('<I', hd, 4)[0]
            if sig == EXTRA_MAGIC:
                offset = 8
                result.extra.file_id = guid_from_bytes_le(hd[offset:offset + 16])
                offset += 16
                result.extra.main_db_id = guid_from_bytes_le(hd[offset:offset + 16])
                offset += 16
                name_len = struct.unpack_from('<I', hd, offset)[0]
                offset += 4
                result.extra.main_db_name = hd[offset:offset + name_len].decode('utf-8')
                offset += name_len
                result.extra.data_type = struct.unpack_from('<I', hd, offset)[0]

    row_length = ROW_ID_LENGTH + (FIELD_LENGTH * result.data_row_values_count)
    data_section = file_data[result.header_length:]
    data_size = len(data_section)

    if row_length > 0 and data_size % row_length != 0:
        print(f"[ПРЕДУПРЕЖДЕНИЕ] Размер данных ({data_size}) не кратен длине строки ({row_length})")

    num_rows = data_size // row_length if row_length > 0 else 0
    offset = 0

    for _ in range(num_rows):
        prop_id = guid_from_bytes_le(data_section[offset:offset + ROW_ID_LENGTH])
        offset += ROW_ID_LENGTH

        row = DataRow(property_id=prop_id)
        for col_idx in range(result.data_row_values_count):
            quality = data_section[offset]
            raw_val = data_section[offset + QUALITY_LENGTH : offset + FIELD_LENGTH]
            ts = result.from_date + timedelta(seconds=col_idx)
            row.values.append(DataValue(quality=quality, raw_value=raw_val, timestamp=ts))
            offset += FIELD_LENGTH

        result.rows.append(row)

    return result


def print_fh_file(fh: FhFile):
    print("=" * 70)
    print(f"  ФАЙЛ: {fh.file_path}")
    print("=" * 70)

    print("\n─── ЗАГОЛОВОК ───")
    print(f"  Магическое слово : 0x{fh.magic:08X} {'✓' if fh.magic == MAGIC_HEX else '✗'}")
    print(f"  Длина заголовка  : {fh.header_length} байт")
    print(f"  Версия файла     : {fh.file_version}")
    print(f"  Дата начала      : {fh.from_date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} UTC")
    print(f"  Дата окончания   : {fh.to_date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} UTC")
    print(f"  Длительность     : {fh.to_date - fh.from_date}")
    print(f"  CRC32 (файл)     : 0x{fh.stored_crc32:08X}")
    print(f"  CRC32 (вычисл.)  : 0x{fh.calculated_crc32:08X}  {'✓ OK' if fh.crc_valid else '✗ ОШИБКА!'}")

    if fh.file_version == 1:
        print(f"\n  [Версия 1]")
        print(f"  Значений на пар. : {fh.data_row_values_count}")

        if fh.extra.file_id is not None:
            print(f"\n  ─── ДОПОЛНИТЕЛЬНЫЕ МЕТАДАННЫЕ ───")
            print(f"  Id               : {{{fh.extra.file_id}}}")
            print(f"  MainDbId         : {{{fh.extra.main_db_id}}}")
            print(f"  MainDbName       : {fh.extra.main_db_name}")
            print(f"  DataType         : {fh.extra.data_type}")

    print(f"\n─── ДАННЫЕ ──")
    print(f"  Строк (параметров) : {len(fh.rows)}")
    total_values = sum(len(r.values) for r in fh.rows)
    print(f"  Всего значений     : {total_values}")

    quality_stats = {0: 0, 1: 0, 2: 0, 3: 0}
    for row in fh.rows:
        for v in row.values:
            if v.quality in quality_stats:
                quality_stats[v.quality] += 1
    print(f"  Статистика качеств :")
    for q, name in QUALITY_NAMES.items():
        print(f"    {name:10s} : {quality_stats.get(q, 0)}")

    print(f"\n─── СТРОКИ ──")
    for i, row in enumerate(fh.rows):
        print(f"\n  [{i+1}] Property ID: {{{row.property_id}}}")
        print(f"      Значений: {len(row.values)}")

        if SHOW_ALL_VALUES:
            for j, v in enumerate(row.values):
                print(f"        [{j:5d}] {v.timestamp.strftime('%H:%M:%S')} | "
                      f"{v.quality_name:7s} | double={v.as_double:>15.6f}  int64={v.as_int64}")
        else:
            shown = 0
            for j, v in enumerate(row.values):
                if v.quality in (2, 3):
                    print(f"        [{j:5d}] {v.timestamp.strftime('%H:%M:%S')} | "
                          f"{v.quality_name:7s} | double={v.as_double:>15.6f}  int64={v.as_int64}")
                    shown += 1
                    if shown >= MAX_PREVIEW:
                        remaining = sum(1 for x in row.values if x.quality in (2, 3)) - shown
                        if remaining > 0:
                            print(f"        ... ещё {remaining} заполненных значений")
                        break
            if shown == 0:
                print(f"        (нет заполненных значений)")


def main():
    dir_path = Path(DIRECTORY)
    if RECURSIVE:
        fh_files = sorted(dir_path.rglob('*.fh'))
    else:
        fh_files = sorted(dir_path.glob('*.fh'))

    if not fh_files:
        print(f"В директории '{DIRECTORY}' .fh файлы не найдены.")
        return

    print(f"Найдено файлов: {len(fh_files)}\n")

    for fpath in fh_files:
        fh = read_fh_file(str(fpath))
        if fh:
            print_fh_file(fh)
            print()


if __name__ == "__main__":
    main()