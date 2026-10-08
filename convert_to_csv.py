import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import os

def convert_parquet_to_csv(directory, output_dir=None):
    """
    Конвертирует все parquet файлы в директории в CSV формат
    
    Args:
        directory: Путь к папке с parquet файлами
        output_dir: Путь к папке для сохранения CSV (если None, сохраняется в ту же папку)
    """
    dir_path = Path(directory).resolve()
    
    if output_dir is None:
        output_dir = dir_path
    else:
        output_dir = Path(output_dir).resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"🔍 Ищем parquet файлы в: {dir_path}")
    
    # Рекурсивно находим все parquet файлы
    parquet_files = list(dir_path.rglob("*.parquet"))
    
    if not parquet_files:
        print(f"❌ Parquet файлы не найдены в {dir_path}")
        return
    
    print(f"📁 Найдено {len(parquet_files)} parquet файлов")
    print(f"📤 Сохранение в: {output_dir}")
    print("=" * 70)
    
    success_count = 0
    error_count = 0
    
    for idx, parquet_file in enumerate(parquet_files, 1):
        try:
            # Формируем имя выходного CSV файла
            relative_path = parquet_file.relative_to(dir_path)
            csv_file = output_dir / relative_path.with_suffix('.csv')
            
            # Создаем подпапки если нужно
            csv_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Читаем parquet
            df = pd.read_parquet(parquet_file)
            
            # Сохраняем в CSV
            df.to_csv(csv_file, index=False, decimal='.', sep=',')
            
            print(f"[{idx}/{len(parquet_files)}] ✅ {parquet_file.name} -> {csv_file.name} ({len(df)} строк)")
            success_count += 1
            
        except Exception as e:
            print(f"[{idx}/{len(parquet_files)}] ❌ Ошибка {parquet_file.name}: {e}")
            error_count += 1
    
    print("=" * 70)
    print(f"📊 ИТОГО: успешно {success_count}, ошибок {error_count}")
    print("🎉 Готово!")


if __name__ == "__main__":
    # Укажите путь к вашей папке с parquet файлами
    INPUT_DIR = "./data_for_test"
    OUTPUT_DIR = "./output_csv_2"  # папка для CSV файлов (или None для сохранения в ту же папку)
    
    print("=" * 70)
    print("🔄 Конвертация Parquet → CSV")
    print("=" * 70)
    
    convert_parquet_to_csv(INPUT_DIR, OUTPUT_DIR)