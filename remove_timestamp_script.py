#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
import sys

def process_parquet_files():
    # Путь к папке с исходными файлами (относительно расположения скрипта или абсолютный)
    # Лучше использовать абсолютный путь или запускать из корня проекта
    folder_path = Path("output") 
    
    # Папка для вывода
    output_folder = Path("output_no_timestamp")
    output_folder.mkdir(parents=True, exist_ok=True)

    if not folder_path.exists():
        print(f"Ошибка: Папка {folder_path} не найдена.")
        sys.exit(1)

    parquet_files = list(folder_path.glob("*.parquet"))

    if not parquet_files:
        print(f"В папке {folder_path} не найдено файлов .parquet")
        sys.exit(0)

    print(f"Найдено файлов: {len(parquet_files)}")
    
    for file_path in parquet_files:
        try:
            df = pd.read_parquet(file_path)
            
            if 'timestamp' in df.columns:
                df = df.drop(columns=['timestamp'])
                output_path = output_folder / file_path.name
                df.to_parquet(output_path, index=False)
                print(f"✓ Сохранено: {output_path.name}")
            else:
                print(f"⊘ Нет столбца 'timestamp' в: {file_path.name}")
                
        except Exception as e:
            print(f" Ошибка при обработке {file_path.name}: {e}")

    print("\nГотово!")

if __name__ == "__main__":
    process_parquet_files()