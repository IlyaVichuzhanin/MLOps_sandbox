# airflow/scripts/test_iceberg_create_rest.py
"""
Создание таблицы через Iceberg REST Catalog с правильными кредами.
"""
import pandas as pd
from datetime import datetime
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, StringType, TimestampType, NestedField

print("🔍 Iceberg Table Creation via REST Catalog")
print("=" * 60)

# 🔑 ПРАВИЛЬНЫЕ КРЕДЫ:
config = {
    "uri": "http://iceberg-rest:8181",
    "warehouse": "s3://data-storage-warehouse/iceberg_warehouse",
    "s3.endpoint": "http://minio:9000",
    "s3.access-key-id": "admin",
    "s3.secret-access-key": "password",
    "s3.region": "us-east-1",
    "s3.path-style-access": "true",
    "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"
}

try:
    catalog = load_catalog("default", **config)
    print("✓ Connected to REST catalog")
    
    # Список неймспейсов
    namespaces = catalog.list_namespaces()
    print(f"✓ Namespaces: {[n[0] for n in namespaces] if namespaces else '[empty]'}")
    
    # Создание неймспейса, если не существует
    if ("ml_sandbox",) not in namespaces:
        catalog.create_namespace("ml_sandbox")
        print("✓ Created namespace: ml_sandbox")
    
    # Схема таблицы
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "name", StringType()),
        NestedField(3, "ts", TimestampType()),
        identifier_field_ids=[1]
    )
    
    # Данные
    df = pd.DataFrame([
        {"id": 1, "name": "test", "ts": datetime.now()}
    ])
    
    # Имя таблицы как кортеж (надёжнее для REST Catalog)
    table_identifier = ("ml_sandbox", "iceberg_test")
    
    # Создание или загрузка таблицы
    try:
        table = catalog.load_table(table_identifier)
        print(f"✓ Loaded existing table: {'.'.join(table_identifier)}")
    except Exception:
        print(f"ℹ Table not found, creating: {'.'.join(table_identifier)}")
        table = catalog.create_table(table_identifier, schema=schema)
        print(f"✓ Table created successfully")
    
    # Запись данных
    table.append(df)
    print(f"✓ Appended {len(df)} row(s)")
    
    # Чтение и проверка
    result = table.scan().to_arrow().to_pandas()
    print(f"✓ Read back: {len(result)} row(s)")
    print(result)
    
    # Проверка формата
    print(f"\n📦 Metadata location: {table.metadata_location}")
    
    print("\n✅ Iceberg table creation via REST Catalog SUCCESSFUL!")
    
except Exception as e:
    print(f"\n❌ FATAL: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()