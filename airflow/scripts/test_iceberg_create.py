# airflow/scripts/test_iceberg_create.py
import pandas as pd
from datetime import datetime
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, StringType, TimestampType, NestedField
from pyiceberg.exceptions import NoSuchNamespaceError, TableAlreadyExistsError

print("🔍 Iceberg Table Creation Test")
print("=" * 50)

config = {
    "uri": "http://iceberg-rest:8181",
    "warehouse": "s3://data-storage-warehouse/iceberg_warehouse",  # ← ЯВНЫЙ ПУТЬ
    "s3.endpoint": "http://minio:9000",
    "s3.access-key-id": "minioadmin",
    "s3.secret-access-key": "minioadmin",
    "s3.region": "us-east-1",
    "s3.path-style-access": "true",
    "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"
}

try:
    catalog = load_catalog("default", **config)
    print("✓ Catalog connected")
    
    # Проверка неймспейса
    namespaces = catalog.list_namespaces()
    print(f"✓ Namespaces: {[n[0] for n in namespaces]}")
    
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
    
    # Имя таблицы как кортеж (надежнее для REST Catalog)
    table_identifier = ("ml_sandbox", "iceberg_test")
    
    # Создание или загрузка таблицы
    try:
        table = catalog.load_table(table_identifier)
        print(f"✓ Loaded existing table: {'.'.join(table_identifier)}")
    except Exception as e:
        print(f"ℹ Table not found, creating: {'.'.join(table_identifier)}")
        try:
            table = catalog.create_table(table_identifier, schema=schema)
            print(f"✓ Table created successfully")
        except Exception as create_err:
            print(f"❌ Failed to create table: {type(create_err).__name__}: {create_err}")
            # Попробуем загрузить ещё раз (возможно, таблица создалась, но ответ не дошёл)
            try:
                table = catalog.load_table(table_identifier)
                print(f"✓ Table loaded after creation attempt")
            except:
                raise create_err
    
    # Запись данных
    table.append(df)
    print(f"✓ Appended {len(df)} row(s)")
    
    # Чтение и проверка
    result = table.scan().to_arrow().to_pandas()
    print(f"✓ Read back: {len(result)} row(s)")
    print(result)
    
    # Проверка формата
    print(f"\n📦 Metadata location: {table.metadata_location}")
    
    # Проверка, что это действительно Iceberg-файл
    if "metadata/v" in table.metadata_location and table.metadata_location.endswith(".json"):
        print("✓ Metadata file follows Iceberg convention")
    
    print("\n✅ Iceberg table creation and write SUCCESSFUL!")
    
except Exception as e:
    print(f"\n❌ FATAL: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()