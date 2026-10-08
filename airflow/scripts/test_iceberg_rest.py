# airflow/scripts/test_iceberg_rest.py
"""
Тест подключения к Iceberg REST Catalog с правильными кредами.
"""
from pyiceberg.catalog import load_catalog

print("🔍 Iceberg REST Catalog Test (correct credentials)")
print("=" * 60)

# 🔑 ПРАВИЛЬНЫЕ КРЕДЫ:
config = {
    "uri": "http://iceberg-rest:8181",
    "warehouse": "s3://data-storage-warehouse/iceberg_warehouse",
    "s3.endpoint": "http://minio:9000",
    "s3.access-key-id": "admin",           # ← _MINIO_ROOT_USER
    "s3.secret-access-key": "password",    # ← _MINIO_ROOT_PASSWORD
    "s3.region": "us-east-1",
    "s3.path-style-access": "true",
    "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"
}

try:
    catalog = load_catalog("default", **config)
    print("✓ Catalog object created")
    
    namespaces = catalog.list_namespaces()
    print(f"✓ Namespaces: {[n[0] for n in namespaces] if namespaces else '[empty]'}")
    
    print("\n✅ REST Catalog connection SUCCESSFUL!")
    
except Exception as e:
    print(f"\n❌ Error: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()