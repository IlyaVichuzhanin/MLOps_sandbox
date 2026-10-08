# airflow/scripts/test_iceberg_simple.py
from pyiceberg.catalog import load_catalog

print("🔍 Simple Iceberg Connection Test")

config = {
    "uri": "http://iceberg-rest:8181",
    "warehouse": "s3://ml_sandbox/iceberg_warehouse",
    "s3.endpoint": "http://minio:9000",
    "s3.access-key-id": "minioadmin",
    "s3.secret-access-key": "minioadmin",
    "s3.region": "us-east-1",
    "s3.path-style-access": "true",
    "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"
}

try:
    catalog = load_catalog("default", **config)
    print("✓ Catalog object created")
    
    # Только список неймспейсов (без записи)
    namespaces = catalog.list_namespaces()
    print(f"✓ Namespaces: {namespaces}")
    
    print("\n✅ Basic connection OK — problem is in table creation, not connectivity")
    
except Exception as e:
    print(f"\n❌ Connection failed: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()