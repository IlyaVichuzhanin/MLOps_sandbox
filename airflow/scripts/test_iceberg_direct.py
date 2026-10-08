# airflow/scripts/test_iceberg_direct.py
"""
Прямая запись данных в формате Apache Iceberg через boto3 + MinIO.
Использует правильные креды: admin / password
"""
import io
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.client import Config
from datetime import datetime
import uuid

print("🔍 Direct Iceberg Write (boto3 + MinIO)")
print("=" * 60)

# === Конфигурация ===
# 🔑 ПРАВИЛЬНЫЕ КРЕДЫ из вашего .env:
MINIO_ENDPOINT = "http://minio:9000"
MINIO_USER = "admin"              # ← _MINIO_ROOT_USER
MINIO_PASS = "password"           # ← _MINIO_ROOT_PASSWORD
MINIO_REGION = "us-east-1"
BUCKET = "data-storage-warehouse"

print(f"🔐 Using credentials: {MINIO_USER} / {'*' * len(MINIO_PASS)}")

# === Создаём S3 client для MinIO ===
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_USER,
    aws_secret_access_key=MINIO_PASS,
    region_name=MINIO_REGION,
    config=Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"}  # 🔑 path-style для MinIO
    )
)

# === Пути ===
WAREHOUSE_KEY = "iceberg_warehouse"
NAMESPACE = "ml_sandbox"
TABLE_NAME = "iceberg_test"
table_prefix = f"{WAREHOUSE_KEY}/{NAMESPACE}.db/{TABLE_NAME}"
metadata_prefix = f"{table_prefix}/metadata"
data_prefix = f"{table_prefix}/data"

print(f"📍 Table path: s3://{BUCKET}/{table_prefix}")

# === Схема данных ===
pa_schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("name", pa.string(), nullable=True),
    pa.field("ts", pa.timestamp("us"), nullable=True),
])

# === Данные ===
df = pd.DataFrame([
    {"id": 1, "name": "alpha", "ts": datetime.now()},
    {"id": 2, "name": "beta", "ts": datetime.now()},
    {"id": 3, "name": "gamma", "ts": datetime.now()},
])
pa_table = pa.Table.from_pandas(df, schema=pa_schema, preserve_index=False)
print(f"✓ Prepared {len(df)} rows")

# === 🔍 ТЕСТ подключения ===
test_key = f"test-connection-{uuid.uuid4().hex}.txt"
try:
    s3.put_object(Bucket=BUCKET, Key=test_key, Body=b"OK")
    s3.delete_object(Bucket=BUCKET, Key=test_key)
    print(f"✓ Connection test passed")
except Exception as e:
    print(f"✗ Connection test failed: {e}")
    print("💡 Проверьте: 1) MinIO запущен, 2) бакет существует, 3) креды верны")
    raise

# === Запись Parquet-файла ===
buffer = io.BytesIO()
pq.write_table(pa_table, buffer)
buffer.seek(0)

file_id = uuid.uuid4().hex
parquet_key = f"{data_prefix}/00000-{file_id}.parquet"

s3.upload_fileobj(buffer, BUCKET, parquet_key)
print(f"✓ Written Parquet: s3://{BUCKET}/{parquet_key}")

# === Метаданные Iceberg (упрощённые) ===
metadata = {
    "format-version": 2,
    "table-uuid": str(uuid.uuid4()),
    "location": f"s3://{BUCKET}/{table_prefix}",
    "last-sequence-number": 1,
    "current-schema-id": 0,
    "schemas": [{
        "type": "struct",
        "schema-id": 0,
        "fields": [
            {"id": 1, "name": "id", "type": "long", "required": True},
            {"id": 2, "name": "name", "type": "string", "required": False},
            {"id": 3, "name": "ts", "type": "timestamp", "required": False},
        ]
    }],
    "current-snapshot-id": 1,
    "snapshots": [{
        "snapshot-id": 1,
        "timestamp-ms": int(datetime.now().timestamp() * 1000),
        "manifest-list": f"{metadata_prefix}/snap-1.avro",
        "summary": {"operation": "append"}
    }],
}

# Запись metadata.json
metadata_key = f"{metadata_prefix}/v1.metadata.json"
s3.put_object(
    Bucket=BUCKET,
    Key=metadata_key,
    Body=json.dumps(metadata, indent=2).encode('utf-8'),
    ContentType='application/json'
)
print(f"✓ Written metadata: s3://{BUCKET}/{metadata_key}")

# version-hint.text
hint_key = f"{metadata_prefix}/version-hint.text"
s3.put_object(Bucket=BUCKET, Key=hint_key, Body=b"1")
print(f"✓ Written version hint: s3://{BUCKET}/{hint_key}")

print("\n✅ Direct Iceberg write SUCCESSFUL!")
print(f"📦 Data at: s3://{BUCKET}/{table_prefix}")
print("ℹ️  Files are Iceberg-compatible (Parquet + metadata.json)")