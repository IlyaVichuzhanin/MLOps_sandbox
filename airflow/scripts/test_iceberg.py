#!/usr/bin/env python3
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "default",
    uri="http://iceberg-rest:8181",
    warehouse="s3://ml-sandbox/iceberg_warehouse",
    **{
        "s3.endpoint": "http://minio:9000",
        "s3.access-key-id": "your_access_key",
        "s3.secret-access-key": "your_secret_key",
        "s3.region": "us-east-1",
        "s3.path-style-access": "true",
    }
)

print("📋 Namespaces:", catalog.list_namespaces())
print("✅ Iceberg connection successful!")