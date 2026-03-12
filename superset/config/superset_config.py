import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "unsafe-default")

SQLALCHEMY_DATABASE_URI = os.environ.get("SUPERSET_DB_URI")

# Redis cache (опционально)
CACHE_CONFIG = {
    'CACHE_TYPE': 'redis',
    'CACHE_REDIS_HOST': 'redis',
    'CACHE_REDIS_PORT': 6379,
    'CACHE_REDIS_DB': 1,
    'CACHE_REDIS_URL': 'redis://redis:6379/1'
}

# Для async-запросов (опционально)
class CeleryConfig:
    broker_url = 'redis://redis:6379/1'
    result_backend = 'redis://redis:6379/1'

CELERY_CONFIG = CeleryConfig