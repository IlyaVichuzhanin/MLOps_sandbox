#!/bin/bash
set -e

echo "🔄 Checking MLflow database schema..."

# Функция проверки подключения к PostgreSQL через Python/psycopg2
wait_for_postgres() {
  local host="${POSTGRES_HOST:-postgres_ml_flow}"
  local port="${POSTGRES_PORT:-5432}"
  local user="${POSTGRES_USER:-mlflow}"
  local db="${POSTGRES_DB:-mlflow}"
  local max_attempts=30
  local attempt=0

  # Парсим database_uri, если задан (например: postgresql+psycopg2://user:pass@host:port/db)
  if [[ -n "$MLFLOW_BACKEND_STORE_URI" ]]; then
    # Извлекаем host, port, user, db из URI с помощью Python
    read -r host port user db < <(python3 -c "
import re, sys
uri = '${MLFLOW_BACKEND_STORE_URI}'
m = re.match(r'postgresql(\+[^:]+)?://([^:]+):?([^@]*)?@([^:/]+):?(\d*)/(\w+)', uri)
if m:
    print(m.group(4) or 'localhost', m.group(5) or '5432', m.group(2) or 'postgres', m.group(6) or 'postgres')
else:
    print('postgres_ml_flow', '5432', 'mlflow', 'mlflow')
")
  fi

  echo "🔍 Checking connection to $host:$port as $user@$db..."

  while [ $attempt -lt $max_attempts ]; do
    if python3 -c "
import psycopg2, sys, os
try:
    # Пробуем подключиться без пароля (если в URI есть пароль — он подхватится из env)
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', '$host'),
        port=os.getenv('POSTGRES_PORT', '$port'),
        user=os.getenv('POSTGRES_USER', '$user'),
        password=os.getenv('POSTGRES_PASSWORD', ''),
        dbname=os.getenv('POSTGRES_DB', '$db'),
        connect_timeout=2
    )
    conn.close()
    sys.exit(0)
except Exception as e:
    sys.exit(1)
" 2>/dev/null; then
      echo "✅ PostgreSQL is ready!"
      return 0
    fi
    attempt=$((attempt + 1))
    echo "⏳ Waiting for PostgreSQL... (attempt $attempt/$max_attempts)"
    sleep 2
  done

  echo "❌ Failed to connect to PostgreSQL after $max_attempts attempts"
  return 1
}

# Ждём готовности БД
wait_for_postgres

# Выполняем миграцию схемы
echo "🔧 Running mlflow db upgrade..."
mlflow db upgrade "${MLFLOW_BACKEND_STORE_URI}"

echo "✅ Schema up to date. Starting MLflow server..."

# Запускаем оригинальную команду
exec "$@"