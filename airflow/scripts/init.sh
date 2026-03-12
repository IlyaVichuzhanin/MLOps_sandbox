#!/bin/bash
set -e

# Устанавливаем права на скрипты ДО любых операций
chmod +x /opt/airflow/scripts/*.sh

# Проверка и установка AIRFLOW_UID
if [[ -z "${AIRFLOW_UID}" ]]; then
    echo "WARNING: AIRFLOW_UID not set, using default 50000"
    export AIRFLOW_UID=50000
fi

# Создаём директории
mkdir -p /opt/airflow/{logs,dags,plugins,config}

# Ожидаем готовности PostgreSQL (КРИТИЧЕСКИ ВАЖНО!)
echo "Waiting for PostgreSQL to be ready..."
until pg_isready -h postgres_airflow -U airflow 2>/dev/null; do
  echo "PostgreSQL not ready yet, waiting 2 seconds..."
  sleep 2
done
echo "✓ PostgreSQL is ready!"

# Инициализация БД Airflow (основная причина ошибки!)
echo "Initializing Airflow database..."
airflow db init

# Создание администратора
if [[ -n "${_AIRFLOW_WWW_USER_USERNAME}" && -n "${_AIRFLOW_WWW_USER_PASSWORD}" ]]; then
    echo "Creating admin user '${_AIRFLOW_WWW_USER_USERNAME}'..."
    airflow users create \
        --username "${_AIRFLOW_WWW_USER_USERNAME}" \
        --password "${_AIRFLOW_WWW_USER_PASSWORD}" \
        --firstname "Admin" \
        --lastname "User" \
        --role "Admin" \
        --email "admin@example.com" || echo "User may already exist, skipping..."
fi

# Установка прав на файлы
echo "Setting ownership to ${AIRFLOW_UID}:0..."
chown -R "${AIRFLOW_UID}:0" /opt/airflow/
chown -v -R "${AIRFLOW_UID}:0" /opt/airflow/{logs,dags,plugins,config}

echo "✓ Airflow initialization completed successfully!"
exit 0