#!/bin/bash
set -e

echo "=== Starting Cloudberry Demo Cluster ==="

# Инициализация окружения
source /usr/local/cloudberry-db/greenplum_path.sh

DEMO_DIR="/home/gpadmin/cloudberry/gpAux/gpdemo"
ENV_FILE="$DEMO_DIR/gpdemo-env.sh"

# Создаём демо-кластер, если его нет
if [ ! -f "$ENV_FILE" ]; then
    echo "Demo cluster not found. Creating..."
    cd /home/gpadmin/cloudberry
    
    # Убедимся, что директория gpdemo принадлежит gpadmin
    chown -R gpadmin:gpadmin "$DEMO_DIR"
    
    # Создаём кластер
    make create-demo-cluster
fi

# Загружаем окружение кластера
source "$ENV_FILE"

# УБЕЖДАЕМСЯ, ЧТО ДИРЕКТОРИЯ ЛОГОВ СУЩЕСТВУЕТ
LOG_DIR="$MASTER_DATA_DIRECTORY/pg_log"
mkdir -p "$LOG_DIR"

# Проверяем, не запущен ли уже сервер
if pg_ctl status -D "$MASTER_DATA_DIRECTORY" > /dev/null 2>&1; then
    echo "Cloudberry is already running."
else
    # Очищаем возможные остатки от предыдущего запуска
    echo "Cleaning up any stale processes or lock files..."
    rm -f "$MASTER_DATA_DIRECTORY/postmaster.pid" "$MASTER_DATA_DIRECTORY/pg_internal.init"
    
    # Запускаем кластер
    echo "Starting Cloudberry cluster..."
    pg_ctl start -D "$MASTER_DATA_DIRECTORY" -l "$LOG_DIR/startup.log"
fi

# Ждём готовности
until psql -d template1 -c "SELECT 1;" >/dev/null 2>&1; do
    echo "Waiting for Cloudberry to be ready..."
    sleep 2
done

echo "✅ Cloudberry is ready on port 7000."
tail -f "$LOG_DIR/startup.log"

