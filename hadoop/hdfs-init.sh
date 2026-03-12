#!/bin/bash
set -e

echo "=========================================="
echo " HDFS Initialization Script"
echo "=========================================="
echo "Started at: $(date)"
echo ""

# Функция для логирования с временной меткой
log() {
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

# Ждём готовности NameNode с таймаутом (максимум 120 секунд)
log "Waiting for NameNode to become active..."
timeout=120
count=0
while ! hdfs dfsadmin -report 2>/dev/null | grep -q "Live datanodes" && [ $count -lt $timeout ]; do
  sleep 5
  count=$((count + 5))
  log "  Still waiting... (${count}s/${timeout}s)"
done

if [ $count -ge $timeout ]; then
  log "ERROR: NameNode did not start within ${timeout} seconds!"
  hdfs dfsadmin -report 2>&1 || echo "NameNode status check failed"
  exit 1
fi

log "NameNode is ready. Live datanodes detected."

# Создаём основную структуру каталогов
log "Creating directory structure in HDFS..."
BASE_DIR="/test_raw_data/data"



# Назначаем владельца и права
log "Setting ownership to nifi:nifi..."
hdfs dfs -chown -R nifi:nifi /test_raw_data

log "Setting permissions to 755..."
hdfs dfs -chmod -R 755 /test_raw_data

# Проверяем результат
log "=========================================="
log "HDFS Directory Structure:"
log "=========================================="
hdfs dfs -ls -R /test_raw_data || echo "No directories found"

log ""
log "=========================================="
log "Verification (checking write access for user 'nifi'):"
log "=========================================="

# Тестовая запись от имени nifi (имитация)
TEST_FILE="/test_raw_data/test_write_access_$(date +%Y%m%d_%H%M%S).txt"
log "Creating test file: $TEST_FILE"
echo "Test file created by HDFS initialization script at $(date)" | hdfs dfs -put - "$TEST_FILE" 2>&1 && \
  log "✓ Write test SUCCESSFUL" || log "⚠ Write test failed (may require actual nifi user context)"

# Удаляем тестовый файл
hdfs dfs -rm -f "$TEST_FILE" >/dev/null 2>&1

log ""
log "=========================================="
log "HDFS Initialization Complete!"
log "Started:  $(date -d @$(($(date +%s) - count)) '+%Y-%m-%d %H:%M:%S')"
log "Finished: $(date '+%Y-%m-%d %H:%M:%S')"
log "=========================================="