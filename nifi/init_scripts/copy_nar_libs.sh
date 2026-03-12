#!/bin/bash
set -e

echo "→ Копирование пользовательских NAR-библиотек в lib..."
cp -rn /opt/nifi/nar_libs/. /opt/nifi/nifi-current/lib/ 2>/dev/null || true
echo "→ Готово. Добавлено $(ls -1 /opt/nifi/nar_libs/ 2>/dev/null | wc -l) файлов."