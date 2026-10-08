import clickhouse_connect

# Все 4 ноды ClickHouse (HTTP-порты)
ports = [9123, 9124, 9125, 9126]

# Все таблицы (и распределённые, и локальные) - hist и nonhist версии
tables = [
    # ==================== HIST ТАБЛИЦЫ ====================
    # Integer
    "hist_integer_data",
    "hist_integer_data_local",
    # Bigint
    "hist_bigint_data",
    "hist_bigint_data_local",
    # Double
    "hist_double_data",
    "hist_double_data_local",
    # Boolean
    "hist_boolean_data",
    "hist_boolean_data_local",
    # Timestamp
    "hist_timestamp_data",
    "hist_timestamp_data_local",
    # Interval
    "hist_interval_data",
    "hist_interval_data_local",
    # Text
    "hist_text_data",
    "hist_text_data_local",
    # Integer Array
    "hist_integer_array_data",
    "hist_integer_array_data_local",
    # Text Array
    "hist_text_array_data",
    "hist_text_array_data_local",
    # Double Array
    "hist_double_array_data",
    "hist_double_array_data_local",
    # Timestamp Array
    "hist_timestamp_array_data",
    "hist_timestamp_array_data_local",
    # Creyt Timeseries
    "hist_creyt_timeseries_data",
    "hist_creyt_timeseries_data_local",
    # Lcard Timeseries
    "hist_lcard_timeseries_data",
    "hist_lcard_timeseries_data_local",
    # Sample Timeseries
    "hist_sample_timeseries_data",
    "hist_sample_timeseries_data_local",
    # Siemens Sample Timeseries
    "hist_siemens_sample_timeseries_data",
    "hist_siemens_sample_timeseries_data_local",
    
    # ==================== NONHIST ТАБЛИЦЫ ====================
    # Integer
    "nonhist_integer_data",
    "nonhist_integer_data_local",
    # Bigint
    "nonhist_bigint_data",
    "nonhist_bigint_data_local",
    # Double
    "nonhist_double_data",
    "nonhist_double_data_local",
    # Boolean
    "nonhist_boolean_data",
    "nonhist_boolean_data_local",
    # Timestamp
    "nonhist_timestamp_data",
    "nonhist_timestamp_data_local",
    # Interval
    "nonhist_interval_data",
    "nonhist_interval_data_local",
    # Text
    "nonhist_text_data",
    "nonhist_text_data_local",
    # Integer Array
    "nonhist_integer_array_data",
    "nonhist_integer_array_data_local",
    # Text Array
    "nonhist_text_array_data",
    "nonhist_text_array_data_local",
    # Double Array
    "nonhist_double_array_data",
    "nonhist_double_array_data_local",
    # Timestamp Array
    "nonhist_timestamp_array_data",
    "nonhist_timestamp_array_data_local",
    # Creyt Timeseries
    "nonhist_creyt_timeseries_data",
    "nonhist_creyt_timeseries_data_local",
    # Lcard Timeseries
    "nonhist_lcard_timeseries_data",
    "nonhist_lcard_timeseries_data_local",
    # Sample Timeseries
    "nonhist_sample_timeseries_data",
    "nonhist_sample_timeseries_data_local",
    # Siemens Sample Timeseries
    "nonhist_siemens_sample_timeseries_data",
    "nonhist_siemens_sample_timeseries_data_local",
    # Bode
    "nonhist_bode_data",
    "nonhist_bode_data_local",
    "hist_bode_data",
    "hist_bode_data_local",
    # Diagnostic data
    "hist_diagnostic_data_local"
    "hist_diagnostic_data"
    "nonhist_diagnostic_data_local"
    "nonhist_diagnostic_data"
]

total_tables = len(tables)
total_ports = len(ports)
print(f"Всего таблиц для удаления: {total_tables}")
print(f"Количество нод: {total_ports}")
print(f"Итого операций: {total_tables * total_ports}")
print("="*60)

for port in ports:
    print(f"\n{'='*60}")
    print(f"Подключение к localhost:{port}")
    print(f"{'='*60}")
    try:
        client = clickhouse_connect.get_client(host='localhost', port=port, database='default')
        
        success_count = 0
        error_count = 0
        
        for i, table in enumerate(tables, 1):
            sql = f"DROP TABLE IF EXISTS default.{table}"
            print(f"[{i:3d}/{total_tables}] Удаление {table:<50s}", end=" ")
            try:
                client.command(sql)
                print("✓ OK")
                success_count += 1
            except Exception as e:
                print(f"✗ Ошибка: {e}")
                error_count += 1
        
        print(f"\n→ Порт {port}: успешно {success_count}, ошибок {error_count}")
        
    except Exception as e:
        print(f"✗ Не удалось подключиться: {e}")

print("\n" + "="*60)
print("✅ Готово!")
print("="*60)