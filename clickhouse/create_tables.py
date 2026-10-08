import clickhouse_connect
import time

# Подключение к ClickHouse
HOST = '127.0.0.1'
PORT = 8123
DATABASE = 'default'
USERNAME = 'default'
PASSWORD = ''

# Все SQL-запросы для создания таблиц (без кластера и репликации)
SQL_QUERERIES = [
    # ==================== INTEGER DATA ====================
    """CREATE TABLE IF NOT EXISTS hist_integer_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        timestamp DateTime64(6, 'UTC'),
        value Int32,
        load_dttm DateTime64(6, 'UTC')
    )
    ENGINE = MergeTree()
    ORDER BY (data_record_sk, h_object_property_sk)""",
    
    """CREATE TABLE IF NOT EXISTS nonhist_integer_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        timestamp DateTime64(6, 'UTC'),
        value Int32,
        load_dttm DateTime64(6, 'UTC')
    )
    ENGINE = MergeTree()
    ORDER BY (data_record_sk, h_object_property_sk)""",
    
    # ==================== BIGINT DATA ====================
    """CREATE TABLE IF NOT EXISTS hist_bigint_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        load_dttm DateTime64(6, 'UTC'),
        timestamp DateTime64(6, 'UTC'),
        value Int64
    )
    ENGINE = MergeTree()
    ORDER BY (data_record_sk, h_object_property_sk)""",
    
    """CREATE TABLE IF NOT EXISTS nonhist_bigint_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        load_dttm DateTime64(6, 'UTC'),
        timestamp DateTime64(6, 'UTC'),
        value Int64
    )
    ENGINE = MergeTree()
    ORDER BY (data_record_sk, h_object_property_sk)""",
    
    # ==================== DOUBLE DATA ====================
    """CREATE TABLE IF NOT EXISTS hist_double_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        load_dttm DateTime64(6, 'UTC'),
        timestamp DateTime64(6, 'UTC'),
        value Float64
    )
    ENGINE = MergeTree()
    ORDER BY (data_record_sk, h_object_property_sk)""",
    
    """CREATE TABLE IF NOT EXISTS nonhist_double_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        load_dttm DateTime64(6, 'UTC'),
        timestamp DateTime64(6, 'UTC'),
        value Float64
    )
    ENGINE = MergeTree()
    ORDER BY (data_record_sk, h_object_property_sk)""",
    
    # ==================== BOOLEAN DATA ====================
    """CREATE TABLE IF NOT EXISTS hist_boolean_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        load_dttm DateTime64(6, 'UTC'),
        timestamp DateTime64(6, 'UTC'),
        value Bool
    )
    ENGINE = MergeTree()
    ORDER BY (data_record_sk, h_object_property_sk)""",
    
    """CREATE TABLE IF NOT EXISTS nonhist_boolean_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        load_dttm DateTime64(6, 'UTC'),
        timestamp DateTime64(6, 'UTC'),
        value Bool
    )
    ENGINE = MergeTree()
    ORDER BY (data_record_sk, h_object_property_sk)""",
    
    # ==================== TIMESTAMP DATA ====================
    """CREATE TABLE IF NOT EXISTS hist_timestamp_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        load_dttm DateTime64(6, 'UTC'),
        timestamp DateTime64(6, 'UTC'),
        value DateTime64(6, 'UTC')
    )
    ENGINE = MergeTree()
    ORDER BY (data_record_sk, h_object_property_sk)""",
    
    """CREATE TABLE IF NOT EXISTS nonhist_timestamp_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        load_dttm DateTime64(6, 'UTC'),
        timestamp DateTime64(6, 'UTC'),
        value DateTime64(6, 'UTC')
    )
    ENGINE = MergeTree()
    ORDER BY (data_record_sk, h_object_property_sk)""",
    
    # ==================== INTERVAL DATA ====================
    """CREATE TABLE IF NOT EXISTS hist_interval_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        load_dttm DateTime64(6, 'UTC'),
        timestamp DateTime64(6, 'UTC'),
        value String
    )
    ENGINE = MergeTree()
    ORDER BY (data_record_sk, h_object_property_sk)""",
    
    """CREATE TABLE IF NOT EXISTS nonhist_interval_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        load_dttm DateTime64(6, 'UTC'),
        timestamp DateTime64(6, 'UTC'),
        value String
    )
    ENGINE = MergeTree()
    ORDER BY (data_record_sk, h_object_property_sk)""",
    
    # ==================== TEXT DATA ====================
    """CREATE TABLE IF NOT EXISTS hist_text_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        load_dttm DateTime64(6, 'UTC'),
        timestamp DateTime64(6, 'UTC'),
        value String
    )
    ENGINE = MergeTree()
    ORDER BY (data_record_sk, h_object_property_sk)""",
    
    """CREATE TABLE IF NOT EXISTS nonhist_text_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        load_dttm DateTime64(6, 'UTC'),
        timestamp DateTime64(6, 'UTC'),
        value String
    )
    ENGINE = MergeTree()
    ORDER BY (data_record_sk, h_object_property_sk)""",
    
    # ==================== INTEGER ARRAY DATA ====================
    """CREATE TABLE IF NOT EXISTS hist_integer_array_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        load_dttm DateTime64(6, 'UTC'),
        timestamp DateTime64(6, 'UTC'),
        value Array(Int32)
    )
    ENGINE = MergeTree()
    ORDER BY (data_record_sk, h_object_property_sk)""",
    
    """CREATE TABLE IF NOT EXISTS nonhist_integer_array_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        load_dttm DateTime64(6, 'UTC'),
        timestamp DateTime64(6, 'UTC'),
        value Array(Int32)
    )
    ENGINE = MergeTree()
    ORDER BY (data_record_sk, h_object_property_sk)""",
    
    # ==================== TEXT ARRAY DATA ====================
    """CREATE TABLE IF NOT EXISTS hist_text_array_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        load_dttm DateTime64(6, 'UTC'),
        timestamp DateTime64(6, 'UTC'),
        value Array(String)
    )
    ENGINE = MergeTree()
    ORDER BY (data_record_sk, h_object_property_sk)""",
    
    """CREATE TABLE IF NOT EXISTS nonhist_text_array_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        load_dttm DateTime64(6, 'UTC'),
        timestamp DateTime64(6, 'UTC'),
        value Array(String)
    )
    ENGINE = MergeTree()
    ORDER BY (data_record_sk, h_object_property_sk)""",
    
    # ==================== DOUBLE ARRAY DATA ====================
    """CREATE TABLE IF NOT EXISTS hist_double_array_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        load_dttm DateTime64(6, 'UTC'),
        timestamp DateTime64(6, 'UTC'),
        value Array(Float64)
    )
    ENGINE = MergeTree()
    ORDER BY (data_record_sk, h_object_property_sk)""",
    
    """CREATE TABLE IF NOT EXISTS nonhist_double_array_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        load_dttm DateTime64(6, 'UTC'),
        timestamp DateTime64(6, 'UTC'),
        value Array(Float64)
    )
    ENGINE = MergeTree()
    ORDER BY (data_record_sk, h_object_property_sk)""",
    
    # ==================== TIMESTAMP ARRAY DATA ====================
    """CREATE TABLE IF NOT EXISTS hist_timestamp_array_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        load_dttm DateTime64(6, 'UTC'),
        timestamp DateTime64(6, 'UTC'),
        value Array(DateTime64(6, 'UTC'))
    )
    ENGINE = MergeTree()
    ORDER BY (data_record_sk, h_object_property_sk)""",
    
    """CREATE TABLE IF NOT EXISTS nonhist_timestamp_array_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        load_dttm DateTime64(6, 'UTC'),
        timestamp DateTime64(6, 'UTC'),
        value Array(DateTime64(6, 'UTC'))
    )
    ENGINE = MergeTree()
    ORDER BY (data_record_sk, h_object_property_sk)""",
    
    # ==================== TIMESERIES DATA ====================
    """CREATE TABLE IF NOT EXISTS hist_creyt_timeseries_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        timestamp DateTime64(6, 'UTC'),
        value Float64,
        load_dttm DateTime64(6, 'UTC')
    )
    ENGINE = MergeTree()
    ORDER BY (h_object_property_sk, timestamp)""",
    
    """CREATE TABLE IF NOT EXISTS nonhist_creyt_timeseries_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        timestamp DateTime64(6, 'UTC'),
        value Float64,
        load_dttm DateTime64(6, 'UTC')
    )
    ENGINE = MergeTree()
    ORDER BY (h_object_property_sk, timestamp)""",
    
    """CREATE TABLE IF NOT EXISTS hist_lcard_timeseries_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        timestamp DateTime64(6, 'UTC'),
        value Float64,
        load_dttm DateTime64(6, 'UTC')
    )
    ENGINE = MergeTree()
    ORDER BY (h_object_property_sk, timestamp)""",
    
    """CREATE TABLE IF NOT EXISTS nonhist_lcard_timeseries_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        timestamp DateTime64(6, 'UTC'),
        value Float64,
        load_dttm DateTime64(6, 'UTC')
    )
    ENGINE = MergeTree()
    ORDER BY (h_object_property_sk, timestamp)""",
    
    """CREATE TABLE IF NOT EXISTS hist_bode_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        timestamp DateTime64(6, 'UTC'),
        magnitude_value Float64,
        phase_value Float64,
        turnover_frequency_value Float64,
        load_dttm DateTime64(6, 'UTC')
    )
    ENGINE = MergeTree()
    ORDER BY (h_object_property_sk, timestamp)""",
    
    """CREATE TABLE IF NOT EXISTS nonhist_bode_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        timestamp DateTime64(6, 'UTC'),
        magnitude_value Float64,
        phase_value Float64,
        turnover_frequency_value Float64,
        load_dttm DateTime64(6, 'UTC')
    )
    ENGINE = MergeTree()
    ORDER BY (h_object_property_sk, timestamp)""",
    
    """CREATE TABLE IF NOT EXISTS hist_diagnostic_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        timestamp DateTime64(6, 'UTC'),
        diagnostic_data_id UUID,
        h_defect_state_sk UUID,
        priority Int32,
        tag_name String,
        defect_name String,
        defect_details String,
        group_name String,
        recommendations String,
        load_dttm DateTime64(6, 'UTC')
    )
    ENGINE = MergeTree()
    ORDER BY (h_object_property_sk, timestamp)""",
    
    """CREATE TABLE IF NOT EXISTS nonhist_diagnostic_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        timestamp DateTime64(6, 'UTC'),
        diagnostic_data_id UUID,
        h_defect_state_sk UUID,
        priority Int32,
        tag_name String,
        defect_name String,
        defect_details String,
        group_name String,
        recommendations String,
        load_dttm DateTime64(6, 'UTC')
    )
    ENGINE = MergeTree()
    ORDER BY (h_object_property_sk, timestamp)""",
    
    """CREATE TABLE IF NOT EXISTS hist_sample_timeseries_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        timestamp DateTime64(6, 'UTC'),
        value Float64,
        load_dttm DateTime64(6, 'UTC')
    )
    ENGINE = MergeTree()
    ORDER BY (h_object_property_sk, timestamp)""",
    
    """CREATE TABLE IF NOT EXISTS nonhist_sample_timeseries_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        timestamp DateTime64(6, 'UTC'),
        value Float64,
        load_dttm DateTime64(6, 'UTC')
    )
    ENGINE = MergeTree()
    ORDER BY (h_object_property_sk, timestamp)""",
    
    """CREATE TABLE IF NOT EXISTS hist_siemens_sample_timeseries_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        timestamp DateTime64(6, 'UTC'),
        value Float64,
        load_dttm DateTime64(6, 'UTC')
    )
    ENGINE = MergeTree()
    ORDER BY (h_object_property_sk, timestamp)""",
    
    """CREATE TABLE IF NOT EXISTS nonhist_siemens_sample_timeseries_data
    (
        data_record_sk UUID,
        h_object_property_sk UUID,
        data_source_id UUID,
        data_file_id UUID,
        main_db_id UUID,
        timestamp DateTime64(6, 'UTC'),
        value Float64,
        load_dttm DateTime64(6, 'UTC')
    )
    ENGINE = MergeTree()
    ORDER BY (h_object_property_sk, timestamp)""",
]


def main():
    print(f"Подключение к ClickHouse на {HOST}:{PORT}...")
    
    try:
        client = clickhouse_connect.get_client(
            host=HOST,
            port=PORT,
            database=DATABASE,
            username=USERNAME,
            password=PASSWORD,
            send_receive_timeout=300
        )
        print("✓ Подключение успешно\n")
    except Exception as e:
        print(f"✗ Ошибка подключения: {e}")
        return
    
    total = len(SQL_QUERERIES)
    success = 0
    errors = []
    
    print(f"Начинаю создание {total} таблиц...\n")
    print("="*60)
    
    for i, sql in enumerate(SQL_QUERERIES, 1):
        try:
            if 'CREATE TABLE IF NOT EXISTS' in sql:
                table_name = sql.split('CREATE TABLE IF NOT EXISTS')[1].split()[0]
            else:
                table_name = 'unknown'
        except:
            table_name = f'table_{i}'
        
        print(f"[{i:3d}/{total}] {table_name:<50s}", end=" ")
        
        try:
            client.command(sql)
            print("✓")
            success += 1
            time.sleep(0.1)
        except Exception as e:
            print(f"✗")
            errors.append((table_name, str(e)))
    
    print("="*60)
    print(f"\n{'='*60}")
    print(f"РЕЗУЛЬТАТ: {success}/{total} таблиц создано успешно")
    
    if errors:
        print(f"\nОШИБКИ ({len(errors)}):")
        print("-"*60)
        for table, error in errors:
            print(f"  ✗ {table}")
            print(f"    {error[:150]}...")
    
    # Проверка созданных таблиц
    print(f"\n{'='*60}")
    print("Проверка созданных таблиц в базе данных...")
    print("-"*60)
    
    try:
        result = client.query("""
            SELECT name, engine 
            FROM system.tables 
            WHERE database='default' 
            AND (name LIKE '%hist_%' OR name LIKE '%nonhist_%')
            ORDER BY name
        """)
        tables = result.result_rows
        
        if tables:
            print(f"Всего найдено таблиц: {len(tables)}\n")
            
            hist_tables = [t for t in tables if t[0].startswith('hist_')]
            nonhist_tables = [t for t in tables if t[0].startswith('nonhist_')]
            
            print(f"HIST таблицы: {len(hist_tables)}")
            for table in sorted(hist_tables):
                print(f"  - {table[0]} ({table[1]})")
            
            print(f"\nNONHIST таблицы: {len(nonhist_tables)}")
            for table in sorted(nonhist_tables):
                print(f"  - {table[0]} ({table[1]})")
        else:
            print("Таблицы не найдены!")
            
    except Exception as e:
        print(f"Ошибка проверки: {e}")
    
    print(f"\n{'='*60}")
    print("✅ Скрипт завершён!")
    print("="*60)


if __name__ == "__main__":
    main()