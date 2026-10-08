import psycopg2
import sys

# ==============================================================================
# Конфигурация подключения к DWH (Greenplum/PostgreSQL)
# ==============================================================================
DWH_HOST = 'localhost'
DWH_PORT = 7000
DWH_DB = 'dwh'
DWH_USER = 'gpadmin'
DWH_PASSWORD = ''

# ==============================================================================
# Список Link-таблиц, содержащих h_io_device_config_sk
# ==============================================================================
LINK_TABLES = [
    "l_config_types_io_device_configs",
    "l_io_device_config_nodes_io_device_configs",
    "l_io_creyt_channel_configs_io_device_configs",
    "l_io_creyt_channel_states_io_device_configs",
    "l_io_creyt_configs_io_device_configs",
    "l_io_creyt_im_oper_times_io_device_configs",
    "l_io_creyt_controller_states_io_device_configs",
    "l_io_lcard_channel_configs_io_device_configs",
    "l_io_lcard_crate_configs_io_device_configs",
    "l_io_lcard_configs_io_device_configs",
    "l_io_lcard_sync_crates_io_device_configs",
    "l_io_lcard_input_configs_io_device_configs",
    "l_io_lcard_logic_input_configs_io_device_configs",
    "l_io_lcard_module_configs_io_device_configs",
    "l_io_modbus_tcp_register_configs_io_device_configs",
    "l_io_modbus_tcp_configs_io_device_configs",
    "l_io_opc_da_client_item_configs_io_device_configs",
    "l_io_opc_da_client_configs_io_device_configs",
    "l_io_opc_da_client_group_configs_io_device_configs",
    "l_io_opc_ua_client_transform_item_configs_io_device_configs",
    "l_io_opc_ua_client_configs_io_device_configs",
    "l_io_opc_ua_client_group_configs_io_device_configs",
    "l_io_opc_ua_client_item_configs_io_device_configs"
]

def check_hub_link_coverage():
    """
    Проверяет, что каждый h_io_device_config_sk из таблицы h_io_device_configs
    присутствует хотя бы в одной из Link-таблиц.
    """
    
    # 1. Формируем часть запроса с UNION для всех Link-таблиц
    # Это создает единый набор всех "занятых" ключей
    union_parts = [f"SELECT h_io_device_config_sk FROM public.{table}" for table in LINK_TABLES]
    union_sql = " UNION ".join(union_parts)
    
    # 2. Формируем итоговый запрос с EXCEPT
    # EXCEPT вернет только те строки из левого запроса (Hub), 
    # которых нет в правом запросе (Union Links).
    query = f"""
        SELECT h_io_device_config_sk 
        FROM public.h_io_device_configs
        EXCEPT
        (
            {union_sql}
        );
    """

    conn = None
    try:
        print(f"Подключение к {DWH_HOST}:{DWH_PORT}/{DWH_DB}...")
        conn = psycopg2.connect(
            host=DWH_HOST,
            port=DWH_PORT,
            dbname=DWH_DB,
            user=DWH_USER,
            password=DWH_PASSWORD
        )
        cur = conn.cursor()
        
        print(f"Выполнение проверки целостности (Hub vs {len(LINK_TABLES)} Link tables)...")
        cur.execute(query)
        
        # fetchall() вернет список кортежей с "потерянными" ID
        orphans = cur.fetchall()
        
        if not orphans:
            print("✅ [SUCCESS] Тест пройден.")
            print("   Все записи в public.h_io_device_configs имеют ссылки в Link-таблицах.")
            return True
        else:
            print(f"❌ [FAILED] Обнаружено {len(orphans)} записей в Hub без ссылок в Link-таблицах.")
            print("   Примеры 'сиротских' h_io_device_config_sk:")
            for row in orphans[:10]: # Выводим первые 10 для примера
                print(f"   - {row[0]}")
            if len(orphans) > 10:
                print(f"   ... и еще {len(orphans) - 10} записей.")
            return False

    except psycopg2.Error as e:
        print(f"❌ Ошибка базы данных: {e}")
        return False
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    success = check_hub_link_coverage()
    sys.exit(0 if success else 1)