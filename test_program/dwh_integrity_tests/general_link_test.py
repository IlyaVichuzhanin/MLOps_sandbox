import psycopg2

# ==============================================================================
# Конфигурация подключения к DWH (Greenplum/PostgreSQL)
# ==============================================================================
DWH_HOST = 'localhost'
DWH_PORT = 7000
DWH_DB = 'dwh'
DWH_USER = 'gpadmin'
DWH_PASSWORD = ''

# ==============================================================================
# Конфиг исключений (Ignored Foreign Keys)
# Сюда добавлены все известные "массовые сироты", которые временно 
# исключаются из проверки. Формат: (link_table, link_column, hub_table, hub_column)
# ==============================================================================
IGNORED_FKS = {
    ("l_object_properties_diagnostic_alarms", "h_object_property_sk", "h_object_properties", "h_object_property_sk"),
    # ("l_io_device_config_nodes_io_device_configs", "h_io_device_config_node_sk", "h_io_device_config_nodes", "h_io_device_config_node_sk"),
    # ("l_model_templates_model_template_tree_nodes", "h_model_template_tree_node_sk", "h_model_template_tree_nodes", "h_model_template_tree_node_sk"),
    ("l_object_property_descriptor_nodes_object_property_descriptors", "h_object_property_descriptor_node_sk", "h_object_property_descriptor_nodes", "h_object_property_descriptor_node_sk"),
    # ("l_pou_user_tree_items_pou_user_defined_items", "h_pou_user_tree_item_sk", "h_pou_user_tree_items", "h_pou_user_tree_item_sk"),
    ("l_object_rules_objects", "h_object_sk", "h_objects", "h_object_sk"),
    ("l_object_property_descriptors_objects", "h_object_sk", "h_objects", "h_object_sk"),
    ("l_object_properties_tik_expert_slices", "h_object_property_sk", "h_object_properties", "h_object_property_sk"),
    ("l_objects_object_type_descriptors", "h_object_sk", "h_objects", "h_object_sk"),
    # ("l_images_object_type_descriptors", "h_image_sk", "h_images", "h_image_sk"),
    ("l_user_defined_tiles_property_configs_objects", "h_object_sk", "h_objects", "h_object_sk"),
    ("l_io_creyt_channel_configs_object_properties", "h_object_property_sk", "h_object_properties", "h_object_property_sk"),
    ("l_io_creyt_channel_states_object_properties", "h_object_property_sk", "h_object_properties", "h_object_property_sk"),
    ("l_io_creyt_channel_configs_io_device_configs", "h_io_device_config_sk", "h_io_device_configs", "h_io_device_config_sk"),
    ("l_io_creyt_channel_states_io_device_configs", "h_io_device_config_sk", "h_io_device_configs", "h_io_device_config_sk"),
    ("l_io_creyt_configs_io_device_configs", "h_io_device_config_sk", "h_io_device_configs", "h_io_device_config_sk"),
    ("l_io_creyt_im_oper_times_object_properties", "h_object_property_sk", "h_object_properties", "h_object_property_sk"),
    ("l_io_creyt_im_oper_times_io_device_configs", "h_io_device_config_sk", "h_io_device_configs", "h_io_device_config_sk"),
    ("l_io_creyt_controller_states_object_properties", "h_object_property_sk", "h_object_properties", "h_object_property_sk"),
    ("l_io_creyt_controller_states_io_device_configs", "h_io_device_config_sk", "h_io_device_configs", "h_io_device_config_sk"),
    ("l_io_creyt_controller_states_oper_time_properties", "h_opertime_properties_sk", "h_object_properties", "h_object_property_sk"),
    ("l_io_lcard_configs_io_device_configs", "h_io_device_config_sk", "h_io_device_configs", "h_io_device_config_sk"),
    ("l_io_modbus_tcp_register_configs_write_property_ids", "h_write_property_id_sk", "h_object_properties", "h_object_property_sk"),
    ("l_io_modbus_tcp_register_configs_object_properties", "h_object_property_sk", "h_object_properties", "h_object_property_sk"),
    ("l_io_modbus_tcp_register_configs_io_device_configs", "h_io_device_config_sk", "h_io_device_configs", "h_io_device_config_sk"),
    # ("l_io_modbus_tcp_reg_configs_io_modbus_tcp_bit_decomn_configs", "h_io_modbus_tcp_register_config_sk", "h_io_modbus_tcp_register_configs", "h_io_modbus_tcp_register_config_sk"),
    # ("l_io_modbus_tcp_reg_configs_io_modbus_tcp_bit_decomn_configs", "h_io_modbus_tcp_bit_decompression_config_sk", "h_io_modbus_tcp_bit_decompression_configs", "h_io_modbus_tcp_bit_decompression_config_sk"),
    ("l_io_modbus_tcp_bit_decompression_configs_write_properties", "h_write_property_id_sk", "h_object_properties", "h_object_property_sk"),
    ("l_io_modbus_tcp_bit_decompression_configs_object_properties", "h_object_property_sk", "h_object_properties", "h_object_property_sk"),
    ("l_io_opc_da_client_item_configs_object_properties", "h_object_property_sk", "h_object_properties", "h_object_property_sk"),
    ("l_io_opc_da_client_item_configs_io_device_configs", "h_io_device_config_sk", "h_io_device_configs", "h_io_device_config_sk"),
    # ("l_io_opc_da_client_item_configs_opc_da_data_types", "h_opc_ua_data_type_sk", "h_opc_ua_data_types", "h_opc_ua_data_type_sk"),
    ("l_io_opc_da_client_configs_io_device_configs", "h_io_device_config_sk", "h_io_device_configs", "h_io_device_config_sk"),
    ("l_io_opc_da_client_group_configs_io_device_configs", "h_io_device_config_sk", "h_io_device_configs", "h_io_device_config_sk"),
    ("l_io_opc_ua_client_transform_item_configs_object_properties", "h_object_property_sk", "h_object_properties", "h_object_property_sk"),
    ("l_io_opc_ua_client_transform_item_configs_io_device_configs", "h_io_device_config_sk", "h_io_device_configs", "h_io_device_config_sk"),
    ("l_io_opc_ua_client_configs_io_device_configs", "h_io_device_config_sk", "h_io_device_configs", "h_io_device_config_sk"),
    ("l_io_opc_ua_client_group_configs_io_device_configs", "h_io_device_config_sk", "h_io_device_configs", "h_io_device_config_sk"),
    ("l_io_opc_ua_client_item_configs_object_properties", "h_object_property_sk", "h_object_properties", "h_object_property_sk"),
    # ("l_io_opc_ua_client_item_configs_opc_ua_data_types", "h_opc_ua_data_type_sk", "h_opc_ua_data_types", "h_opc_ua_data_type_sk"),
}

def check_link_to_hub_integrity():
    """
    Динамически находит все Foreign Keys из Link-таблиц (l_*) в Hub-таблицы (h_*)
    и проверяет, что все значения внешних ключей в Линках существуют в соответствующих Хабах.
    Связи, описанные в IGNORED_FKS, пропускаются.
    """
    conn = None
    ignored_count = 0
    ok_count = 0
    failures = []
    
    print("=" * 80)
    print("🔍 ОТЧЕТ: Проверка ссылочной целостности (Link -> Hub)")
    print("=" * 80)
    print(f"🌐 Подключение: {DWH_USER}@{DWH_HOST}:{DWH_PORT}/{DWH_DB}\n")
    
    try:
        conn = psycopg2.connect(
            host=DWH_HOST, port=DWH_PORT, dbname=DWH_DB,
            user=DWH_USER, password=DWH_PASSWORD
        )
        cur = conn.cursor()
        
        # 1. Динамический поиск всех FK из Link (l_*) в Hub (h_*) через pg_catalog
        print("⏳ Поиск всех Foreign Keys в системных каталогах (pg_catalog)...")
        
        fk_query = """
            SELECT
                tc.relname AS link_table,
                (SELECT attname FROM pg_attribute WHERE attrelid = con.conrelid AND attnum = con.conkey[1]) AS link_column,
                fc.relname AS hub_table,
                (SELECT attname FROM pg_attribute WHERE attrelid = con.confrelid AND attnum = con.confkey[1]) AS hub_column
            FROM pg_constraint con
            JOIN pg_class tc ON tc.oid = con.conrelid
            JOIN pg_namespace tnsp ON tnsp.oid = tc.relnamespace
            JOIN pg_class fc ON fc.oid = con.confrelid
            JOIN pg_namespace fnsp ON fnsp.oid = fc.relnamespace
            WHERE con.contype = 'f'
              AND tnsp.nspname = 'public'
              AND fnsp.nspname = 'public'
              AND tc.relname LIKE 'l!_%' ESCAPE '!'
              AND fc.relname LIKE 'h!_%' ESCAPE '!';
        """
        cur.execute(fk_query)
        fks = cur.fetchall()
        
        if not fks:
            print("⚠️ [WARNING] Не найдено ни одного FK из Link-таблиц в Hub-таблицы.")
            return
            
        print(f"✅ Найдено {len(fks)} внешних ключей (Link -> Hub) для анализа.\n")
        print("-" * 80)
        
        # 2. Проверка каждого FK
        for link_table, link_column, hub_table, hub_column in fks:
            fk_tuple = (link_table, link_column, hub_table, hub_column)
            
            # Проверка на наличие в списке исключений
            if fk_tuple in IGNORED_FKS:
                print(f"⏭️  [IGNORED] {link_table}.{link_column} -> {hub_table}.{hub_column}")
                ignored_count += 1
                continue

            # Используем NOT EXISTS для быстрой проверки с использованием индексов (PK хаба)
            check_query = f"""
                SELECT count(1)
                FROM public.{link_table} l
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM public.{hub_table} h
                    WHERE h.{hub_column} = l.{link_column}
                );
            """
            
            cur.execute(check_query)
            orphan_count = cur.fetchone()[0]
            
            if orphan_count > 0:
                failures.append({
                    'link_table': link_table,
                    'link_column': link_column,
                    'hub_table': hub_table,
                    'hub_column': hub_column,
                    'orphans': orphan_count
                })
                print(f"❌ [FAILED] {link_table}.{link_column} -> {hub_table}.{hub_column} | Сирот: {orphan_count}")
            else:
                print(f"✅ [OK]     {link_table}.{link_column} -> {hub_table}.{hub_column}")
                ok_count += 1
                
        # 3. Итоговый отчет
        print("\n" + "=" * 80)
        print("📊 ИТОГОВАЯ СТАТИСТИКА")
        print("=" * 80)
        print(f"🔗 Всего FK проверено : {len(fks)}")
        print(f"✅ Успешно (OK)       : {ok_count}")
        print(f"⏭️  Исключено (Ignore): {ignored_count}")
        print(f"❌ Ошибок (Failed)    : {len(failures)}")
        print("-" * 80)
        
        if not failures:
            print("🎉 [SUCCESS] Тест пройден!")
            print("   Все непроигнорированные записи в Link-таблицах имеют соответствующие записи в Hub-таблицах.")
        else:
            print(f"⚠️  [FAILED] Обнаружено {len(failures)} НОВЫХ нарушенных ссылочных целостностей.")
            print("\n   Детали ошибок:")
            for f in failures:
                print(f"   - {f['orphans']} записей в {f['link_table']}.{f['link_column']} не найдены в {f['hub_table']}.{f['hub_column']}")
        print("=" * 80)

    except psycopg2.Error as e:
        print(f"\n❌ Ошибка базы данных: {e}")
    finally:
        if conn:
            conn.close()
            print("\n🔌 Соединение с БД закрыто.")

if __name__ == "__main__":
    check_link_to_hub_integrity()