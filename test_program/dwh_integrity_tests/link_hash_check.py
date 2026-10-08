import logging
import socket
from typing import Dict, List, Tuple, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# =============================================================================
# КОНФИГУРАЦИЯ ПОДКЛЮЧЕНИЯ К DWH
# =============================================================================

DWH_HOST = 'localhost'
DWH_PORT = 7000
DWH_DB = 'dwh'
DWH_USER = 'gpadmin'
DWH_PASSWORD = ''

# =============================================================================
# КОНФИГ: ТАБЛИЦЫ ЛИНКОВ И ИХ СВЯЗИ С ХАБАМИ
# =============================================================================
# Формат:
# 'link_table_name': {
#     'fk_columns': [
#         {'column': 'fk_column_name', 'hub_table': 'h_hub_table_name', 'hub_pk': 'hub_pk_column'},
#         ...
#     ]
# }

LINK_TO_HUB_MAPPING = {
    # ==================== АГРЕГАТЫ И УВЕДОМЛЕНИЯ ====================
    'l_aggregate_notification_configs_aggregates': {
        'fk_columns': [
            {'column': 'h_aggregate_config_sk', 'hub_table': 'h_aggregate_configs', 'hub_pk': 'h_aggregate_config_sk'},
            {'column': 'h_aggregate_notification_config_sk', 'hub_table': 'h_aggregate_notification_configs', 'hub_pk': 'h_aggregate_notification_config_sk'},
        ]
    },
    'l_aggregate_notification_configs_email_addresses': {
        'fk_columns': [
            {'column': 'h_aggregate_notification_config_sk', 'hub_table': 'h_aggregate_notification_configs', 'hub_pk': 'h_aggregate_notification_config_sk'},
            {'column': 'h_email_address_sk', 'hub_table': 'h_email_addresses', 'hub_pk': 'h_email_address_sk'},
        ]
    },
    'l_diagnostic_defect_states_aggregate_notification_configs': {
        'fk_columns': [
            {'column': 'h_aggregate_notification_config_sk', 'hub_table': 'h_aggregate_notification_configs', 'hub_pk': 'h_aggregate_notification_config_sk'},
            {'column': 'h_diagnostic_defect_state_sk', 'hub_table': 'h_diagnostic_defect_states', 'hub_pk': 'h_diagnostic_defect_state_sk'},
        ]
    },
    
    # ==================== ДИАГНОСТИКА ====================
    'l_diagnostic_defect_types_diagnostic_data_records': {
        'fk_columns': [
            {'column': 'h_diagnostic_data_record_sk', 'hub_table': 'h_diagnostic_data_records', 'hub_pk': 'h_diagnostic_data_record_sk'},
            {'column': 'h_diagnostic_defect_type_sk', 'hub_table': 'h_diagnostic_defect_types', 'hub_pk': 'h_diagnostic_defect_type_sk'},
        ]
    },
    'l_diagnostic_defect_states_diagnostic_data_records': {
        'fk_columns': [
            {'column': 'h_diagnostic_data_record_sk', 'hub_table': 'h_diagnostic_data_records', 'hub_pk': 'h_diagnostic_data_record_sk'},
            {'column': 'h_diagnostic_defect_state_sk', 'hub_table': 'h_diagnostic_defect_states', 'hub_pk': 'h_diagnostic_defect_state_sk'},
        ]
    },
    'l_diagnostic_data_records_diagnostic_alarms': {
        'fk_columns': [
            {'column': 'h_diagnostic_alarm_sk', 'hub_table': 'h_diagnostic_alarms', 'hub_pk': 'h_diagnostic_alarm_sk'},
            {'column': 'h_diagnostic_data_record_sk', 'hub_table': 'h_diagnostic_data_records', 'hub_pk': 'h_diagnostic_data_record_sk'},
        ]
    },
    'l_diagnostic_alarm_states_diagnostic_alarms': {
        'fk_columns': [
            {'column': 'h_diagnostic_alarm_sk', 'hub_table': 'h_diagnostic_alarms', 'hub_pk': 'h_diagnostic_alarm_sk'},
            {'column': 'h_diagnostic_alarm_state_sk', 'hub_table': 'h_diagnostic_alarm_states', 'hub_pk': 'h_diagnostic_alarm_state_sk'},
        ]
    },
    
    # ==================== ОБЪЕКТЫ И СВОЙСТВА ====================
    'l_object_properties_diagnostic_alarms': {
        'fk_columns': [
            {'column': 'h_diagnostic_alarm_sk', 'hub_table': 'h_diagnostic_alarms', 'hub_pk': 'h_diagnostic_alarm_sk'},
            {'column': 'h_object_property_sk', 'hub_table': 'h_object_properties', 'hub_pk': 'h_object_property_sk'},
        ]
    },
    'l_object_properties_storage_types': {
        'fk_columns': [
            {'column': 'h_object_property_sk', 'hub_table': 'h_object_properties', 'hub_pk': 'h_object_property_sk'},
            {'column': 'h_storage_type_sk', 'hub_table': 'h_storage_types', 'hub_pk': 'h_storage_type_sk'},
        ]
    },
    'l_object_properties_object_property_descriptors': {
        'fk_columns': [
            {'column': 'h_object_property_descriptor_sk', 'hub_table': 'h_object_property_descriptors', 'hub_pk': 'h_object_property_descriptor_sk'},
            {'column': 'h_object_property_sk', 'hub_table': 'h_object_properties', 'hub_pk': 'h_object_property_sk'},
        ]
    },
    'l_object_property_descriptor_nodes_object_property_descriptors': {
        'fk_columns': [
            {'column': 'h_object_property_descriptor_sk', 'hub_table': 'h_object_property_descriptors', 'hub_pk': 'h_object_property_descriptor_sk'},
            {'column': 'h_object_property_descriptor_node_sk', 'hub_table': 'h_object_property_descriptor_nodes', 'hub_pk': 'h_object_property_descriptor_node_sk'},
        ]
    },
    'l_object_property_descriptors_objects': {
        'fk_columns': [
            {'column': 'h_object_property_sk', 'hub_table': 'h_object_properties', 'hub_pk': 'h_object_property_sk'},
            {'column': 'h_object_sk', 'hub_table': 'h_objects', 'hub_pk': 'h_object_sk'},
        ]
    },
    'l_object_group_descriptors_objects': {
        'fk_columns': [
            {'column': 'h_object_group_descriptor_sk', 'hub_table': 'h_object_group_descriptors', 'hub_pk': 'h_object_group_descriptor_sk'},
            {'column': 'h_object_group_sk', 'hub_table': 'h_object_groups', 'hub_pk': 'h_object_group_sk'},
        ]
    },
    'l_objects_object_type_descriptors': {
        'fk_columns': [
            {'column': 'h_object_sk', 'hub_table': 'h_objects', 'hub_pk': 'h_object_sk'},
            {'column': 'h_object_type_descriptor_sk', 'hub_table': 'h_object_type_descriptors', 'hub_pk': 'h_object_type_descriptor_sk'},
        ]
    },
    'l_images_object_type_descriptors': {
        'fk_columns': [
            {'column': 'h_image_sk', 'hub_table': 'h_images', 'hub_pk': 'h_image_sk'},
            {'column': 'h_object_type_descriptor_sk', 'hub_table': 'h_object_type_descriptors', 'hub_pk': 'h_object_type_descriptor_sk'},
        ]
    },
    
    # ==================== ПРАВИЛА ОБЪЕКТОВ ====================
    'l_object_rules_running_types': {
        'fk_columns': [
            {'column': 'h_object_rule_sk', 'hub_table': 'h_object_rules', 'hub_pk': 'h_object_rule_sk'},
            {'column': 'h_running_type_sk', 'hub_table': 'h_running_types', 'hub_pk': 'h_running_type_sk'},
        ]
    },
    'l_object_rules_objects': {
        'fk_columns': [
            {'column': 'h_object_rule_sk', 'hub_table': 'h_object_rules', 'hub_pk': 'h_object_rule_sk'},
            {'column': 'h_object_sk', 'hub_table': 'h_objects', 'hub_pk': 'h_object_sk'},
        ]
    },
    'l_object_rules_pou_user_defined_items': {
        'fk_columns': [
            {'column': 'h_object_rule_sk', 'hub_table': 'h_object_rules', 'hub_pk': 'h_object_rule_sk'},
            {'column': 'h_pou_user_defined_item_sk', 'hub_table': 'h_pou_user_defined_items', 'hub_pk': 'h_pou_user_defined_item_sk'},
        ]
    },
    'l_object_rules_object_properties': {
        'fk_columns': [
            {'column': 'h_object_rule_sk', 'hub_table': 'h_object_rules', 'hub_pk': 'h_object_rule_sk'},
            {'column': 'h_object_property_sk', 'hub_table': 'h_object_properties', 'hub_pk': 'h_object_property_sk'},
        ]
    },
    
    # ==================== IO DEVICE CONFIGS ====================
    'l_config_types_io_device_configs': {
        'fk_columns': [
            {'column': 'h_io_device_config_sk', 'hub_table': 'h_io_device_configs', 'hub_pk': 'h_io_device_config_sk'},
            {'column': 'h_config_type_sk', 'hub_table': 'h_config_types', 'hub_pk': 'h_config_type_sk'},
        ]
    },
    'l_io_device_config_nodes_io_device_configs': {
        'fk_columns': [
            {'column': 'h_io_device_config_sk', 'hub_table': 'h_io_device_configs', 'hub_pk': 'h_io_device_config_sk'},
            {'column': 'h_io_device_config_node_sk', 'hub_table': 'h_io_device_config_nodes', 'hub_pk': 'h_io_device_config_node_sk'},
        ]
    },
    
    # ==================== ЕДИНИЦЫ ИЗМЕРЕНИЯ ====================
    'l_convert_types_measure_converts': {
        'fk_columns': [
            {'column': 'h_convert_type_sk', 'hub_table': 'h_convert_types', 'hub_pk': 'h_convert_type_sk'},
            {'column': 'h_measure_convert_sk', 'hub_table': 'h_measure_converts', 'hub_pk': 'h_measure_convert_sk'},
        ]
    },
    'l_measure_convert_to_ids': {
        'fk_columns': [
            {'column': 'h_measure_unit_sk', 'hub_table': 'h_measure_units', 'hub_pk': 'h_measure_unit_sk'},
            {'column': 'h_measure_convert_sk', 'hub_table': 'h_measure_converts', 'hub_pk': 'h_measure_convert_sk'},
        ]
    },
    'l_measure_convert_from_ids': {
        'fk_columns': [
            {'column': 'h_measure_unit_sk', 'hub_table': 'h_measure_units', 'hub_pk': 'h_measure_unit_sk'},
            {'column': 'h_measure_convert_sk', 'hub_table': 'h_measure_converts', 'hub_pk': 'h_measure_convert_sk'},
        ]
    },
    'l_measure_units_measure_groups': {
        'fk_columns': [
            {'column': 'h_measure_unit_sk', 'hub_table': 'h_measure_units', 'hub_pk': 'h_measure_unit_sk'},
            {'column': 'h_measure_group_sk', 'hub_table': 'h_measure_groups', 'hub_pk': 'h_measure_group_sk'},
        ]
    },
    'l_measure_units_object_properties': {
        'fk_columns': [
            {'column': 'h_measure_unit_sk', 'hub_table': 'h_measure_units', 'hub_pk': 'h_measure_unit_sk'},
            {'column': 'h_object_property_sk', 'hub_table': 'h_object_properties', 'hub_pk': 'h_object_property_sk'},
        ]
    },
    
    # ==================== МОДЕЛИ И ШАБЛОНЫ ====================
    'l_model_templates_model_template_tree_nodes': {
        'fk_columns': [
            {'column': 'h_model_template_sk', 'hub_table': 'h_model_templates', 'hub_pk': 'h_model_template_sk'},
            {'column': 'h_model_template_tree_node_sk', 'hub_table': 'h_model_template_tree_nodes', 'hub_pk': 'h_model_template_tree_node_sk'},
        ]
    },
    
    # ==================== POU (Program Organization Units) ====================
    'l_pou_user_tree_items_pou_user_defined_items': {
        'fk_columns': [
            {'column': 'h_pou_user_tree_item_sk', 'hub_table': 'h_pou_user_tree_items', 'hub_pk': 'h_pou_user_tree_item_sk'},
            {'column': 'h_pou_user_defined_item_sk', 'hub_table': 'h_pou_user_defined_items', 'hub_pk': 'h_pou_user_defined_item_sk'},
        ]
    },
    
    # ==================== ЛОГИ И ДЕЙСТВИЯ ====================
    'l_user_logs_user_action_types': {
        'fk_columns': [
            {'column': 'h_user_log_sk', 'hub_table': 'h_user_logs', 'hub_pk': 'h_user_log_sk'},
            {'column': 'h_user_action_type_sk', 'hub_table': 'h_user_action_types', 'hub_pk': 'h_user_action_type_sk'},
        ]
    },
    'l_tik_scada_logs_tik_scada_log_types': {
        'fk_columns': [
            {'column': 'h_tik_scada_log_sk', 'hub_table': 'h_tik_scada_logs', 'hub_pk': 'h_tik_scada_log_sk'},
            {'column': 'h_tik_scada_log_type_sk', 'hub_table': 'h_tik_scada_log_types', 'hub_pk': 'h_tik_scada_log_type_sk'},
        ]
    },
    'l_object_properties_tik_expert_slices': {
        'fk_columns': [
            {'column': 'h_object_property_sk', 'hub_table': 'h_object_properties', 'hub_pk': 'h_object_property_sk'},
            {'column': 'h_tik_expert_slice_sk', 'hub_table': 'h_tik_expert_slices', 'hub_pk': 'h_tik_expert_slice_sk'},
        ]
    },
    
    # ==================== USER DEFINED ====================
    'l_user_defined_prop_lists_user_defined_prop_list_types': {
        'fk_columns': [
            {'column': 'h_user_defined_property_list_sk', 'hub_table': 'h_user_defined_property_lists', 'hub_pk': 'h_user_defined_property_list_sk'},
            {'column': 'h_user_defined_property_list_type_sk', 'hub_table': 'h_user_defined_property_lists_types', 'hub_pk': 'h_user_defined_property_list_type_sk'},
        ]
    },
    'l_user_defined_property_lists_objects': {
        'fk_columns': [
            {'column': 'h_user_defined_property_list_sk', 'hub_table': 'h_user_defined_property_lists', 'hub_pk': 'h_user_defined_property_list_sk'},
            {'column': 'h_object_sk', 'hub_table': 'h_objects', 'hub_pk': 'h_object_sk'},
        ]
    },

    
    # ==================== PROPERTY TYPES ====================
    'l_property_types_object_property_descriptors': {
        'fk_columns': [
            {'column': 'h_property_types_sk', 'hub_table': 'h_property_types', 'hub_pk': 'h_property_type_sk'},
            {'column': 'h_object_property_descriptor_sk', 'hub_table': 'h_object_property_descriptors', 'hub_pk': 'h_object_property_descriptor_sk'},
        ]
    },
    
    # ==================== IO CREYT ====================
    'l_io_creyt_channel_configs_measure_units': {
        'fk_columns': [
            {'column': 'h_io_creyt_channel_config_sk', 'hub_table': 'h_io_creyt_channel_configs', 'hub_pk': 'h_io_creyt_channel_config_sk'},
            {'column': 'h_measure_unit_sk', 'hub_table': 'h_measure_units', 'hub_pk': 'h_measure_unit_sk'},
        ]
    },






    
    # ==================== IO LCARD ====================
    'l_io_lcard_channel_configs_measure_units': {
        'fk_columns': [
            {'column': 'h_io_lcard_channel_config_sk', 'hub_table': 'h_io_lcard_channel_configs', 'hub_pk': 'h_io_lcard_channel_config_sk'},
            {'column': 'h_measure_unit_sk', 'hub_table': 'h_measure_units', 'hub_pk': 'h_measure_unit_sk'},
        ]
    },



    'l_io_lcard_crate_configs_crate_types': {
        'fk_columns': [
            {'column': 'h_io_lcard_crate_config_sk', 'hub_table': 'h_io_lcard_crate_configs', 'hub_pk': 'h_io_lcard_crate_config_sk'},
            {'column': 'h_crate_type_sk', 'hub_table': 'h_crate_types', 'hub_pk': 'h_crate_type_sk'},
        ]
    },


    'l_io_lcard_input_configs_measure_units': {
        'fk_columns': [
            {'column': 'h_io_lcard_input_config_sk', 'hub_table': 'h_io_lcard_input_configs', 'hub_pk': 'h_io_lcard_input_config_sk'},
            {'column': 'h_measure_unit_sk', 'hub_table': 'h_measure_units', 'hub_pk': 'h_measure_unit_sk'},
        ]
    },




    'l_io_lcard_logic_input_configs_l_card_logic_input_types': {
        'fk_columns': [
            {'column': 'h_io_lcard_logic_input_config_sk', 'hub_table': 'h_io_lcard_logic_input_configs', 'hub_pk': 'h_io_lcard_logic_input_config_sk'},
            {'column': 'h_l_card_logic_input_type_sk', 'hub_table': 'h_l_card_logic_input_types', 'hub_pk': 'h_l_card_logic_input_type_sk'},
        ]
    },

    'l_io_lcard_module_configs_l_card_crate_module_types': {
        'fk_columns': [
            {'column': 'h_io_lcard_module_config_sk', 'hub_table': 'h_io_lcard_module_configs', 'hub_pk': 'h_io_lcard_module_config_sk'},
            {'column': 'h_l_card_crate_module_type_sk', 'hub_table': 'h_l_card_crate_module_types', 'hub_pk': 'h_l_card_crate_module_type_sk'},
        ]
    },
    


    'l_io_modbus_tcp_reg_configs_io_modbus_tcp_bit_decomn_configs': {
        'fk_columns': [
            {'column': 'h_io_modbus_tcp_register_config_sk', 'hub_table': 'h_io_modbus_tcp_register_configs', 'hub_pk': 'h_io_modbus_tcp_register_config_sk'},
            {'column': 'h_io_modbus_tcp_bit_decompression_config_sk', 'hub_table': 'h_io_modbus_tcp_bit_decompression_configs', 'hub_pk': 'h_io_modbus_tcp_bit_decompression_config_sk'},
        ]
    },
    'l_io_modbus_tcp_register_configs_measure_units': {
        'fk_columns': [
            {'column': 'h_io_modbus_tcp_register_config_sk', 'hub_table': 'h_io_modbus_tcp_register_configs', 'hub_pk': 'h_io_modbus_tcp_register_config_sk'},
            {'column': 'h_measure_unit_sk', 'hub_table': 'h_measure_units', 'hub_pk': 'h_measure_unit_sk'},
        ]
    },

    'l_io_modbus_tcp_bit_decompression_configs_io_modbus_tcp_register_configs': {
        'fk_columns': [
            {'column': 'h_io_modbus_tcp_bit_decompression_config_sk', 'hub_table': 'h_io_modbus_tcp_bit_decompression_configs', 'hub_pk': 'h_io_modbus_tcp_bit_decompression_config_sk'},
            {'column': 'h_io_modbus_tcp_register_config_sk', 'hub_table': 'h_io_modbus_tcp_register_configs', 'hub_pk': 'h_io_modbus_tcp_register_config_sk'},
        ]
    },
    'l_io_modbus_tcp_register_configs_register_types': {
        'fk_columns': [
            {'column': 'h_io_modbus_tcp_register_config_sk', 'hub_table': 'h_io_modbus_tcp_register_configs', 'hub_pk': 'h_io_modbus_tcp_register_config_sk'},
            {'column': 'h_register_type_sk', 'hub_table': 'h_register_types', 'hub_pk': 'h_register_type_sk'},
        ]
    },
    'l_io_modbus_tcp_register_configs_data_type_names': {
        'fk_columns': [
            {'column': 'h_io_modbus_tcp_register_config_sk', 'hub_table': 'h_io_modbus_tcp_register_configs', 'hub_pk': 'h_io_modbus_tcp_register_config_sk'},
            {'column': 'h_data_type_name_sk', 'hub_table': 'h_data_type_names', 'hub_pk': 'h_data_type_name_sk'},
        ]
    },

    'l_io_modbus_tcp_configs_device_types': {
        'fk_columns': [
            {'column': 'h_io_modbus_tcp_config_sk', 'hub_table': 'h_io_modbus_tcp_configs', 'hub_pk': 'h_io_modbus_tcp_config_sk'},
            {'column': 'h_device_type_sk', 'hub_table': 'h_device_types', 'hub_pk': 'h_device_type_sk'},
        ]
    },
    'l_io_modbus_tcp_configs_interface_types': {
        'fk_columns': [
            {'column': 'h_io_modbus_tcp_config_sk', 'hub_table': 'h_io_modbus_tcp_configs', 'hub_pk': 'h_io_modbus_tcp_config_sk'},
            {'column': 'h_interface_type_sk', 'hub_table': 'h_interface_types', 'hub_pk': 'h_interface_type_sk'},
        ]
    },
    


    'l_io_opc_da_client_item_configs_opc_da_data_types': {
        'fk_columns': [
            {'column': 'h_io_opc_da_client_item_config_sk', 'hub_table': 'h_io_opc_da_client_item_configs', 'hub_pk': 'h_io_opc_da_client_item_config_sk'},
            {'column': 'h_opc_ua_data_type_sk', 'hub_table': 'h_opc_ua_data_types', 'hub_pk': 'h_opc_ua_data_type_sk'},
        ]
    },



    'l_io_opc_ua_client_item_configs_data_type_servers': {
        'fk_columns': [
            {'column': 'h_io_opc_ua_client_item_config_sk', 'hub_table': 'h_io_opc_ua_client_item_configs', 'hub_pk': 'h_io_opc_ua_client_item_config_sk'},
            {'column': 'h_data_type_server_sk', 'hub_table': 'h_data_type_servers', 'hub_pk': 'h_data_type_server_sk'},
        ]
    },
    'l_io_opc_ua_client_item_configs_opc_ua_data_types': {
        'fk_columns': [
            {'column': 'h_io_opc_ua_client_item_config_sk', 'hub_table': 'h_io_opc_ua_client_item_configs', 'hub_pk': 'h_io_opc_ua_client_item_config_sk'},
            {'column': 'h_opc_ua_data_type_sk', 'hub_table': 'h_opc_ua_data_types', 'hub_pk': 'h_opc_ua_data_type_sk'},
        ]
    },
}

# =============================================================================
# ПРОВЕРКА ДОСТУПНОСТИ ПОРТА
# =============================================================================

def check_port(host: str, port: int, timeout: int = 3) -> bool:
    try:
        sock = socket.create_connection((host, port), timeout)
        sock.close()
        return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False

# =============================================================================
# ПОДКЛЮЧЕНИЕ К DWH
# =============================================================================

def get_dwh_engine() -> Engine:
    db_url = f"postgresql+psycopg2://{DWH_USER}:{DWH_PASSWORD}@{DWH_HOST}:{DWH_PORT}/{DWH_DB}"
    return create_engine(db_url, echo=False, pool_pre_ping=True, pool_size=5, max_overflow=10)

# =============================================================================
# ПРОВЕРКА ССЫЛОЧНОЙ ЦЕЛОСТНОСТИ
# =============================================================================

def check_link_to_hub_integrity(
    dwh_engine: Engine,
    link_table: str,
    fk_column: str,
    hub_table: str,
    hub_pk: str
) -> Dict:
    """
    Проверяет, что все FK из таблицы линка существуют в таблице хаба.
    Возвращает словарь с результатами:
    {
        'link_total': int,        # Всего записей в линке
        'hub_total': int,         # Всего записей в хабе
        'orphans_count': int,     # Количество осиротевших записей (FK нет в хабе)
        'null_fk_count': int,     # Количество записей с NULL FK
        'valid_count': int,       # Количество валидных записей
        'status': str             # 'OK' или 'BROKEN'
    }
    """
    result = {
        'link_total': 0,
        'hub_total': 0,
        'orphans_count': 0,
        'null_fk_count': 0,
        'valid_count': 0,
        'status': 'OK',
        'error': None
    }
    
    try:
        with dwh_engine.begin() as conn:
            # 1. Общее количество записей в линке
            link_count_result = conn.execute(text(f"""
                SELECT COUNT(*) FROM public.{link_table}
            """))
            result['link_total'] = link_count_result.fetchone()[0]
            
            # 2. Общее количество записей в хабе
            hub_count_result = conn.execute(text(f"""
                SELECT COUNT(*) FROM public.{hub_table}
            """))
            result['hub_total'] = hub_count_result.fetchone()[0]
            
            # 3. Количество записей с NULL FK
            null_fk_result = conn.execute(text(f"""
                SELECT COUNT(*) FROM public.{link_table}
                WHERE {fk_column} IS NULL
            """))
            result['null_fk_count'] = null_fk_result.fetchone()[0]
            
            # 4. 🔥 КЛЮЧЕВАЯ ПРОВЕРКА: количество "осиротевших" записей
            # (FK в линке не найден в хабе)
            orphans_result = conn.execute(text(f"""
                SELECT COUNT(*)
                FROM public.{link_table} l
                LEFT JOIN public.{hub_table} h 
                    ON l.{fk_column} = h.{hub_pk}
                WHERE l.{fk_column} IS NOT NULL 
                  AND h.{hub_pk} IS NULL
            """))
            result['orphans_count'] = orphans_result.fetchone()[0]
            
            # 5. Валидные записи
            result['valid_count'] = result['link_total'] - result['orphans_count'] - result['null_fk_count']
            
            # 6. Статус
            if result['orphans_count'] > 0:
                result['status'] = 'BROKEN'
            else:
                result['status'] = 'OK'
                
    except Exception as e:
        result['error'] = str(e)
        result['status'] = 'ERROR'
    
    return result


def check_all_links_integrity(dwh_engine: Engine) -> Dict[str, List[Dict]]:
    """
    Проверяет все линки из конфига.
    Возвращает словарь: {link_table_name: [result_for_fk1, result_for_fk2, ...]}
    """
    all_results = {}
    
    total_checks = sum(len(cfg['fk_columns']) for cfg in LINK_TO_HUB_MAPPING.values())
    current_check = 0
    
    for link_table, config in LINK_TO_HUB_MAPPING.items():
        link_results = []
        
        for fk_info in config['fk_columns']:
            current_check += 1
            
            log.info(f"[{current_check}/{total_checks}] Проверка: {link_table}.{fk_info['column']} → {fk_info['hub_table']}")
            
            result = check_link_to_hub_integrity(
                dwh_engine=dwh_engine,
                link_table=link_table,
                fk_column=fk_info['column'],
                hub_table=fk_info['hub_table'],
                hub_pk=fk_info['hub_pk']
            )
            
            result['fk_column'] = fk_info['column']
            result['hub_table'] = fk_info['hub_table']
            result['hub_pk'] = fk_info['hub_pk']
            
            link_results.append(result)
        
        all_results[link_table] = link_results
    
    return all_results

# =============================================================================
# ФОРМИРОВАНИЕ ОТЧЕТА
# =============================================================================

def print_integrity_report(all_results: Dict[str, List[Dict]]):
    """Выводит подробный отчёт о проверке ссылочной целостности."""
    
    print("\n" + "=" * 160)
    print("ОТЧЕТ О ПРОВЕРКЕ ССЫЛОЧНОЙ ЦЕЛОСТНОСТИ: LINKS → HUBS")
    print("=" * 160)
    
    # Общая статистика
    total_links = len(all_results)
    total_fk_checks = sum(len(results) for results in all_results.values())
    broken_links = 0
    ok_links = 0
    error_links = 0
    total_orphans = 0
    total_link_records = 0
    
    # Собираем сломанные связи для детального отчёта
    broken_details = []
    
    for link_table, results in all_results.items():
        for r in results:
            total_link_records += r['link_total']
            total_orphans += r['orphans_count']
            
            if r['status'] == 'BROKEN':
                broken_links += 1
                broken_details.append({
                    'link_table': link_table,
                    'fk_column': r['fk_column'],
                    'hub_table': r['hub_table'],
                    'hub_pk': r['hub_pk'],
                    'orphans_count': r['orphans_count'],
                    'link_total': r['link_total'],
                    'hub_total': r['hub_total']
                })
            elif r['status'] == 'ERROR':
                error_links += 1
            else:
                ok_links += 1
    
    # Заголовок таблицы
    print(f"\n{'Линк-таблица':<55} {'FK колонка':<45} {'Хаб-таблица':<45} {'В линке':>10} {'В хабе':>10} {'Orphans':>10} {'Статус':>8}")
    print("-" * 160)
    
    # Вывод по каждой связи
    for link_table, results in sorted(all_results.items()):
        for r in results:
            fk_col = r['fk_column']
            hub_table = r['hub_table']
            
            # Обрезаем длинные имена для отображения
            link_display = link_table if len(link_table) <= 55 else link_table[:52] + "..."
            fk_display = fk_col if len(fk_col) <= 45 else fk_col[:42] + "..."
            hub_display = hub_table if len(hub_table) <= 45 else hub_table[:42] + "..."
            
            if r['status'] == 'BROKEN':
                status_marker = "❌ BROKEN"
            elif r['status'] == 'ERROR':
                status_marker = "⚠️ ERROR"
            else:
                status_marker = "✅ OK"
            
            print(f"{link_display:<55} {fk_display:<45} {hub_display:<45} {r['link_total']:>10,} {r['hub_total']:>10,} {r['orphans_count']:>10,} {status_marker:>10}")
    
    print("-" * 160)
    
    # Общая статистика
    print(f"\n📊 ОБЩАЯ СТАТИСТИКА:")
    print(f"   Всего линк-таблиц: {total_links}")
    print(f"   Всего проверенных FK-связей: {total_fk_checks}")
    print(f"   Всего записей во всех линках: {total_link_records:,}")
    print(f"\n   ✅ Целостных связей: {ok_links}")
    print(f"   ❌ Нарушенных связей: {broken_links}")
    print(f"   ⚠️ Ошибок при проверке: {error_links}")
    print(f"\n   🔴 Всего осиротевших записей (orphans): {total_orphans:,}")
    
    if total_fk_checks > 0:
        integrity_pct = 100 * ok_links / total_fk_checks
        print(f"   📈 Процент целостности: {integrity_pct:.2f}%")
    
    # Детальный отчёт по нарушенным связям
    if broken_details:
        print(f"\n" + "=" * 160)
        print(f"❌ ДЕТАЛЬНЫЙ ОТЧЁТ ПО НАРУШЕННЫМ СВЯЗЯМ ({len(broken_details)}):")
        print("=" * 160)
        
        for i, detail in enumerate(broken_details, 1):
            print(f"\n[{i}] {detail['link_table']}.{detail['fk_column']}")
            print(f"    → Должен ссылаться на: {detail['hub_table']}.{detail['hub_pk']}")
            print(f"    📊 Записей в линке: {detail['link_total']:,}")
            print(f"    📊 Записей в хабе: {detail['hub_total']:,}")
            print(f"    🔴 Осиротевших записей: {detail['orphans_count']:,}")
            
            if detail['link_total'] > 0:
                orphan_pct = 100 * detail['orphans_count'] / detail['link_total']
                print(f"    📈 Процент осиротевших: {orphan_pct:.2f}%")
    
    # Отчёт по ошибкам
    error_details = []
    for link_table, results in all_results.items():
        for r in results:
            if r['status'] == 'ERROR':
                error_details.append({
                    'link_table': link_table,
                    'fk_column': r['fk_column'],
                    'hub_table': r['hub_table'],
                    'error': r['error']
                })
    
    if error_details:
        print(f"\n" + "=" * 160)
        print(f"⚠️ ОШИБКИ ПРИ ПРОВЕРКЕ ({len(error_details)}):")
        print("=" * 160)
        for i, detail in enumerate(error_details, 1):
            print(f"\n[{i}] {detail['link_table']}.{detail['fk_column']} → {detail['hub_table']}")
            print(f"    ❗ Ошибка: {detail['error']}")
    
    print("\n" + "=" * 160)
    
    # Итоговый вердикт
    if broken_links == 0 and error_links == 0:
        print("🎉 ВЕРДИКТ: Все связи целостны! Нарушений не обнаружено.")
    elif broken_links > 0:
        print(f"⚠️ ВЕРДИКТ: Обнаружено {broken_links} нарушенных связей. Требуется анализ!")
    else:
        print(f"⚠️ ВЕРДИКТ: Обнаружено {error_links} ошибок при проверке. Требуется анализ!")
    
    print("=" * 160 + "\n")

# =============================================================================
# ОСНОВНАЯ ФУНКЦИЯ
# =============================================================================

def main():
    print("🚀 Запуск проверки ссылочной целостности Links → Hubs...")
    print(f"📊 Подключение к DWH: {DWH_HOST}:{DWH_PORT}/{DWH_DB}")
    print(f"📋 Таблиц линков в конфиге: {len(LINK_TO_HUB_MAPPING)}")
    total_fk_checks = sum(len(cfg['fk_columns']) for cfg in LINK_TO_HUB_MAPPING.values())
    print(f"📋 Всего FK-связей для проверки: {total_fk_checks}")
    print()
    
    # Проверка порта
    if not check_port(DWH_HOST, DWH_PORT):
        log.error(f"❌ Порт DWH {DWH_HOST}:{DWH_PORT} недоступен!")
        return
    
    log.info("✅ Порт DWH доступен")
    
    # Подключение к DWH
    try:
        dwh_engine = get_dwh_engine()
        with dwh_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log.info("✅ Подключение к DWH установлено")
    except Exception as e:
        log.error(f"❌ Не удалось подключиться к DWH: {e}")
        return
    
    # Проверка всех линков
    try:
        log.info(f"🔍 Начинаем проверку {total_fk_checks} FK-связей...")
        all_results = check_all_links_integrity(dwh_engine)
        
        # Вывод отчёта
        print_integrity_report(all_results)
        
    except Exception as e:
        log.error(f"❌ Критическая ошибка: {e}", exc_info=True)
    finally:
        dwh_engine.dispose()
    
    print("\n🎉 Проверка завершена!")


if __name__ == "__main__":
    main()