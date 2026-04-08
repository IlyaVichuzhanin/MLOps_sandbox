staging_to_link_info = [
    {
        'stg_table_name': 'stg_aggregate_notification_configs',
        'link_table_name': 'l_aggregate_notification_configs_email_addresses',
        'link_sk_column_name': 'l_aggregate_notification_configs_email_addresses_sk',
        'link_first_hub_sk_column_name': 'h_aggregate_notification_config_sk',
        'link_second_hub_sk_column_name': 'h_email_address_sk',
        'stg_link_sk_column_name': 'l_hub_emailaddress_sk',
        'stg_second_hub_sk_column_name': 'emailaddress_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    # {
    #     'stg_table_name': 'stg_aggregate_notification_configs',
    #     'link_table_name': 'l_aggregate_notification_configs_aggregates',
    #     'link_sk_column_name': 'l_aggregate_notification_configs_aggregates_sk',
    #     'link_first_hub_sk_column_name': 'h_aggregate_notification_config_sk',
    #     'link_second_hub_sk_column_name': 'h_aggregate_sk',
    #     'stg_link_sk_column_name': 'l_hub_aggregate_sk',
    #     'stg_second_hub_sk_column_name': 'aggregate_sk'
    # },
    {
        'stg_table_name': 'stg_diagnostic_alarms',
        'link_table_name': 'l_object_properties_diagnostic_alarms',
        'link_sk_column_name': 'l_object_properties_diagnostic_alarms_sk',
        'link_first_hub_sk_column_name': 'h_diagnostic_alarm_sk',
        'link_second_hub_sk_column_name': 'h_object_property_sk',
        'stg_link_sk_column_name': 'l_diag_alarm_propertyid_sk',
        'stg_first_hub_sk_column_name': 'hash_diag_alarm_sk',
        'stg_second_hub_sk_column_name': 'propertyid_sk'
    },
    {
        'stg_table_name': 'stg_diagnostic_alarms',
        'link_table_name': 'l_diagnostic_data_records_diagnostic_alarms',
        'link_sk_column_name': 'l_diagnostic_data_records_diagnostic_alarms_sk',
        'link_first_hub_sk_column_name': 'h_diagnostic_alarm_sk',
        'link_second_hub_sk_column_name': 'h_diagnostic_data_record_sk',
        'stg_link_sk_column_name': 'l_diag_alarm_diag_data_sk',
        'stg_first_hub_sk_column_name': 'hash_diag_alarm_sk',
        'stg_second_hub_sk_column_name': 'hash_diag_data_sk'
    },
    {
        'stg_table_name': 'stg_diagnostic_alarms',
        'link_table_name': 'l_diagnostic_alarm_states_diagnostic_alarms',
        'link_sk_column_name': 'l_diagnostic_alarm_states_diagnostic_alarms_sk',
        'link_first_hub_sk_column_name': 'h_diagnostic_alarm_sk',
        'link_second_hub_sk_column_name': 'h_diagnostic_alarm_state_sk',
        'stg_link_sk_column_name': 'l_diag_alarm_alarmstate_sk',
        'stg_first_hub_sk_column_name': 'hash_diag_alarm_sk',
        'stg_second_hub_sk_column_name': 'alarmstate_sk'
    },
    {
        'stg_table_name': 'stg_diagnostic_alarms',
        'link_table_name': 'l_diagnostic_defect_states_diagnostic_data_records',
        'link_sk_column_name': 'l_diagnostic_defect_states_diagnostic_data_records_sk',
        'link_first_hub_sk_column_name': 'h_diagnostic_data_record_sk',
        'link_second_hub_sk_column_name': 'h_diagnostic_defect_state_sk',
        'stg_link_sk_column_name': 'l_diag_data_defectstate_sk',
        'stg_first_hub_sk_column_name': 'hash_diag_data_sk',
        'stg_second_hub_sk_column_name': 'defectstate_sk'
    },
    {
        'stg_table_name': 'stg_diagnostic_alarms',
        'link_table_name': 'l_diagnostic_defect_types_diagnostic_data_records',
        'link_sk_column_name': 'l_diagnostic_defect_types_diagnostic_data_records_sk',
        'link_first_hub_sk_column_name': 'h_diagnostic_data_record_sk',
        'link_second_hub_sk_column_name': 'h_diagnostic_defect_type_sk',
        'stg_link_sk_column_name': 'l_diag_data_defecttype_sk',
        'stg_first_hub_sk_column_name': 'hash_diag_data_sk',
        'stg_second_hub_sk_column_name': 'defecttype_sk'
    },
    {
        'stg_table_name': 'stg_io_creyt_channel_configs',
        'link_table_name': 'l_io_creyt_channel_configs_object_properties',
        'link_sk_column_name': 'l_io_creyt_channel_configs_object_properties_sk',
        'link_first_hub_sk_column_name': 'h_io_creyt_channel_config_sk',
        'link_second_hub_sk_column_name': 'h_object_property_sk',
        'stg_link_sk_column_name': 'l_hub_propertyid_sk',
        'stg_second_hub_sk_column_name': 'propertyid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_creyt_channel_configs',
        'link_table_name': 'l_io_creyt_channel_configs_measure_units',
        'link_sk_column_name': 'l_io_creyt_channel_configs_measure_units_sk',
        'link_first_hub_sk_column_name': 'h_io_creyt_channel_config_sk',
        'link_second_hub_sk_column_name': 'h_measure_unit_sk',
        'stg_link_sk_column_name': 'l_hub_measureunitid_sk',
        'stg_second_hub_sk_column_name': 'measureunitid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    # {
    #     'stg_table_name': 'stg_io_creyt_channel_configs_v5',
    #     'link_table_name': 'l_io_creyt_channel_configs_object_properties',
    #     'link_sk_column_name': 'l_io_creyt_channel_configs_object_properties_sk',
    #     'link_first_hub_sk_column_name': 'h_io_creyt_channel_config_sk',
    #     'link_second_hub_sk_column_name': 'h_object_property_sk',
    #     'stg_link_sk_column_name': 'l_hub_propertyid_sk',
    #     'stg_second_hub_sk_column_name': 'propertyid_sk'
    # },
    # {
    #     'stg_table_name': 'stg_io_creyt_channel_configs_v5',
    #     'link_table_name': 'l_io_creyt_channel_configs_measure_units',
    #     'link_sk_column_name': 'l_io_creyt_channel_configs_measure_units_sk',
    #     'link_first_hub_sk_column_name': 'h_io_creyt_channel_config_sk',
    #     'link_second_hub_sk_column_name': 'h_measure_unit_sk',
    #     'stg_link_sk_column_name': 'l_hub_measureunitid_sk',
    #     'stg_second_hub_sk_column_name': 'measureunitid_sk'
    # },
    {
        'stg_table_name': 'stg_io_creyt_channel_states',
        'link_table_name': 'l_io_creyt_channel_states_object_properties',
        'link_sk_column_name': 'l_io_creyt_channel_states_object_properties_sk',
        'link_first_hub_sk_column_name': 'h_io_creyt_channel_state_sk',
        'link_second_hub_sk_column_name': 'h_object_property_sk',
        'stg_link_sk_column_name': 'l_hub_propertyid_sk',
        'stg_second_hub_sk_column_name': 'propertyid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    # {
    #     'stg_table_name': 'stg_io_creyt_channel_states_v5',
    #     'link_table_name': 'l_io_creyt_channel_states_object_properties',
    #     'link_sk_column_name': 'l_io_creyt_channel_states_object_properties_sk',
    #     'link_first_hub_sk_column_name': 'h_io_creyt_channel_state_sk',
    #     'link_second_hub_sk_column_name': 'h_object_property_sk',
    #     'stg_link_sk_column_name': 'l_hub_propertyid_sk',
    #     'stg_second_hub_sk_column_name': 'propertyid_sk'
    # },
    {
        'stg_table_name': 'stg_io_creyt_controller_states',
        'link_table_name': 'l_io_creyt_controller_states_object_properties',
        'link_sk_column_name': 'l_io_creyt_controller_states_object_properties_sk',
        'link_first_hub_sk_column_name': 'h_io_creyt_controller_state_sk',
        'link_second_hub_sk_column_name': 'h_object_property_sk',
        'stg_link_sk_column_name': 'l_hub_opertimepropertyid_sk',
        'stg_second_hub_sk_column_name': 'opertimepropertyid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_creyt_controller_states',
        'link_table_name': 'l_io_creyt_controller_states_object_properties',
        'link_sk_column_name': 'l_io_creyt_controller_states_object_properties_sk',
        'link_first_hub_sk_column_name': 'h_io_creyt_controller_state_sk',
        'link_second_hub_sk_column_name': 'h_object_property_sk',
        'stg_link_sk_column_name': 'l_hub_statepropertyid_sk',
        'stg_second_hub_sk_column_name': 'statepropertyid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    # {
    #     'stg_table_name': 'stg_io_creyt_controller_states_v5',
    #     'link_table_name': 'l_io_creyt_controller_states_object_properties',
    #     'link_sk_column_name': 'l_io_creyt_controller_states_object_properties_sk',
    #     'link_first_hub_sk_column_name': 'h_io_creyt_controller_state_sk',
    #     'link_second_hub_sk_column_name': 'h_object_property_sk',
    #     'stg_link_sk_column_name': 'l_hub_opertimepropertyid_sk',
    #     'stg_second_hub_sk_column_name': 'opertimepropertyid_sk'
    # },
    # {
    #     'stg_table_name': 'stg_io_creyt_controller_states_v5',
    #     'link_table_name': 'l_io_creyt_controller_states_object_properties',
    #     'link_sk_column_name': 'l_io_creyt_controller_states_object_properties_sk',
    #     'link_first_hub_sk_column_name': 'h_io_creyt_controller_state_sk',
    #     'link_second_hub_sk_column_name': 'h_object_property_sk',
    #     'stg_link_sk_column_name': 'l_hub_statepropertyid_sk',
    #     'stg_second_hub_sk_column_name': 'statepropertyid_sk'
    # },
    {
        'stg_table_name': 'stg_io_creyt_im_oper_times',
        'link_table_name': 'l_io_creyt_im_oper_times_object_properties',
        'link_sk_column_name': 'l_io_creyt_im_oper_times_object_properties_sk',
        'link_first_hub_sk_column_name': 'h_io_creyt_im_oper_time_sk',
        'link_second_hub_sk_column_name': 'h_object_property_sk',
        'stg_link_sk_column_name': 'l_hub_propertyid_sk',
        'stg_second_hub_sk_column_name': 'propertyid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    # {
    #     'stg_table_name': 'stg_io_creyt_im_oper_times_v5',
    #     'link_table_name': 'l_io_creyt_im_oper_times_object_properties',
    #     'link_sk_column_name': 'l_io_creyt_im_oper_times_object_properties_sk',
    #     'link_first_hub_sk_column_name': 'h_io_creyt_im_oper_time_sk',
    #     'link_second_hub_sk_column_name': 'h_object_property_sk',
    #     'stg_link_sk_column_name': 'l_hub_propertyid_sk',
    #     'stg_second_hub_sk_column_name': 'propertyid_sk'
    # },
    {
        'stg_table_name': 'stg_io_device_configs',
        'link_table_name': 'l_config_types_io_device_configs',
        'link_sk_column_name': 'l_config_types_io_device_configs_sk',
        'link_first_hub_sk_column_name': 'h_io_device_config_sk',
        'link_second_hub_sk_column_name': 'h_config_type_sk',
        'stg_link_sk_column_name': 'l_hub_typeid_sk',
        'stg_second_hub_sk_column_name': 'typeid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_device_configs',
        'link_table_name': 'l_io_device_config_nodes_io_device_configs',
        'link_sk_column_name': 'l_io_device_config_nodes_io_device_configs_sk',
        'link_first_hub_sk_column_name': 'h_io_device_config_sk',
        'link_second_hub_sk_column_name': 'h_io_device_config_node_sk',
        'stg_link_sk_column_name': 'l_hub_confignodeid_sk',
        'stg_second_hub_sk_column_name': 'confignodeid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_l_card_channel_configs',
        'link_table_name': 'l_io_lcard_channel_configs_object_properties',
        'link_sk_column_name': 'l_io_lcard_channel_configs_object_properties_sk',
        'link_first_hub_sk_column_name': 'h_io_lcard_channel_config_sk',
        'link_second_hub_sk_column_name': 'h_object_property_sk',
        'stg_link_sk_column_name': 'l_hub_propertyid_sk',
        'stg_second_hub_sk_column_name': 'propertyid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_l_card_channel_configs',
        'link_table_name': 'l_io_lcard_channel_configs_measure_units',
        'link_sk_column_name': 'l_io_lcard_channel_configs_measure_units_sk',
        'link_first_hub_sk_column_name': 'h_io_lcard_channel_config_sk',
        'link_second_hub_sk_column_name': 'h_measure_unit_sk',
        'stg_link_sk_column_name': 'l_hub_measureunitid_sk',
        'stg_second_hub_sk_column_name': 'measureunitid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_l_card_crate_configs',
        'link_table_name': 'l_io_lcard_crate_configs_crate_types',
        'link_sk_column_name': 'l_io_lcard_crate_configs_crate_types_sk',
        'link_first_hub_sk_column_name': 'h_io_lcard_crate_config_sk',
        'link_second_hub_sk_column_name': 'h_crate_type_sk',
        'stg_link_sk_column_name': 'l_hub_cratetype_sk',
        'stg_second_hub_sk_column_name': 'cratetype_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_l_card_input_configs',
        'link_table_name': 'l_io_lcard_input_configs_object_properties',
        'link_sk_column_name': 'l_io_lcard_input_configs_object_properties_sk',
        'link_first_hub_sk_column_name': 'h_io_lcard_input_config_sk',
        'link_second_hub_sk_column_name': 'h_object_property_sk',
        'stg_link_sk_column_name': 'l_hub_propertyid_sk',
        'stg_second_hub_sk_column_name': 'propertyid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_l_card_input_configs',
        'link_table_name': 'l_io_lcard_input_configs_measure_units',
        'link_sk_column_name': 'l_io_lcard_input_configs_measure_units_sk',
        'link_first_hub_sk_column_name': 'h_io_lcard_input_config_sk',
        'link_second_hub_sk_column_name': 'h_measure_unit_sk',
        'stg_link_sk_column_name': 'l_hub_measureunitid_sk',
        'stg_second_hub_sk_column_name': 'measureunitid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_l_card_logic_input_configs',
        'link_table_name': 'l_io_lcard_logic_input_configs_timer_property_ids',
        'link_sk_column_name': 'l_io_lcard_logic_input_configs_timer_property_ids_sk',
        'link_first_hub_sk_column_name': 'h_io_lcard_logic_input_config_sk',
        'link_second_hub_sk_column_name': 'h_timer_property_id_sk',
        'stg_link_sk_column_name': 'l_hub_timerpropertyid_sk',
        'stg_second_hub_sk_column_name': 'timerpropertyid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_l_card_logic_input_configs',
        'link_table_name': 'l_io_lcard_logic_input_configs_sync_property_ids',
        'link_sk_column_name': 'l_io_lcard_logic_input_configs_sync_property_ids_sk',
        'link_first_hub_sk_column_name': 'h_io_lcard_logic_input_config_sk',
        'link_second_hub_sk_column_name': 'h_sync_property_id_sk',
        'stg_link_sk_column_name': 'l_hub_syncpropertyid_sk',
        'stg_second_hub_sk_column_name': 'syncpropertyid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_l_card_logic_input_configs',
        'link_table_name': 'l_io_lcard_logic_input_configs_l_card_logic_input_types',
        'link_sk_column_name': 'l_io_lcard_logic_input_configs_l_card_logic_input_types_sk',
        'link_first_hub_sk_column_name': 'h_io_lcard_logic_input_config_sk',
        'link_second_hub_sk_column_name': 'h_l_card_logic_input_type_sk',
        'stg_link_sk_column_name': 'l_hub_inputtype_sk',
        'stg_second_hub_sk_column_name': 'inputtype_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_l_card_logic_input_configs',
        'link_table_name': 'l_io_lcard_input_configs_measure_units',
        'link_sk_column_name': 'l_io_lcard_input_configs_measure_units_sk',
        'link_first_hub_sk_column_name': 'h_io_lcard_input_config_sk',
        'link_second_hub_sk_column_name': 'h_measure_unit_sk',
        'stg_link_sk_column_name': 'l_hub_measureunitid_sk',
        'stg_second_hub_sk_column_name': 'measureunitid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_l_card_module_configs',
        'link_table_name': 'l_io_lcard_module_configs_l_card_crate_module_types',
        'link_sk_column_name': 'l_io_lcard_module_configs_l_card_crate_module_types_sk',
        'link_first_hub_sk_column_name': 'h_io_lcard_module_config_sk',
        'link_second_hub_sk_column_name': 'h_l_card_crate_module_type_sk',
        'stg_link_sk_column_name': 'l_hub_moduletype_sk',
        'stg_second_hub_sk_column_name': 'moduletype_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_modbus_tcp_configs',
        'link_table_name': 'l_io_modbus_tcp_configs_device_types',
        'link_sk_column_name': 'l_io_modbus_tcp_configs_device_types_sk',
        'link_first_hub_sk_column_name': 'h_io_modbus_tcp_config_sk',
        'link_second_hub_sk_column_name': 'h_device_type_sk',
        'stg_link_sk_column_name': 'l_hub_devicetype_sk',
        'stg_second_hub_sk_column_name': 'devicetype_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_modbus_tcp_configs',
        'link_table_name': 'l_io_modbus_tcp_configs_interface_types',
        'link_sk_column_name': 'l_io_modbus_tcp_configs_interface_types_sk',
        'link_first_hub_sk_column_name': 'h_io_modbus_tcp_config_sk',
        'link_second_hub_sk_column_name': 'h_interface_type_sk',
        'stg_link_sk_column_name': 'l_hub_interfacetype_sk',
        'stg_second_hub_sk_column_name': 'interfacetype_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_modbus_tcp_register_configs',
        'link_table_name': 'l_io_modbus_tcp_register_configs_register_types',
        'link_sk_column_name': 'l_io_modbus_tcp_register_configs_register_types_sk',
        'link_first_hub_sk_column_name': 'h_io_modbus_tcp_register_config_sk',
        'link_second_hub_sk_column_name': 'h_register_type_sk',
        'stg_link_sk_column_name': 'l_hub_registertype_sk',
        'stg_second_hub_sk_column_name': 'registertype_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_modbus_tcp_register_configs',
        'link_table_name': 'l_io_modbus_tcp_register_configs_object_properties',
        'link_sk_column_name': 'l_io_modbus_tcp_register_configs_object_properties_sk',
        'link_first_hub_sk_column_name': 'h_io_modbus_tcp_register_config_sk',
        'link_second_hub_sk_column_name': 'h_object_property_sk',
        'stg_link_sk_column_name': 'l_hub_propertyid_sk',
        'stg_second_hub_sk_column_name': 'propertyid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_modbus_tcp_register_configs',
        'link_table_name': 'l_io_modbus_tcp_register_configs_measure_units',
        'link_sk_column_name': 'l_io_modbus_tcp_register_configs_measure_units_sk',
        'link_first_hub_sk_column_name': 'h_io_modbus_tcp_register_config_sk',
        'link_second_hub_sk_column_name': 'h_measure_unit_sk',
        'stg_link_sk_column_name': 'l_hub_measureunitid_sk',
        'stg_second_hub_sk_column_name': 'measureunitid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_modbus_tcp_register_configs',
        'link_table_name': 'l_io_modbus_tcp_register_configs_write_property_ids',
        'link_sk_column_name': 'l_io_modbus_tcp_register_configs_write_property_ids_sk',
        'link_first_hub_sk_column_name': 'h_io_modbus_tcp_register_config_sk',
        'link_second_hub_sk_column_name': 'h_write_property_id_sk',
        'stg_link_sk_column_name': 'l_hub_writepropertyid_sk',
        'stg_second_hub_sk_column_name': 'writepropertyid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_modbus_tcp_register_configs',
        'link_table_name': 'l_io_modbus_tcp_register_configs_data_type_names',
        'link_sk_column_name': 'l_io_modbus_tcp_register_configs_data_type_names_sk',
        'link_first_hub_sk_column_name': 'h_io_modbus_tcp_register_config_sk',
        'link_second_hub_sk_column_name': 'h_data_type_name_sk',
        'stg_link_sk_column_name': 'l_hub_datatype_sk',
        'stg_second_hub_sk_column_name': 'datatype_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    # {
    #     'stg_table_name': 'stg_io_modbus_tcp_register_configs_tmp',
    #     'link_table_name': 'l_io_modbus_tcp_register_configs_register_types',
    #     'link_sk_column_name': 'l_io_modbus_tcp_register_configs_register_types_sk',
    #     'link_first_hub_sk_column_name': 'h_io_modbus_tcp_register_config_sk',
    #     'link_second_hub_sk_column_name': 'h_register_type_sk',
    #     'stg_link_sk_column_name': 'l_hub_registertype_sk',
    #     'stg_second_hub_sk_column_name': 'registertype_sk'
    # },
    # {
    #     'stg_table_name': 'stg_io_modbus_tcp_register_configs_tmp',
    #     'link_table_name': 'l_io_modbus_tcp_register_configs_object_properties',
    #     'link_sk_column_name': 'l_io_modbus_tcp_register_configs_object_properties_sk',
    #     'link_first_hub_sk_column_name': 'h_io_modbus_tcp_register_config_sk',
    #     'link_second_hub_sk_column_name': 'h_object_property_sk',
    #     'stg_link_sk_column_name': 'l_hub_propertyid_sk',
    #     'stg_second_hub_sk_column_name': 'propertyid_sk'
    # },
    {
        'stg_table_name': 'stg_io_modbus_tcp_register_configs_tmp',
        'link_table_name': 'l_io_modbus_tcp_register_configs_measure_units',
        'link_sk_column_name': 'l_io_modbus_tcp_register_configs_measure_units_sk',
        'link_first_hub_sk_column_name': 'h_io_modbus_tcp_register_config_sk',
        'link_second_hub_sk_column_name': 'h_measure_unit_sk',
        'stg_link_sk_column_name': 'l_hub_measureunitid_sk',
        'stg_second_hub_sk_column_name': 'measureunitid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_modbus_tcp_register_bit_decompression_configs',
        'link_table_name': 'l_io_modbus_tcp_bit_decompression_configs_object_properties',
        'link_sk_column_name': 'l_io_modbus_tcp_bit_decompression_configs_object_properties_sk',
        'link_first_hub_sk_column_name': 'h_io_modbus_tcp_bit_decompression_config_sk',
        'link_second_hub_sk_column_name': 'h_object_property_sk',
        'stg_link_sk_column_name': 'l_hub_propertyid_sk',
        'stg_second_hub_sk_column_name': 'propertyid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_modbus_tcp_register_bit_decompression_configs',
        'link_table_name': 'l_io_modbus_tcp_bit_decompression_configs_write_properties',
        'link_sk_column_name': 'l_io_modbus_tcp_bit_decompression_configs_write_properties_sk',
        'link_first_hub_sk_column_name': 'h_io_modbus_tcp_bit_decompression_config_sk',
        'link_second_hub_sk_column_name': 'h_write_property_id_sk',
        'stg_link_sk_column_name': 'l_hub_writepropertyid_sk',
        'stg_second_hub_sk_column_name': 'writepropertyid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_opc_da_client_item_configs',
        'link_table_name': 'l_io_opc_da_client_item_configs_object_properties',
        'link_sk_column_name': 'l_io_opc_da_client_item_configs_object_properties_sk',
        'link_first_hub_sk_column_name': 'h_io_opc_da_client_item_config_sk',
        'link_second_hub_sk_column_name': 'h_object_property_sk',
        'stg_link_sk_column_name': 'l_hub_propertyid_sk',
        'stg_second_hub_sk_column_name': 'propertyid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_opc_da_client_item_configs',
        'link_table_name': 'l_io_opc_da_client_item_configs_opc_da_data_types',
        'link_sk_column_name': 'l_io_opc_da_client_item_configs_opc_da_data_types_sk',
        'link_first_hub_sk_column_name': 'h_io_opc_da_client_item_config_sk',
        'link_second_hub_sk_column_name': 'h_opc_ua_data_type_sk',
        'stg_link_sk_column_name': 'l_hub_datatype_sk',
        'stg_second_hub_sk_column_name': 'datatype_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_opc_ua_client_item_configs',
        'link_table_name': 'l_io_opc_ua_client_item_configs_object_properties',
        'link_sk_column_name': 'l_io_opc_ua_client_item_configs_object_properties_sk',
        'link_first_hub_sk_column_name': 'h_io_opc_ua_client_item_config_sk',
        'link_second_hub_sk_column_name': 'h_object_property_sk',
        'stg_link_sk_column_name': 'l_hub_propertyid_sk',
        'stg_second_hub_sk_column_name': 'propertyid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_opc_ua_client_item_configs',
        'link_table_name': 'l_io_opc_ua_client_item_configs_opc_ua_data_types',
        'link_sk_column_name': 'l_io_opc_ua_client_item_configs_opc_ua_data_types_sk',
        'link_first_hub_sk_column_name': 'h_io_opc_ua_client_item_config_sk',
        'link_second_hub_sk_column_name': 'h_opc_ua_data_type_sk',
        'stg_link_sk_column_name': 'l_hub_datatype_sk',
        'stg_second_hub_sk_column_name': 'datatype_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_opc_ua_client_item_configs',
        'link_table_name': 'l_io_opc_ua_client_item_configs_data_type_servers',
        'link_sk_column_name': 'l_io_opc_ua_client_item_configs_data_type_server_sk',
        'link_first_hub_sk_column_name': 'h_io_opc_ua_client_item_config_sk',
        'link_second_hub_sk_column_name': 'h_data_type_server_sk',
        'stg_link_sk_column_name': 'l_hub_datatypeserver_sk',
        'stg_second_hub_sk_column_name': 'datatypeserver_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_opc_ua_client_transform_item_configs',
        'link_table_name': 'l_io_opc_ua_client_transform_item_configs_object_properties',
        'link_sk_column_name': 'l_io_opc_ua_client_transform_item_configs_object_properties_sk',
        'link_first_hub_sk_column_name': 'h_io_opc_ua_client_transform_item_config_sk',
        'link_second_hub_sk_column_name': 'h_object_property_sk',
        'stg_link_sk_column_name': 'l_hub_propertyid_sk',
        'stg_second_hub_sk_column_name': 'propertyid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_measure_convert',
        'link_table_name': 'l_convert_types_measure_converts',
        'link_sk_column_name': 'l_convert_types_measure_converts_sk',
        'link_first_hub_sk_column_name': 'h_measure_convert_sk',
        'link_second_hub_sk_column_name': 'h_convert_type_sk',
        'stg_link_sk_column_name': 'l_hub_converttype_sk',
        'stg_second_hub_sk_column_name': 'converttype_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_measure_convert',
        'link_table_name': 'l_measure_convert_from_ids',
        'link_sk_column_name': 'l_measure_convert_from_ids_sk',
        'link_first_hub_sk_column_name': 'h_measure_convert_sk',
        'link_second_hub_sk_column_name': 'h_measure_unit_sk',
        'stg_link_sk_column_name': 'l_hub_fromid_sk',
        'stg_second_hub_sk_column_name': 'fromid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_measure_convert',
        'link_table_name': 'l_measure_convert_to_ids',
        'link_sk_column_name': 'l_measure_convert_to_ids_sk',
        'link_first_hub_sk_column_name': 'h_measure_convert_sk',
        'link_second_hub_sk_column_name': 'h_measure_unit_sk',
        'stg_link_sk_column_name': 'l_hub_toid_sk',
        'stg_second_hub_sk_column_name': 'toid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_measure_units',
        'link_table_name': 'l_measure_units_measure_groups',
        'link_sk_column_name': 'l_measure_units_measure_groups_sk',
        'link_first_hub_sk_column_name': 'h_measure_unit_sk',
        'link_second_hub_sk_column_name': 'h_measure_group_sk',
        'stg_link_sk_column_name': 'l_hub_measuregroupid_sk',
        'stg_second_hub_sk_column_name': 'measuregroupid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_model_templates',
        'link_table_name': 'l_model_templates_model_template_tree_nodes',
        'link_sk_column_name': 'l_model_templates_model_template_tree_nodes_sk',
        'link_first_hub_sk_column_name': 'h_model_template_sk',
        'link_second_hub_sk_column_name': 'h_model_template_tree_node_sk',
        'stg_link_sk_column_name': 'l_hub_ownernodeid_sk',
        'stg_second_hub_sk_column_name': 'ownernodeid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    # {
    #     'stg_table_name': 'stg_object_data_values_diagnostic_array_live_v2',
    #     'link_table_name': 'l_object_properties_diagnostic_array_data_records',
    #     'link_sk_column_name': 'l_object_properties_diagnostic_array_data_records_sk',
    #     'link_first_hub_sk_column_name': 'h_diagnostic_array_data_record_sk',
    #     'link_second_hub_sk_column_name': 'h_object_property_sk',
    #     'stg_link_sk_column_name': 'l_hub_diagid_sk',
    #     'stg_second_hub_sk_column_name': 'diagid_sk',
    #     'stg_first_hub_sk_column_name': 'hash_sk'
    # },
    # {
    #     'stg_table_name': 'stg_object_data_values_diagnostic_array_v2',
    #     'link_table_name': 'l_object_properties_diagnostic_array_data_records',
    #     'link_sk_column_name': 'l_object_properties_diagnostic_array_data_records_sk',
    #     'link_first_hub_sk_column_name': 'h_diagnostic_array_data_record_sk',
    #     'link_second_hub_sk_column_name': 'h_object_property_sk',
    #     'stg_link_sk_column_name': 'l_hub_diagid_sk',
    #     'stg_second_hub_sk_column_name': 'diagid_sk',
    #     'stg_first_hub_sk_column_name': 'hash_sk'
    # },
    # {
    #     'stg_table_name': 'stg_object_data_values_diagnostic_array_v2',
    #     'link_table_name': 'l_diagnostic_defect_states_diagnostic_data_records',
    #     'link_sk_column_name': 'l_diagnostic_defect_states_diagnostic_data_records_sk',
    #     'link_first_hub_sk_column_name': 'h_diagnostic_data_record_sk',
    #     'link_second_hub_sk_column_name': 'h_diagnostic_defect_state_sk',
    #     'stg_link_sk_column_name': 'l_hub_defectstate_sk',
    #     'stg_second_hub_sk_column_name': 'defectstate_sk',
    #     'stg_first_hub_sk_column_name': 'hash_sk'
    # },
    # {
    #     'stg_table_name': 'stg_object_data_values_live_v2',
    #     'link_table_name': 'l_object_data_values_object_properties',
    #     'link_sk_column_name': 'l_object_data_values_object_properties_sk',
    #     'link_first_hub_sk_column_name': 'h_object_data_value_sk',
    #     'link_second_hub_sk_column_name': 'h_object_property_sk',
    #     'stg_link_sk_column_name': 'l_hub_propertyid_sk',
    #     'stg_second_hub_sk_column_name': 'propertyid_sk'
    # },
    # {
    #     'stg_table_name': 'stg_object_data_values_live_v2',
    #     'link_table_name': 'l_object_data_values_measure_units',
    #     'link_sk_column_name': 'l_object_data_values_measure_units_sk',
    #     'link_first_hub_sk_column_name': 'h_object_data_value_sk',
    #     'link_second_hub_sk_column_name': 'h_measure_unit_sk',
    #     'stg_link_sk_column_name': 'l_hub_measureunitid_sk',
    #     'stg_second_hub_sk_column_name': 'measureunitid_sk'
    # },
    # {
    #     'stg_table_name': 'stg_object_data_values_live_v2',
    #     'link_table_name': 'l_object_data_values_data_types',
    #     'link_sk_column_name': 'l_object_data_values_data_types_sk',
    #     'link_first_hub_sk_column_name': 'h_object_data_value_sk',
    #     'link_second_hub_sk_column_name': 'h_data_type_sk',
    #     'stg_link_sk_column_name': 'l_hub_datatypeid_sk',
    #     'stg_second_hub_sk_column_name': 'datatypeid_sk'
    # },
    # {
    #     'stg_table_name': 'stg_object_data_values_common_snapshot',
    #     'link_table_name': 'l_object_data_values_object_properties',
    #     'link_sk_column_name': 'l_object_data_values_object_properties_sk',
    #     'link_first_hub_sk_column_name': 'h_object_data_value_sk',
    #     'link_second_hub_sk_column_name': 'h_object_property_sk',
    #     'stg_link_sk_column_name': 'l_hub_propertyid_sk',
    #     'stg_second_hub_sk_column_name': 'propertyid_sk'
    # },
    # {
    #     'stg_table_name': 'stg_object_data_values_common_snapshot',
    #     'link_table_name': 'l_object_data_values_measure_units',
    #     'link_sk_column_name': 'l_object_data_values_measure_units_sk',
    #     'link_first_hub_sk_column_name': 'h_object_data_value_sk',
    #     'link_second_hub_sk_column_name': 'h_measure_unit_sk',
    #     'stg_link_sk_column_name': 'l_hub_measureunitid_sk',
    #     'stg_second_hub_sk_column_name': 'measureunitid_sk'
    # },
    # {
    #     'stg_table_name': 'stg_object_data_values_common_snapshot',
    #     'link_table_name': 'l_object_data_values_data_types',
    #     'link_sk_column_name': 'l_object_data_values_data_types_sk',
    #     'link_first_hub_sk_column_name': 'h_object_data_value_sk',
    #     'link_second_hub_sk_column_name': 'h_data_type_sk',
    #     'stg_link_sk_column_name': 'l_hub_datatypeid_sk',
    #     'stg_second_hub_sk_column_name': 'datatypeid_sk'
    # },
    # {
    #     'stg_table_name': 'stg_object_data_values_history_snapshot',
    #     'link_table_name': 'l_object_data_values_object_properties',
    #     'link_sk_column_name': 'l_object_data_values_object_properties_sk',
    #     'link_first_hub_sk_column_name': 'h_object_data_value_sk',
    #     'link_second_hub_sk_column_name': 'h_object_property_sk',
    #     'stg_link_sk_column_name': 'l_hub_propertyid_sk',
    #     'stg_second_hub_sk_column_name': 'propertyid_sk'
    # },
    # {
    #     'stg_table_name': 'stg_object_data_values_history_snapshot',
    #     'link_table_name': 'l_object_data_values_measure_units',
    #     'link_sk_column_name': 'l_object_data_values_measure_units_sk',
    #     'link_first_hub_sk_column_name': 'h_object_data_value_sk',
    #     'link_second_hub_sk_column_name': 'h_measure_unit_sk',
    #     'stg_link_sk_column_name': 'l_hub_measureunitid_sk',
    #     'stg_second_hub_sk_column_name': 'measureunitid_sk'
    # },
    # {
    #     'stg_table_name': 'stg_object_data_values_history_snapshot',
    #     'link_table_name': 'l_object_data_values_data_types',
    #     'link_sk_column_name': 'l_object_data_values_data_types_sk',
    #     'link_first_hub_sk_column_name': 'h_object_data_value_sk',
    #     'link_second_hub_sk_column_name': 'h_data_type_sk',
    #     'stg_link_sk_column_name': 'l_hub_datatypeid_sk',
    #     'stg_second_hub_sk_column_name': 'datatypeid_sk'
    # },
    # {
    #     'stg_table_name': 'stg_object_data_values_snapshot',
    #     'link_table_name': 'l_object_data_values_object_properties',
    #     'link_sk_column_name': 'l_object_data_values_object_properties_sk',
    #     'link_first_hub_sk_column_name': 'h_object_data_value_sk',
    #     'link_second_hub_sk_column_name': 'h_object_property_sk',
    #     'stg_link_sk_column_name': 'l_hub_propertyid_sk',
    #     'stg_second_hub_sk_column_name': 'propertyid_sk'
    # },
    # {
    #     'stg_table_name': 'stg_object_data_values_snapshot',
    #     'link_table_name': 'l_object_data_values_measure_units',
    #     'link_sk_column_name': 'l_object_data_values_measure_units_sk',
    #     'link_first_hub_sk_column_name': 'h_object_data_value_sk',
    #     'link_second_hub_sk_column_name': 'h_measure_unit_sk',
    #     'stg_link_sk_column_name': 'l_hub_measureunitid_sk',
    #     'stg_second_hub_sk_column_name': 'measureunitid_sk'
    # },
    # {
    #     'stg_table_name': 'stg_object_data_values_snapshot',
    #     'link_table_name': 'l_object_data_values_data_types',
    #     'link_sk_column_name': 'l_object_data_values_data_types_sk',
    #     'link_first_hub_sk_column_name': 'h_object_data_value_sk',
    #     'link_second_hub_sk_column_name': 'h_data_type_sk',
    #     'stg_link_sk_column_name': 'l_hub_datatypeid_sk',
    #     'stg_second_hub_sk_column_name': 'datatypeid_sk'
    # },
    # {
    #     'stg_table_name': 'stg_object_data_values_snapshot_single',
    #     'link_table_name': 'l_object_data_values_object_properties',
    #     'link_sk_column_name': 'l_object_data_values_object_properties_sk',
    #     'link_first_hub_sk_column_name': 'h_object_data_value_sk',
    #     'link_second_hub_sk_column_name': 'h_object_property_sk',
    #     'stg_link_sk_column_name': 'l_hub_propertyid_sk',
    #     'stg_second_hub_sk_column_name': 'propertyid_sk'
    # },
    # {
    #     'stg_table_name': 'stg_object_data_values_snapshot_single',
    #     'link_table_name': 'l_object_data_values_measure_units',
    #     'link_sk_column_name': 'l_object_data_values_measure_units_sk',
    #     'link_first_hub_sk_column_name': 'h_object_data_value_sk',
    #     'link_second_hub_sk_column_name': 'h_measure_unit_sk',
    #     'stg_link_sk_column_name': 'l_hub_measureunitid_sk',
    #     'stg_second_hub_sk_column_name': 'measureunitid_sk'
    # },
    # {
    #     'stg_table_name': 'stg_object_data_values_snapshot_single',
    #     'link_table_name': 'l_object_data_values_data_types',
    #     'link_sk_column_name': 'l_object_data_values_data_types_sk',
    #     'link_first_hub_sk_column_name': 'h_object_data_value_sk',
    #     'link_second_hub_sk_column_name': 'h_data_type_sk',
    #     'stg_link_sk_column_name': 'l_hub_datatypeid_sk',
    #     'stg_second_hub_sk_column_name': 'datatypeid_sk'
    # },
    # {
    #     'stg_table_name': 'stg_object_data_values_v2',
    #     'link_table_name': 'l_object_data_values_object_properties',
    #     'link_sk_column_name': 'l_object_data_values_object_properties_sk',
    #     'link_first_hub_sk_column_name': 'h_object_data_value_sk',
    #     'link_second_hub_sk_column_name': 'h_object_property_sk',
    #     'stg_link_sk_column_name': 'l_hub_propertyid_sk',
    #     'stg_second_hub_sk_column_name': 'propertyid_sk'
    # },
    # {
    #     'stg_table_name': 'stg_object_data_values_v2',
    #     'link_table_name': 'l_object_data_values_measure_units',
    #     'link_sk_column_name': 'l_object_data_values_measure_units_sk',
    #     'link_first_hub_sk_column_name': 'h_object_data_value_sk',
    #     'link_second_hub_sk_column_name': 'h_measure_unit_sk',
    #     'stg_link_sk_column_name': 'l_hub_measureunitid_sk',
    #     'stg_second_hub_sk_column_name': 'measureunitid_sk'
    # },
    # {
    #     'stg_table_name': 'stg_object_data_values_v2',
    #     'link_table_name': 'l_object_data_values_data_types',
    #     'link_sk_column_name': 'l_object_data_values_data_types_sk',
    #     'link_first_hub_sk_column_name': 'h_object_data_value_sk',
    #     'link_second_hub_sk_column_name': 'h_data_type_sk',
    #     'stg_link_sk_column_name': 'l_hub_datatypeid_sk',
    #     'stg_second_hub_sk_column_name': 'datatypeid_sk'
    # },
    {
        'stg_table_name': 'stg_object_groups',
        'link_table_name': 'l_object_group_descriptors_objects',
        'link_sk_column_name': 'l_object_group_descriptors_objects_sk',
        'link_first_hub_sk_column_name': 'h_object_group_descriptor_sk',
        'link_second_hub_sk_column_name': 'h_object_group_sk',
        'stg_link_sk_column_name': 'l_hub_groupdescriptorid_sk',
        'stg_second_hub_sk_column_name': 'groupdescriptorid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_object_groups',
        'link_table_name': 'l_user_defined_property_lists_objects',
        'link_sk_column_name': 'l_user_defined_property_lists_objects_sk',
        'link_first_hub_sk_column_name': 'h_user_defined_property_list_sk',
        'link_second_hub_sk_column_name': 'h_object_sk',
        'stg_link_sk_column_name': 'l_hub_objectid_sk',
        'stg_second_hub_sk_column_name': 'objectid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_object_properties',
        'link_table_name': 'l_object_properties_object_property_descriptors',
        'link_sk_column_name': 'l_object_properties_object_property_descriptors_sk',
        'link_first_hub_sk_column_name': 'h_object_property_sk',
        'link_second_hub_sk_column_name': 'h_object_property_descriptor_sk',
        'stg_link_sk_column_name': 'l_hub_propertydescriptorid_sk',
        'stg_second_hub_sk_column_name': 'propertydescriptorid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_object_properties',
        'link_table_name': 'l_object_property_descriptors_objects',
        'link_sk_column_name': 'l_object_property_descriptors_objects_sk',
        'link_first_hub_sk_column_name': 'h_object_property_sk',
        'link_second_hub_sk_column_name': 'h_object_sk',
        'stg_link_sk_column_name': 'l_hub_objectid_sk',
        'stg_second_hub_sk_column_name': 'objectid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_object_properties',
        'link_table_name': 'l_measure_units_object_property_descriptors',
        'link_sk_column_name': 'l_measure_units_object_property_descriptors_sk',
        'link_first_hub_sk_column_name': 'h_object_property_descriptor_sk',
        'link_second_hub_sk_column_name': 'h_measure_unit_sk',
        'stg_link_sk_column_name': 'l_hub_measureunitid_sk',
        'stg_second_hub_sk_column_name': 'measureunitid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_object_properties',
        'link_table_name': 'l_object_properties_storage_types',
        'link_sk_column_name': 'l_object_properties_storage_types_sk',
        'link_first_hub_sk_column_name': 'h_object_property_sk',
        'link_second_hub_sk_column_name': 'h_storage_type_sk',
        'stg_link_sk_column_name': 'l_hub_storagetype_sk',
        'stg_second_hub_sk_column_name': 'storagetype_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_object_property_descriptors',
        'link_table_name': 'l_object_property_descriptor_nodes_object_property_descriptors',
        'link_sk_column_name': 'l_object_property_descriptor_nodes_object_property_descriptors_sk',
        'link_first_hub_sk_column_name': 'h_object_property_descriptor_sk',
        'link_second_hub_sk_column_name': 'h_object_property_descriptor_node_sk',
        'stg_link_sk_column_name': 'l_hub_parentnodeid_sk',
        'stg_second_hub_sk_column_name': 'parentnodeid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_object_property_descriptors',
        'link_table_name': 'l_property_types_object_property_descriptors',
        'link_sk_column_name': 'l_property_types_object_property_descriptors_uuid_sk',
        'link_first_hub_sk_column_name': 'h_object_property_descriptor_sk',
        'link_second_hub_sk_column_name': 'h_property_types_sk',
        'stg_link_sk_column_name': 'l_hub_propertytype_sk',
        'stg_second_hub_sk_column_name': 'propertytype_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_object_property_descriptors',
        'link_table_name': 'l_measure_units_object_property_descriptors',
        'link_sk_column_name': 'l_measure_units_object_property_descriptors_sk',
        'link_first_hub_sk_column_name': 'h_measure_unit_sk',
        'link_second_hub_sk_column_name': 'h_object_property_descriptor_sk',
        'stg_link_sk_column_name': 'l_hub_measureunitid_sk',
        'stg_second_hub_sk_column_name': 'measureunitid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_object_rules',
        'link_table_name': 'l_object_rules_objects',
        'link_sk_column_name': 'l_object_rules_objects_sk',
        'link_first_hub_sk_column_name': 'h_object_rule_sk',
        'link_second_hub_sk_column_name': 'h_object_sk',
        'stg_link_sk_column_name': 'l_hub_objectid_sk',
        'stg_second_hub_sk_column_name': 'objectid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_object_rules',
        'link_table_name': 'l_object_rules_running_types',
        'link_sk_column_name': 'l_object_rules_running_types_sk',
        'link_first_hub_sk_column_name': 'h_object_rule_sk',
        'link_second_hub_sk_column_name': 'h_running_type_sk',
        'stg_link_sk_column_name': 'l_hub_runningtype_sk',
        'stg_second_hub_sk_column_name': 'runningtype_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_object_types',
        'link_table_name': 'l_objects_object_type_descriptors',
        'link_sk_column_name': 'l_objects_object_type_descriptors_sk',
        'link_first_hub_sk_column_name': 'h_object_sk',
        'link_second_hub_sk_column_name': 'h_object_type_descriptor_sk',
        'stg_link_sk_column_name': 'l_hub_typedescriptorid_sk',
        'stg_second_hub_sk_column_name': 'typedescriptorid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_pou_user_defined_items',
        'link_table_name': 'l_pou_user_tree_items_pou_user_defined_items',
        'link_sk_column_name': 'l_pou_user_tree_items_pou_user_defined_items_sk',
        'link_first_hub_sk_column_name': 'h_pou_user_defined_item_sk',
        'link_second_hub_sk_column_name': 'h_pou_user_tree_item_sk',
        'stg_link_sk_column_name': 'l_hub_categoryid_sk',
        'stg_second_hub_sk_column_name': 'categoryid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_tik_expert_slices',
        'link_table_name': 'l_object_properties_tik_expert_slices',
        'link_sk_column_name': 'l_object_properties_tik_expert_slices_sk',
        'link_first_hub_sk_column_name': 'h_tik_expert_slice_sk',
        'link_second_hub_sk_column_name': 'h_object_property_sk',
        'stg_link_sk_column_name': 'l_hub_propertyid_sk',
        'stg_second_hub_sk_column_name': 'propertyid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_tik_scada_log',
        'link_table_name': 'l_tik_scada_logs_tik_scada_log_types',
        'link_sk_column_name': 'l_tik_scada_logs_tik_scada_log_types_sk',
        'link_first_hub_sk_column_name': 'h_tik_scada_log_sk',
        'link_second_hub_sk_column_name': 'h_tik_scada_log_type_sk',
        'stg_link_sk_column_name': 'l_hub_type_sk',
        'stg_second_hub_sk_column_name': 'type_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_user_defined_property_lists',
        'link_table_name': 'l_user_defined_prop_lists_user_defined_prop_list_types',
        'link_sk_column_name': 'l_user_defined_property_lists_user_defined_property_lists_type_sk',
        'link_first_hub_sk_column_name': 'h_user_defined_property_list_sk',
        'link_second_hub_sk_column_name': 'h_user_defined_property_list_type_sk',
        'stg_link_sk_column_name': 'l_hub_propertylisttypeid_sk',
        'stg_second_hub_sk_column_name': 'propertylisttypeid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_user_defined_property_lists',
        'link_table_name': 'l_user_defined_property_lists_objects',
        'link_sk_column_name': 'l_user_defined_property_lists_objects_sk',
        'link_first_hub_sk_column_name': 'h_user_defined_property_list_sk',
        'link_second_hub_sk_column_name': 'h_object_sk',
        'stg_link_sk_column_name': 'l_hub_objectid_sk',
        'stg_second_hub_sk_column_name': 'objectid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_user_defined_tiles_properties_configs',
        'link_table_name': 'l_user_defined_tiles_property_configs_objects',
        'link_sk_column_name': 'l_user_defined_tiles_property_configs_objects_sk',
        'link_first_hub_sk_column_name': 'h_user_defined_tiles_property_config_sk',
        'link_second_hub_sk_column_name': 'h_object_sk',
        'stg_link_sk_column_name': 'l_hub_objectid_sk',
        'stg_second_hub_sk_column_name': 'objectid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_user_log',
        'link_table_name': 'l_user_logs_user_action_types',
        'link_sk_column_name': 'l_user_logs_user_action_types_sk',
        'link_first_hub_sk_column_name': 'h_user_log_sk',
        'link_second_hub_sk_column_name': 'h_user_action_type_sk',
        'stg_link_sk_column_name': 'l_hub_actiontype_sk',
        'stg_second_hub_sk_column_name': 'actiontype_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_object_type_descriptors',
        'link_table_name': 'l_images_object_type_descriptors',
        'link_sk_column_name': 'l_images_object_type_descriptors_sk',
        'link_first_hub_sk_column_name': 'h_object_type_descriptor_sk',
        'link_second_hub_sk_column_name': 'h_image_sk',
        'stg_link_sk_column_name': 'l_hub_iconid_sk',
        'stg_second_hub_sk_column_name': 'iconid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_creyt_channel_configs',
        'link_table_name': 'l_io_creyt_channel_configs_io_device_configs',
        'link_sk_column_name': 'l_io_creyt_channel_configs_io_device_configs_sk',
        'link_first_hub_sk_column_name': 'h_io_creyt_channel_config_sk',
        'link_second_hub_sk_column_name': 'h_io_device_config_sk',
        'stg_link_sk_column_name': 'l_hub_configid_sk',
        'stg_second_hub_sk_column_name': 'configid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    # {
    #     'stg_table_name': 'stg_io_creyt_channel_configs_v5',
    #     'link_table_name': 'l_io_creyt_channel_configs_io_device_configs',
    #     'link_sk_column_name': 'l_io_creyt_channel_configs_io_device_configs_sk',
    #     'link_first_hub_sk_column_name': 'h_io_creyt_channel_config_sk',
    #     'link_second_hub_sk_column_name': 'h_io_device_config_sk',
    #     'stg_link_sk_column_name': 'l_hub_configid_sk',
    #     'stg_second_hub_sk_column_name': 'configid_sk'
    # },
    {
        'stg_table_name': 'stg_io_creyt_channel_states',
        'link_table_name': 'l_io_creyt_channel_states_io_device_configs',
        'link_sk_column_name': 'l_io_creyt_channel_states_io_device_configs_sk',
        'link_first_hub_sk_column_name': 'h_io_creyt_channel_state_sk',
        'link_second_hub_sk_column_name': 'h_io_device_config_sk',
        'stg_link_sk_column_name': 'l_hub_configid_sk',
        'stg_second_hub_sk_column_name': 'configid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    # {
    #     'stg_table_name': 'stg_io_creyt_channel_states_v5',
    #     'link_table_name': 'l_io_creyt_channel_states_io_device_configs',
    #     'link_sk_column_name': 'l_io_creyt_channel_states_io_device_configs_sk',
    #     'link_first_hub_sk_column_name': 'h_io_creyt_channel_state_sk',
    #     'link_second_hub_sk_column_name': 'h_io_device_config_sk',
    #     'stg_link_sk_column_name': 'l_hub_configid_sk',
    #     'stg_second_hub_sk_column_name': 'configid_sk'
    # },
    {
        'stg_table_name': 'stg_io_creyt_configs',
        'link_table_name': 'l_io_creyt_configs_io_device_configs',
        'link_sk_column_name': 'l_io_creyt_configs_io_device_configs_sk',
        'link_first_hub_sk_column_name': 'h_io_creyt_config_sk',
        'link_second_hub_sk_column_name': 'h_io_device_config_sk',
        'stg_link_sk_column_name': 'l_hub_configid_sk',
        'stg_second_hub_sk_column_name': 'configid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    # {
    #     'stg_table_name': 'stg_io_creyt_configs_v5',
    #     'link_table_name': 'l_io_creyt_configs_io_device_configs',
    #     'link_sk_column_name': 'l_io_creyt_configs_io_device_configs_sk',
    #     'link_first_hub_sk_column_name': 'h_io_creyt_config_sk',
    #     'link_second_hub_sk_column_name': 'h_io_device_config_sk',
    #     'stg_link_sk_column_name': 'l_hub_configid_sk',
    #     'stg_second_hub_sk_column_name': 'configid_sk'
    # },
    {
        'stg_table_name': 'stg_io_creyt_controller_states',
        'link_table_name': 'l_io_creyt_controller_states_io_device_configs',
        'link_sk_column_name': 'l_io_creyt_controller_states_io_device_configs_sk',
        'link_first_hub_sk_column_name': 'h_io_creyt_controller_state_sk',
        'link_second_hub_sk_column_name': 'h_io_device_config_sk',
        'stg_link_sk_column_name': 'l_hub_configid_sk',
        'stg_second_hub_sk_column_name': 'configid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    # {
    #     'stg_table_name': 'stg_io_creyt_controller_states_v5',
    #     'link_table_name': 'l_io_creyt_controller_states_io_device_configs',
    #     'link_sk_column_name': 'l_io_creyt_controller_states_io_device_configs_sk',
    #     'link_first_hub_sk_column_name': 'h_io_creyt_controller_state_sk',
    #     'link_second_hub_sk_column_name': 'h_io_device_config_sk',
    #     'stg_link_sk_column_name': 'l_hub_configid_sk',
    #     'stg_second_hub_sk_column_name': 'configid_sk'
    # },
    {
        'stg_table_name': 'stg_io_creyt_controller_states',
        'link_table_name': 'l_io_creyt_controller_states_oper_time_properties',
        'link_sk_column_name': 'l_io_creyt_controller_states_oper_time_properties_sk',
        'link_first_hub_sk_column_name': 'h_io_creyt_controller_state_sk',
        'link_second_hub_sk_column_name': 'h_opertime_properties_sk',
        'stg_link_sk_column_name': 'l_hub_opertimepropertyid_sk',
        'stg_second_hub_sk_column_name': 'opertimepropertyid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    # {
    #     'stg_table_name': 'stg_io_creyt_controller_states_v5',
    #     'link_table_name': 'l_io_creyt_controller_states_oper_time_properties',
    #     'link_sk_column_name': 'l_io_creyt_controller_states_oper_time_properties_sk',
    #     'link_first_hub_sk_column_name': 'h_io_creyt_controller_state_sk',
    #     'link_second_hub_sk_column_name': 'h_opertime_properties_sk',
    #     'stg_link_sk_column_name': 'l_hub_opertimepropertyid_sk',
    #     'stg_second_hub_sk_column_name': 'opertimepropertyid_sk'
    # },
    {
        'stg_table_name': 'stg_io_creyt_im_oper_times',
        'link_table_name': 'l_io_creyt_im_oper_times_io_device_configs',
        'link_sk_column_name': 'l_io_creyt_im_oper_times_io_device_configs_sk',
        'link_first_hub_sk_column_name': 'h_io_creyt_im_oper_time_sk',
        'link_second_hub_sk_column_name': 'h_io_device_config_sk',
        'stg_link_sk_column_name': 'l_hub_configid_sk',
        'stg_second_hub_sk_column_name': 'configid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    # {
    #     'stg_table_name': 'stg_io_creyt_im_oper_times_v5',
    #     'link_table_name': 'l_io_creyt_im_oper_times_io_device_configs',
    #     'link_sk_column_name': 'l_io_creyt_im_oper_times_io_device_configs_sk',
    #     'link_first_hub_sk_column_name': 'h_io_creyt_im_oper_time_sk',
    #     'link_second_hub_sk_column_name': 'h_io_device_config_sk',
    #     'stg_link_sk_column_name': 'l_hub_configid_sk',
    #     'stg_second_hub_sk_column_name': 'configid_sk'
    # },
    {
        'stg_table_name': 'stg_io_l_card_channel_configs',
        'link_table_name': 'l_io_lcard_channel_configs_io_device_configs',
        'link_sk_column_name': 'l_io_lcard_channel_configs_io_device_configs_sk',
        'link_first_hub_sk_column_name': 'h_io_lcard_channel_config_sk',
        'link_second_hub_sk_column_name': 'h_io_device_config_sk',
        'stg_link_sk_column_name': 'l_hub_configid_sk',
        'stg_second_hub_sk_column_name': 'configid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_l_card_configs',
        'link_table_name': 'l_io_lcard_configs_io_device_configs',
        'link_sk_column_name': 'l_io_lcard_configs_io_device_configs_sk',
        'link_first_hub_sk_column_name': 'h_io_lcard_config_sk',
        'link_second_hub_sk_column_name': 'h_io_device_config_sk',
        'stg_link_sk_column_name': 'l_hub_configid_sk',
        'stg_second_hub_sk_column_name': 'configid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_l_card_crate_configs',
        'link_table_name': 'l_io_lcard_crate_configs_io_device_configs',
        'link_sk_column_name': 'l_io_lcard_crate_configs_io_device_configs_sk',
        'link_first_hub_sk_column_name': 'h_io_lcard_crate_config_sk',
        'link_second_hub_sk_column_name': 'h_io_device_config_sk',
        'stg_link_sk_column_name': 'l_hub_configid_sk',
        'stg_second_hub_sk_column_name': 'configid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_l_card_input_configs',
        'link_table_name': 'l_io_lcard_input_configs_io_device_configs',
        'link_sk_column_name': 'l_io_lcard_input_configs_io_device_configs_sk',
        'link_first_hub_sk_column_name': 'h_io_lcard_input_config_sk',
        'link_second_hub_sk_column_name': 'h_io_device_config_sk',
        'stg_link_sk_column_name': 'l_hub_configid_sk',
        'stg_second_hub_sk_column_name': 'configid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_l_card_logic_input_configs',
        'link_table_name': 'l_io_lcard_logic_input_configs_io_device_configs',
        'link_sk_column_name': 'l_io_lcard_logic_input_configs_io_device_configs_sk',
        'link_first_hub_sk_column_name': 'h_io_lcard_logic_input_config_sk',
        'link_second_hub_sk_column_name': 'h_io_device_config_sk',
        'stg_link_sk_column_name': 'l_hub_configid_sk',
        'stg_second_hub_sk_column_name': 'configid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_l_card_module_configs',
        'link_table_name': 'l_io_lcard_module_configs_io_device_configs',
        'link_sk_column_name': 'l_io_lcard_module_configs_io_device_configs_sk',
        'link_first_hub_sk_column_name': 'h_io_lcard_module_config_sk',
        'link_second_hub_sk_column_name': 'h_io_device_config_sk',
        'stg_link_sk_column_name': 'l_hub_configid_sk',
        'stg_second_hub_sk_column_name': 'configid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_l_card_crate_sync',
        'link_table_name': 'l_io_lcard_sync_crates_io_device_configs',
        'link_sk_column_name': 'l_io_lcard_sync_crate_io_device_configs_sk',
        'link_first_hub_sk_column_name': 'h_io_lcard_sync_crate_sk',
        'link_second_hub_sk_column_name': 'h_io_device_config_sk',
        'stg_link_sk_column_name': 'l_hub_configid_sk',
        'stg_second_hub_sk_column_name': 'configid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_modbus_tcp_register_bit_decompression_configs',
        'link_table_name': 'l_io_modbus_tcp_reg_configs_io_modbus_tcp_bit_decomn_configs',
        'link_sk_column_name': 'l_io_modbus_tcp_reg_configs_io_modbus_tcp_bit_decomn_configs_sk',
        'link_first_hub_sk_column_name': 'h_io_modbus_tcp_register_config_sk',
        'link_second_hub_sk_column_name': 'h_io_modbus_tcp_bit_decompression_config_sk',
        'stg_link_sk_column_name': 'l_hub_registerid_sk',
        'stg_second_hub_sk_column_name': 'registerid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_modbus_tcp_register_configs',
        'link_table_name': 'l_io_modbus_tcp_register_configs_io_device_configs',
        'link_sk_column_name': 'l_io_modbus_tcp_register_configs_io_device_configs_sk',
        'link_first_hub_sk_column_name': 'h_io_modbus_tcp_register_config_sk',
        'link_second_hub_sk_column_name': 'h_io_device_config_sk',
        'stg_link_sk_column_name': 'l_hub_configid_sk',
        'stg_second_hub_sk_column_name': 'configid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_modbus_tcp_register_configs_tmp',
        'link_table_name': 'l_io_modbus_tcp_register_configs_io_device_configs',
        'link_sk_column_name': 'l_io_modbus_tcp_register_configs_io_device_configs_sk',
        'link_first_hub_sk_column_name': 'h_io_modbus_tcp_register_config_sk',
        'link_second_hub_sk_column_name': 'h_io_device_config_sk',
        'stg_link_sk_column_name': 'l_hub_configid_sk',
        'stg_second_hub_sk_column_name': 'configid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_opc_da_client_configs',
        'link_table_name': 'l_io_opc_da_client_configs_io_device_configs',
        'link_sk_column_name': 'l_io_opc_da_client_configs_io_device_configs_sk',
        'link_first_hub_sk_column_name': 'h_io_opc_da_client_config_sk',
        'link_second_hub_sk_column_name': 'h_io_device_config_sk',
        'stg_link_sk_column_name': 'l_hub_configid_sk',
        'stg_second_hub_sk_column_name': 'configid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_opc_da_client_group_configs',
        'link_table_name': 'l_io_opc_da_client_group_configs_io_device_configs',
        'link_sk_column_name': 'l_io_opc_da_client_group_configs_io_device_configs_sk',
        'link_first_hub_sk_column_name': 'h_io_opc_da_client_group_config_sk',
        'link_second_hub_sk_column_name': 'h_io_device_config_sk',
        'stg_link_sk_column_name': 'l_hub_configid_sk',
        'stg_second_hub_sk_column_name': 'configid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_opc_da_client_item_configs',
        'link_table_name': 'l_io_opc_da_client_item_configs_io_device_configs',
        'link_sk_column_name': 'l_io_opc_da_client_item_configs_io_device_configs_sk',
        'link_first_hub_sk_column_name': 'h_io_opc_da_client_item_config_sk',
        'link_second_hub_sk_column_name': 'h_io_device_config_sk',
        'stg_link_sk_column_name': 'l_hub_configid_sk',
        'stg_second_hub_sk_column_name': 'configid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_opc_ua_client_configs',
        'link_table_name': 'l_io_opc_ua_client_configs_io_device_configs',
        'link_sk_column_name': 'l_io_opc_ua_client_configs_io_device_configs_sk',
        'link_first_hub_sk_column_name': 'h_io_opc_ua_client_config_sk',
        'link_second_hub_sk_column_name': 'h_io_device_config_sk',
        'stg_link_sk_column_name': 'l_hub_configid_sk',
        'stg_second_hub_sk_column_name': 'configid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_opc_ua_client_group_configs',
        'link_table_name': 'l_io_opc_ua_client_group_configs_io_device_configs',
        'link_sk_column_name': 'l_io_opc_ua_client_group_configs_io_device_configs_sk',
        'link_first_hub_sk_column_name': 'h_io_opc_ua_client_group_config_sk',
        'link_second_hub_sk_column_name': 'h_io_device_config_sk',
        'stg_link_sk_column_name': 'l_hub_configid_sk',
        'stg_second_hub_sk_column_name': 'configid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_opc_ua_client_item_configs',
        'link_table_name': 'l_io_opc_ua_client_item_configs_io_device_configs',
        'link_sk_column_name': 'l_io_opc_ua_client_item_configs_io_device_configs_sk',
        'link_first_hub_sk_column_name': 'h_io_opc_ua_client_item_config_sk',
        'link_second_hub_sk_column_name': 'h_io_device_config_sk',
        'stg_link_sk_column_name': 'l_hub_configid_sk',
        'stg_second_hub_sk_column_name': 'configid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
    {
        'stg_table_name': 'stg_io_opc_ua_client_transform_item_configs',
        'link_table_name': 'l_io_opc_ua_client_transform_item_configs_io_device_configs',
        'link_sk_column_name': 'l_io_opc_ua_client_transform_item_configs_io_device_configs_sk',
        'link_first_hub_sk_column_name': 'h_io_opc_ua_client_transform_item_config_sk',
        'link_second_hub_sk_column_name': 'h_io_device_config_sk',
        'stg_link_sk_column_name': 'l_hub_configid_sk',
        'stg_second_hub_sk_column_name': 'configid_sk',
        'stg_first_hub_sk_column_name': 'hash_sk'
    },
]

