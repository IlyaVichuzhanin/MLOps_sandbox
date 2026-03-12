

main_db_reference_data_adresses = {
    "Diagnostic Defect Types": {
        "main_db_adress": {
            "table_name": "DiagnosticAlarmHistory_v2",
            "column_name": "DefectType"
        },
        "dwh_adress": {
            "table_name": "s_diagnostic_defect_types",
            "column_name": "type_id"
        },
    },
    "Diagnostic Alarm States": {
        "main_db_adress": {
            "table_name": "DiagnosticAlarmHistory_v2",
            "column_name": "AlarmState"
        },
        "dwh_adress": {
            "table_name": "s_diagnostic_alarm_states",
            "column_name": "state_id"
        },
    },
    "Diagnostic Defect States": {
        "main_db_adress": {
            "table_name": "DiagnosticAlarmHistory_v2",
            "column_name": "DefectState"
        },
        "dwh_adress": {
            "table_name": "s_diagnostic_defect_states",
            "column_name": "state_id"
        },
    },
    "Config Types": {
        "main_db_adress": {
            "table_name": "IODeviceConfigs",
            "column_name": "TypeID"
        },
        "dwh_adress": {
            "table_name": "s_config_types",
            "column_name": "type_id"
        },
    },
    "Index Type Data": {
        "main_db_adress": {
            "table_name": "IOMQTTDevEUIConfigs",
            "column_name": "IndexTypeData"
        },
        "dwh_adress": {
            "table_name": "s_index_type_data_records",
            "column_name": "index_type_id"
        },
    },
    "OPC DA Data Types": {
        "main_db_adress": {
            "table_name": "IOOpcDaClientItemConfigs",
            "column_name": "DataType"
        },
        "dwh_adress": {
            "table_name": "s_opc_da_data_types",
            "column_name": "data_type_id"
        },
    },
    "OPC UA Data Types": {
        "main_db_adress": {
            "table_name": "IOOpcUaClientItemConfigs",
            "column_name": "DataType"
        },
        "dwh_adress": {
            "table_name": "s_opc_ua_data_types",
            "column_name": "data_type_id"
        },
    },
    "OpcUaClientItemConfigs Server Data Types": {
        "main_db_adress": {
            "table_name": "IOOpcUaClientItemConfigs",
            "column_name": "DataTypeServer"
        },
        "dwh_adress": {
            "table_name": "s_opc_ua_data_types",
            "column_name": "data_type_id"
        },
    },
    "TIK SCADA Log Types": {
        "main_db_adress": {
            "table_name": "TikScadaLog",
            "column_name": "Type"
        },
        "dwh_adress": {
            "table_name": "s_tik_scada_log_types",
            "column_name": "log_type_id"
        },
    },
    "Action Types": {
        "main_db_adress": {
            "table_name": "UserLog",
            "column_name": "ActionType"
        },
        "dwh_adress": {
            "table_name": "s_user_action_types",
            "column_name": "action_type_id"
        },
    },
    "Running Types": {
        "main_db_adress": {
            "table_name": "ObjectRules",
            "column_name": "RunningType"
        },
        "dwh_adress": {
            "table_name": "s_running_types",
            "column_name": "type_id"
        },
    },
    "Crate Types": {
        "main_db_adress": {
            "table_name": "IOLCardCrateConfigs",
            "column_name": "CrateType"
        },
        "dwh_adress": {
            "table_name": "s_crate_types",
            "column_name": "type_id"
        },
    },
    "L Card Logic Input Types": {
        "main_db_adress": {
            "table_name": "IOLCardLogicInputConfigs",
            "column_name": "InputType"
        },
        "dwh_adress": {
            "table_name": "s_l_card_logic_input_types",
            "column_name": "type_id"
        },
    },
    "L Card Crate Module Types": {
        "main_db_adress": {
            "table_name": "IOLCardModuleConfigs",
            "column_name": "ModuleType"
        },
        "dwh_adress": {
            "table_name": "s_l_card_crate_module_types",
            "column_name": "type_id"
        },
    },
    "Register Types": {
        "main_db_adress": {
            "table_name": "IOModbusTcpRegisterConfigs",
            "column_name": "RegisterType"
        },
        "dwh_adress": {
            "table_name": "s_register_types",
            "column_name": "type_id"
        },
    },
    "Type Names": {
        "main_db_adress": {
            "table_name": "IOModbusTcpRegisterConfigs",
            "column_name": "DataType"
        },
        "dwh_adress": {
            "table_name": "s_type_names",
            "column_name": "type_id"
        },
    },
    "Type Names in TMP": {
        "main_db_adress": {
            "table_name": "IOModbusTcpRegisterConfigs_TMP",
            "column_name": "DataType"
        },
        "dwh_adress": {
            "table_name": "s_type_names",
            "column_name": "type_id"
        },
    },
    "Data Types in common snapshot": {
        "main_db_adress": {
            "table_name": "ObjectDataValues_common_snapshot",
            "column_name": "DataTypeID"
        },
        "dwh_adress": {
            "table_name": "s_data_types",
            "column_name": "type_id"
        },
    },
    "Data Types in common snapshot save": {
        "main_db_adress": {
            "table_name": "ObjectDataValues_common_snapshot_save",
            "column_name": "DataTypeID"
        },
        "dwh_adress": {
            "table_name": "s_data_types",
            "column_name": "type_id"
        },
    },
    "Data Types in history snapshot": {
        "main_db_adress": {
            "table_name": "ObjectDataValues_history_snapshot",
            "column_name": "DataTypeID"
        },
        "dwh_adress": {
            "table_name": "s_data_types",
            "column_name": "type_id"
        },
    },
    "Data Types in history snapshot": {
        "main_db_adress": {
            "table_name": "ObjectDataValues_history_snapshot",
            "column_name": "DataTypeID"
        },
        "dwh_adress": {
            "table_name": "s_data_types",
            "column_name": "type_id"
        },
    },
    "Data Types in snapshot": {
        "main_db_adress": {
            "table_name": "ObjectDataValues_snapshot",
            "column_name": "DataTypeID"
        },
        "dwh_adress": {
            "table_name": "s_data_types",
            "column_name": "type_id"
        },
    },
    "Data Types in snapshot single": {
        "main_db_adress": {
            "table_name": "ObjectDataValues_snapshot_single",
            "column_name": "DataTypeID"
        },
        "dwh_adress": {
            "table_name": "s_data_types",
            "column_name": "type_id"
        },
    },
    "Data Types in ObjectDataValues_v2": {
        "main_db_adress": {
            "table_name": "ObjectDataValues_v2",
            "column_name": "DataTypeID"
        },
        "dwh_adress": {
            "table_name": "s_data_types",
            "column_name": "type_id"
        },
    },
    "Property Types": {
        "main_db_adress": {
            "table_name": "ObjectPropertyDescriptors",
            "column_name": "PropertyType"
        },
        "dwh_adress": {
            "table_name": "s_property_types",
            "column_name": "type_id"
        },
    },
        "Convert Types": {
        "main_db_adress": {
            "table_name": "MeasureConvert",
            "column_name": "ConvertType"
        },
        "dwh_adress": {
            "table_name": "s_convert_types",
            "column_name": "convert_type_id"
        },
    },
        "Storage Types": {
        "main_db_adress": {
            "table_name": "ObjectProperties",
            "column_name": "StorageType"
        },
        "dwh_adress": {
            "table_name": "s_storage_types",
            "column_name": "storage_type_id"
        },
    },
}