main_db_system_type_data_info = {
    "Diagnostic Defect Types": {
        "source_adress": {
            "table_name": "DiagnosticAlarmHistory_v2",
            "column_name": "DefectType"
        },
        "dwh_adress": {
            "table_name": "s_diagnostic_defect_types",
            "column_name": "type_id"
        },
    },
    "Diagnostic Alarm States": {
        "source_adress": {
            "table_name": "DiagnosticAlarmHistory_v2",
            "column_name": "AlarmState"
        },
        "dwh_adress": {
            "table_name": "s_diagnostic_alarm_states",
            "column_name": "state_id"
        },
    },
    "Diagnostic Defect States": {
        "source_adress": {
            "table_name": "DiagnosticAlarmHistory_v2",
            "column_name": "DefectState"
        },
        "dwh_adress": {
            "table_name": "s_diagnostic_defect_states",
            "column_name": "state_id"
        },
    },
    "Config Types": {
        "source_adress": {
            "table_name": "IODeviceConfigs",
            "column_name": "TypeID"
        },
        "dwh_adress": {
            "table_name": "s_config_types",
            "column_name": "type_id"
        },
    },
    "OPC DA Data Types": {
        "source_adress": {
            "table_name": "IOOpcDaClientItemConfigs",
            "column_name": "DataType"
        },
        "dwh_adress": {
            "table_name": "s_opc_da_data_types",
            "column_name": "data_type_id"
        },
    },
    "OPC UA Data Types": {
        "source_adress": {
            "table_name": "IOOpcUaClientItemConfigs",
            "column_name": "DataType"
        },
        "dwh_adress": {
            "table_name": "s_opc_ua_data_types",
            "column_name": "data_type_id"
        },
    },
    "OpcUaClientItemConfigs Server Data Types": {
        "source_adress": {
            "table_name": "IOOpcUaClientItemConfigs",
            "column_name": "DataTypeServer"
        },
        "dwh_adress": {
            "table_name": "s_opc_ua_data_types",
            "column_name": "data_type_id"
        },
    },
    "TIK SCADA Log Types": {
        "source_adress": {
            "table_name": "TikScadaLog",
            "column_name": "Type"
        },
        "dwh_adress": {
            "table_name": "s_tik_scada_log_types",
            "column_name": "log_type_id"
        },
    },
    "Action Types": {
        "source_adress": {
            "table_name": "UserLog",
            "column_name": "ActionType"
        },
        "dwh_adress": {
            "table_name": "s_user_action_types",
            "column_name": "action_type_id"
        },
    },
    "Running Types": {
        "source_adress": {
            "table_name": "ObjectRules",
            "column_name": "RunningType"
        },
        "dwh_adress": {
            "table_name": "s_running_types",
            "column_name": "type_id"
        },
    },
    "Crate Types": {
        "source_adress": {
            "table_name": "IOLCardCrateConfigs",
            "column_name": "CrateType"
        },
        "dwh_adress": {
            "table_name": "s_crate_types",
            "column_name": "type_id"
        },
    },
    "L Card Logic Input Types": {
        "source_adress": {
            "table_name": "IOLCardLogicInputConfigs",
            "column_name": "InputType"
        },
        "dwh_adress": {
            "table_name": "s_l_card_logic_input_types",
            "column_name": "type_id"
        },
    },
    "L Card Crate Module Types": {
        "source_adress": {
            "table_name": "IOLCardModuleConfigs",
            "column_name": "ModuleType"
        },
        "dwh_adress": {
            "table_name": "s_l_card_crate_module_types",
            "column_name": "type_id"
        },
    },
    "Register Types": {
        "source_adress": {
            "table_name": "IOModbusTcpRegisterConfigs",
            "column_name": "RegisterType"
        },
        "dwh_adress": {
            "table_name": "s_register_types",
            "column_name": "type_id"
        },
    },
    "Data Type Names": {
        "source_adress": {
            "table_name": "IOModbusTcpRegisterConfigs",
            "column_name": "DataType"
        },
        "dwh_adress": {
            "table_name": "s_data_type_names",
            "column_name": "type_id"
        },
    },
    "Type Names in TMP": {
        "source_adress": {
            "table_name": "IOModbusTcpRegisterConfigs_TMP",
            "column_name": "DataType"
        },
        "dwh_adress": {
            "table_name": "s_data_type_names",
            "column_name": "type_id"
        },
    },
    "Data Types in common snapshot": {
        "source_adress": {
            "table_name": "ObjectDataValues_common_snapshot",
            "column_name": "DataTypeID"
        },
        "dwh_adress": {
            "table_name": "s_data_types",
            "column_name": "type_id"
        },
    },
    "Data Types in history snapshot": {
        "source_adress": {
            "table_name": "ObjectDataValues_history_snapshot",
            "column_name": "DataTypeID"
        },
        "dwh_adress": {
            "table_name": "s_data_types",
            "column_name": "type_id"
        },
    },
    "Data Types in history snapshot": {
        "source_adress": {
            "table_name": "ObjectDataValues_history_snapshot",
            "column_name": "DataTypeID"
        },
        "dwh_adress": {
            "table_name": "s_data_types",
            "column_name": "type_id"
        },
    },
    "Data Types in snapshot": {
        "source_adress": {
            "table_name": "ObjectDataValues_snapshot",
            "column_name": "DataTypeID"
        },
        "dwh_adress": {
            "table_name": "s_data_types",
            "column_name": "type_id"
        },
    },
    "Data Types in snapshot single": {
        "source_adress": {
            "table_name": "ObjectDataValues_snapshot_single",
            "column_name": "DataTypeID"
        },
        "dwh_adress": {
            "table_name": "s_data_types",
            "column_name": "type_id"
        },
    },
    "Data Types in ObjectDataValues_v2": {
        "source_adress": {
            "table_name": "ObjectDataValues_v2",
            "column_name": "DataTypeID"
        },
        "dwh_adress": {
            "table_name": "s_data_types",
            "column_name": "type_id"
        },
    },
    "Property Types": {
        "source_adress": {
            "table_name": "ObjectPropertyDescriptors",
            "column_name": "PropertyType"
        },
        "dwh_adress": {
            "table_name": "s_property_types",
            "column_name": "type_id"
        },
    },
        "Convert Types": {
        "source_adress": {
            "table_name": "MeasureConvert",
            "column_name": "ConvertType"
        },
        "dwh_adress": {
            "table_name": "s_convert_types",
            "column_name": "convert_type_id"
        },
    },
        "Storage Types": {
        "source_adress": {
            "table_name": "ObjectProperties",
            "column_name": "StorageType"
        },
        "dwh_adress": {
            "table_name": "s_storage_types",
            "column_name": "storage_type_id"
        },
    },
        "Device Types": {
        "source_adress": {
            "table_name": "IOModbusTcpConfigs",
            "column_name": "DeviceType"
        },
        "dwh_adress": {
            "table_name": "s_device_types",
            "column_name": "type_id"
        },
    },
        "Interface Types": {
        "source_adress": {
            "table_name": "IOModbusTcpConfigs",
            "column_name": "InterfaceType"
        },
        "dwh_adress": {
            "table_name": "s_interface_types",
            "column_name": "type_id"
        },
    },
}

double_system_type_data_info = {
    "Data types": {
        "source_adress": {
            "table_name": "DayTable",
            "column_name": "dataTypeID"
        },
        "dwh_adress": {
            "table_name": "s_data_types",
            "column_name": "type_id"
        },
    },
}


any_system_type_data_info = {
    "Data types": {
        "source_adress": {
            "table_name": "DayTable",
            "column_name": "dataTypeID"
        },
        "dwh_adress": {
            "table_name": "s_data_types",
            "column_name": "type_id"
        },
    },
}



fh_system_type_data_info = {
    "Data types": {
        "source_adress": {
            "file_format": "fh",
            "header_field": "DataType",
            "header_offset": "HeaderData[40:44]",  # Смещение в HeaderData после метаданных
            "value_type": "uint32"
        },
        "dwh_adress": {
            "table_name": "s_data_types",
            "column_name": "type_id"
        },
        "type_mapping": {
            1: "MainDb",
            2: "Double",
            3: "Any"
        }
    },
    
    "File structure": {
        "magic_hex": "0xBEBAADDE",
        "extra_magic": "0x4B52494F",  # "KRIO"
        "version": 1,
        "header": {
            "minimum_length": 30,
            "fields": {
                "magic": {"offset": 0, "size": 4, "type": "uint32"},
                "header_length": {"offset": 4, "size": 4, "type": "uint32"},
                "from_date": {"offset": 8, "size": 8, "type": "uint64", "format": "unix_timestamp_ms"},
                "to_date": {"offset": 16, "size": 8, "type": "uint64", "format": "unix_timestamp_ms"},
                "version": {"offset": 24, "size": 2, "type": "uint16"},
                "header_data": {"offset": 26, "size": "variable", "type": "bytes"},
                "crc32": {"offset": "header_length-4", "size": 4, "type": "uint32"}
            }
        },
        "header_data": {
            "data_row_values_count": {"offset": 0, "size": 4, "type": "uint32"},
            "extra_metadata": {
                "signature": {"offset": 4, "size": 4, "type": "uint32", "value": "0x4B52494F"},
                "file_id": {"offset": 8, "size": 16, "type": "guid_le"},
                "main_db_id": {"offset": 24, "size": 16, "type": "guid_le"},
                "main_db_name_length": {"offset": 40, "size": 4, "type": "uint32"},
                "main_db_name": {"offset": 44, "size": "variable", "type": "utf8_string"},
                "data_type": {"offset": "44+name_length", "size": 4, "type": "uint32"}
            }
        },
        "data_section": {
            "row_structure": {
                "property_id": {"size": 16, "type": "guid_le"},
                "values_array": {
                    "count": "data_row_values_count",
                    "value_structure": {
                        "quality": {"size": 1, "type": "uint8"},
                        "value": {"size": 8, "type": "bytes", "interpretation": "double/int64"}
                    },
                    "field_length": 9
                },
                "row_length_formula": "16 + (9 * data_row_values_count)"
            }
        }
    },
    
    "Quality codes": {
        "0": "UNKNOWN",
        "1": "NO_DATA",
        "2": "GOOD",
        "3": "BAD"
    },
    
    "Value interpretations": {
        "double": {
            "size": 8,
            "format": "little-endian IEEE 754",
            "python_unpack": "<d"
        },
        "int64": {
            "size": 8,
            "format": "little-endian signed integer",
            "python_unpack": "<q"
        }
    },
    
    "Validation rules": {
        "magic_check": "magic == 0xBEBAADDE",
        "crc32_check": "stored_crc32 == calculated_crc32(header_data)",
        "header_length_check": "header_length >= 30 AND header_length <= file_size",
        "version_check": "version == 1",
        "extra_magic_check": "header_data[4:8] == 0x4B52494F",
        "data_size_check": "(file_size - header_length) % row_length == 0",
        "dates_check": "from_date < to_date",
        "data_type_check": "data_type IN (1, 2, 3)"
    }
}