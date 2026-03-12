source_to_staging_info = {
    'MeasureGroups': {
        'source_table_name': 'MeasureGroups',
        'hash_gener_columns': ['ID'],
        'source_column_names': ['ID', 'Name'],
        'staging_table_name': 'stg_measure_groups',
        'column_types': {
            'id': 'uuid',
            'name': 'string'
        }
    },
    'AggregateNotificationConfigs': {
        'source_table_name': 'AggregateNotificationConfigs',
        'hash_gener_columns': ['Id'],
        'source_column_names': ['Id', 'ConfigId', 'EmailAddress', 'Aggregate', 'SendIsEnabled', 'CountRepeats', 'MinDefectLevel', 'ResendIsEnabled', 'ResendTimeSpan', 'NotificationLanguage'],
        'staging_table_name': 'stg_aggregate_notification_configs',
        'column_types': {
            'id': 'uuid',
            'config_id': 'uuid',
            'email_address': 'uuid',
            'aggregate': 'uuid',
            'send_is_enabled': 'integer',
            'count_repeats': 'integer',
            'min_defect_level': 'integer',
            'resend_is_enabled': 'integer',
            'resend_time_span': 'integer',
            'notification_language': 'integer'
        }
    },
    'Aggregates': {
        'source_table_name': 'Aggregates',
        'hash_gener_columns': ['Id'],
        'source_column_names': ['Id', 'AggregatePath'],
        'staging_table_name': 'stg_aggregates',
        'column_types': {
            'id': 'uuid',
            'aggregatepath': 'string'
        }
    },
    'Annotation': {
        'source_table_name': 'Annotation',
        'hash_gener_columns': ['AnnotationId'],
        'source_column_names': ['Id', 'AnnotationId', 'NameGraphics', 'FullPath', 'Property', 'SelectedInterval', 'AnnotationType', 'UserLogin', 'DateCreate', 'JsonData'],
        'staging_table_name': 'stg_annotation',
        'column_types': {
            'id': 'integer',
            'annotationid': 'uuid',
            'namegraphics': 'string',
            'fullpath': 'string',
            'property': 'string',
            'selectedinterval': 'string',
            'annotationtype': 'string',
            'userlogin': 'string',
            'datecreate': 'string',  # ⚠️ В DDL это text, не timestamp!
            'jsondata': 'string'
        }
    },
    'Bearings': {
        'source_table_name': 'Bearings',
        'hash_gener_columns': ['BearingID'],
        'source_column_names': ['BearingID', 'Number', 'OuterRace_D', 'InnerRace_D', 'RollingElement_D', 'RollingElement_Count', 'ContactAngle', 'BPFI', 'BPFO', 'BSF', 'FTF', 'ServiceLife', 'DateCreated', 'DateModified', 'CdbSyncDate', 'CdbVersion', 'CdbID'],
        'staging_table_name': 'stg_bearings',
        'column_types': {
            'bearingid': 'uuid',
            'number': 'string',
            'outerrace_d': 'float',
            'innerrace_d': 'float',
            'rollingelement_d': 'float',
            'rollingelement_count': 'integer',
            'contactangle': 'float',
            'bpfi': 'float',
            'bpfo': 'float',
            'bsf': 'float',
            'ftf': 'float',
            'servicelife': 'integer',
            'datecreated': 'timestamp',
            'datemodified': 'timestamp',
            'cdbsyncdate': 'timestamp',
            'cdbversion': 'integer',
            'cdbid': 'uuid'
        }
    },
    'DiagnosticAlarmHistory_v2': {
        'source_table_name': 'DiagnosticAlarmHistory_v2',
        'hash_gener_columns': ['DiagAlarmID'],
        'source_column_names': ['DiagAlarmID', 'PropertyID', 'DiagID', 'DiagTagName', 'AlarmState', 'Date', 'Confirmed', 'Comment', 'DefectState', 'DefectName', 'DefectDetails', 'Recommendation', 'Priority', 'GroupName', 'DefectType'],
        'staging_table_name': 'stg_diagnostic_alarm_history_v2',
        'column_types': {
            'diagalarmid': 'uuid',
            'propertyid': 'uuid',
            'diagid': 'uuid',
            'diagtagname': 'string',
            'alarmstate': 'integer',
            'date': 'timestamp',
            'confirmed': 'boolean',
            'comment': 'string',
            'defectstate': 'integer',
            'defectname': 'string',
            'defectdetails': 'string',
            'recommendation': 'string',
            'priority': 'integer',
            'groupname': 'string',
            'defecttype': 'integer'
        }
    },
    'DiagnosticAlarms_v2': {
        'source_table_name': 'DiagnosticAlarms_v2',
        'hash_gener_columns': ['DiagAlarmID'],
        'source_column_names': ['DiagAlarmID', 'PropertyID', 'DiagID', 'DiagTagName', 'AlarmState', 'Date', 'Confirmed', 'Comment', 'DefectState', 'DefectName', 'DefectDetails', 'Recommendation', 'Priority', 'GroupName', 'DefectType'],
        'staging_table_name': 'stg_diagnostic_alarms_v2',
        'column_types': {
            'diagalarmid': 'uuid',
            'propertyid': 'uuid',
            'diagid': 'uuid',
            'diagtagname': 'string',
            'alarmstate': 'integer',
            'date': 'timestamp',
            'confirmed': 'boolean',
            'comment': 'string',
            'defectstate': 'integer',
            'defectname': 'string',
            'defectdetails': 'string',
            'recommendation': 'string',
            'priority': 'integer',
            'groupname': 'string',
            'defecttype': 'integer'
        }
    },
    'EmailAddresses': {
        'source_table_name': 'EmailAddresses',
        'hash_gener_columns': ['Id'],
        'source_column_names': ['Id', 'EmailAddress'],
        'staging_table_name': 'stg_email_addresses',
        'column_types': {
            'id': 'uuid',
            'emailaddress': 'string'
        }
    },
    'IOCreytChannelConfigs': {
        'source_table_name': 'IOCreytChannelConfigs',
        'hash_gener_columns': ['ConfigID'],
        'source_column_names': ['ConfigID', 'ChannelNum', 'PropertyID', 'MinRaw', 'MaxRaw', 'MinEU', 'MaxEU', 'MeasureUnitID'],
        'staging_table_name': 'stg_io_creyt_channel_configs',
        'column_types': {
            'configid': 'uuid',
            'channelnum': 'integer',
            'propertyid': 'uuid',
            'minraw': 'float',
            'maxraw': 'float',
            'mineu': 'float',
            'maxeu': 'float',
            'measureunitid': 'uuid'
        }
    },
    'IOCreytChannelConfigs_v5': {
        'source_table_name': 'IOCreytChannelConfigs_v5',
        'hash_gener_columns': ['ConfigID'],
        'source_column_names': ['ConfigID', 'ChannelNum', 'PropertyID', 'MinRaw', 'MaxRaw', 'MinEU', 'MaxEU', 'MeasureUnitID'],
        'staging_table_name': 'stg_io_creyt_channel_configs_v5',
        'column_types': {
            'configid': 'uuid',
            'channelnum': 'integer',
            'propertyid': 'uuid',
            'minraw': 'float',
            'maxraw': 'float',
            'mineu': 'float',
            'maxeu': 'float',
            'measureunitid': 'uuid'
        }
    },
    'IOCreytChannelStates': {
        'source_table_name': 'IOCreytChannelStates',
        'hash_gener_columns': ['ConfigID'],
        'source_column_names': ['ConfigID', 'ChannelNum', 'PropertyID'],
        'staging_table_name': 'stg_io_creyt_channel_states',
        'column_types': {
            'configid': 'uuid',
            'channelnum': 'integer',
            'propertyid': 'uuid'
        }
    },
    'IOCreytChannelStates_v5': {
        'source_table_name': 'IOCreytChannelStates_v5',
        'hash_gener_columns': ['ConfigID'],
        'source_column_names': ['ConfigID', 'ChannelNum', 'PropertyID'],
        'staging_table_name': 'stg_io_creyt_channel_states_v5',
        'column_types': {
            'configid': 'uuid',
            'channelnum': 'integer',
            'propertyid': 'uuid'
        }
    },
    'IOCreytConfigs': {
        'source_table_name': 'IOCreytConfigs',
        'hash_gener_columns': ['ConfigID'],
        'source_column_names': ['ConfigID', 'PrimaryIP', 'SecondaryIP', 'PrimaryModbusPort', 'SecondaryModbusPort', 'PrimaryHttpPort', 'SecondaryHttpPort', 'ScanTime', 'ConnectRetries', 'SamplesFileName', 'IsSyncReadSlave', 'SyncReadGroupName', 'DetectFailures', 'DetectFailureLength', 'EnabledSecondaryIP', 'NumBlockSamples'],
        'staging_table_name': 'stg_io_creyt_configs',
        'column_types': {
            'configid': 'uuid',
            'primaryip': 'string',
            'secondaryip': 'string',
            'primarymodbusport': 'integer',
            'secondarymodbusport': 'integer',
            'primaryhttpport': 'integer',
            'secondaryhttpport': 'integer',
            'scantime': 'integer',
            'connectretries': 'integer',
            'samplesfilename': 'string',
            'issyncreadslave': 'boolean',
            'syncreadgroupname': 'string',
            'detectfailures': 'boolean',
            'detectfailurelength': 'integer',
            'enabledsecondaryip': 'boolean',
            'numblocksamples': 'integer'
        }
    },
    'IOCreytConfigs_v5': {
        'source_table_name': 'IOCreytConfigs_v5',
        'hash_gener_columns': ['ConfigID'],
        'source_column_names': ['ConfigID', 'PrimaryIP', 'SecondaryIP', 'PrimaryModbusPort', 'SecondaryModbusPort', 'PrimaryHttpPort', 'SecondaryHttpPort', 'ScanTime', 'ConnectRetries', 'SamplesFileName', 'IsSyncReadSlave', 'SyncReadGroupName', 'DetectFailures', 'DetectFailureLength', 'EnabledSecondaryIP', 'NumBlockSamples'],
        'staging_table_name': 'stg_io_creyt_configs_v5',
        'column_types': {
            'configid': 'uuid',
            'primaryip': 'string',
            'secondaryip': 'string',
            'primarymodbusport': 'integer',
            'secondarymodbusport': 'integer',
            'primaryhttpport': 'integer',
            'secondaryhttpport': 'integer',
            'scantime': 'integer',
            'connectretries': 'integer',
            'samplesfilename': 'string',
            'issyncreadslave': 'boolean',
            'syncreadgroupname': 'string',
            'detectfailures': 'boolean',
            'detectfailurelength': 'integer',
            'enabledsecondaryip': 'boolean',
            'numblocksamples': 'integer'
        }
    },
    'IOCreytControllerStates': {
        'source_table_name': 'IOCreytControllerStates',
        'hash_gener_columns': ['ConfigID'],
        'source_column_names': ['ConfigID', 'RowNum', 'OperTimePropertyID', 'StatePropertyID'],
        'staging_table_name': 'stg_io_creyt_controller_states',
        'column_types': {
            'configid': 'uuid',
            'rownum': 'integer',
            'opertimepropertyid': 'uuid',
            'statepropertyid': 'uuid'
        }
    },
    'IOCreytControllerStates_v5': {
        'source_table_name': 'IOCreytControllerStates_v5',
        'hash_gener_columns': ['ConfigID'],
        'source_column_names': ['ConfigID', 'RowNum', 'OperTimePropertyID', 'StatePropertyID'],
        'staging_table_name': 'stg_io_creyt_controller_states_v5',
        'column_types': {
            'configid': 'uuid',
            'rownum': 'integer',
            'opertimepropertyid': 'uuid',
            'statepropertyid': 'uuid'
        }
    },
    'IOCreytIMOperTimes': {
        'source_table_name': 'IOCreytIMOperTimes',
        'hash_gener_columns': ['ConfigID'],
        'source_column_names': ['ConfigID', 'PropertyID'],
        'staging_table_name': 'stg_io_creyt_im_oper_times',
        'column_types': {
            'configid': 'uuid',
            'propertyid': 'uuid'
        }
    },
    'IOCreytIMOperTimes_v5': {
        'source_table_name': 'IOCreytIMOperTimes_v5',
        'hash_gener_columns': ['ConfigID'],
        'source_column_names': ['ConfigID', 'PropertyID'],
        'staging_table_name': 'stg_io_creyt_im_oper_times_v5',
        'column_types': {
            'configid': 'uuid',
            'propertyid': 'uuid'
        }
    },
    'IODeviceConfigNodes': {
        'source_table_name': 'IODeviceConfigNodes',
        'hash_gener_columns': ['ID'],
        'source_column_names': ['ID', 'Name', 'ParentNodeID'],
        'staging_table_name': 'stg_io_device_config_nodes',
        'column_types': {
            'id': 'uuid',
            'name': 'string',
            'parentnodeid': 'uuid'
        }
    },
    'IODeviceConfigs': {
        'source_table_name': 'IODeviceConfigs',
        'hash_gener_columns': ['ID'],
        'source_column_names': ['ID', 'Name', 'TypeID', 'ConfigNodeID', 'Enabled', 'SetNumber'],
        'staging_table_name': 'stg_io_device_configs',
        'column_types': {
            'id': 'uuid',
            'name': 'string',
            'typeid': 'uuid',
            'confignodeid': 'uuid',
            'enabled': 'boolean',  # ✅ В DDL это boolean, не integer!
            'setnumber': 'integer'
        }
    },
    'IOLCardChannelConfigs': {
        'source_table_name': 'IOLCardChannelConfigs',
        'hash_gener_columns': ['ConfigID'],
        'source_column_names': ['ConfigID', 'Slot', 'ChannelNumber', 'PropertyID', 'IsScaled', 'MinRaw', 'MaxRaw', 'MinEU', 'MaxEU', 'MeasureUnitID', 'PropertyMarkID'],
        'staging_table_name': 'stg_io_l_card_channel_configs',
        'column_types': {
            'configid': 'uuid',
            'slot': 'integer',
            'channelnumber': 'integer',
            'propertyid': 'uuid',
            'isscaled': 'integer',  # ⚠️ В DDL это integer, не boolean
            'minraw': 'float',
            'maxraw': 'float',
            'mineu': 'float',
            'maxeu': 'float',
            'measureunitid': 'uuid',
            'propertymarkid': 'uuid'
        }
    },
    'IOLCardConfigs': {
        'source_table_name': 'IOLCardConfigs',
        'hash_gener_columns': ['ConfigID'],
        'source_column_names': ['ConfigID', 'VirtualSlot', 'SamplingRate', 'OperationMode', 'TimerSamplingTime', 'TimerScanTime', 'SyncSamplingTime'],
        'staging_table_name': 'stg_io_l_card_configs',
        'column_types': {
            'configid': 'uuid',
            'virtualslot': 'integer',
            'samplingrate': 'float',
            'operationmode': 'integer',
            'timersamplingtime': 'integer',
            'timerscantime': 'integer',
            'syncsamplingtime': 'integer'
        }
    },
    'IOLCardCrateConfigs': {
        'source_table_name': 'IOLCardCrateConfigs',
        'hash_gener_columns': ['ConfigID'],
        'source_column_names': ['ConfigID', 'IP', 'Port', 'CrateType', 'SerialNumber', 'SamplingTime', 'ScanTime'],
        'staging_table_name': 'stg_io_l_card_crate_configs',
        'column_types': {
            'configid': 'uuid',
            'ip': 'string',
            'port': 'integer',
            'cratetype': 'integer',
            'serialnumber': 'string',
            'samplingtime': 'integer',
            'scantime': 'integer'
        }
    },
    'IOLCardCrateSync': {
        'source_table_name': 'IOLCardCrateSync',
        'hash_gener_columns': ['ConfigID'],
        'source_column_names': ['ConfigID', 'IsSync', 'IsLeader', 'IsSlave', 'LeaderID'],
        'staging_table_name': 'stg_io_l_card_crate_sync',
        'column_types': {
            'configid': 'uuid',
            'issync': 'integer',  # ⚠️ В DDL это integer, не boolean
            'isleader': 'integer',
            'isslave': 'integer',
            'leaderid': 'uuid'
        }
    },
    'IOLCardInputConfigs': {
        'source_table_name': 'IOLCardInputConfigs',
        'hash_gener_columns': ['ConfigID'],
        'source_column_names': ['ConfigID', 'InputNum', 'PropertyID', 'InputRange', 'IsScale', 'MinRaw', 'MaxRaw', 'MinEU', 'MaxEU', 'MeasureUnitID'],
        'staging_table_name': 'stg_io_l_card_input_configs',
        'column_types': {
            'configid': 'uuid',
            'inputnum': 'integer',
            'propertyid': 'uuid',
            'inputrange': 'integer',
            'isscale': 'boolean',
            'minraw': 'float',
            'maxraw': 'float',
            'mineu': 'float',
            'maxeu': 'float',
            'measureunitid': 'uuid'
        }
    },
    'IOLCardLogicInputConfigs': {
        'source_table_name': 'IOLCardLogicInputConfigs',
        'hash_gener_columns': ['ConfigID'],
        'source_column_names': ['ConfigID', 'InputNumber', 'InputType', 'TimerPropertyID', 'SyncPropertyID', 'InputRange', 'IsScale', 'MinRaw', 'MaxRaw', 'MinEU', 'MaxEU', 'MeasureUnitID'],
        'staging_table_name': 'stg_io_l_card_logic_input_configs',
        'column_types': {
            'configid': 'uuid',
            'inputnumber': 'integer',
            'inputtype': 'integer',
            'timerpropertyid': 'uuid',
            'syncpropertyid': 'uuid',
            'inputrange': 'integer',
            'isscale': 'boolean',
            'minraw': 'float',
            'maxraw': 'float',
            'mineu': 'float',
            'maxeu': 'float',
            'measureunitid': 'uuid'
        }
    },
    'IOLCardModuleConfigs': {
        'source_table_name': 'IOLCardModuleConfigs',
        'hash_gener_columns': ['ConfigID'],
        'source_column_names': ['ConfigID', 'Slot', 'ModuleType', 'FrequencyDivisor'],
        'staging_table_name': 'stg_io_l_card_module_configs',
        'column_types': {
            'configid': 'uuid',
            'slot': 'integer',
            'moduletype': 'integer',
            'frequencydivisor': 'integer'
        }
    },
    'IOMQTTConfigs': {
        'source_table_name': 'IOMQTTConfigs',
        'hash_gener_columns': ['ConfigID'],
        'source_column_names': ['ConfigID', 'Address', 'ClientID', 'Login', 'Password', 'Tls', 'CleanSession'],
        'staging_table_name': 'stg_io_mqtt_configs',
        'column_types': {
            'configid': 'uuid',
            'address': 'string',
            'clientid': 'string',
            'login': 'string',
            'password': 'string',
            'tls': 'boolean',
            'cleansession': 'boolean'
        }
    },
    'IOMQTTDevEUIConfigs': {
        'source_table_name': 'IOMQTTDevEUIConfigs',
        'hash_gener_columns': ['ConfigID'],
        'source_column_names': ['ConfigID', 'Topic', 'DevEUI', 'Version', 'Parser', 'IndexTypeData', 'PropertyID', 'MeasureUnitID'],
        'staging_table_name': 'stg_io_mqtt_dev_eui_configs',
        'column_types': {
            'configid': 'uuid',
            'topic': 'string',
            'deveui': 'string',
            'version': 'integer',
            'parser': 'integer',
            'indextypedata': 'integer',
            'propertyid': 'uuid',
            'measureunitid': 'uuid'
        }
    },
    'IOModbusTcpConfigs': {
        'source_table_name': 'IOModbusTcpConfigs',
        'hash_gener_columns': ['ConfigID'],
        'source_column_names': ['ConfigID', 'DeviceID', 'PrimaryIP', 'SecondaryIP', 'PrimaryPort', 'SecondaryPort', 'ScanTime', 'CoilStatusMax', 'InputStatusMax', 'HoldingRegisterMax', 'InputRegisterMax', 'ConnectRetries', 'SecondaryEnabled', 'DeviceType', 'ConnectToPtimaryIfAvailable', 'SyncReadGroupName', 'InterfaceType', 'Serial_PortName_Primary', 'Serial_PortName_Secondary', 'Serial_BaudRate', 'Serial_Parity', 'Serial_BitCount', 'Serial_StopBits', 'LogLevel', 'ReceiveTimeout', 'WriteTimeout', 'PeriodWrite'],
        'staging_table_name': 'stg_io_modbus_tcp_configs',
        'column_types': {
            'configid': 'uuid',
            'deviceid': 'integer',
            'primaryip': 'string',
            'secondaryip': 'string',
            'primaryport': 'integer',
            'secondaryport': 'integer',
            'scantime': 'integer',
            'coilstatusmax': 'integer',
            'inputstatusmax': 'integer',
            'holdingregistermax': 'integer',
            'inputregistermax': 'integer',
            'connectretries': 'integer',
            'secondaryenabled': 'boolean',
            'devicetype': 'integer',
            'connecttoptimaryifavailable': 'boolean',
            'syncreadgroupname': 'string',
            'interfacetype': 'integer',
            'serial_portname_primary': 'string',
            'serial_portname_secondary': 'string',
            'serial_baudrate': 'integer',
            'serial_parity': 'integer',
            'serial_bitcount': 'integer',
            'serial_stopbits': 'integer',
            'loglevel': 'integer',
            'receivetimeout': 'integer',
            'writetimeout': 'integer',
            'periodwrite': 'integer'
        }
    },
    'IOModbusTcpRegisterConfigs': {
        'source_table_name': 'IOModbusTcpRegisterConfigs',
        'hash_gener_columns': ['ConfigID'],
        'source_column_names': ['ConfigID', 'RegisterType', 'RegisterAddress', 'DataType', 'ByteOrder', 'PropertyID', 'MeasureUnitID', 'MinRaw', 'MaxRaw', 'MinEU', 'MaxEU', 'IsScale', 'Factor', 'Id', 'IsWrite', 'WritePropertyID'],
        'staging_table_name': 'stg_io_modbus_tcp_register_configs',
        'column_types': {
            'configid': 'uuid',
            'registertype': 'integer',
            'registeraddress': 'integer',
            'datatype': 'integer',
            'byteorder': 'string',
            'propertyid': 'uuid',
            'measureunitid': 'uuid',
            'minraw': 'float',
            'maxraw': 'float',
            'mineu': 'float',
            'maxeu': 'float',
            'isscale': 'boolean',
            'factor': 'float',
            'id': 'uuid',
            'iswrite': 'boolean',
            'writepropertyid': 'uuid'
        }
    },
    'IOModbusTcpRegisterConfigs_TMP': {
        'source_table_name': 'IOModbusTcpRegisterConfigs_TMP',
        'hash_gener_columns': ['ConfigID'],
        'source_column_names': ['ConfigID', 'RegisterType', 'RegisterAddress', 'DataType', 'ByteOrder', 'PropertyID', 'MeasureUnitID', 'MinRaw', 'MaxRaw', 'MinEU', 'MaxEU', 'IsScale', 'Factor'],
        'staging_table_name': 'stg_io_modbus_tcp_register_configs_tmp',
        'column_types': {
            'configid': 'uuid',
            'registertype': 'integer',
            'registeraddress': 'integer',
            'datatype': 'integer',
            'byteorder': 'string',
            'propertyid': 'uuid',
            'measureunitid': 'uuid',
            'minraw': 'float',
            'maxraw': 'float',
            'mineu': 'float',
            'maxeu': 'float',
            'isscale': 'boolean',
            'factor': 'float'
        }
    },
    'IOModbustcpRegisterBitDecompressionConfigs': {
        'source_table_name': 'IOModbustcpRegisterBitDecompressionConfigs',
        'hash_gener_columns': ['Id'],
        'source_column_names': ['Id', 'RegisterId', 'PropertyId', 'BitNumber', 'IsWrite', 'WritePropertyId', 'MakeInversion'],
        'staging_table_name': 'stg_io_modbus_tcp_register_bit_decompression_configs',
        'column_types': {
            'id': 'uuid',
            'registerid': 'uuid',
            'propertyid': 'uuid',
            'bitnumber': 'integer',
            'iswrite': 'boolean',
            'writepropertyid': 'uuid',
            'makeinversion': 'boolean'
        }
    },
    'IOOpcDaClientConfigs': {
        'source_table_name': 'IOOpcDaClientConfigs',
        'hash_gener_columns': ['ConfigID'],
        'source_column_names': ['ConfigID', 'IP', 'ServerName', 'ConnectRetries'],
        'staging_table_name': 'stg_io_opc_da_client_configs',
        'column_types': {
            'configid': 'uuid',
            'ip': 'string',
            'servername': 'string',
            'connectretries': 'integer'
        }
    },
    'IOOpcDaClientGroupConfigs': {
        'source_table_name': 'IOOpcDaClientGroupConfigs',
        'hash_gener_columns': ['ConfigID'],
        'source_column_names': ['ConfigID', 'Name', 'IsActive', 'UpdateRate'],
        'staging_table_name': 'stg_io_opc_da_client_group_configs',
        'column_types': {
            'configid': 'uuid',
            'name': 'string',
            'isactive': 'boolean',
            'updaterate': 'integer'
        }
    },
    'IOOpcDaClientItemConfigs': {
        'source_table_name': 'IOOpcDaClientItemConfigs',
        'hash_gener_columns': ['ConfigID'],
        'source_column_names': ['ConfigID', 'PropertyID', 'ItemID', 'GroupName', 'IsActive', 'DataType'],
        'staging_table_name': 'stg_io_opc_da_client_item_configs',
        'column_types': {
            'configid': 'uuid',
            'propertyid': 'uuid',
            'itemid': 'string',
            'groupname': 'string',
            'isactive': 'boolean',
            'datatype': 'integer'
        }
    },
    'IOOpcUaClientConfigs': {
        'source_table_name': 'IOOpcUaClientConfigs',
        'hash_gener_columns': ['ConfigID'],
        'source_column_names': ['ConfigID', 'IP', 'ServerName', 'ConnectRetries', 'AuthenticationId', 'UserName', 'Password', 'CertificateFilePath', 'PrivateKeyFilePath', 'NumBlockSamples', 'NumGroupSample'],
        'staging_table_name': 'stg_io_opc_ua_client_configs',
        'column_types': {
            'configid': 'uuid',
            'ip': 'string',
            'servername': 'string',
            'connectretries': 'integer',
            'authenticationid': 'integer',
            'username': 'string',
            'password': 'string',
            'certificatefilepath': 'string',
            'privatekeyfilepath': 'string',
            'numblocksamples': 'integer',
            'numgroupsample': 'integer'
        }
    },
    'IOOpcUaClientGroupConfigs': {
        'source_table_name': 'IOOpcUaClientGroupConfigs',
        'hash_gener_columns': ['ConfigID'],
        'source_column_names': ['ConfigID', 'Name', 'IsActive', 'UpdateRate', 'IsSubscription'],
        'staging_table_name': 'stg_io_opc_ua_client_group_configs',
        'column_types': {
            'configid': 'uuid',
            'name': 'string',
            'isactive': 'boolean',
            'updaterate': 'integer',
            'issubscription': 'boolean'
        }
    },
    'IOOpcUaClientItemConfigs': {
        'source_table_name': 'IOOpcUaClientItemConfigs',
        'hash_gener_columns': ['ConfigID'],
        'source_column_names': ['ConfigID', 'PropertyID', 'ItemID', 'GroupName', 'IsActive', 'DataType', 'FullPathName', 'ToServer', 'DataTypeServer'],
        'staging_table_name': 'stg_io_opc_ua_client_item_configs',
        'column_types': {
            'configid': 'uuid',
            'propertyid': 'uuid',
            'itemid': 'string',
            'groupname': 'string',
            'isactive': 'boolean',
            'datatype': 'integer',
            'fullpathname': 'string',
            'toserver': 'boolean',
            'datatypeserver': 'integer'
        }
    },
    'IOOpcUaClientTransformItemConfigs': {
        'source_table_name': 'IOOpcUaClientTransformItemConfigs',
        'hash_gener_columns': ['ConfigID'],
        'source_column_names': ['ConfigID', 'PropertyID', 'FullPathName', 'SampleItemID', 'FreqItemID', 'MinRaw', 'MaxRaw', 'MinEU', 'MaxEU', 'IsScale', 'Factor'],
        'staging_table_name': 'stg_io_opc_ua_client_transform_item_configs',
        'column_types': {
            'configid': 'uuid',
            'propertyid': 'uuid',
            'fullpathname': 'string',
            'sampleitemid': 'string',
            'freqitemid': 'string',
            'minraw': 'float',
            'maxraw': 'float',
            'mineu': 'float',
            'maxeu': 'float',
            'isscale': 'boolean',
            'factor': 'float'
        }
    },
    'Images': {
        'source_table_name': 'Images',
        'hash_gener_columns': ['Id'],
        'source_column_names': ['Id', 'ImageData'],
        'staging_table_name': 'stg_images',
        'column_types': {
            'id': 'uuid',
            'imagedata': 'bytea'  # ✅ binary data
        }
    },
    'MeasureConvert': {
        'source_table_name': 'MeasureConvert',
        'hash_gener_columns': ['FromID'],
        'source_column_names': ['FromID', 'ToID', 'ConvertType', 'Factor', 'Formula'],
        'staging_table_name': 'stg_measure_convert',
        'column_types': {
            'fromid': 'uuid',
            'toid': 'uuid',
            'converttype': 'integer',
            'factor': 'float',
            'formula': 'string'
        }
    },
    'MeasureUnits': {
        'source_table_name': 'MeasureUnits',
        'hash_gener_columns': ['ID'],
        'source_column_names': ['ID', 'Name', 'Abbreviation', 'MeasureGroupID'],
        'staging_table_name': 'stg_measure_units',
        'column_types': {
            'id': 'uuid',
            'name': 'string',
            'abbreviation': 'string',
            'measuregroupid': 'uuid'
        }
    },
    'ModelTemplateTreeNodes': {
        'source_table_name': 'ModelTemplateTreeNodes',
        'hash_gener_columns': ['ID'],
        'source_column_names': ['ID', 'ParentID', 'Name', 'TagName', 'Description'],
        'staging_table_name': 'stg_model_template_tree_nodes',
        'column_types': {
            'id': 'uuid',
            'parentid': 'uuid',
            'name': 'string',
            'tagname': 'string',
            'description': 'string'
        }
    },
    'ModelTemplates': {
        'source_table_name': 'ModelTemplates',
        'hash_gener_columns': ['ID'],
        'source_column_names': ['ID', 'OwnerNodeID', 'Name', 'TagName', 'Description', 'BaseTemplateName', 'DateCreated', 'DateModified', 'CdbSyncDate', 'CdbVersion', 'CdbID'],
        'staging_table_name': 'stg_model_templates',
        'column_types': {
            'id': 'uuid',
            'ownernodeid': 'uuid',
            'name': 'string',
            'tagname': 'string',
            'description': 'string',
            'basetemplatename': 'string',
            'datecreated': 'timestamp',
            'datemodified': 'timestamp',
            'cdbsyncdate': 'timestamp',
            'cdbversion': 'integer',
            'cdbid': 'uuid'
        }
    },
    'ObjectDataValues_DiagnosticArray_Live_v2': {
        'source_table_name': 'ObjectDataValues_DiagnosticArray_Live_v2',
        'hash_gener_columns': ['ValueID'],
        'source_column_names': ['ValueID', 'DiagID', 'TagName', 'DefectState', 'DefectName', 'DefectDetails', 'Recomendation', 'Priority', 'GroupName'],
        'staging_table_name': 'stg_object_data_values_diagnostic_array_live_v2',
        'column_types': {
            'valueid': 'uuid',
            'diagid': 'uuid',
            'tagname': 'string',
            'defectstate': 'integer',
            'defectname': 'string',
            'defectdetails': 'string',
            'recomendation': 'string',
            'priority': 'integer',
            'groupname': 'string'
        }
    },
    'ObjectDataValues_DiagnosticArray_v2': {
        'source_table_name': 'ObjectDataValues_DiagnosticArray_v2',
        'hash_gener_columns': ['ValueID'],
        'source_column_names': ['ValueID', 'DiagID', 'TagName', 'DefectState', 'DefectName', 'DefectDetails', 'Recomendation', 'Priority', 'GroupName'],
        'staging_table_name': 'stg_object_data_values_diagnostic_array_v2',
        'column_types': {
            'valueid': 'uuid',
            'diagid': 'uuid',
            'tagname': 'string',
            'defectstate': 'integer',
            'defectname': 'string',
            'defectdetails': 'string',
            'recomendation': 'string',
            'priority': 'integer',
            'groupname': 'string'
        }
    },
    'ObjectDataValues_Live_v2': {
        'source_table_name': 'ObjectDataValues_Live_v2',
        'hash_gener_columns': ['ID'],
        'source_column_names': ['ID', 'PropertyID', 'MeasureUnitID', 'DataTypeID', 'Date', 'Quality', 'Comment', 'Value'],
        'staging_table_name': 'stg_object_data_values_live_v2',
        'column_types': {
            'id': 'uuid',
            'propertyid': 'uuid',
            'measureunitid': 'uuid',
            'datatypeid': 'uuid',
            'date': 'timestamp',
            'quality': 'integer',
            'comment': 'string',
            'value': 'bytea'  # ✅ binary data
        }
    },
    'ObjectDataValues_common_snapshot': {
        'source_table_name': 'ObjectDataValues_common_snapshot',
        'hash_gener_columns': ['ID'],
        'source_column_names': ['ID', 'PropertyID', 'MeasureUnitID', 'DataTypeID', 'Date', 'Quality', 'Comment', 'Value'],
        'staging_table_name': 'stg_object_data_values_common_snapshot',
        'column_types': {
            'id': 'uuid',
            'propertyid': 'uuid',
            'measureunitid': 'uuid',
            'datatypeid': 'uuid',
            'date': 'timestamp',
            'quality': 'integer',
            'comment': 'string',
            'value': 'bytea'
        }
    },
    'ObjectDataValues_common_snapshot_save': {
        'source_table_name': 'ObjectDataValues_common_snapshot_save',
        'hash_gener_columns': ['ID'],
        'source_column_names': ['ID', 'PropertyID', 'MeasureUnitID', 'DataTypeID', 'Date', 'Quality', 'Comment', 'Value'],
        'staging_table_name': 'stg_object_data_values_common_snapshot_save',
        'column_types': {
            'id': 'uuid',
            'propertyid': 'uuid',
            'measureunitid': 'uuid',
            'datatypeid': 'uuid',
            'date': 'timestamp',
            'quality': 'integer',
            'comment': 'string',
            'value': 'bytea'
        }
    },
    'ObjectDataValues_history_snapshot': {
        'source_table_name': 'ObjectDataValues_history_snapshot',
        'hash_gener_columns': ['ID'],
        'source_column_names': ['ID', 'PropertyID', 'MeasureUnitID', 'DataTypeID', 'Date', 'Quality', 'Comment', 'Value'],
        'staging_table_name': 'stg_object_data_values_history_snapshot',
        'column_types': {
            'id': 'uuid',
            'propertyid': 'uuid',
            'measureunitid': 'uuid',
            'datatypeid': 'uuid',
            'date': 'timestamp',
            'quality': 'integer',
            'comment': 'string',
            'value': 'bytea'
        }
    },
    'ObjectDataValues_snapshot': {
        'source_table_name': 'ObjectDataValues_snapshot',
        'hash_gener_columns': ['ID'],
        'source_column_names': ['ID', 'PropertyID', 'MeasureUnitID', 'DataTypeID', 'Date', 'Quality', 'Comment', 'Value'],
        'staging_table_name': 'stg_object_data_values_snapshot',
        'column_types': {
            'id': 'uuid',
            'propertyid': 'uuid',
            'measureunitid': 'uuid',
            'datatypeid': 'uuid',
            'date': 'timestamp',
            'quality': 'integer',
            'comment': 'string',
            'value': 'bytea'
        }
    },
    'ObjectDataValues_snapshot_single': {
        'source_table_name': 'ObjectDataValues_snapshot_single',
        'hash_gener_columns': ['ID'],
        'source_column_names': ['ID', 'PropertyID', 'MeasureUnitID', 'DataTypeID', 'Date', 'Quality', 'Comment', 'Value'],
        'staging_table_name': 'stg_object_data_values_snapshot_single',
        'column_types': {
            'id': 'uuid',
            'propertyid': 'uuid',
            'measureunitid': 'uuid',
            'datatypeid': 'uuid',
            'date': 'timestamp',
            'quality': 'integer',
            'comment': 'string',
            'value': 'bytea'
        }
    },
    'ObjectDataValues_v2': {
        'source_table_name': 'ObjectDataValues_v2',
        'hash_gener_columns': ['ID'],
        'source_column_names': ['ID', 'PropertyID', 'MeasureUnitID', 'DataTypeID', 'Date', 'Quality', 'Comment', 'Value', 'IsDeleted'],
        'staging_table_name': 'stg_object_data_values_v2',
        'column_types': {
            'id': 'uuid',
            'propertyid': 'uuid',
            'measureunitid': 'uuid',
            'datatypeid': 'uuid',
            'date': 'timestamp',
            'quality': 'integer',
            'comment': 'string',
            'value': 'bytea',
            'isdeleted': 'boolean'
        }
    },
    'ObjectGroupDescriptors': {
        'source_table_name': 'ObjectGroupDescriptors',
        'hash_gener_columns': ['GroupDescriptorId'],
        'source_column_names': ['GroupDescriptorId', 'Name'],
        'staging_table_name': 'stg_object_group_descriptors',
        'column_types': {
            'groupdescriptorid': 'uuid',
            'name': 'string'
        }
    },
    'ObjectGroups': {
        'source_table_name': 'ObjectGroups',
        'hash_gener_columns': ['Id'],
        'source_column_names': ['Id', 'ObjectId', 'GroupDescriptorId'],
        'staging_table_name': 'stg_object_groups',
        'column_types': {
            'id': 'uuid',
            'objectid': 'uuid',
            'groupdescriptorid': 'uuid'
        }
    },
    'ObjectProperties': {
        'source_table_name': 'ObjectProperties',
        'hash_gener_columns': ['PropertyID'],
        'source_column_names': ['PropertyID', 'ObjectID', 'PropertyDescriptorID', 'IsAlias', 'MeasureUnitID', 'MaxRecords', 'FromTemplateName', 'ToCopy', 'SaveHistory', 'StorageType', 'VisibleForScada', 'IsValuesReplicable'],
        'staging_table_name': 'stg_object_properties',
        'column_types': {
            'propertyid': 'uuid',
            'objectid': 'uuid',
            'propertydescriptorid': 'uuid',
            'isalias': 'boolean',
            'measureunitid': 'uuid',
            'maxrecords': 'integer',
            'fromtemplatename': 'string',
            'tocopy': 'boolean',
            'savehistory': 'boolean',
            'storagetype': 'integer',
            'visibleforscada': 'boolean',
            'isvaluesreplicable': 'boolean'
        }
    },
    'ObjectPropertyDescriptorNodes': {
        'source_table_name': 'ObjectPropertyDescriptorNodes',
        'hash_gener_columns': ['ID'],
        'source_column_names': ['ID', 'Name', 'Description', 'ParentNodeID', 'TranslateId'],
        'staging_table_name': 'stg_object_property_descriptor_nodes',
        'column_types': {
            'id': 'uuid',
            'name': 'string',
            'description': 'string',
            'parentnodeid': 'uuid',
            'translateid': 'integer'
        }
    },
    'ObjectPropertyDescriptors': {
        'source_table_name': 'ObjectPropertyDescriptors',
        'hash_gener_columns': ['PropertyDescriptorID'],
        'source_column_names': ['PropertyDescriptorID', 'Name', 'Description', 'Tag', 'ParentNodeID', 'PropertyType', 'MeasureUnitID', 'MaxRecords', 'SaveHistory', 'DefaultValue', 'VisibleForScada', 'DateCreated', 'DateModified', 'CdbSyncDate', 'CdbVersion', 'TranslateId'],
        'staging_table_name': 'stg_object_property_descriptors',
        'column_types': {
            'propertydescriptorid': 'uuid',
            'name': 'string',
            'description': 'string',
            'tag': 'string',
            'parentnodeid': 'uuid',
            'propertytype': 'integer',
            'measureunitid': 'uuid',
            'maxrecords': 'integer',
            'savehistory': 'integer',  # ⚠️ В DDL это integer, не boolean
            'defaultvalue': 'string',
            'visibleforscada': 'integer',  # ⚠️ В DDL это integer, не boolean
            'datecreated': 'timestamp',
            'datemodified': 'timestamp',
            'cdbsyncdate': 'timestamp',
            'cdbversion': 'integer',
            'translateid': 'integer'
        }
    },
    'ObjectRules': {
        'source_table_name': 'ObjectRules',
        'hash_gener_columns': ['ObjectRuleID'],
        'source_column_names': ['ObjectRuleID', 'ObjectID', 'PouCallName', 'Comment', 'Enabled', 'RunLevel', 'FromTemplateName', 'IsInTemplate', 'Tag', 'RunningType', 'RunningPeriod'],
        'staging_table_name': 'stg_object_rules',
        'column_types': {
            'objectruleid': 'uuid',
            'objectid': 'uuid',
            'poucallname': 'string',
            'comment': 'string',
            'enabled': 'boolean',
            'runlevel': 'integer',
            'fromtemplatename': 'string',
            'isintemplate': 'boolean',
            'tag': 'string',
            'runningtype': 'integer',
            'runningperiod': 'integer'
        }
    },
    'ObjectTemplates': {
        'source_table_name': 'ObjectTemplates',
        'hash_gener_columns': ['ID'],
        'source_column_names': ['ID', 'Name', 'TagName', 'ParentID', 'Description', 'TemplateName', 'FromTemplateName'],
        'staging_table_name': 'stg_object_templates',
        'column_types': {
            'id': 'uuid',
            'name': 'string',
            'tagname': 'string',
            'parentid': 'uuid',
            'description': 'string',
            'templatename': 'string',
            'fromtemplatename': 'string'
        }
    },
    'ObjectTypeDescriptors': {
        'source_table_name': 'ObjectTypeDescriptors',
        'hash_gener_columns': ['TypeDescriptorID'],
        'source_column_names': ['TypeDescriptorID', 'Name', 'Description', 'Tag', 'ParentID', 'IconID'],
        'staging_table_name': 'stg_object_type_descriptors',
        'column_types': {
            'typedescriptorid': 'uuid',
            'name': 'string',
            'description': 'string',
            'tag': 'string',
            'parentid': 'uuid',
            'iconid': 'uuid'
        }
    },
    'ObjectTypes': {
        'source_table_name': 'ObjectTypes',
        'hash_gener_columns': ['ObjectID'],
        'source_column_names': ['ObjectID', 'TypeDescriptorID', 'FromTemplateName'],
        'staging_table_name': 'stg_object_types',
        'column_types': {
            'objectid': 'uuid',
            'typedescriptorid': 'uuid',
            'fromtemplatename': 'string'
        }
    },
    'Objects': {
        'source_table_name': 'Objects',
        'hash_gener_columns': ['ID'],
        'source_column_names': ['ID', 'Name', 'TagName', 'ParentID', 'Description', 'TemplateName', 'FromTemplateName', 'DateCreated', 'DateModified', 'CdbSyncDate', 'CdbVersion', 'CdbID'],
        'staging_table_name': 'stg_objects',
        'column_types': {
            'id': 'uuid',
            'name': 'string',
            'tagname': 'string',
            'parentid': 'uuid',
            'description': 'string',
            'templatename': 'string',
            'fromtemplatename': 'string',
            'datecreated': 'timestamp',
            'datemodified': 'timestamp',
            'cdbsyncdate': 'timestamp',
            'cdbversion': 'integer',
            'cdbid': 'uuid'
        }
    },
    'PionRouteObjectsLists': {
        'source_table_name': 'PionRouteObjectsLists',
        'hash_gener_columns': ['ID'],
        'source_column_names': ['ID', 'Name', 'Description', 'xmlObjectsList', 'DateModified', 'DateModifiedCBD'],
        'staging_table_name': 'stg_pion_route_objects_lists',
        'column_types': {
            'id': 'uuid',
            'name': 'string',
            'description': 'string',
            'xmlobjectslist': 'string',
            'datemodified': 'timestamp',
            'datemodifiedcbd': 'timestamp'
        }
    },
    'PouUserDefinedItems': {
        'source_table_name': 'PouUserDefinedItems',
        'hash_gener_columns': ['CallName', 'BodyType', 'Name', 'Description', 'Author', 'CategoryID', 'XmlInterface', 'XmlBody'],
        'source_column_names': ['CallName', 'BodyType', 'Name', 'Description', 'Author', 'CategoryID', 'XmlInterface', 'XmlBody', 'DateOfCreate', 'DateOfEdit'],
        'staging_table_name': 'stg_pou_user_defined_items',
        'column_types': {
            'callname': 'string',
            'bodytype': 'integer',
            'name': 'string',
            'description': 'string',
            'author': 'string',
            'categoryid': 'uuid',
            'xmlinterface': 'string',
            'xmlbody': 'string',
            'dateofcreate': 'timestamp',
            'dateofedit': 'timestamp'
        }
    },
    'PouUserItemsTree': {
        'source_table_name': 'PouUserItemsTree',
        'hash_gener_columns': ['ID'],
        'source_column_names': ['ID', 'Name', 'Description', 'ParentID'],
        'staging_table_name': 'stg_pou_user_items_tree',
        'column_types': {
            'id': 'uuid',
            'name': 'string',
            'description': 'string',
            'parentid': 'uuid'
        }
    },
    'PresetChartSettings': {
        'source_table_name': 'PresetChartSettings',
        'hash_gener_columns': ['Id'],
        'source_column_names': ['Id', 'UserId', 'PresetChartSettings'],
        'staging_table_name': 'stg_preset_chart_settings',
        'column_types': {
            'id': 'uuid',
            'userid': 'uuid',
            'presetchartsettings': 'string'
        }
    },
    'TikExpertSlices': {
        'source_table_name': 'TikExpertSlices',
        'hash_gener_columns': ['PropertyID'],
        'source_column_names': ['PropertyID', 'SliceDate', 'NumericX', 'Comment'],
        'staging_table_name': 'stg_tik_expert_slices',
        'column_types': {
            'propertyid': 'uuid',
            'slicedate': 'timestamp',
            'numericx': 'float',
            'comment': 'string'
        }
    },
    'TikScadaLog': {
        'source_table_name': 'TikScadaLog',
        'hash_gener_columns': ['ID'],
        'source_column_names': ['ID', 'Type', 'Timestamp', 'Message', 'Acked', 'AckedTimestamp', 'SourcePath', 'AggregatePath', 'Participant'],
        'staging_table_name': 'stg_tik_scada_log',
        'column_types': {
            'id': 'uuid',
            'type': 'integer',
            'timestamp': 'timestamp',
            'message': 'string',
            'acked': 'boolean',
            'ackedtimestamp': 'timestamp',
            'sourcepath': 'string',
            'aggregatepath': 'string',
            'participant': 'string'
        }
    },
    'UserDefinedPropertyLists': {
        'source_table_name': 'UserDefinedPropertyLists',
        'hash_gener_columns': ['PropertyListID'],
        'source_column_names': ['PropertyListID', 'PropertyListName', 'Description', 'XmlPropertiesList', 'PropertyListTypeID', 'ObjectID'],
        'staging_table_name': 'stg_user_defined_property_lists',
        'column_types': {
            'propertylistid': 'uuid',
            'propertylistname': 'string',
            'description': 'string',
            'xmlpropertieslist': 'string',
            'propertylisttypeid': 'uuid',
            'objectid': 'uuid'
        }
    },
    'UserDefinedPropertyListsTypes': {
        'source_table_name': 'UserDefinedPropertyListsTypes',
        'hash_gener_columns': ['PropertyListTypeID'],
        'source_column_names': ['PropertyListTypeID', 'PropertyListTypeName', 'Description'],
        'staging_table_name': 'stg_user_defined_property_lists_types',
        'column_types': {
            'propertylisttypeid': 'uuid',
            'propertylisttypename': 'string',
            'description': 'string'
        }
    },
    'UserDefinedTilesPropertiesConfigs': {
        'source_table_name': 'UserDefinedTilesPropertiesConfigs',
        'hash_gener_columns': ['Id'],
        'source_column_names': ['Id', 'Name', 'TilesProperties', 'ObjectId'],
        'staging_table_name': 'stg_user_defined_tiles_properties_configs',
        'column_types': {
            'id': 'uuid',
            'name': 'string',
            'tilesproperties': 'string',
            'objectid': 'uuid'
        }
    },
    'UserLog': {
        'source_table_name': 'UserLog',
        'hash_gener_columns': ['ID'],
        'source_column_names': ['ID', 'UserID', 'Date', 'ActionType', 'ActionDescription'],
        'staging_table_name': 'stg_user_log',
        'column_types': {
            'id': 'uuid',
            'userid': 'uuid',
            'date': 'timestamp',
            'actiontype': 'integer',
            'actiondescription': 'string'
        }
    }
}