main_db_source_to_staging_info=[
    {
        'source_table_name': 'MeasureGroups',
        'staging_table_name': 'stg_measure_groups',
        'source_column_descriptions': [
            {
                'column_name': 'ID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Name',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'AggregateNotificationConfigs',
        'staging_table_name': 'stg_aggregate_notification_configs',
        'source_column_descriptions': [
            {
                'column_name': 'Id',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ConfigId',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'EmailAddress',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Aggregate',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'SendIsEnabled',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'CountRepeats',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MinDefectLevel',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ResendIsEnabled',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ResendTimeSpan',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'NotificationLanguage',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'Aggregates',
        'staging_table_name': 'stg_aggregates',
        'source_column_descriptions': [
            {
                'column_name': 'Id',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'AggregatePath',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'Annotation',
        'staging_table_name': 'stg_annotation',
        'source_column_descriptions': [
            {
                'column_name': 'Id',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'AnnotationId',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'NameGraphics',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'FullPath',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Property',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'SelectedInterval',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'AnnotationType',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'UserLogin',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DateCreate',
                'type': 'timestamp',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'JsonData',
                'type': 'json',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'Bearings',
        'staging_table_name': 'stg_bearings',
        'source_column_descriptions': [
            {
                'column_name': 'BearingID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Number',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'OuterRace_D',
                'type': 'float',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'InnerRace_D',
                'type': 'float',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'RollingElement_D',
                'type': 'float',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'RollingElement_Count',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ContactAngle',
                'type': 'float',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'BPFI',
                'type': 'float',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'BPFO',
                'type': 'float',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'BSF',
                'type': 'float',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'FTF',
                'type': 'float',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ServiceLife',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DateCreated',
                'type': 'timestamp',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DateModified',
                'type': 'timestamp',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'CdbSyncDate',
                'type': 'timestamp',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'CdbVersion',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'CdbID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'EmailAddresses',
        'staging_table_name': 'stg_email_addresses',
        'source_column_descriptions': [
            {
                'column_name': 'Id',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'EmailAddress',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'IOCreytChannelConfigs',
        'staging_table_name': 'stg_io_creyt_channel_configs',
        'source_column_descriptions': [
            {
                'column_name': 'ConfigID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ChannelNum',
                'type': 'integer',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'PropertyID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MinRaw',
                'type': 'float',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MaxRaw',
                'type': 'float',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MinEU',
                'type': 'float',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MaxEU',
                'type': 'float',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MeasureUnitID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    # {
    #     'source_table_name': 'IOCreytChannelConfigs_v5',
    #     'staging_table_name': 'stg_io_creyt_channel_configs_v5',
    #     'source_column_descriptions': [
    #         {
    #             'column_name': 'ConfigID',
    #             'type': 'uuid',
    #             'hub_sk_gener': 'True',
    #             'is_satellite_column': 'False',
    #             'is_link_columns': 'True',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'ChannelNum',
    #             'type': 'integer',
    #             'hub_sk_gener': 'True',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'PropertyID',
    #             'type': 'uuid',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'False',
    #             'is_link_columns': 'True',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'MinRaw',
    #             'type': 'float',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'MaxRaw',
    #             'type': 'float',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'MinEU',
    #             'type': 'float',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'MaxEU',
    #             'type': 'float',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'MeasureUnitID',
    #             'type': 'uuid',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'False',
    #             'is_link_columns': 'True',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #     ]
    # },
    {
        'source_table_name': 'IOCreytChannelStates',
        'staging_table_name': 'stg_io_creyt_channel_states',
        'source_column_descriptions': [
            {
                'column_name': 'ConfigID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ChannelNum',
                'type': 'integer',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'PropertyID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    # {
    #     'source_table_name': 'IOCreytChannelStates_v5',
    #     'staging_table_name': 'stg_io_creyt_channel_states_v5',
    #     'source_column_descriptions': [
    #         {
    #             'column_name': 'ConfigID',
    #             'type': 'uuid',
    #             'hub_sk_gener': 'True',
    #             'is_satellite_column': 'False',
    #             'is_link_columns': 'True',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'ChannelNum',
    #             'type': 'integer',
    #             'hub_sk_gener': 'True',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'PropertyID',
    #             'type': 'uuid',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'False',
    #             'is_link_columns': 'True',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #     ]
    # },
    {
        'source_table_name': 'IOCreytConfigs',
        'staging_table_name': 'stg_io_creyt_configs',
        'source_column_descriptions': [
            {
                'column_name': 'ConfigID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'PrimaryIP',
                'type': 'string',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'SecondaryIP',
                'type': 'string',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'PrimaryModbusPort',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'SecondaryModbusPort',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'PrimaryHttpPort',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'SecondaryHttpPort',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ScanTime',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ConnectRetries',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'SamplesFileName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'IsSyncReadSlave',
                'type': 'boolean',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'SyncReadGroupName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DetectFailures',
                'type': 'boolean',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DetectFailureLength',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'EnabledSecondaryIP',
                'type': 'boolean',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'NumBlockSamples',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    # {
    #     'source_table_name': 'IOCreytConfigs_v5',
    #     'staging_table_name': 'stg_io_creyt_configs_v5',
    #     'source_column_descriptions': [
    #         {
    #             'column_name': 'ConfigID',
    #             'type': 'uuid',
    #             'hub_sk_gener': 'True',
    #             'is_satellite_column': 'False',
    #             'is_link_columns': 'True',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'PrimaryIP',
    #             'type': 'string',
    #             'hub_sk_gener': 'True',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'SecondaryIP',
    #             'type': 'string',
    #             'hub_sk_gener': 'True',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'PrimaryModbusPort',
    #             'type': 'integer',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'SecondaryModbusPort',
    #             'type': 'integer',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'PrimaryHttpPort',
    #             'type': 'integer',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'SecondaryHttpPort',
    #             'type': 'integer',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'ScanTime',
    #             'type': 'integer',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'ConnectRetries',
    #             'type': 'integer',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'SamplesFileName',
    #             'type': 'string',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'IsSyncReadSlave',
    #             'type': 'boolean',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'SyncReadGroupName',
    #             'type': 'string',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'DetectFailures',
    #             'type': 'boolean',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'DetectFailureLength',
    #             'type': 'integer',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'EnabledSecondaryIP',
    #             'type': 'boolean',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'NumBlockSamples',
    #             'type': 'integer',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #     ]
    # },
    {
        'source_table_name': 'IOCreytControllerStates',
        'staging_table_name': 'stg_io_creyt_controller_states',
        'source_column_descriptions': [
            {
                'column_name': 'ConfigID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'RowNum',
                'type': 'integer',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'OperTimePropertyID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'StatePropertyID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    # {
    #     'source_table_name': 'IOCreytControllerStates_v5',
    #     'staging_table_name': 'stg_io_creyt_controller_states_v5',
    #     'source_column_descriptions': [
    #         {
    #             'column_name': 'ConfigID',
    #             'type': 'uuid',
    #             'hub_sk_gener': 'True',
    #             'is_satellite_column': 'False',
    #             'is_link_columns': 'True',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'RowNum',
    #             'type': 'integer',
    #             'hub_sk_gener': 'True',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'OperTimePropertyID',
    #             'type': 'uuid',
    #             'hub_sk_gener': 'True',
    #             'is_satellite_column': 'False',
    #             'is_link_columns': 'True',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'StatePropertyID',
    #             'type': 'uuid',
    #             'hub_sk_gener': 'True',
    #             'is_satellite_column': 'False',
    #             'is_link_columns': 'True',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #     ]
    # },
    {
        'source_table_name': 'IOCreytIMOperTimes',
        'staging_table_name': 'stg_io_creyt_im_oper_times',
        'source_column_descriptions': [
            {
                'column_name': 'ConfigID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'PropertyID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    # {
    #     'source_table_name': 'IOCreytIMOperTimes_v5',
    #     'staging_table_name': 'stg_io_creyt_im_oper_times_v5',
    #     'source_column_descriptions': [
    #         {
    #             'column_name': 'ConfigID',
    #             'type': 'uuid',
    #             'hub_sk_gener': 'True',
    #             'is_satellite_column': 'False',
    #             'is_link_columns': 'True',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'PropertyID',
    #             'type': 'uuid',
    #             'hub_sk_gener': 'True',
    #             'is_satellite_column': 'False',
    #             'is_link_columns': 'True',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #     ]
    # },
    {
        'source_table_name': 'IODeviceConfigNodes',
        'staging_table_name': 'stg_io_device_config_nodes',
        'source_column_descriptions': [
            {
                'column_name': 'ID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Name',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ParentNodeID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'IODeviceConfigs',
        'staging_table_name': 'stg_io_device_configs',
        'source_column_descriptions': [
            {
                'column_name': 'ID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Name',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'TypeID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'True',
                'prefix_entity_name': 'h_config_types'
            },
            {
                'column_name': 'ConfigNodeID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Enabled',
                'type': 'boolean',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'SetNumber',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'IOLCardChannelConfigs',
        'staging_table_name': 'stg_io_l_card_channel_configs',
        'source_column_descriptions': [
            {
                'column_name': 'ConfigID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Slot',
                'type': 'integer',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ChannelNumber',
                'type': 'integer',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'PropertyID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'IsScaled',
                'type': 'boolean',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MinRaw',
                'type': 'float',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MaxRaw',
                'type': 'float',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MinEU',
                'type': 'float',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MaxEU',
                'type': 'float',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MeasureUnitID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'PropertyMarkID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'IOLCardConfigs',
        'staging_table_name': 'stg_io_l_card_configs',
        'source_column_descriptions': [
            {
                'column_name': 'ConfigID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'VirtualSlot',
                'type': 'integer',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'SamplingRate',
                'type': 'float',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'OperationMode',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'TimerSamplingTime',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'TimerScanTime',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'SyncSamplingTime',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'IOLCardCrateConfigs',
        'staging_table_name': 'stg_io_l_card_crate_configs',
        'source_column_descriptions': [
            {
                'column_name': 'ConfigID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'IP',
                'type': 'string',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Port',
                'type': 'integer',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'CrateType',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'True',
                'prefix_entity_name': 'h_crate_types'
            },
            {
                'column_name': 'SerialNumber',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'SamplingTime',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ScanTime',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'IOLCardCrateSync',
        'staging_table_name': 'stg_io_l_card_crate_sync',
        'source_column_descriptions': [
            {
                'column_name': 'ConfigID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'IsSync',
                'type': 'boolean',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'IsLeader',
                'type': 'boolean',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'IsSlave',
                'type': 'boolean',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'LeaderID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'IOLCardInputConfigs',
        'staging_table_name': 'stg_io_l_card_input_configs',
        'source_column_descriptions': [
            {
                'column_name': 'ConfigID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'InputNum',
                'type': 'integer',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'PropertyID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'InputRange',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'IsScale',
                'type': 'boolean',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MinRaw',
                'type': 'float',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MaxRaw',
                'type': 'float',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MinEU',
                'type': 'float',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MaxEU',
                'type': 'float',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MeasureUnitID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'IOLCardLogicInputConfigs',
        'staging_table_name': 'stg_io_l_card_logic_input_configs',
        'source_column_descriptions': [
            {
                'column_name': 'ConfigID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'InputNumber',
                'type': 'integer',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'InputType',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'True',
                'prefix_entity_name': 'h_l_card_logic_input_types'
            },
            {
                'column_name': 'TimerPropertyID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'SyncPropertyID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'InputRange',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'IsScale',
                'type': 'boolean',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MinRaw',
                'type': 'float',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MaxRaw',
                'type': 'float',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MinEU',
                'type': 'float',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MaxEU',
                'type': 'float',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MeasureUnitID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'IOLCardModuleConfigs',
        'staging_table_name': 'stg_io_l_card_module_configs',
        'source_column_descriptions': [
            {
                'column_name': 'ConfigID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Slot',
                'type': 'integer',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ModuleType',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'True',
                'prefix_entity_name': 'h_l_card_crate_module_types'
            },
            {
                'column_name': 'FrequencyDivisor',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'IOModbusTcpConfigs',
        'staging_table_name': 'stg_io_modbus_tcp_configs',
        'source_column_descriptions': [
            {
                'column_name': 'ConfigID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DeviceID',
                'type': 'integer',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'PrimaryIP',
                'type': 'string',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'SecondaryIP',
                'type': 'string',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'PrimaryPort',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'SecondaryPort',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ScanTime',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'CoilStatusMax',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'InputStatusMax',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'HoldingRegisterMax',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'InputRegisterMax',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ConnectRetries',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'SecondaryEnabled',
                'type': 'boolean',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DeviceType',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'True',
                'prefix_entity_name': 'h_device_types'
            },
            {
                'column_name': 'ConnectToPtimaryIfAvailable',
                'type': 'boolean',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'SyncReadGroupName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'InterfaceType',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'True',
                'prefix_entity_name': 'h_interface_types'
            },
            {
                'column_name': 'Serial_PortName_Primary',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Serial_PortName_Secondary',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Serial_BaudRate',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Serial_Parity',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Serial_BitCount',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Serial_StopBits',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'LogLevel',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ReceiveTimeout',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'WriteTimeout',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'PeriodWrite',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'IOModbusTcpRegisterConfigs',
        'staging_table_name': 'stg_io_modbus_tcp_register_configs',
        'source_column_descriptions': [
            {
                'column_name': 'ConfigID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'RegisterType',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'True',
                'prefix_entity_name': 'h_register_types'
            },
            {
                'column_name': 'RegisterAddress',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DataType',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'True',
                'prefix_entity_name': 'h_data_type_names'
            },
            {
                'column_name': 'ByteOrder',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'PropertyID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MeasureUnitID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MinRaw',
                'type': 'float',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MaxRaw',
                'type': 'float',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MinEU',
                'type': 'float',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MaxEU',
                'type': 'float',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'IsScale',
                'type': 'boolean',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Factor',
                'type': 'float',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Id',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'IsWrite',
                'type': 'boolean',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'WritePropertyID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    # {
    #     'source_table_name': 'IOModbusTcpRegisterConfigs_TMP',
    #     'staging_table_name': 'stg_io_modbus_tcp_register_configs_tmp',
    #     'source_column_descriptions': [
    #         {
    #             'column_name': 'ConfigID',
    #             'type': 'uuid',
    #             'hub_sk_gener': 'True',
    #             'is_satellite_column': 'False',
    #             'is_link_columns': 'True',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'RegisterType',
    #             'type': 'integer',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'False',
    #             'is_link_columns': 'True',
    #             'is_system_type_data': 'True',
    #             'prefix_entity_name': 'h_register_types'
    #         },
    #         {
    #             'column_name': 'RegisterAddress',
    #             'type': 'integer',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'DataType',
    #             'type': 'integer',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'True',
    #             'prefix_entity_name': 'h_data_type_names'
    #         },
    #         {
    #             'column_name': 'ByteOrder',
    #             'type': 'string',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'PropertyID',
    #             'type': 'uuid',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'False',
    #             'is_link_columns': 'True',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'MeasureUnitID',
    #             'type': 'uuid',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'False',
    #             'is_link_columns': 'True',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'MinRaw',
    #             'type': 'float',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'MaxRaw',
    #             'type': 'float',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'MinEU',
    #             'type': 'float',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'MaxEU',
    #             'type': 'float',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'IsScale',
    #             'type': 'boolean',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #         {
    #             'column_name': 'Factor',
    #             'type': 'float',
    #             'hub_sk_gener': 'False',
    #             'is_satellite_column': 'True',
    #             'is_link_columns': 'False',
    #             'is_system_type_data': 'False',
    #             'prefix_entity_name': ''
    #         },
    #     ]
    # },
    {
        'source_table_name': 'IOModbustcpRegisterBitDecompressionConfigs',
        'staging_table_name': 'stg_io_modbus_tcp_register_bit_decompression_configs',
        'source_column_descriptions': [
            {
                'column_name': 'Id',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'RegisterId',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'PropertyId',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'BitNumber',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'IsWrite',
                'type': 'boolean',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'WritePropertyId',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MakeInversion',
                'type': 'boolean',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'IOOpcDaClientConfigs',
        'staging_table_name': 'stg_io_opc_da_client_configs',
        'source_column_descriptions': [
            {
                'column_name': 'ConfigID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'IP',
                'type': 'string',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ServerName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ConnectRetries',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'IOOpcDaClientGroupConfigs',
        'staging_table_name': 'stg_io_opc_da_client_group_configs',
        'source_column_descriptions': [
            {
                'column_name': 'ConfigID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Name',
                'type': 'string',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'IsActive',
                'type': 'boolean',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'UpdateRate',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'IOOpcDaClientItemConfigs',
        'staging_table_name': 'stg_io_opc_da_client_item_configs',
        'source_column_descriptions': [
            {
                'column_name': 'ConfigID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'PropertyID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ItemID',
                'type': 'string',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'GroupName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'IsActive',
                'type': 'boolean',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DataType',
                'type': 'integer',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'True',
                'prefix_entity_name': 'h_opc_da_data_types'
            },
        ]
    },
    {
        'source_table_name': 'IOOpcUaClientConfigs',
        'staging_table_name': 'stg_io_opc_ua_client_configs',
        'source_column_descriptions': [
            {
                'column_name': 'ConfigID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'IP',
                'type': 'string',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ServerName',
                'type': 'string',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ConnectRetries',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'AuthenticationId',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'UserName',
                'type': 'string',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Password',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'CertificateFilePath',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'PrivateKeyFilePath',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'NumBlockSamples',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'NumGroupSample',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'IOOpcUaClientGroupConfigs',
        'staging_table_name': 'stg_io_opc_ua_client_group_configs',
        'source_column_descriptions': [
            {
                'column_name': 'ConfigID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Name',
                'type': 'string',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'IsActive',
                'type': 'boolean',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'UpdateRate',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'IsSubscription',
                'type': 'boolean',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'IOOpcUaClientItemConfigs',
        'staging_table_name': 'stg_io_opc_ua_client_item_configs',
        'source_column_descriptions': [
            {
                'column_name': 'ConfigID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'PropertyID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ItemID',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'GroupName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'IsActive',
                'type': 'boolean',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DataType',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'True',
                'prefix_entity_name': 'h_data_types'
            },
            {
                'column_name': 'FullPathName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ToServer',
                'type': 'boolean',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DataTypeServer',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'True',
                'prefix_entity_name': 'h_data_type_servers'
            },
        ]
    },
    {
        'source_table_name': 'IOOpcUaClientTransformItemConfigs',
        'staging_table_name': 'stg_io_opc_ua_client_transform_item_configs',
        'source_column_descriptions': [
            {
                'column_name': 'ConfigID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'PropertyID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'FullPathName',
                'type': 'string',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'SampleItemID',
                'type': 'string',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'FreqItemID',
                'type': 'string',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MinRaw',
                'type': 'float',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MaxRaw',
                'type': 'float',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MinEU',
                'type': 'float',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MaxEU',
                'type': 'float',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'IsScale',
                'type': 'boolean',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Factor',
                'type': 'float',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'Images',
        'staging_table_name': 'stg_images',
        'source_column_descriptions': [
            {
                'column_name': 'Id',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ImageData',
                'type': 'bytea',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'MeasureConvert',
        'staging_table_name': 'stg_measure_convert',
        'source_column_descriptions': [
            {
                'column_name': 'FromID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ToID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ConvertType',
                'type': 'integer',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'True',
                'prefix_entity_name': 'h_convert_types'
            },
            {
                'column_name': 'Factor',
                'type': 'float',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Formula',
                'type': 'string',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'MeasureUnits',
        'staging_table_name': 'stg_measure_units',
        'source_column_descriptions': [
            {
                'column_name': 'ID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Name',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Abbreviation',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MeasureGroupID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'ModelTemplateTreeNodes',
        'staging_table_name': 'stg_model_template_tree_nodes',
        'source_column_descriptions': [
            {
                'column_name': 'ID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ParentID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Name',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'TagName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Description',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'ModelTemplates',
        'staging_table_name': 'stg_model_templates',
        'source_column_descriptions': [
            {
                'column_name': 'ID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'OwnerNodeID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Name',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'TagName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Description',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'BaseTemplateName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DateCreated',
                'type': 'timestamp',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DateModified',
                'type': 'timestamp',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'CdbSyncDate',
                'type': 'timestamp',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'CdbVersion',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'CdbID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'ObjectDataValues_DiagnosticArray_Live_v2',
        'staging_table_name': 'stg_object_data_values_diagnostic_array_live_v2',
        'source_column_descriptions': [
            {
                'column_name': 'ValueID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DiagID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'TagName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DefectState',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DefectName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DefectDetails',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Recomendation',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Priority',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'GroupName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'ObjectDataValues_DiagnosticArray_v2',
        'staging_table_name': 'stg_object_data_values_diagnostic_array_v2',
        'source_column_descriptions': [
            {
                'column_name': 'ValueID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DiagID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'TagName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DefectState',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'True',
                'prefix_entity_name': 'h_diagnostic_defect_states'
            },
            {
                'column_name': 'DefectName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DefectDetails',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Recomendation',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Priority',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'GroupName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'ObjectGroupDescriptors',
        'staging_table_name': 'stg_object_group_descriptors',
        'source_column_descriptions': [
            {
                'column_name': 'GroupDescriptorId',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Name',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'ObjectGroups',
        'staging_table_name': 'stg_object_groups',
        'source_column_descriptions': [
            {
                'column_name': 'Id',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ObjectId',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'GroupDescriptorId',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'ObjectProperties',
        'staging_table_name': 'stg_object_properties',
        'source_column_descriptions': [
            {
                'column_name': 'PropertyID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ObjectID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'PropertyDescriptorID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'IsAlias',
                'type': 'boolean',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MeasureUnitID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MaxRecords',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'FromTemplateName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ToCopy',
                'type': 'boolean',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'SaveHistory',
                'type': 'boolean',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'StorageType',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'True',
                'prefix_entity_name': 'h_storage_types'
            },
            {
                'column_name': 'VisibleForScada',
                'type': 'boolean',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'IsValuesReplicable',
                'type': 'boolean',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'ObjectPropertyDescriptorNodes',
        'staging_table_name': 'stg_object_property_descriptor_nodes',
        'source_column_descriptions': [
            {
                'column_name': 'ID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Name',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Description',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ParentNodeID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'TranslateId',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'ObjectPropertyDescriptors',
        'staging_table_name': 'stg_object_property_descriptors',
        'source_column_descriptions': [
            {
                'column_name': 'PropertyDescriptorID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Name',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Description',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Tag',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ParentNodeID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'PropertyType',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'True',
                'prefix_entity_name': 'h_property_types'
            },
            {
                'column_name': 'MeasureUnitID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MaxRecords',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'SaveHistory',
                'type': 'boolean',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DefaultValue',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'VisibleForScada',
                'type': 'boolean',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DateCreated',
                'type': 'timestamp',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DateModified',
                'type': 'timestamp',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'CdbSyncDate',
                'type': 'timestamp',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'CdbVersion',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'TranslateId',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'ObjectRules',
        'staging_table_name': 'stg_object_rules',
        'source_column_descriptions': [
            {
                'column_name': 'ObjectRuleID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ObjectID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'PouCallName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Comment',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Enabled',
                'type': 'boolean',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'RunLevel',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'FromTemplateName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'IsInTemplate',
                'type': 'boolean',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Tag',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'RunningType',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'True',
                'prefix_entity_name': 'h_running_types'
            },
            {
                'column_name': 'RunningPeriod',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'ObjectTemplates',
        'staging_table_name': 'stg_object_templates',
        'source_column_descriptions': [
            {
                'column_name': 'ID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Name',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'TagName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ParentID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Description',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'TemplateName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'FromTemplateName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'ObjectTypeDescriptors',
        'staging_table_name': 'stg_object_type_descriptors',
        'source_column_descriptions': [
            {
                'column_name': 'TypeDescriptorID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Name',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Description',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Tag',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ParentID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'IconID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'ObjectTypes',
        'staging_table_name': 'stg_object_types',
        'source_column_descriptions': [
            {
                'column_name': 'ObjectID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'TypeDescriptorID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'FromTemplateName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'Objects',
        'staging_table_name': 'stg_objects',
        'source_column_descriptions': [
            {
                'column_name': 'ID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Name',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'TagName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ParentID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Description',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'TemplateName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'FromTemplateName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DateCreated',
                'type': 'timestamp',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DateModified',
                'type': 'timestamp',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'CdbSyncDate',
                'type': 'timestamp',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'CdbVersion',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'CdbID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'PionRouteObjectsLists',
        'staging_table_name': 'stg_pion_route_objects_lists',
        'source_column_descriptions': [
            {
                'column_name': 'ID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Name',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Description',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'xmlObjectsList',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DateModified',
                'type': 'timestamp',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DateModifiedCBD',
                'type': 'timestamp',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'PouUserDefinedItems',
        'staging_table_name': 'stg_pou_user_defined_items',
        'source_column_descriptions': [
            {
                'column_name': 'CallName',
                'type': 'string',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'BodyType',
                'type': 'integer',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Name',
                'type': 'string',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Description',
                'type': 'string',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Author',
                'type': 'string',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'CategoryID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'XmlInterface',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'XmlBody',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DateOfCreate',
                'type': 'timestamp',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DateOfEdit',
                'type': 'timestamp',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'PouUserItemsTree',
        'staging_table_name': 'stg_pou_user_items_tree',
        'source_column_descriptions': [
            {
                'column_name': 'ID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Name',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Description',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ParentID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'PresetChartSettings',
        'staging_table_name': 'stg_preset_chart_settings',
        'source_column_descriptions': [
            {
                'column_name': 'Id',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'UserId',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'PresetChartSettings',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'TikExpertSlices',
        'staging_table_name': 'stg_tik_expert_slices',
        'source_column_descriptions': [
            {
                'column_name': 'PropertyID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'SliceDate',
                'type': 'timestamp',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'NumericX',
                'type': 'float',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Comment',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'TikScadaLog',
        'staging_table_name': 'stg_tik_scada_log',
        'source_column_descriptions': [
            {
                'column_name': 'ID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Type',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'True',
                'prefix_entity_name': 'h_tik_scada_log_types'
            },
            {
                'column_name': 'Timestamp',
                'type': 'timestamp',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Message',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Acked',
                'type': 'boolean',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'AckedTimestamp',
                'type': 'timestamp',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'SourcePath',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'AggregatePath',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Participant',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'UserDefinedPropertyLists',
        'staging_table_name': 'stg_user_defined_property_lists',
        'source_column_descriptions': [
            {
                'column_name': 'PropertyListID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'PropertyListName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Description',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'XmlPropertiesList',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'PropertyListTypeID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ObjectID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'UserDefinedPropertyListsTypes',
        'staging_table_name': 'stg_user_defined_property_lists_types',
        'source_column_descriptions': [
            {
                'column_name': 'PropertyListTypeID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'PropertyListTypeName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Description',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'UserDefinedTilesPropertiesConfigs',
        'staging_table_name': 'stg_user_defined_tiles_properties_configs',
        'source_column_descriptions': [
            {
                'column_name': 'Id',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Name',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'TilesProperties',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ObjectId',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
    {
        'source_table_name': 'UserLog',
        'staging_table_name': 'stg_user_log',
        'source_column_descriptions': [
            {
                'column_name': 'ID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'UserID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Date',
                'type': 'timestamp',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'ActionType',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'True',
                'prefix_entity_name': 'h_user_action_types'
            },
            {
                'column_name': 'ActionDescription',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
]

diagnostic_data_source_to_staging_info=[
    {
        'source_table_name': 'DiagnosticAlarmHistory_v2',
        'staging_table_name': 'stg_diagnostic_alarms',
        'source_column_descriptions': [
            {
                'column_name': 'DiagAlarmID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'PropertyID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DiagID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DiagTagName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'AlarmState',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'True',
                'prefix_entity_name': 'h_diagnostic_alarm_states'
            },
            {
                'column_name': 'Date',
                'type': 'timestamp',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Confirmed',
                'type': 'boolean',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Comment',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DefectState',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'True',
                'prefix_entity_name': 'h_diagnostic_defect_states'
            },
            {
                'column_name': 'DefectName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DefectDetails',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Recommendation',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Priority',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'GroupName',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DefectType',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'True',
                'is_system_type_data': 'True',
                'prefix_entity_name': 'h_diagnostic_defect_types'
            },
        ]
    },
]

object_data_value_records_source_to_staging_info=[
    {
        'source_table_name': 'ObjectDataValues_common_snapshot',
        'staging_table_name': 'stg_main_object_data_values_nonhist',
        'source_column_descriptions': [
            {
                'column_name': 'ID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'PropertyID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MeasureUnitID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DataTypeID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'True',
                'prefix_entity_name': 'h_data_types'
            },
            {
                'column_name': 'Date',
                'type': 'timestamp',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Quality',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Comment',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Value',
                'type': 'bytea',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
     {
        'source_table_name': 'ObjectDataValues_snapshot_single',
        'staging_table_name': 'stg_main_object_data_values_nonhist',
        'source_column_descriptions': [
            {
                'column_name': 'ID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'PropertyID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'MeasureUnitID',
                'type': 'uuid',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'DataTypeID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'True',
                'prefix_entity_name': 'h_data_types'
            },
            {
                'column_name': 'Date',
                'type': 'timestamp',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Quality',
                'type': 'integer',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Comment',
                'type': 'string',
                'hub_sk_gener': 'False',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'Value',
                'type': 'bytea',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
        ]
    },
]


staging_tables_list=[
    'stg_measure_groups',
    'stg_aggregate_notification_configs',
    'stg_aggregates',
    'stg_annotation',
    'stg_bearings',
    'stg_diagnostic_alarms',
    'stg_email_addresses',
    'stg_io_creyt_channel_configs',
    'stg_io_creyt_channel_configs_v5',
    'stg_io_creyt_channel_states',
    'stg_io_creyt_channel_states_v5',
    'stg_io_creyt_configs',
    'stg_io_creyt_configs_v5',
    'stg_io_creyt_controller_states',
    'stg_io_creyt_controller_states_v5',
    'stg_io_creyt_im_oper_times',
    'stg_io_creyt_im_oper_times_v5',
    'stg_io_device_config_nodes',
    'stg_io_device_configs',
    'stg_io_l_card_channel_configs',
    'stg_io_l_card_configs',
    'stg_io_l_card_crate_configs',
    'stg_io_l_card_crate_sync',
    'stg_io_l_card_input_configs',
    'stg_io_l_card_logic_input_configs',
    'stg_io_l_card_module_configs',
    'stg_io_modbus_tcp_configs',
    'stg_io_modbus_tcp_register_configs',
    'stg_io_modbus_tcp_register_configs_tmp',
    'stg_io_modbus_tcp_register_bit_decompression_configs',
    'stg_io_opc_da_client_configs',
    'stg_io_opc_da_client_group_configs',
    'stg_io_opc_da_client_item_configs',
    'stg_io_opc_ua_client_configs',
    'stg_io_opc_ua_client_group_configs',
    'stg_io_opc_ua_client_item_configs',
    'stg_io_opc_ua_client_transform_item_configs',
    'stg_images',
    'stg_measure_convert',
    'stg_measure_units',
    'stg_model_template_tree_nodes',
    'stg_model_templates',
    'stg_object_data_values_diagnostic_array_live_v2',
    'stg_object_data_values_diagnostic_array_v2',
    'stg_object_group_descriptors',
    'stg_object_groups',
    'stg_object_properties',
    'stg_object_property_descriptor_nodes',
    'stg_object_property_descriptors',
    'stg_object_rules',
    'stg_object_templates',
    'stg_object_type_descriptors',
    'stg_object_types',
    'stg_objects',
    'stg_pion_route_objects_lists',
    'stg_pou_user_defined_items',
    'stg_pou_user_items_tree',
    'stg_preset_chart_settings',
    'stg_tik_expert_slices',
    'stg_tik_scada_log',
    'stg_user_defined_property_lists',
    'stg_user_defined_property_lists_types',
    'stg_user_defined_tiles_properties_configs',
    'stg_user_log',
    'stg_main_object_data_values_nonhist'
]