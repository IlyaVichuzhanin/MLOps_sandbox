any_source_to_staging_upload_config={
        'source_table_name': 'DayTable',
        'staging_table_name': 'stg_any_object_data_values_hist',
        'source_column_descriptions': [
            {
                'column_name': 'id',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'timestamp',
                'type': 'timestamp',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'value',
                'type': 'bytea',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
                        {
                'column_name': 'dataTypeID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'True',
                'prefix_entity_name': 'h_data_types'
            },
        ]
    }


any_staging_tables_list=[
    'stg_any_object_data_values_hist',
]

double_source_to_staging_upload_config={
        'source_table_name': 'DayTable',
        'staging_table_name': 'stg_double_object_data_values_hist',
        'source_column_descriptions': [
            {
                'column_name': 'id',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'False',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'timestamp',
                'type': 'timestamp',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
            {
                'column_name': 'value',
                'type': 'bytea',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'False',
                'prefix_entity_name': ''
            },
                        {
                'column_name': 'dataTypeID',
                'type': 'uuid',
                'hub_sk_gener': 'True',
                'is_satellite_column': 'True',
                'is_link_columns': 'False',
                'is_system_type_data': 'True',
                'prefix_entity_name': 'h_data_types'
            },
        ]
    }

double_staging_tables_list=[
    'stg_double_object_data_values_hist',
]