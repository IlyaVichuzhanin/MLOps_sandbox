staging_to_satellite_info = [
    {
        'config_name': 's_measure_groups',
        'sat_table_name': 's_measure_groups',
        'hab_sk_column_name': 'h_measure_group_sk',
        'sat_column_names': ['group_name'],
        'stg_table_name': 'stg_measure_groups',
        'stg_column_names': ['Name'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    # # {
    # #     'config_name': 'c',
    # #     'sat_table_name': 's_aggregate_notification_configs',
    # #     'hab_sk_column_name': 'h_aggregate_notification_config_sk',
    # #     'sat_column_names': [
    # #         'send_is_enabled', 'count_repeats', 'resend_is_enabled',
    # #         'resend_time_span', 'notification_language'
    # #     ],
    # #     'stg_table_name': 'stg_aggregate_notification_configs',
    # #     'stg_column_names': [
    # #         'SendIsEnabled',
    # #         'CountRepeats', 'MinDefectLevel', 'ResendIsEnabled', 'ResendTimeSpan', 'NotificationLanguage'
    # #     ]
    # # },
    {
        'config_name': 's_diagnostic_alarms',
        'sat_table_name': 's_diagnostic_alarms',
        'hab_sk_column_name': 'h_diagnostic_alarm_sk',
        'sat_column_names': ['date_datetime', 'confirmed', 'comment'],
        'stg_table_name': 'stg_diagnostic_alarms',
        'stg_column_names': ['Date', 'Confirmed', 'Comment'],
        'stg_hash_sk_column_name': 'hash_diag_alarm_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diag_alarm_diff'
    },
    {
        'config_name': 's_diagnostic_data_records',
        'sat_table_name': 's_diagnostic_data_records',
        'hab_sk_column_name': 'h_diagnostic_data_record_sk',
        'sat_column_names': ['diag_tag_name', 'defect_name',
            'defect_details', 'recommendation', 'priority', 'group_name'
        ],
        'stg_table_name': 'stg_diagnostic_alarms',
        'stg_column_names': ['DiagTagName', 'DefectName',
            'DefectDetails', 'Recommendation', 'Priority', 'GroupName'
        ],
        'stg_hash_sk_column_name': 'hash_diag_data_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diag_data_diff'
    },
    {
        'config_name': 's_aggregates',
        'sat_table_name': 's_aggregates',
        'hab_sk_column_name': 'h_aggregate_sk',
        'sat_column_names': ['aggregate_path'],
        'stg_table_name': 'stg_aggregates',
        'stg_column_names': ['AggregatePath'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_annotations',
        'sat_table_name': 's_annotations',
        'hab_sk_column_name': 'h_annotation_sk',
        'sat_column_names': [
            'name_graphics', 'full_path', 'property', 'selected_interval',
            'annotation_type', 'user_login', 'date_create', 'json_annotation_settings'
        ],
        'stg_table_name': 'stg_annotation',
        'stg_column_names': [
            'NameGraphics', 'FullPath', 'Property',
            'SelectedInterval', 'AnnotationType', 'UserLogin', 'DateCreate', 'JsonData'
        ],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_bearings',
        'sat_table_name': 's_bearings',
        'hab_sk_column_name': 'h_bearing_sk',
        'sat_column_names': [
            'number', 'outer_race_d', 'inner_race_d', 'rolling_element_d',
            'rolling_element_count', 'contact_angle', 'bpfi', 'bpfo', 'bsf', 'ftf',
            'service_life', 'date_created', 'date_modified', 'cdb_sync_date',
            'cdb_version', 'cdb_id'
        ],
        'stg_table_name': 'stg_bearings',
        'stg_column_names': [
            'Number', 'OuterRace_D', 'InnerRace_D', 'RollingElement_D',
            'RollingElement_Count', 'ContactAngle', 'BPFI', 'BPFO', 'BSF', 'FTF',
            'ServiceLife', 'DateCreated', 'DateModified', 'CdbSyncDate', 'CdbVersion', 'CdbID'
        ],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_object_properties',
        'sat_table_name': 's_object_properties',
        'hab_sk_column_name': 'h_object_property_sk',
        'sat_column_names': [
            'is_alias', 'max_records', 'from_template_name', 'to_copy',
            'save_history', 'visible_for_scada', 'is_values_replicable'
        ],
        'stg_table_name': 'stg_object_properties',
        'stg_column_names': ['IsAlias', 'MaxRecords', 'FromTemplateName', 'ToCopy', 'SaveHistory',
            'VisibleForScada', 'IsValuesReplicable'
        ],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_images',
        'sat_table_name': 's_images',
        'hab_sk_column_name': 'h_image_sk',
        'sat_column_names': ['image_data'],
        'stg_table_name': 'stg_images',
        'stg_column_names': ['ImageData'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_io_device_config_nodes',
        'sat_table_name': 's_io_device_config_nodes',
        'hab_sk_column_name': 'h_io_device_config_node_sk',
        'sat_column_names': ['name', 'parent_node_id'],
        'stg_table_name': 'stg_io_device_config_nodes',
        'stg_column_names': ['Name', 'ParentNodeID'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_io_device_configs',
        'sat_table_name': 's_io_device_configs',
        'hab_sk_column_name': 'h_io_device_config_sk',
        'sat_column_names': ['name', 'enabled', 'set_number'],
        'stg_table_name': 'stg_io_device_configs',
        'stg_column_names': ['Name', 'Enabled', 'SetNumber'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_model_templates',
        'sat_table_name': 's_model_templates',
        'hab_sk_column_name': 'h_model_template_sk',
        'sat_column_names': [ 'name', 'tag_name', 'description', 'base_template_name',
            'date_created', 'date_modified', 'cdb_sync_date', 'cdb_version', 'cdb_id'
        ],
        'stg_table_name': 'stg_model_templates',
        'stg_column_names': ['Name', 'TagName', 'Description', 'BaseTemplateName',
            'DateCreated', 'DateModified', 'CdbSyncDate', 'CdbVersion', 'CdbID'
        ],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_model_template_tree_nodes',
        'sat_table_name': 's_model_template_tree_nodes',
        'hab_sk_column_name': 'h_model_template_tree_node_sk',
        'sat_column_names': ['name', 'parent_id', 'tag_name', 'description'],
        'stg_table_name': 'stg_model_template_tree_nodes',
        'stg_column_names': ['Name', 'ParentID', 'TagName', 'Description'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_measure_converts',
        'sat_table_name': 's_measure_converts',
        'hab_sk_column_name': 'h_measure_convert_sk',
        'sat_column_names': ['from_id', 'to_id', 'factor', 'formula'],
        'stg_table_name': 'stg_measure_convert',
        'stg_column_names': ['FromID', 'ToID', 'Factor', 'Formula'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_measure_units',
        'sat_table_name': 's_measure_units',
        'hab_sk_column_name': 'h_measure_unit_sk',
        'sat_column_names': ['name', 'abbreviation'],
        'stg_table_name': 'stg_measure_units',
        'stg_column_names': ['Name', 'Abbreviation'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_object_property_descriptor_nodes',
        'sat_table_name': 's_object_property_descriptor_nodes',
        'hab_sk_column_name': 'h_object_property_descriptor_node_sk',
        'sat_column_names': ['name', 'description', 'parent_id', 'translated_id'],
        'stg_table_name': 'stg_object_property_descriptor_nodes',
        'stg_column_names': ['Name', 'Description', 'ParentNodeID', 'TranslateId'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_object_property_descriptors',
        'sat_table_name': 's_object_property_descriptors',
        'hab_sk_column_name': 'h_object_property_descriptor_sk',
        'sat_column_names': ['name', 'description','tag', 'max_records', 'save_history',
            'default_value', 'visible_for_scada', 'date_created', 'date_modified',
            'cdb_sync_date', 'cdb_version', 'translated_id'
        ],
        'stg_table_name': 'stg_object_property_descriptors',
        'stg_column_names': ['Name', 'Description', 'Tag', 
            'MaxRecords', 'SaveHistory', 'DefaultValue',
            'VisibleForScada', 'DateCreated', 'DateModified', 'CdbSyncDate', 'CdbVersion', 'TranslateId'
        ],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_object_group_descriptors',
        'sat_table_name': 's_object_group_descriptors',
        'hab_sk_column_name': 'h_object_group_descriptor_sk',
        'sat_column_names': ['name'],
        'stg_table_name': 'stg_object_group_descriptors',
        'stg_column_names': ['Name'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_object_templates',
        'sat_table_name': 's_object_templates',
        'hab_sk_column_name': 'h_object_templates_sk',
        'sat_column_names': [
            'name', 'tag_name', 'parent_id', 'description',
            'template_name', 'from_template_name'
        ],
        'stg_table_name': 'stg_object_templates',
        'stg_column_names': ['Name', 'TagName', 'ParentID', 'Description', 'TemplateName', 'FromTemplateName'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_pou_user_defined_items',
        'sat_table_name': 's_pou_user_defined_items',
        'hab_sk_column_name': 'h_pou_user_defined_item_sk',
        'sat_column_names': [
            'call_name', 'body_type', 'name', 'description', 'author',
            'xml_interface', 'xml_body', 'date_of_create', 'date_of_edit'
        ],
        'stg_table_name': 'stg_pou_user_defined_items',
        'stg_column_names': [
            'CallName', 'BodyType', 'Name', 'Description', 'Author',
            'XmlInterface', 'XmlBody', 'DateOfCreate', 'DateOfEdit'
        ],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_pou_user_tree_items',
        'sat_table_name': 's_pou_user_tree_items',
        'hab_sk_column_name': 'h_pou_user_tree_item_sk',
        'sat_column_names': ['name', 'description', 'parent_id'],
        'stg_table_name': 'stg_pou_user_items_tree',
        'stg_column_names': ['Name', 'Description', 'ParentID'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_preset_chart_settings',
        'sat_table_name': 's_preset_chart_settings',
        'hab_sk_column_name': 'h_preset_chart_setting_sk',
        'sat_column_names': ['preset_chart_settings', 'user_id'],
        'stg_table_name': 'stg_preset_chart_settings',
        'stg_column_names': ['PresetChartSettings', 'UserId'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_object_rules',
        'sat_table_name': 's_object_rules',
        'hab_sk_column_name': 'h_object_rule_sk',
        'sat_column_names': [ 'pou_call_name',
            'comment', 'enabled', 'run_level', 'from_template_name',
            'is_in_template', 'tag', 'running_period'
        ],
        'stg_table_name': 'stg_object_rules',
        'stg_column_names': ['PouCallName', 'Comment', 'Enabled', 'RunLevel',
            'FromTemplateName', 'IsInTemplate', 'Tag', 'RunningPeriod'
        ],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_objects',
        'sat_table_name': 's_objects',
        'hab_sk_column_name': 'h_object_sk',
        'sat_column_names': [
            'name', 'tag_name', 'parent_id', 'description', 'template_name',
            'from_template_name', 'date_created', 'date_modified',
            'cdb_sync_date', 'cdb_version', 'cdb_id'
        ],
        'stg_table_name': 'stg_objects',
        'stg_column_names': [
            'Name', 'TagName', 'ParentID', 'Description', 'TemplateName',
            'FromTemplateName', 'DateCreated', 'DateModified', 'CdbSyncDate', 'CdbVersion', 'CdbID'
        ],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_user_logs',
        'sat_table_name': 's_user_logs',
        'hab_sk_column_name': 'h_user_log_sk',
        'sat_column_names': ['user_id', 'date_datetime', 'action_description'],
        'stg_table_name': 'stg_user_log',
        'stg_column_names': ['UserID', 'Date', 'ActionDescription'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_tik_scada_logs',
        'sat_table_name': 's_tik_scada_logs',
        'hab_sk_column_name': 'h_tik_scada_log_sk',
        'sat_column_names': [
            'timestamp_datetime', 'message', 'acked',
            'acked_timestamp', 'source_path', 'aggregate_path', 'participant'
        ],
        'stg_table_name': 'stg_tik_scada_log',
        'stg_column_names': [
            'Timestamp', 'Message', 'Acked', 'AckedTimestamp',
            'SourcePath', 'AggregatePath', 'Participant'
        ],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_tik_expert_slices',
        'sat_table_name': 's_tik_expert_slices',
        'hab_sk_column_name': 'h_tik_expert_slice_sk',
        'sat_column_names': ['slice_date', 'numeric_x', 'comment'],
        'stg_table_name': 'stg_tik_expert_slices',
        'stg_column_names': ['SliceDate', 'NumericX', 'Comment'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_object_type_descriptors',
        'sat_table_name': 's_object_type_descriptors',
        'hab_sk_column_name': 'h_object_type_descriptor_sk',
        'sat_column_names': ['name', 'description','tag', 'parent_id'],
        'stg_table_name': 'stg_object_type_descriptors',
        'stg_column_names': ['Name', 'Description', 'Tag', 'ParentID'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_pion_route_objects_lists',
        'sat_table_name': 's_pion_route_objects_lists',
        'hab_sk_column_name': 'h_pion_route_objects_list_sk',
        'sat_column_names': [
            'name', 'description', 'xml_objects_list',
            'date_modified', 'date_modified_cbd'
        ],
        'stg_table_name': 'stg_pion_route_objects_lists',
        'stg_column_names': ['Name', 'Description', 'xmlObjectsList', 'DateModified', 'DateModifiedCBD'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_user_defined_property_lists',
        'sat_table_name': 's_user_defined_property_lists',
        'hab_sk_column_name': 'h_user_defined_property_list_sk',
        'sat_column_names': ['property_list_name', 'description', 'xml_properties_list'],
        'stg_table_name': 'stg_user_defined_property_lists',
        'stg_column_names': ['PropertyListName', 'Description', 'XmlPropertiesList'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_user_defined_property_lists_types',
        'sat_table_name': 's_user_defined_property_lists_types',
        'hab_sk_column_name': 'h_user_defined_property_list_type_sk',
        'sat_column_names': ['property_list_type_name', 'description'],
        'stg_table_name': 'stg_user_defined_property_lists_types',
        'stg_column_names': ['PropertyListTypeName', 'Description'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_user_defined_tiles_property_configs',
        'sat_table_name': 's_user_defined_tiles_property_configs',
        'hab_sk_column_name': 'h_user_defined_tiles_property_config_sk',
        'sat_column_names': ['name', 'tiles_properties'],
        'stg_table_name': 'stg_user_defined_tiles_properties_configs',
        'stg_column_names': ['Name', 'TilesProperties'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_io_creyt_channel_configs',
        'sat_table_name': 's_io_creyt_channel_configs',
        'hab_sk_column_name': 'h_io_creyt_channel_config_sk',
        'sat_column_names': ['channel_num', 'min_raw', 'max_raw', 'min_eu', 'max_eu'],
        'stg_table_name': 'stg_io_creyt_channel_configs',
        'stg_column_names': ['ChannelNum', 'MinRaw', 'MaxRaw', 'MinEU', 'MaxEU'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_io_creyt_channel_states',
        'sat_table_name': 's_io_creyt_channel_states',
        'hab_sk_column_name': 'h_io_creyt_channel_state_sk',
        'sat_column_names': ['channel_num'],
        'stg_table_name': 'stg_io_creyt_channel_states',
        'stg_column_names': ['ChannelNum'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_io_creyt_configs',
        'sat_table_name': 's_io_creyt_configs',
        'hab_sk_column_name': 'h_io_creyt_config_sk',
        'sat_column_names': [
            'primary_ip', 'secondary_ip', 'primary_modbus_port', 'secondary_modbus_port',
            'primary_http_port', 'secondary_http_port', 'scan_time', 'connect_retries',
            'samples_file_name', 'is_sync_read_slave', 'sync_read_group_name', 'detect_failures',
            'detect_failures_length', 'enabled_secondary_ip', 'num_block_samples'
        ],
        'stg_table_name': 'stg_io_creyt_configs',
        'stg_column_names': [
            'PrimaryIP', 'SecondaryIP', 'PrimaryModbusPort', 'SecondaryModbusPort',
            'PrimaryHttpPort', 'SecondaryHttpPort', 'ScanTime', 'ConnectRetries', 'SamplesFileName',
            'IsSyncReadSlave', 'SyncReadGroupName', 'DetectFailures', 'DetectFailureLength',
            'EnabledSecondaryIP', 'NumBlockSamples'
        ],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_io_creyt_im_oper_times',
        'sat_table_name': 's_io_creyt_im_oper_times',
        'hab_sk_column_name': 'h_io_creyt_im_oper_time_sk',
        'sat_column_names': [], 
        'stg_table_name': 'stg_io_creyt_im_oper_times',
        'stg_column_names': [],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    # {
    #     'config_name': 's_io_creyt_channel_configs',
    #     'sat_table_name': 's_io_creyt_channel_configs',
    #     'hab_sk_column_name': 'h_io_creyt_channel_config_sk',
    #     'sat_column_names': ['channel_num', 'min_raw', 'max_raw', 'min_eu', 'max_eu'],
    #     'stg_table_name': 'stg_io_creyt_channel_configs_v5',
    #     'stg_column_names': ['ChannelNum', 'MinRaw', 'MaxRaw', 'MinEU', 'MaxEU']
    # },
    # {
    #     'config_name': 's_io_creyt_channel_states',
    #     'sat_table_name': 's_io_creyt_channel_states',
    #     'hab_sk_column_name': 'h_io_creyt_channel_state_sk',
    #     'sat_column_names': ['channel_num'],
    #     'stg_table_name': 'stg_io_creyt_channel_states_v5',
    #     'stg_column_names': ['ChannelNum']
    # },
    # {
    #     'config_name': 's_io_creyt_configs',
    #     'sat_table_name': 's_io_creyt_configs',
    #     'hab_sk_column_name': 'h_io_creyt_config_sk',
    #     'sat_column_names': [
    #         'primary_ip', 'secondary_ip', 'primary_modbus_port', 'secondary_modbus_port',
    #         'primary_http_port', 'secondary_http_port', 'scan_time', 'connect_retries',
    #         'samples_file_name', 'is_sync_read_slave', 'sync_read_group_name', 'detect_failures',
    #         'detect_failures_length', 'enabled_secondary_ip', 'num_block_samples'
    #     ],
    #     'stg_table_name': 'stg_io_creyt_configs_v5',
    #     'stg_column_names': [
    #         'PrimaryIP', 'SecondaryIP', 'PrimaryModbusPort', 'SecondaryModbusPort',
    #         'PrimaryHttpPort', 'SecondaryHttpPort', 'ScanTime', 'ConnectRetries', 'SamplesFileName',
    #         'IsSyncReadSlave', 'SyncReadGroupName', 'DetectFailures', 'DetectFailureLength',
    #         'EnabledSecondaryIP', 'NumBlockSamples'
    #     ]
    # },
    # {
    #     'config_name': 's_io_creyt_im_oper_times',
    #     'sat_table_name': 's_io_creyt_im_oper_times',
    #     'hab_sk_column_name': 'h_io_creyt_im_oper_time_sk',
    #     'sat_column_names': [], 
    #     'stg_table_name': 'stg_io_creyt_im_oper_times_v5',
    #     'stg_column_names': []
    # },
    {
        'config_name': 's_io_creyt_controller_states',
        'sat_table_name': 's_io_creyt_controller_states',
        'hab_sk_column_name': 'h_io_creyt_controller_state_sk',
        'sat_column_names': ['row_num'],
        'stg_table_name': 'stg_io_creyt_controller_states',
        'stg_column_names': ['RowNum'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_io_lcard_channel_configs',
        'sat_table_name': 's_io_lcard_channel_configs',
        'hab_sk_column_name': 'h_io_lcard_channel_config_sk',
        'sat_column_names': [
            'slot', 'channel_number', 'is_scaled', 'min_raw',
            'max_raw', 'min_eu', 'max_eu',
        ],
        'stg_table_name': 'stg_io_l_card_channel_configs',
        'stg_column_names': [
            'Slot', 'ChannelNumber', 'IsScaled',
            'MinRaw', 'MaxRaw', 'MinEU', 'MaxEU'
        ],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_io_lcard_crate_configs',
        'sat_table_name': 's_io_lcard_crate_configs',
        'hab_sk_column_name': 'h_io_lcard_crate_config_sk',
        'sat_column_names': ['ip', 'port', 'serial_number', 'sampling_time', 'scan_time'],
        'stg_table_name': 'stg_io_l_card_crate_configs',
        'stg_column_names': ['IP', 'Port', 'SerialNumber', 'SamplingTime', 'ScanTime'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_io_lcard_configs',
        'sat_table_name': 's_io_lcard_configs',
        'hab_sk_column_name': 'h_io_lcard_config_sk',
        'sat_column_names': [
            'virtual_slot', 'sampling_rate', 'operation_mode',
            'timer_sampling_time', 'timer_scan_time', 'sync_sampling_time'
        ],
        'stg_table_name': 'stg_io_l_card_configs',
        'stg_column_names': [
            'VirtualSlot', 'SamplingRate', 'OperationMode',
            'TimerSamplingTime', 'TimerScanTime', 'SyncSamplingTime'
        ],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_io_lcard_sync_crates',
        'sat_table_name': 's_io_lcard_sync_crates',
        'hab_sk_column_name': 'h_io_lcard_sync_crate_sk',
        'sat_column_names': ['is_sync', 'is_leader', 'is_slave', 'leader_id'],
        'stg_table_name': 'stg_io_l_card_crate_sync',
        'stg_column_names': ['IsSync', 'IsLeader', 'IsSlave', 'LeaderID'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_io_lcard_input_configs',
        'sat_table_name': 's_io_lcard_input_configs',
        'hab_sk_column_name': 'h_io_lcard_input_config_sk',
        'sat_column_names': [
            'input_num', 'input_range', 'is_scaled',
            'min_raw', 'max_raw', 'min_eu', 'max_eu'
        ],
        'stg_table_name': 'stg_io_l_card_input_configs',
        'stg_column_names': [
            'InputNum', 'InputRange', 'IsScale',
            'MinRaw', 'MaxRaw', 'MinEU', 'MaxEU'
        ],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_io_lcard_logic_input_configs',
        'sat_table_name': 's_io_lcard_logic_input_configs',
        'hab_sk_column_name': 'h_io_lcard_logic_input_config_sk',
        'sat_column_names': [
            'input_number', 'input_range', 'is_scaled',
            'min_raw', 'max_raw', 'min_eu', 'max_eu'
        ],
        'stg_table_name': 'stg_io_l_card_logic_input_configs',
        'stg_column_names': [
            'InputNumber', 
            'InputRange', 'IsScale', 'MinRaw', 'MaxRaw', 'MinEU', 'MaxEU'
        ],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_io_lcard_module_configs',
        'sat_table_name': 's_io_lcard_module_configs',
        'hab_sk_column_name': 'h_io_lcard_module_config_sk',
        'sat_column_names': ['slot', 'frequency_divisor'],
        'stg_table_name': 'stg_io_l_card_module_configs',
        'stg_column_names': ['Slot', 'FrequencyDivisor'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_io_modbus_tcp_register_configs',
        'sat_table_name': 's_io_modbus_tcp_register_configs',
        'hab_sk_column_name': 'h_io_modbus_tcp_register_config_sk',
        'sat_column_names': [
            'register_address', 'byte_order', 'min_raw', 'max_raw', 'min_eu',
            'max_eu', 'is_scaled', 'factor', 'is_write'
        ],
        'stg_table_name': 'stg_io_modbus_tcp_register_configs',
        'stg_column_names': [
            'RegisterAddress', 'ByteOrder',
            'MinRaw', 'MaxRaw', 'MinEU', 'MaxEU',
            'IsScale', 'Factor', 'IsWrite'
        ],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_io_modbus_tcp_bit_decompression_configs',
        'sat_table_name': 's_io_modbus_tcp_bit_decompression_configs',
        'hab_sk_column_name': 'h_io_modbus_tcp_bit_decompression_config_sk',
        'sat_column_names': [
            'bit_number', 'is_write',
            'mask_inversion'
        ],
        'stg_table_name': 'stg_io_modbus_tcp_register_bit_decompression_configs',
        'stg_column_names': ['BitNumber', 'IsWrite', 'MakeInversion'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_io_modbus_tcp_configs',
        'sat_table_name': 's_io_modbus_tcp_configs',
        'hab_sk_column_name': 'h_io_modbus_tcp_config_sk',
        'sat_column_names': [
            'device_id', 'primary_ip', 'secondary_ip', 'primary_port', 'secondary_port',
            'scan_time', 'coil_status_max', 'input_status_max', 'holding_register_max', 'input_register_max',
            'connected_retries', 'secondary_enabled', 'connect_to_primary_available',
            'sync_read_group_name',  'serial_port_name_primary',
            'serial_port_name_secondary', 'serial_baud_rate', 'serial_parity',
            'serial_bit_count', 'serial_stop_bits', 'log_level', 'receive_timeout',
            'write_timeout', 'period_write'
        ],
        'stg_table_name': 'stg_io_modbus_tcp_configs',
        'stg_column_names': [
            'DeviceID', 'PrimaryIP', 'SecondaryIP', 'PrimaryPort', 'SecondaryPort',
            'ScanTime', 'CoilStatusMax', 'InputStatusMax', 'HoldingRegisterMax', 'InputRegisterMax',
            'ConnectRetries', 'SecondaryEnabled',  'ConnectToPtimaryIfAvailable',
            'SyncReadGroupName', 'Serial_PortName_Primary', 'Serial_PortName_Secondary',
            'Serial_BaudRate', 'Serial_Parity', 'Serial_BitCount', 'Serial_StopBits', 'LogLevel',
            'ReceiveTimeout', 'WriteTimeout', 'PeriodWrite'
        ],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_io_opc_da_client_item_configs',
        'sat_table_name': 's_io_opc_da_client_item_configs',
        'hab_sk_column_name': 'h_io_opc_da_client_item_config_sk',
        'sat_column_names': ['item_id', 'group_name', 'is_active'],
        'stg_table_name': 'stg_io_opc_da_client_item_configs',
        'stg_column_names': ['ItemID', 'GroupName', 'IsActive'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_io_opc_da_client_configs',
        'sat_table_name': 's_io_opc_da_client_configs',
        'hab_sk_column_name': 'h_io_opc_da_client_config_sk',
        'sat_column_names': ['ip', 'server_name', 'connect_retries'],
        'stg_table_name': 'stg_io_opc_da_client_configs',
        'stg_column_names': ['IP', 'ServerName', 'ConnectRetries'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_io_opc_da_client_group_configs',
        'sat_table_name': 's_io_opc_da_client_group_configs',
        'hab_sk_column_name': 'h_io_opc_da_client_group_config_sk',
        'sat_column_names': ['name', 'is_active', 'update_rate'],
        'stg_table_name': 'stg_io_opc_da_client_group_configs',
        'stg_column_names': ['Name', 'IsActive', 'UpdateRate'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_io_opc_ua_client_transform_item_configs',
        'sat_table_name': 's_io_opc_ua_client_transform_item_configs',
        'hab_sk_column_name': 'h_io_opc_ua_client_transform_item_config_sk',
        'sat_column_names': [
            'full_path_name', 'sample_item_id', 'min_raw', 'max_raw',
            'min_eu', 'max_eu', 'is_scale', 'factor'
        ],
        'stg_table_name': 'stg_io_opc_ua_client_transform_item_configs',
        'stg_column_names': [
            'FullPathName', 'SampleItemID',
            'MinRaw', 'MaxRaw', 'MinEU', 'MaxEU', 'IsScale', 'Factor'
        ],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_io_opc_ua_client_configs',
        'sat_table_name': 's_io_opc_ua_client_configs',
        'hab_sk_column_name': 'h_io_opc_ua_client_config_sk',
        'sat_column_names': [
            'ip', 'server_name', 'connect_retries', 'authentication_id',
            'user_name', 'password', 'certificate_file_path', 'private_key_file_path',
            'num_block_samples', 'num_group_sample'
        ],
        'stg_table_name': 'stg_io_opc_ua_client_configs',
        'stg_column_names': [
            'IP', 'ServerName', 'ConnectRetries', 'AuthenticationId',
            'UserName', 'Password', 'CertificateFilePath', 'PrivateKeyFilePath',
            'NumBlockSamples', 'NumGroupSample'
        ],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_io_opc_ua_client_group_configs',
        'sat_table_name': 's_io_opc_ua_client_group_configs',
        'hab_sk_column_name': 'h_io_opc_ua_client_group_config_sk',
        'sat_column_names': ['name', 'is_active', 'update_rate', 'is_subscription'],
        'stg_table_name': 'stg_io_opc_ua_client_group_configs',
        'stg_column_names': ['Name', 'IsActive', 'UpdateRate', 'IsSubscription'],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    {
        'config_name': 's_io_opc_ua_client_item_configs',
        'sat_table_name': 's_io_opc_ua_client_item_configs',
        'hab_sk_column_name': 'h_io_opc_ua_client_item_config_sk',
        'sat_column_names': [
            'item_id', 'group_name', 'is_active',
            'full_path_name', 'to_server'
        ],
        'stg_table_name': 'stg_io_opc_ua_client_item_configs',
        'stg_column_names': [
            'ItemID', 'GroupName', 'IsActive', 
            'FullPathName', 'ToServer'
        ],
        'stg_hash_sk_column_name': 'hash_sk',
        'stg_hash_sat_diff_column_name': 'hash_sat_diff'
    },
    # {
    #     'config_name': 's_object_data_values',
    #     'sat_table_name': 's_object_data_values',
    #     'hab_sk_column_name': 'h_object_data_value_sk',
    #     'sat_column_names': ['date_time', 'quality', 'comment', 'value', 'is_deleted'],
    #     'stg_table_name': 'stg_object_data_values_v2',
    #     'stg_column_names': ['Date', 'Quality', 'Comment', 'Value', 'IsDeleted']
    # },
    # {
    #     'config_name': 's_any_double_data_records',
    #     'sat_table_name': 's_any_double_data_records',
    #     'hab_sk_column_name': 'h_any_double_data_record_sk',
    #     'sat_column_names': ['date_time', 'value'],
    #     'stg_table_name': 'stg_object_data_values_v2',
    #     'stg_column_names': ['Date', 'Quality', 'Comment', 'Value', 'IsDeleted']
    # },
    # {
    #     'config_name': 's_bode_data_records',
    #     'sat_table_name': 's_bode_data_records',
    #     'hab_sk_column_name': 'h_bode_data_record_sk',
    #     'sat_column_names': [
    #         'DataTime', 'Quality', 'Comment', 'MagnitudeValue',
    #         'PhaseValue', 'TurnoverFrequencyValue', 'ComparisonPosition', 'isDeleted'
    #     ],
    #     'stg_table_name': 'stg_object_data_values_v2',
    #     'stg_column_names': ['ID', 'PropertyID', 'MeasureUnitID', 'DataTypeID', 'Date', 'Quality', 'Comment', 'Value', 'IsDeleted']
    # },
    # {
    #     'config_name': 's_boolean_data_records',
    #     'sat_table_name': 's_boolean_data_records',
    #     'hab_sk_column_name': 'h_boolean_data_record_sk',
    #     'sat_column_names': ['DataTime', 'Quality', 'Comment', 'Value', 'isDeleted'],
    #     'stg_table_name': 'stg_object_data_values_v2',
    #     'stg_column_names': ['ID', 'PropertyID', 'MeasureUnitID', 'DataTypeID', 'Date', 'Quality', 'Comment', 'Value', 'IsDeleted']
    # },
    # {
    #     'config_name': 's_date_sample_data_records',
    #     'sat_table_name': 's_date_sample_data_records',
    #     'hab_sk_column_name': 'h_date_sample_data_record_sk',
    #     'sat_column_names': ['DataID', 'DateTime', 'Quality', 'Comment', 'SampleRate', 'RawValues', 'isDeleted'],
    #     'stg_table_name': 'stg_object_data_values_v2',
    #     'stg_column_names': ['ID', 'PropertyID', 'MeasureUnitID', 'DataTypeID', 'Date', 'Quality', 'Comment', 'Value', 'IsDeleted']
    # },
    # {
    #     'config_name': 's_date_time_data_records',
    #     'sat_table_name': 's_date_time_data_records',
    #     'hab_sk_column_name': 'h_date_time_data_record_sk',
    #     'sat_column_names': ['DataID', 'DateTime', 'Quality', 'Comment', 'Value', 'isDeleted'],
    #     'stg_table_name': 'stg_object_data_values_v2',
    #     'stg_column_names': ['ID', 'PropertyID', 'MeasureUnitID', 'DataTypeID', 'Date', 'Quality', 'Comment', 'Value', 'IsDeleted']
    # },
    # {
    #     'config_name': 's_date_time_array_data_records',
    #     'sat_table_name': 's_date_time_array_data_records',
    #     'hab_sk_column_name': 'h_date_time_array_data_record_sk',
    #     'sat_column_names': ['DataID', 'DateTime', 'Quality', 'Comment', 'Value', 'isDeleted'],
    #     'stg_table_name': 'stg_object_data_values_v2',
    #     'stg_column_names': ['ID', 'PropertyID', 'MeasureUnitID', 'DataTypeID', 'Date', 'Quality', 'Comment', 'Value', 'IsDeleted']
    # },
    # {
    #     'config_name': 's_diagnostic_array_data_records',
    #     'sat_table_name': 's_diagnostic_array_data_records',
    #     'hab_sk_column_name': 'h_diagnostic_array_data_record_sk',
    #     'sat_column_names': ['DataID', 'DateTime', 'Quality', 'Comment', 'Value', 'isDeleted'],
    #     'stg_table_name': 'stg_object_data_values_diagnostic_array_v2',
    #     'stg_column_names': ['ValueID', 'DiagID', 'TagName', 'DefectState', 'DefectName', 'DefectDetails', 'Recomendation', 'Priority', 'GroupName']
    # },
    # {
    #     'config_name': 's_duration_data_records',
    #     'sat_table_name': 's_duration_data_records',
    #     'hab_sk_column_name': 'h_duration_data_record_sk',
    #     'sat_column_names': ['DataID', 'DateTime', 'Quality', 'Comment', 'Value', 'isDeleted'],
    #     'stg_table_name': 'stg_object_data_values_v2',
    #     'stg_column_names': ['ID', 'PropertyID', 'MeasureUnitID', 'DataTypeID', 'Date', 'Quality', 'Comment', 'Value', 'IsDeleted']
    # },
    # {
    #     'config_name': 's_file_data_records',
    #     'sat_table_name': 's_file_data_records',
    #     'hab_sk_column_name': 'h_file_data_record_sk',
    #     'sat_column_names': ['DataID', 'DateTime', 'Quality', 'Comment', 'Value', 'FilePath', 'File', 'Checksum', 'isDeleted'],
    #     'stg_table_name': 'stg_object_data_values_v2',
    #     'stg_column_names': ['ID', 'PropertyID', 'MeasureUnitID', 'DataTypeID', 'Date', 'Quality', 'Comment', 'Value', 'IsDeleted']
    # },
    # {
    #     'config_name': 's_float64_data_records',
    #     'sat_table_name': 's_float64_data_records',
    #     'hab_sk_column_name': 'h_float64_data_record_sk',
    #     'sat_column_names': ['DataID', 'DateTime', 'Quality', 'Comment', 'Value', 'isDeleted'],
    #     'stg_table_name': 'stg_object_data_values_v2',
    #     'stg_column_names': ['ID', 'PropertyID', 'MeasureUnitID', 'DataTypeID', 'Date', 'Quality', 'Comment', 'Value', 'IsDeleted']
    # },
    # {
    #     'config_name': 's_float_array_data_records',
    #     'sat_table_name': 's_float_array_data_records',
    #     'hab_sk_column_name': 'h_float_array_data_record_sk',
    #     'sat_column_names': ['DataID', 'DateTime', 'Quality', 'Comment', 'Value', 'isDeleted'],
    #     'stg_table_name': 'stg_object_data_values_v2',
    #     'stg_column_names': ['ID', 'PropertyID', 'MeasureUnitID', 'DataTypeID', 'Date', 'Quality', 'Comment', 'Value', 'IsDeleted']
    # },
    # {
    #     'config_name': 's_int32_data_records',
    #     'sat_table_name': 's_int32_data_records',
    #     'hab_sk_column_name': 'h_int32_data_record_sk',
    #     'sat_column_names': ['DataID', 'DateTime', 'Quality', 'Comment', 'Value', 'isDeleted'],
    #     'stg_table_name': 'stg_object_data_values_v2',
    #     'stg_column_names': ['ID', 'PropertyID', 'MeasureUnitID', 'DataTypeID', 'Date', 'Quality', 'Comment', 'Value', 'IsDeleted']
    # },
    # {
    #     'config_name': 's_int64_data_records',
    #     'sat_table_name': 's_int64_data_records',
    #     'hab_sk_column_name': 'h_int64_data_record_sk',
    #     'sat_column_names': ['DataID', 'DateTime', 'Quality', 'Comment', 'Value', 'isDeleted'],
    #     'stg_table_name': 'stg_object_data_values_v2',
    #     'stg_column_names': ['ID', 'PropertyID', 'MeasureUnitID', 'DataTypeID', 'Date', 'Quality', 'Comment', 'Value', 'IsDeleted']
    # },
    # {
    #     'config_name': 's_int_array_data_records',
    #     'sat_table_name': 's_int_array_data_records',
    #     'hab_sk_column_name': 'h_int_array_data_record_sk',
    #     'sat_column_names': ['DataID_sk', 'DateTime', 'Quality', 'Comment', 'Value', 'isDeleted'],
    #     'stg_table_name': 'stg_object_data_values_v2',
    #     'stg_column_names': ['ID', 'PropertyID', 'MeasureUnitID', 'DataTypeID', 'Date', 'Quality', 'Comment', 'Value', 'IsDeleted']
    # },
    # {
    #     'config_name': 's_spectrum_data_records',
    #     'sat_table_name': 's_spectrum_data_records',
    #     'hab_sk_column_name': 'h_spectrum_data_record_sk',
    #     'sat_column_names': ['DataID', 'DateTime', 'Quality', 'Comment', 'SampleRate', 'Multiplier', 'RawValues', 'isDeleted'],
    #     'stg_table_name': 'stg_object_data_values_v2',
    #     'stg_column_names': ['ID', 'PropertyID', 'MeasureUnitID', 'DataTypeID', 'Date', 'Quality', 'Comment', 'Value', 'IsDeleted']
    # },
    # {
    #     'config_name': 's_string_data_records',
    #     'sat_table_name': 's_string_data_records',
    #     'hab_sk_column_name': 'h_string_data_record_sk',
    #     'sat_column_names': ['DataID', 'DateTime', 'Quality', 'Comment', 'Value', 'isDeleted'],
    #     'stg_table_name': 'stg_object_data_values_v2',
    #     'stg_column_names': ['ID', 'PropertyID', 'MeasureUnitID', 'DataTypeID', 'Date', 'Quality', 'Comment', 'Value', 'IsDeleted']
    # }
]


