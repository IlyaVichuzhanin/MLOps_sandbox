common_object_data_value_config = [
    {
        'dwh_table_name': 'hist_integer_data_records',
        'data_mart_table_name': 'hist_integer_data',
        'is_hist_data': True,
        'data_type': 'Int32',
        'value_is_nullable': False,
        'value_default_on_null': 0,
        'is_hist_data': True,
    },
    {
        'dwh_table_name': 'hist_bigint_data_records',
        'data_mart_table_name': 'hist_bigint_data',
        'is_hist_data': True,
        'data_type': 'Int64',
        'value_is_nullable': False,
        'value_default_on_null': 0,
    },
    {
        'dwh_table_name': 'hist_double_data_records',
        'data_mart_table_name': 'hist_double_data',
        'is_hist_data': True,
        'data_type': 'Float64',
        'value_is_nullable': False,
        'value_default_on_null': 0.0,
    },
    {
        'dwh_table_name': 'hist_boolean_data_records',
        'data_mart_table_name': 'hist_boolean_data',
        'is_hist_data': True,
        'data_type': 'Bool',
        'value_is_nullable': False,
        'value_default_on_null': False,
    },
    {
        'dwh_table_name': 'hist_timestamp_data_records',
        'data_mart_table_name': 'hist_timestamp_data',
        'is_hist_data': True,
        'data_type': 'DateTime',
        'value_is_nullable': True,
        'value_default_on_null': None,
    },
    {
        'dwh_table_name': 'hist_interval_data_records',
        'data_mart_table_name': 'hist_interval_data',
        'is_hist_data': True,
        'data_type': 'String',
        'value_is_nullable': True,
        'value_default_on_null': None,
    },
    {
        'dwh_table_name': 'hist_text_data_records',
        'data_mart_table_name': 'hist_text_data',
        'is_hist_data': True,
        'data_type': 'String',
        'value_is_nullable': True,
        'value_default_on_null': None,
    },
    {
        'dwh_table_name': 'hist_integer_array_data_records',
        'data_mart_table_name': 'hist_integer_array_data',
        'is_hist_data': True,
        'data_type': 'Array(Int64)',
        'value_is_nullable': False,
        'value_default_on_null': [],
    },
    {
        'dwh_table_name': 'hist_text_array_data_records',
        'data_mart_table_name': 'hist_text_array_data',
        'is_hist_data': True,
        'data_type': 'Array(String)',
        'value_is_nullable': False,
        'value_default_on_null': [],
    },
    {
        'dwh_table_name': 'hist_double_array_data_records',
        'data_mart_table_name': 'hist_double_array_data',
        'is_hist_data': True,
        'data_type': 'Array(Float64)',
        'value_is_nullable': False,
        'value_default_on_null': [],
    },
    {
        'dwh_table_name': 'hist_timestamp_array_data_records',
        'data_mart_table_name': 'hist_hist_timestamp_array_data',
        'is_hist_data': True,
        'data_type': 'Array(DateTime)',
        'value_is_nullable': False,
        'value_default_on_null': [],
    },
    
    
    {
        'dwh_table_name': 'nonhist_integer_data_records',
        'data_mart_table_name': 'nonhist_integer_data',
        'is_hist_data': False,
        'data_type': 'Int32',
        'value_is_nullable': False,
        'value_default_on_null': 0,
    },
    {
        'dwh_table_name': 'nonhist_bigint_data_records',
        'data_mart_table_name': 'nonhist_bigint_data',
        'is_hist_data': False,
        'data_type': 'Int64',
        'value_is_nullable': False,
        'value_default_on_null': 0,
    },
    {
        'dwh_table_name': 'nonhist_double_data_records',
        'data_mart_table_name': 'nonhist_double_data',
        'is_hist_data': False,
        'data_type': 'Float64',
        'value_is_nullable': False,
        'value_default_on_null': 0.0,
    },
    {
        'dwh_table_name': 'nonhist_boolean_data_records',
        'data_mart_table_name': 'nonhist_boolean_data',
        'is_hist_data': False,
        'data_type': 'Bool',
        'value_is_nullable': False,
        'value_default_on_null': False,
    },
    {
        'dwh_table_name': 'nonhist_timestamp_data_records',
        'data_mart_table_name': 'nonhist_timestamp_data',
        'is_hist_data': False,
        'data_type': 'DateTime',
        'value_is_nullable': True,
        'value_default_on_null': None,
    },
    {
        'dwh_table_name': 'nonhist_interval_data_records',
        'data_mart_table_name': 'nonhist_interval_data',
        'is_hist_data': False,
        'data_type': 'String',
        'value_is_nullable': True,
        'value_default_on_null': None,
    },
    {
        'dwh_table_name': 'nonhist_text_data_records',
        'data_mart_table_name': 'nonhist_text_data',
        'is_hist_data': False,
        'data_type': 'String',
        'value_is_nullable': True,
        'value_default_on_null': None,
    },
    {
        'dwh_table_name': 'nonhist_integer_array_data_records',
        'data_mart_table_name': 'nonhist_integer_array_data',
        'is_hist_data': False,
        'data_type': 'Array(Int64)',
        'value_is_nullable': False,
        'value_default_on_null': [],
    },
    {
        'dwh_table_name': 'nonhist_text_array_data_records',
        'data_mart_table_name': 'nonhist_text_array_data',
        'is_hist_data': False,
        'data_type': 'Array(String)',
        'value_is_nullable': False,
        'value_default_on_null': [],
    },
    {
        'dwh_table_name': 'nonhist_double_array_data_records',
        'data_mart_table_name': 'nonhist_double_array_data',
        'is_hist_data': False,
        'data_type': 'Array(Float64)',
        'value_is_nullable': False,
        'value_default_on_null': [],
    },
    {
        'dwh_table_name': 'nonhist_timestamp_array_data_records',
        'data_mart_table_name': 'nonhist_hist_timestamp_array_data',
        'is_hist_data': False,
        'data_type': 'Array(DateTime)',
        'value_is_nullable': False,
        'value_default_on_null': [],
    },
]

creyt_iceberg_upload_config = [
    {
        'dwh_table_name': 'hist_creyt_data_records',
        'is_hist_data': True,
    },
    {
        'dwh_table_name': 'nonhist_creyt_data_records',
        'is_hist_data': False,
    },
]

# В upload_data_configs/upload_to_data_mart_configs.py

lcard_iceberg_upload_config = [
    {
        'dwh_table_name': 'hist_lcard_data_records',
        'is_hist_data': True,
    },
    {
        'dwh_table_name': 'nonhist_lcard_data_records',
        'is_hist_data': False,
    },
]

bode_iceberg_upload_config = [
    {
        'dwh_table_name': 'hist_bode_data_records',
        'is_hist_data': True,
    },
    {
        'dwh_table_name': 'nonhist_bode_data_records',
        'is_hist_data': False,
    },
]

# В upload_data_configs/upload_to_data_mart_configs.py

spectrum_iceberg_upload_config = [
    {
        'dwh_table_name': 'hist_spectrum_data_records',
        'is_hist_data': True,
    },
    {
        'dwh_table_name': 'nonhist_spectrum_data_records',
        'is_hist_data': False,
    },
]

# В upload_data_configs/upload_to_data_mart_configs.py

sample_data_iceberg_upload_config = [
    {
        'dwh_table_name': 'hist_sample_data_records',
        'is_hist_data': True,
    },
    {
        'dwh_table_name': 'nonhist_sample_data_records',
        'is_hist_data': False,
    },
]

# В upload_data_configs/upload_to_data_mart_configs.py

siemens_sample_iceberg_upload_config = [
    {
        'dwh_table_name': 'hist_siemens_sample_data_records',
        'is_hist_data': True,
    },
    {
        'dwh_table_name': 'nonhist_siemens_sample_data_records',
        'is_hist_data': False,
    },
]

diagnostic_data_iceberg_upload_config = [
    {
        'dwh_table_name': 'hist_diagnostic_array_data_records',
        'is_hist_data': True,
    },
    {
        'dwh_table_name': 'nonhist_diagnostic_array_data_records',
        'is_hist_data': False,
    },
]


















creyt_clickhouse_upload_config = [
    {
        'dwh_table_name': 'hist_creyt_data_records',
        'clickhouse_table_name': 'hist_creyt_timeseries_data',
        'is_hist_data': True,
    },
    {
        'dwh_table_name': 'nonhist_creyt_data_records',
        'clickhouse_table_name': 'nonhist_creyt_timeseries_data',
        'is_hist_data': False,
    },
]

# В upload_data_configs/upload_to_clickhouse_configs.py

lcard_clickhouse_upload_config = [
    {
        'dwh_table_name': 'hist_lcard_data_records',
        'clickhouse_table_name': 'hist_lcard_timeseries_data',
        'is_hist_data': True,
    },
    {
        'dwh_table_name': 'nonhist_lcard_data_records',
        'clickhouse_table_name': 'nonhist_lcard_timeseries_data',
        'is_hist_data': False,
    },
]

bode_clickhouse_upload_config = [
    {
        'dwh_table_name': 'hist_bode_data_records',
        'clickhouse_table_name': 'hist_bode_data',
        'is_hist_data': True,
    },
    {
        'dwh_table_name': 'nonhist_bode_data_records',
        'clickhouse_table_name': 'nonhist_bode_data',
        'is_hist_data': False,
    },
]

# В upload_data_configs/upload_to_clickhouse_configs.py

spectrum_clickhouse_upload_config = [
    {
        'dwh_table_name': 'hist_spectrum_data_records',
        'clickhouse_table_name': 'hist_spectrum_data',
        'is_hist_data': True,
        'clickhouse_batch_size': 100000,
        'cloudberry_fetch_chunk': 10000,
    },
    {
        'dwh_table_name': 'nonhist_spectrum_data_records',
        'clickhouse_table_name': 'nonhist_spectrum_data',
        'is_hist_data': False,
        'clickhouse_batch_size': 100000,
        'cloudberry_fetch_chunk': 10000,
    },
]

# В upload_data_configs/upload_to_clickhouse_configs.py

sample_data_clickhouse_upload_config = [
    {
        'dwh_table_name': 'hist_sample_data_records',
        'clickhouse_table_name': 'hist_sample_timeseries_data',
        'is_hist_data': True,
    },
    {
        'dwh_table_name': 'nonhist_sample_data_records',
        'clickhouse_table_name': 'nonhist_sample_timeseries_data',
        'is_hist_data': False,
    },
]

# В upload_data_configs/upload_to_clickhouse_configs.py

siemens_sample_clickhouse_upload_config = [
    {
        'dwh_table_name': 'hist_siemens_sample_data_records',
        'clickhouse_table_name': 'hist_siemens_sample_timeseries_data',
        'is_hist_data': True,
    },
    {
        'dwh_table_name': 'nonhist_siemens_sample_data_records',
        'clickhouse_table_name': 'hist_siemens_sample_timeseries_data',
        'is_hist_data': False,
    },
]

diagnostic_data_clickhouse_upload_config = [
    {
        'dwh_table_name': 'hist_diagnostic_array_data_records',
        'clickhouse_table_name': 'hist_diagnostic_data',
        'is_hist_data': True,
    },
    {
        'dwh_table_name': 'nonhist_diagnostic_array_data_records',
        'clickhouse_table_name': 'nonhist_diagnostic_data',
        'is_hist_data': False,
    },
]