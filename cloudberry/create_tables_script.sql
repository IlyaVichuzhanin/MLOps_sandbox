-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ==============================
-- Record Source Referenceks
-- ==============================

CREATE TABLE IF NOT EXISTS public.data_catalogue (
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    main_db_name TEXT NOT NULL,
    file_name TEXT,
    file_size BIGINT,
    file_hash TEXT CHECK (LENGTH("file_hash") = 64),
    last_update TIMESTAMP,
    upload_date_time TIMESTAMP,
    created_dttm TIMESTAMP DEFAULT NOW() NOT NULL,
    hdfs_storage_path TEXT,
    hdfs_full_path TEXT,
    data_type TEXT,
    data_format TEXT,
    is_uploaded_to_dwh BOOL DEFAULT FALSE NOT NULL,
    dwh_upload_dttm TIMESTAMPTZ,
    CONSTRAINT data_catalogue_pk PRIMARY KEY (data_source_id)
) DISTRIBUTED BY (data_source_id);

-- ==============================
-- Diagnostic Defect Types
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_diagnostic_defect_types (
    h_diagnostic_defect_type_sk uuid DEFAULT uuid_generate_v4() NOT NULL,
    load_dttm TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_diagnostic_defect_types_pk PRIMARY KEY (h_diagnostic_defect_type_sk),
    CONSTRAINT h_diagnostic_defect_types_data_catalogue_fk 
        FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_diagnostic_defect_type_sk);

CREATE TABLE IF NOT EXISTS public.s_diagnostic_defect_types (
    h_diagnostic_defect_type_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    type_id int4 NOT NULL,
    description varchar NOT NULL,
    description_ru varchar NULL,
    CONSTRAINT s_diagnostic_defect_type_h_diagnostic_defect_type_fk FOREIGN KEY (h_diagnostic_defect_type_sk) REFERENCES public.h_diagnostic_defect_types (h_diagnostic_defect_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_diagnostic_defect_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_diagnostic_defect_type_sk);

-- ==============================
-- Diagnostic Alarm States
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_diagnostic_alarm_states (
    h_diagnostic_alarm_state_sk uuid DEFAULT uuid_generate_v4() NOT NULL,
    load_dttm TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_diagnostic_alarm_states_pk PRIMARY KEY (h_diagnostic_alarm_state_sk),
    CONSTRAINT h_diagnostic_alarm_states_data_catalogue_fk 
        FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_diagnostic_alarm_state_sk);

CREATE TABLE IF NOT EXISTS public.s_diagnostic_alarm_states (
    h_diagnostic_alarm_state_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    state_id int4 NOT NULL,
    description varchar NOT NULL,
    description_ru varchar NULL,
    CONSTRAINT s_diagnostic_alarm_states_h_diagnostic_alarm_states_fk FOREIGN KEY (h_diagnostic_alarm_state_sk) REFERENCES public.h_diagnostic_alarm_states (h_diagnostic_alarm_state_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_diagnostic_alarm_states_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_diagnostic_alarm_state_sk);

-- ==============================
-- Diagnostic Defect States
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_diagnostic_defect_states (
    h_diagnostic_defect_state_sk uuid DEFAULT uuid_generate_v4() NOT NULL,
    load_dttm TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_diagnostic_defect_states_pk PRIMARY KEY (h_diagnostic_defect_state_sk),
    CONSTRAINT h_diagnostic_defect_states_data_catalogue_fk 
        FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_diagnostic_defect_state_sk);

CREATE TABLE IF NOT EXISTS public.s_diagnostic_defect_states (
    h_diagnostic_defect_state_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    state_id int4 NOT NULL,
    description varchar NOT NULL,
    description_ru varchar NULL,
    CONSTRAINT s_diagnostic_defect_state_h_diagnostic_defect_state_fk FOREIGN KEY (h_diagnostic_defect_state_sk) REFERENCES public.h_diagnostic_defect_states (h_diagnostic_defect_state_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_diagnostic_defect_states_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_diagnostic_defect_state_sk);

-- ==============================
-- Config Types
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_config_types (
    h_config_type_sk uuid DEFAULT uuid_generate_v4() NOT NULL,
    load_dttm TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_config_types_pk PRIMARY KEY (h_config_type_sk),
    CONSTRAINT h_config_types_data_catalogue_fk 
        FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_config_type_sk);

CREATE TABLE IF NOT EXISTS public.s_config_types (
    h_config_type_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    type_id uuid NOT NULL,
    description varchar NOT NULL,
    CONSTRAINT s_config_types_h_config_type_fk FOREIGN KEY (h_config_type_sk) REFERENCES public.h_config_types (h_config_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_config_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_config_type_sk);

-- ==============================
-- Index Type Data
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_index_type_data_records (
    h_index_type_data_record_sk uuid DEFAULT uuid_generate_v4() NOT NULL,
    load_dttm TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_index_type_data_record_pk PRIMARY KEY (h_index_type_data_record_sk),
    CONSTRAINT h_index_type_data_record_data_catalogue_fk 
        FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_index_type_data_record_sk);

CREATE TABLE IF NOT EXISTS public.s_index_type_data_records (
    h_index_type_data_record_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    index_type_id int4 NOT NULL,
    description varchar NOT NULL,
    description_ru varchar NOT NULL,
    CONSTRAINT s_index_type_data_record_h_index_type_data_record_fk FOREIGN KEY (h_index_type_data_record_sk) REFERENCES public.h_index_type_data_records (h_index_type_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_index_type_data_record_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_index_type_data_record_sk);

-- ==============================
-- OPC DA Data Types
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_opc_da_data_types (
    h_opc_da_data_type_sk uuid DEFAULT uuid_generate_v4() NOT NULL,
    load_dttm TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_opc_da_data_type_pk PRIMARY KEY (h_opc_da_data_type_sk),
    CONSTRAINT h_opc_da_data_type_data_catalogue_fk 
        FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_opc_da_data_type_sk);

CREATE TABLE IF NOT EXISTS public.s_opc_da_data_types (
    h_opc_da_data_type_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    data_type_id int4 NOT NULL,
    description varchar NOT NULL,
    description_ru varchar NOT NULL,
    CONSTRAINT s_opc_da_data_type_h_opc_da_data_type_fk FOREIGN KEY (h_opc_da_data_type_sk) REFERENCES public.h_opc_da_data_types (h_opc_da_data_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_opc_da_data_type_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_opc_da_data_type_sk);

-- ==============================
-- OPC UA Data Types
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_opc_ua_data_types (
    h_opc_ua_data_type_sk uuid DEFAULT uuid_generate_v4() NOT NULL,
    load_dttm TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_opc_ua_data_type_pk PRIMARY KEY (h_opc_ua_data_type_sk),
    CONSTRAINT h_opc_ua_data_type_data_catalogue_fk 
        FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_opc_ua_data_type_sk);

CREATE TABLE IF NOT EXISTS public.s_opc_ua_data_types (
    h_opc_ua_data_type_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    data_type_id int4 NOT NULL,
    description varchar NOT NULL,
    description_ru varchar NOT NULL,
    CONSTRAINT s_opc_ua_data_type_h_opc_ua_data_type_fk FOREIGN KEY (h_opc_ua_data_type_sk) REFERENCES public.h_opc_ua_data_types (h_opc_ua_data_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_opc_ua_data_type_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_opc_ua_data_type_sk);

-- ==============================
-- Server Data Types
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_data_type_servers (
    h_data_type_server_sk uuid DEFAULT uuid_generate_v4() NOT NULL,
    load_dttm TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_data_type_servers_pk PRIMARY KEY (h_data_type_server_sk),
    CONSTRAINT h_data_type_servers_data_catalogue_fk 
        FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_data_type_server_sk);

CREATE TABLE IF NOT EXISTS public.s_data_type_servers (
    h_data_type_server_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    data_type_id int4 NOT NULL,
    description varchar NOT NULL,
    description_ru varchar NOT NULL,
    CONSTRAINT s_data_type_servers_h_data_type_server_fk FOREIGN KEY (h_data_type_server_sk) REFERENCES public.h_data_type_servers (h_data_type_server_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_data_type_servers_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_data_type_server_sk);

-- ==============================
-- TIK SCADA Log Types
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_tik_scada_log_types (
    h_tik_scada_log_type_sk uuid DEFAULT uuid_generate_v4() NOT NULL,
    load_dttm TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_tik_scada_log_types_pk PRIMARY KEY (h_tik_scada_log_type_sk),
    CONSTRAINT h_tik_scada_log_types_data_catalogue_fk 
        FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_tik_scada_log_type_sk);

CREATE TABLE IF NOT EXISTS public.s_tik_scada_log_types (
    h_tik_scada_log_type_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    log_type_id int4 NOT NULL,
    description varchar NOT NULL,
    description_ru varchar NOT NULL,
    CONSTRAINT s_tik_scada_log_types_h_tik_scada_log_types_fk FOREIGN KEY (h_tik_scada_log_type_sk) REFERENCES public.h_tik_scada_log_types (h_tik_scada_log_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_tik_scada_log_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_tik_scada_log_type_sk);

-- ==============================
-- User Action Types
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_user_action_types (
    h_user_action_type_sk uuid DEFAULT uuid_generate_v4() NOT NULL,
    load_dttm TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_user_action_type_pk PRIMARY KEY (h_user_action_type_sk),
    CONSTRAINT h_user_action_type_data_catalogue_fk 
        FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_user_action_type_sk);

CREATE TABLE IF NOT EXISTS public.s_user_action_types (
    h_user_action_type_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    action_type_id int4 NOT NULL,
    description varchar NOT NULL,
    description_ru varchar NOT NULL,
    CONSTRAINT s_user_action_type_h_user_action_type_fk FOREIGN KEY (h_user_action_type_sk) REFERENCES public.h_user_action_types (h_user_action_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_user_action_type_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_user_action_type_sk);

-- ==============================
-- Hub: Running Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_running_types (
    h_running_type_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_running_types_pk PRIMARY KEY (h_running_type_sk),
    CONSTRAINT h_running_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_running_type_sk);

-- ==============================
-- Satellite: Running Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_running_types (
    h_running_type_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    type_id integer NOT NULL,
    description text NOT NULL,
    description_ru text NOT NULL,
    CONSTRAINT s_running_types_h_running_type_fk FOREIGN KEY (h_running_type_sk) REFERENCES public.h_running_types (h_running_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_running_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_running_type_sk);

-- ==============================
-- Hub: Crate Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_crate_types (
    h_crate_type_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_crate_types_pk PRIMARY KEY (h_crate_type_sk),
    CONSTRAINT h_crate_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_crate_type_sk);

-- ==============================
-- Satellite: Crate Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_crate_types (
    h_crate_type_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    type_id integer NOT NULL,
    description text NOT NULL,
    description_ru text NOT NULL,
    CONSTRAINT s_crate_types_h_crate_type_fk FOREIGN KEY (h_crate_type_sk) REFERENCES public.h_crate_types (h_crate_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_crate_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_crate_type_sk);

-- ==============================
-- Hub: L Card Logic Input Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_l_card_logic_input_types (
    h_l_card_logic_input_type_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_l_card_logic_input_types_pk PRIMARY KEY (h_l_card_logic_input_type_sk),
    CONSTRAINT h_l_card_logic_input_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_l_card_logic_input_type_sk);

-- ==============================
-- Satellite: L Card Logic Input Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_l_card_logic_input_types (
    h_l_card_logic_input_type_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    type_id integer NOT NULL,
    description text NOT NULL,
    description_ru text NOT NULL,
    CONSTRAINT s_l_card_logic_input_types_h_l_card_logic_input_type_fk FOREIGN KEY (h_l_card_logic_input_type_sk) REFERENCES public.h_l_card_logic_input_types (h_l_card_logic_input_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_l_card_logic_input_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_l_card_logic_input_type_sk);

-- ==============================
-- Hub: L Card Crate Module Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_l_card_crate_module_types (
    h_l_card_crate_module_type_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_l_card_crate_module_types_pk PRIMARY KEY (h_l_card_crate_module_type_sk),
    CONSTRAINT h_l_card_crate_module_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_l_card_crate_module_type_sk);

-- ==============================
-- Satellite: L Card Crate Module Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_l_card_crate_module_types (
    h_l_card_crate_module_type_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    type_id integer NOT NULL,
    description text NOT NULL,
    description_ru text NOT NULL,
    CONSTRAINT s_l_card_crate_module_types_h_l_card_crate_module_type_fk FOREIGN KEY (h_l_card_crate_module_type_sk) REFERENCES public.h_l_card_crate_module_types (h_l_card_crate_module_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_l_card_crate_module_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_l_card_crate_module_type_sk);

-- ==============================
-- Hub: Register Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_register_types (
    h_register_type_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_register_types_pk PRIMARY KEY (h_register_type_sk),
    CONSTRAINT h_register_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_register_type_sk);

-- ==============================
-- Satellite: Register Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_register_types (
    h_register_type_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    type_id integer NOT NULL,
    description text NOT NULL,
    description_ru text NOT NULL,
    CONSTRAINT s_register_types_h_register_type_fk FOREIGN KEY (h_register_type_sk) REFERENCES public.h_register_types (h_register_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_register_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_register_type_sk);

-- ==============================
-- Hub: Type Names
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_data_type_names (
    h_data_type_name_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_data_type_names_pk PRIMARY KEY (h_data_type_name_sk),
    CONSTRAINT h_data_type_names_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_data_type_name_sk);

-- ==============================
-- Satellite: Type Names
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_data_type_names (
    h_data_type_name_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    type_id integer NOT NULL,
    description text NOT NULL,
    description_ru text NOT NULL,
    CONSTRAINT s_data_type_names_h_data_type_name_fk FOREIGN KEY (h_data_type_name_sk) REFERENCES public.h_data_type_names (h_data_type_name_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_data_type_names_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_data_type_name_sk);

-- ==============================
-- Hub: Data Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_data_types (
    h_data_type_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_data_types_pk PRIMARY KEY (h_data_type_sk),
    CONSTRAINT h_data_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_data_type_sk);

-- ==============================
-- Satellite: Data Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_data_types (
    h_data_type_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    type_id uuid NOT NULL,
    description text NOT NULL,
    description_ru text NOT NULL,
    CONSTRAINT s_data_types_h_data_type_fk FOREIGN KEY (h_data_type_sk) REFERENCES public.h_data_types (h_data_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_data_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_data_type_sk);

-- ==============================
-- Hub: Spectrum Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_spectrum_types (
    h_spectrum_type_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_spectrum_types_pk PRIMARY KEY (h_spectrum_type_sk),
    CONSTRAINT h_spectrum_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_spectrum_type_sk);

-- ==============================
-- Satellite: Spectrum Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_spectrum_types (
    h_spectrum_type_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    type_id integer NOT NULL, -- Это значение из enum
    description text NOT NULL, -- Описание на английском
    description_ru text NOT NULL, -- Описание на русском
    CONSTRAINT s_spectrum_types_h_spectrum_type_fk FOREIGN KEY (h_spectrum_type_sk) REFERENCES public.h_spectrum_types (h_spectrum_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_spectrum_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_spectrum_type_sk);

-- ==============================
-- Hub: Property Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_property_types (
    h_property_type_sk uuid DEFAULT uuid_generate_v4() NOT NULL,
    load_dttm TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_property_types_pk PRIMARY KEY (h_property_type_sk),
    CONSTRAINT h_property_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_property_type_sk);

-- ==============================
-- Satellite: Property Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_property_types (
    h_property_type_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    type_id int4 NOT NULL,
    description varchar NOT NULL,
    description_ru varchar NOT NULL,
    CONSTRAINT s_property_types_h_property_type_fk FOREIGN KEY (h_property_type_sk) REFERENCES public.h_property_types (h_property_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_property_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_property_type_sk);

-- ==============================
-- Hub: Storage Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_storage_types (
    h_storage_type_sk uuid DEFAULT uuid_generate_v4() NOT NULL,
    load_dttm TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_storage_types_pk PRIMARY KEY (h_storage_type_sk),
    CONSTRAINT h_storage_types_data_catalogue_fk 
        FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_storage_type_sk);

-- ==============================
-- Satellite: Storage Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_storage_types (
    h_storage_type_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    storage_type_id int4 NOT NULL,
    description varchar NOT NULL,
    description_ru varchar NOT NULL,
    CONSTRAINT s_storage_types_h_storage_type_fk FOREIGN KEY (h_storage_type_sk) REFERENCES public.h_storage_types (h_storage_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_storage_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_storage_type_sk);

-- ==============================
-- Hub: Convert Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_convert_types (
    h_convert_type_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_convert_types_pk PRIMARY KEY (h_convert_type_sk),
    CONSTRAINT h_convert_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_convert_type_sk);

-- ==============================
-- Satellite: Convert Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_convert_types (
    h_convert_type_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    convert_type_id int4 NOT NULL,
    description varchar NOT NULL,
    description_ru varchar NOT NULL,
    CONSTRAINT s_convert_types_h_convert_type_fk FOREIGN KEY (h_convert_type_sk) REFERENCES public.h_convert_types (h_convert_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_convert_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_convert_type_sk);


-- ==============================
-- Hub: Device Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_device_types (
    h_device_type_sk uuid DEFAULT uuid_generate_v4() NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_device_types_pk PRIMARY KEY (h_device_type_sk),
    CONSTRAINT h_device_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_device_type_sk);

-- ==============================
-- Satellite: Device Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_device_types (
    h_device_type_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    type_id int4,
    description varchar NOT NULL,
    description_ru varchar NOT NULL,
    CONSTRAINT s_device_types_h_device_type_fk FOREIGN KEY (h_device_type_sk) REFERENCES public.h_device_types (h_device_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_device_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_device_type_sk);

-- ==============================
-- Hub: Interface Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_interface_types (
    h_interface_type_sk uuid DEFAULT uuid_generate_v4() NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_interface_types_pk PRIMARY KEY (h_interface_type_sk),
    CONSTRAINT h_interface_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_interface_type_sk);

-- ==============================
-- Satellite: Interface Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_interface_types (
    h_interface_type_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    type_id integer NOT NULL,
    description text NOT NULL,
    description_ru text NOT NULL,
    CONSTRAINT s_interface_types_h_interface_type_fk FOREIGN KEY (h_interface_type_sk) REFERENCES public.h_interface_types (h_interface_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_interface_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_interface_type_sk);

-- ==============================
-- Aggregate Configs (Hub)
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_aggregate_configs (
    h_aggregate_config_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_aggregate_configs_pk PRIMARY KEY (h_aggregate_config_sk),
    CONSTRAINT h_aggregate_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_aggregate_config_sk);

-- ==============================
-- Aggregate Notification Configs (Hub)
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_aggregate_notification_configs (
    h_aggregate_notification_config_sk uuid NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_aggregate_notification_configs_pk PRIMARY KEY (
        h_aggregate_notification_config_sk
    ),
    CONSTRAINT h_aggregate_notification_config_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_aggregate_notification_config_sk
);

-- ==============================
-- Aggregate Notification Configs (Satellite)
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_aggregate_notification_configs (
    h_aggregate_notification_config_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    send_is_enabled bool NOT NULL,
    count_repeats int4 NOT NULL,
    resend_is_enabled int4 NOT NULL,
    resend_time_span int4 NOT NULL,
    notification_language int4 NOT NULL,
    CONSTRAINT s_aggregate_notification_config_h_aggregate_notification_config_fk FOREIGN KEY (
        h_aggregate_notification_config_sk
    ) REFERENCES public.h_aggregate_notification_configs (
        h_aggregate_notification_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_aggregate_notification_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_aggregate_notification_config_sk
);

-- ==============================
-- Link: AggregateNotificationConfigs_Aggregates
-- ==============================

CREATE TABLE IF NOT EXISTS public.l_aggregate_notification_configs_aggregates (
    l_aggregate_notification_configs_aggregates_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_aggregate_config_sk uuid NOT NULL,
    h_aggregate_notification_config_sk uuid NOT NULL,
    CONSTRAINT l_aggregate_notification_configs_aggregatess_pk PRIMARY KEY (
        l_aggregate_notification_configs_aggregates_sk
    ),
    CONSTRAINT l_aggregate_notification_configs_aggregates_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_aggregate_notification_configs_aggregates_h_aggregate_config_fk FOREIGN KEY (h_aggregate_config_sk) REFERENCES public.h_aggregate_configs (h_aggregate_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_aggregate_notification_configs_aggregates_h_aggregate_notification_configs_fk FOREIGN KEY (
        h_aggregate_notification_config_sk
    ) REFERENCES public.h_aggregate_notification_configs (
        h_aggregate_notification_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_aggregate_notification_configs_aggregates_sk
);

-- ==============================
-- Hub: Aggregates
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_aggregates (
    h_aggregate_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    aggregate_id uuid NOT NULL,
    CONSTRAINT h_aggregates_pk PRIMARY KEY (h_aggregate_sk),
    CONSTRAINT h_aggregates_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_aggregate_sk);

-- ==============================
-- Satellite: Aggregates
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_aggregates (
    h_aggregate_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    aggregate_path text NOT NULL,
    CONSTRAINT s_aggregates_h_aggregate_fk FOREIGN KEY (h_aggregate_sk) REFERENCES public.h_aggregates (h_aggregate_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_aggregates_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_aggregate_sk);

-- ==============================
-- Link: AggregateNotificationConfigs_Aggregates
-- ==============================

CREATE TABLE IF NOT EXISTS public.l_aggregate_notification_configs_aggregates (
    l_aggregate_notification_configs_aggregates_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_aggregate_notification_config_sk uuid NOT NULL,
    h_aggregate_sk uuid NOT NULL,
    CONSTRAINT l_aggregate_notification_configs_aggregates_pk PRIMARY KEY (
        l_aggregate_notification_configs_aggregates_sk
    ),
    CONSTRAINT l_aggregate_notification_configs_aggregates_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_aggregate_notification_configs_aggregates_h_aggregate_notification_configs_fk FOREIGN KEY (
        h_aggregate_notification_config_sk
    ) REFERENCES public.h_aggregate_notification_configs (
        h_aggregate_notification_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_aggregate_notification_configs_aggregates_h_aggregate_fk FOREIGN KEY (h_aggregate_sk) REFERENCES public.h_aggregates (h_aggregate_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_aggregate_notification_configs_aggregates_sk
);

-- ==============================
-- Hub: Email Addresses
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_email_addresses (
    h_email_address_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    email_address_id uuid NOT NULL,
    email_address text NOT NULL,
    CONSTRAINT h_email_addresses_pk PRIMARY KEY (h_email_address_sk),
    CONSTRAINT h_email_addresses_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_email_address_sk);

-- ==============================
-- Link: AggregateNotificationConfigs_EmailAddresses
-- ==============================

CREATE TABLE IF NOT EXISTS public.l_aggregate_notification_configs_email_addresses (
    l_aggregate_notification_configs_email_addresses_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_aggregate_notification_config_sk uuid NOT NULL,
    h_email_address_sk uuid NOT NULL,
    CONSTRAINT l_aggregate_notification_configs_email_addresses_pk PRIMARY KEY (
        l_aggregate_notification_configs_email_addresses_sk
    ),
    CONSTRAINT l_aggregate_notification_configs_email_addresses_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_aggregate_notification_configs_email_addresses_h_aggregate_notification_configs_fk FOREIGN KEY (
        h_aggregate_notification_config_sk
    ) REFERENCES public.h_aggregate_notification_configs (
        h_aggregate_notification_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_aggregate_notification_configs_email_addresses_h_email_addresses_fk FOREIGN KEY (h_email_address_sk) REFERENCES public.h_email_addresses (h_email_address_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_aggregate_notification_configs_email_addresses_sk
);

-- ==============================
-- Link: DiagnosticDefectState_AggregateNotificationConfig
-- ==============================

CREATE TABLE IF NOT EXISTS public.l_diagnostic_defect_states_aggregate_notification_configs (
    l_diagnostic_defect_states_aggregate_notification_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_aggregate_notification_config_sk uuid NOT NULL,
    h_diagnostic_defect_state_sk uuid NOT NULL,
    CONSTRAINT l_diagnostic_defect_states_aggregate_notification_configs_pk PRIMARY KEY (
        l_diagnostic_defect_states_aggregate_notification_configs_sk
    ),
    CONSTRAINT l_diagnostic_defect_states_aggregate_notification_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_diagnostic_defect_states_aggregate_notification_configs_h_aggregate_notification_configs_fk FOREIGN KEY (
        h_aggregate_notification_config_sk
    ) REFERENCES public.h_aggregate_notification_configs (
        h_aggregate_notification_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_diagnostic_defect_states_aggregate_notification_configs_h_diagnostic_defect_state_fk FOREIGN KEY (h_diagnostic_defect_state_sk) REFERENCES public.h_diagnostic_defect_states (h_diagnostic_defect_state_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_diagnostic_defect_states_aggregate_notification_configs_sk
);

-- ==============================
-- Hub: Annotations
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_annotations (
    h_annotation_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_annotations_pk PRIMARY KEY (h_annotation_sk),
    CONSTRAINT h_annotations_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_annotation_sk);

-- ==============================
-- Satellite: Annotations
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_annotations (
    h_annotation_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    name_graphics text NOT NULL,
    full_path text NOT NULL,
    property text NOT NULL,
    selected_interval int4 NOT NULL,
    annotation_type text NOT NULL,
    user_login text NOT NULL,
    date_create TIMESTAMP NOT NULL,
    json_annotation_settings json,
    CONSTRAINT s_annotations_h_annotations_fk FOREIGN KEY (h_annotation_sk) REFERENCES public.h_annotations (h_annotation_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_annotations_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_annotation_sk);

-- ==============================
-- Hub: Bearings
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_bearings (
    h_bearing_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_bearings_pk PRIMARY KEY (h_bearing_sk),
    CONSTRAINT h_bearings_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_bearing_sk);

-- ==============================
-- Satellite: Bearings
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_bearings (
    h_bearing_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    number text NOT NULL,
    outer_race_d numeric NOT NULL,
    inner_race_d numeric NOT NULL,
    rolling_element_d numeric NOT NULL,
    rolling_element_count int4 NOT NULL,
    contact_angle numeric NOT NULL,
    bpfi numeric NOT NULL,
    bpfo numeric NOT NULL,
    bsf numeric NOT NULL,
    ftf numeric NOT NULL,
    service_life int4 NOT NULL,
    date_created TIMESTAMP,
    date_modified TIMESTAMP,
    cdb_sync_date TIMESTAMP,
    cdb_version int4 NOT NULL,
    cdb_id uuid,
    CONSTRAINT s_bearings_h_bearing_sk_fk FOREIGN KEY (h_bearing_sk) REFERENCES public.h_bearings (h_bearing_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_bearings_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_bearing_sk);

-- ==============================
-- Hub: DatabaseIds
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_database_ids (
    h_database_id_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    database_id_id text NOT NULL,
    data_type int4 NOT NULL,
    CONSTRAINT h_database_ids_pk PRIMARY KEY (h_database_id_sk),
    CONSTRAINT h_database_ids_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_database_id_sk);

-- ==============================
-- Satellite: DatabaseIds
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_database_ids (
    h_database_id_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    main_db_name text NOT NULL,
    CONSTRAINT s_database_ids_h_database_id_fk FOREIGN KEY (h_database_id_sk) REFERENCES public.h_database_ids (h_database_id_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_database_ids_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_database_id_sk);

-- ==============================
-- Hub: Diagnostic Alarms
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_diagnostic_alarms (
    h_diagnostic_alarm_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_diagnostic_alarms_pk PRIMARY KEY (h_diagnostic_alarm_sk),
    CONSTRAINT h_diagnostic_alarms_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_diagnostic_alarm_sk);

-- ==============================
-- Satellite: Diagnostic Alarms
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_diagnostic_alarms (
    h_diagnostic_alarm_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    date_datetime TIMESTAMP NOT NULL,
    confirmed bool NOT NULL,
    comment text NOT NULL,
    CONSTRAINT s_diagnostic_alarms_h_diagnostic_alarm_fk FOREIGN KEY (h_diagnostic_alarm_sk) REFERENCES public.h_diagnostic_alarms (h_diagnostic_alarm_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_diagnostic_alarms_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_diagnostic_alarm_sk);

-- ==============================
-- Hub: Diagnostic Data Records
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_diagnostic_data_records (
    h_diagnostic_data_record_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_diagnostic_data_records_pk PRIMARY KEY (h_diagnostic_data_record_sk),
    CONSTRAINT h_diagnostic_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_diagnostic_data_record_sk);

-- ==============================
-- Satellite: Diagnostic Data Records
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_diagnostic_data_records (
    h_diagnostic_data_record_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    diag_tag_name text NOT NULL,
    defect_name text NOT NULL,
    defect_details text NOT NULL,
    recommendation text NOT NULL,
    priority int4 NOT NULL,
    group_name text NOT NULL,
    CONSTRAINT s_diagnostic_data_records_h_diag_fk FOREIGN KEY (h_diagnostic_data_record_sk) REFERENCES public.h_diagnostic_data_records (h_diagnostic_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_diagnostic_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_diagnostic_data_record_sk);

-- ==============================
-- Link: DiagnosticDefectState_Diag
-- ==============================

CREATE TABLE IF NOT EXISTS public.l_diagnostic_defect_states_diagnostic_data_records (
    l_diagnostic_defect_states_diagnostic_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_diagnostic_data_record_sk uuid NOT NULL,
    h_diagnostic_defect_state_sk uuid NOT NULL,
    CONSTRAINT l_diagnostic_defect_states_diagnostic_data_records_pk PRIMARY KEY (
        l_diagnostic_defect_states_diagnostic_data_records_sk
    ),
    CONSTRAINT l_diagnostic_defect_states_diagnostic_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_diagnostic_defect_states_diagnostic_data_records_h_diag_fk FOREIGN KEY (h_diagnostic_data_record_sk) REFERENCES public.h_diagnostic_data_records (h_diagnostic_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_diagnostic_defect_states_diagnostic_data_records_h_diagnostic_defect_state_fk FOREIGN KEY (h_diagnostic_defect_state_sk) REFERENCES public.h_diagnostic_defect_states (h_diagnostic_defect_state_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_diagnostic_defect_states_diagnostic_data_records_sk
);

-- ==============================
-- Link: Diagnostic Data Records_DiagnosticAlarm
-- ==============================

CREATE TABLE IF NOT EXISTS public.l_diagnostic_data_records_diagnostic_alarms (
    l_diagnostic_data_records_diagnostic_alarms_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_diagnostic_alarm_sk uuid NOT NULL,
    h_diagnostic_data_record_sk uuid NOT NULL,
    CONSTRAINT l_diagnostic_data_records_diagnostic_alarms_pk PRIMARY KEY (l_diagnostic_data_records_diagnostic_alarms_sk),
    CONSTRAINT l_diagnostic_data_records_diagnostic_alarms_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_diagnostic_data_records_diagnostic_alarms_h_diagnostic_alarm_fk FOREIGN KEY (h_diagnostic_alarm_sk) REFERENCES public.h_diagnostic_alarms (h_diagnostic_alarm_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_diagnostic_data_records_diagnostic_alarms_h_diag_fk FOREIGN KEY (h_diagnostic_data_record_sk) REFERENCES public.h_diagnostic_data_records (h_diagnostic_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (l_diagnostic_data_records_diagnostic_alarms_sk);

-- ==============================
-- Link: DiagnosticAlarmState_DiagnosticAlarm
-- ==============================

CREATE TABLE IF NOT EXISTS public.l_diagnostic_alarm_states_diagnostic_alarms (
    l_diagnostic_alarm_states_diagnostic_alarms_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_diagnostic_alarm_sk uuid NOT NULL,
    h_diagnostic_alarm_state_sk uuid NOT NULL,
    CONSTRAINT l_diagnostic_alarm_states_diagnostic_alarms_pk PRIMARY KEY (
        l_diagnostic_alarm_states_diagnostic_alarms_sk
    ),
    CONSTRAINT l_diagnostic_alarm_states_diagnostic_alarms_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_diagnostic_alarm_states_diagnostic_alarm_h_diagnostic_alarm_fk FOREIGN KEY (h_diagnostic_alarm_sk) REFERENCES public.h_diagnostic_alarms (h_diagnostic_alarm_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_diagnostic_alarm_states_diagnostic_alarms_h_diagnostic_alarm_state_fk FOREIGN KEY (h_diagnostic_alarm_state_sk) REFERENCES public.h_diagnostic_alarm_states (h_diagnostic_alarm_state_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_diagnostic_alarm_states_diagnostic_alarms_sk
);

-- ==============================
-- Hub: Object Properties
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_object_properties (
    h_object_property_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_object_properties_pk PRIMARY KEY (h_object_property_sk),
    CONSTRAINT h_object_properties_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_object_property_sk);

-- ==============================
-- Satellite: Object Properties
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_object_properties (
    h_object_property_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    is_alias bool NOT NULL,
    max_records int4 NOT NULL,
    from_template_name text NOT NULL,
    to_copy bool NOT NULL,
    save_history bool NOT NULL,
    visible_for_scada bool NOT NULL,
    is_values_replicable bool NOT NULL,
    CONSTRAINT s_object_properties_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_object_properties_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_object_property_sk);

-- ==============================
-- Link: ObjectProperties_DiagnosticAlarm
-- ==============================

CREATE TABLE IF NOT EXISTS public.l_object_properties_diagnostic_alarms (
    l_object_properties_diagnostic_alarms_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_diagnostic_alarm_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_object_properties_diagnostic_alarms_pk PRIMARY KEY (
        l_object_properties_diagnostic_alarms_sk
    ),
    CONSTRAINT l_object_properties_diagnostic_alarms_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_properties_diagnostic_alarms_h_diagnostic_alarm_fk FOREIGN KEY (h_diagnostic_alarm_sk) REFERENCES public.h_diagnostic_alarms (h_diagnostic_alarm_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_properties_diagnostic_alarms_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_object_properties_diagnostic_alarms_sk
);

-- ==============================
-- Link: DiagnosticDefectType_Diag
-- ==============================

CREATE TABLE IF NOT EXISTS public.l_diagnostic_defect_types_diagnostic_data_records (
    l_diagnostic_defect_types_diagnostic_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_diagnostic_data_record_sk uuid NOT NULL,
    h_diagnostic_defect_type_sk uuid NOT NULL,
    CONSTRAINT l_diagnostic_defect_types_diagnostic_data_records_pk PRIMARY KEY (
        l_diagnostic_defect_types_diagnostic_data_records_sk
    ),
    CONSTRAINT l_diagnostic_defect_types_diagnostic_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_diagnostic_defect_types_diagnostic_data_records_h_diag_fk FOREIGN KEY (h_diagnostic_data_record_sk) REFERENCES public.h_diagnostic_data_records (h_diagnostic_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_diagnostic_defect_types_diagnostic_data_records_h_diagnostic_defect_type_fk FOREIGN KEY (h_diagnostic_defect_type_sk) REFERENCES public.h_diagnostic_defect_types (h_diagnostic_defect_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_diagnostic_defect_types_diagnostic_data_records_sk
);

-- ==============================
-- Link: Object Properties - Storage Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_object_properties_storage_types (
    l_object_properties_storage_types_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    h_storage_type_sk uuid NOT NULL,
    CONSTRAINT l_object_properties_storage_types_pk PRIMARY KEY (
        l_object_properties_storage_types_sk
    ),
    CONSTRAINT l_object_properties_storage_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_properties_storage_types_h_object_property_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_properties_storage_types_h_storage_type_fk FOREIGN KEY (h_storage_type_sk) REFERENCES public.h_storage_types (h_storage_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_object_properties_storage_types_sk
);



-- ==============================
-- Hub: Images
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_images (
    h_image_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_images_pk PRIMARY KEY (h_image_sk),
    CONSTRAINT h_images_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_image_sk);

-- ==============================
-- Satellite: Images
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_images (
    h_image_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    image_data bytea NOT NULL, -- BLOB → в PostgreSQL это bytea
    CONSTRAINT s_images_h_image_fk FOREIGN KEY (h_image_sk) REFERENCES public.h_images (h_image_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_images_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_image_sk);

-- ==============================
-- Hub: IO Device Config Nodes
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_io_device_config_nodes (
    h_io_device_config_node_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_io_device_config_nodes_pk PRIMARY KEY (h_io_device_config_node_sk),
    CONSTRAINT h_io_device_config_nodes_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_device_config_node_sk);

-- ==============================
-- Satellite: IO Device Config Nodes
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_io_device_config_nodes (
    h_io_device_config_node_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    name text NOT NULL,
    parent_node_id uuid NOT NULL,
    CONSTRAINT s_io_device_config_nodes_h_io_device_config_node_fk FOREIGN KEY (h_io_device_config_node_sk) REFERENCES public.h_io_device_config_nodes (h_io_device_config_node_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_io_device_config_nodes_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_device_config_node_sk);

-- ==============================
-- Hub: IO Device Configs
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_io_device_configs (
    h_io_device_config_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_io_device_configs_pk PRIMARY KEY (h_io_device_config_sk),
    CONSTRAINT h_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_device_config_sk);

-- ==============================
-- Satellite: IO Device Configs
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_io_device_configs (
    h_io_device_config_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    name text NOT NULL,
    enabled bool NOT NULL,
    set_number int4 NOT NULL,
    CONSTRAINT s_io_device_configs_h_io_device_config_fk FOREIGN KEY (h_io_device_config_sk) REFERENCES public.h_io_device_configs (h_io_device_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_device_config_sk);

-- ==============================
-- Link: ConfigTypes_io_device_configs
-- ==============================

CREATE TABLE IF NOT EXISTS public.l_config_types_io_device_configs (
    l_config_types_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_device_config_sk uuid NOT NULL,
    h_config_type_sk uuid NOT NULL,
    CONSTRAINT l_config_types_io_device_configs_pk PRIMARY KEY (
        l_config_types_io_device_configs_sk
    ),
    CONSTRAINT l_config_types_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_config_types_io_device_configs_h_io_device_config_fk FOREIGN KEY (h_io_device_config_sk) REFERENCES public.h_io_device_configs (h_io_device_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_config_types_io_device_configs_h_config_type_fk FOREIGN KEY (h_config_type_sk) REFERENCES public.h_config_types (h_config_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_config_types_io_device_configs_sk
);

-- ==============================
-- Link: io_device_configNodes_io_device_configs
-- ==============================

CREATE TABLE IF NOT EXISTS public.l_io_device_config_nodes_io_device_configs (
    l_io_device_config_nodes_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_device_config_sk uuid NOT NULL,
    h_io_device_config_node_sk uuid NOT NULL,
    CONSTRAINT l_io_device_config_nodes_io_device_configs_pk PRIMARY KEY (
        l_io_device_config_nodes_io_device_configs_sk
    ),
    CONSTRAINT l_io_device_config_nodes_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_device_config_nodes_io_device_configs_h_io_device_config_fk FOREIGN KEY (h_io_device_config_sk) REFERENCES public.h_io_device_configs (h_io_device_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_device_config_nodes_io_device_configs_h_io_device_config_node_fk FOREIGN KEY (h_io_device_config_node_sk) REFERENCES public.h_io_device_config_nodes (h_io_device_config_node_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_device_config_nodes_io_device_configs_sk
);

-- ==============================
-- Hub: Model Templates
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_model_templates (
    h_model_template_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_model_templates_pk PRIMARY KEY (h_model_template_sk),
    CONSTRAINT h_model_templates_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_model_template_sk);

-- ==============================
-- Satellite: Model Templates
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_model_templates (
    h_model_template_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    name text NOT NULL,
    tag_name text NOT NULL,
    description text NOT NULL,
    base_template_name text NOT NULL,
    date_created TIMESTAMP,
    date_modified TIMESTAMP,
    cdb_sync_date TIMESTAMP,
    cdb_version int4 NOT NULL,
    cdb_id uuid,
    CONSTRAINT s_model_templates_h_model_template_fk FOREIGN KEY (h_model_template_sk) REFERENCES public.h_model_templates (h_model_template_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_model_templates_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_model_template_sk);

-- ==============================
-- Hub: Model Template Tree Nodes
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_model_template_tree_nodes (
    h_model_template_tree_node_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_model_template_tree_nodes_pk PRIMARY KEY (h_model_template_tree_node_sk),
    CONSTRAINT h_model_template_tree_nodes_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_model_template_tree_node_sk);

-- ==============================
-- Satellite: Model Template Tree Nodes
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_model_template_tree_nodes (
    h_model_template_tree_node_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    name text NOT NULL,
    parent_id uuid NOT NULL,
    tag_name text NOT NULL,
    description text NOT NULL,
    CONSTRAINT s_model_template_tree_nodes_h_model_template_tree_node_fk FOREIGN KEY (h_model_template_tree_node_sk) REFERENCES public.h_model_template_tree_nodes (h_model_template_tree_node_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_model_template_tree_nodes_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_model_template_tree_node_sk);

-- ==============================
-- Link: ModelTemplate_ModelTemplateTreeNodes
-- ==============================

CREATE TABLE IF NOT EXISTS public.l_model_templates_model_template_tree_nodes (
    l_model_templates_model_template_tree_nodes_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_model_template_sk uuid NOT NULL,
    h_model_template_tree_node_sk uuid NOT NULL,
    CONSTRAINT l_model_templates_model_template_tree_nodes_pk PRIMARY KEY (
        l_model_templates_model_template_tree_nodes_sk
    ),
    CONSTRAINT l_model_templates_model_template_tree_nodes_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_model_templates_model_template_tree_nodes_h_model_template_fk FOREIGN KEY (h_model_template_sk) REFERENCES public.h_model_templates (h_model_template_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_model_templates_model_template_tree_nodes_h_model_template_tree_node_fk FOREIGN KEY (h_model_template_tree_node_sk) REFERENCES public.h_model_template_tree_nodes (h_model_template_tree_node_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_model_templates_model_template_tree_nodes_sk
);

-- ==============================
-- Hub: Measure Convert
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_measure_converts (
    h_measure_convert_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_measure_convert_pk PRIMARY KEY (h_measure_convert_sk),
    CONSTRAINT h_measure_convert_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_measure_convert_sk);

-- ==============================
-- Satellite: Measure Convert
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_measure_converts (
    h_measure_convert_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    from_id uuid NOT NULL,
    to_id uuid NOT NULL,
    factor float NOT NULL,
    formula text NOT NULL,
    CONSTRAINT s_measure_convert_h_measure_convert_fk FOREIGN KEY (h_measure_convert_sk) REFERENCES public.h_measure_converts (h_measure_convert_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_measure_convert_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_measure_convert_sk);

-- ==============================
-- Link: Convert Types - Measure Converts
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_convert_types_measure_converts (
    l_convert_types_measure_converts_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_convert_type_sk uuid NOT NULL,
    h_measure_convert_sk uuid NOT NULL,
    CONSTRAINT l_convert_types_measure_converts_pk PRIMARY KEY (
        l_convert_types_measure_converts_sk
    ),
    CONSTRAINT l_convert_types_measure_converts_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_convert_types_measure_converts_h_convert_type_fk FOREIGN KEY (h_convert_type_sk) REFERENCES public.h_convert_types (h_convert_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_convert_types_measure_converts_h_measure_convert_fk FOREIGN KEY (h_measure_convert_sk) REFERENCES public.h_measure_converts (h_measure_convert_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_convert_types_measure_converts_sk
);

-- ==============================
-- Hub: Measure Groups
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_measure_groups (
    h_measure_group_sk uuid NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_measure_groups_pk PRIMARY KEY (h_measure_group_sk),
    CONSTRAINT h_measure_groups_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_measure_group_sk);

-- ==============================
-- Satellite: Measure Groups
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_measure_groups (
    h_measure_group_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    group_name text NOT NULL,
    CONSTRAINT s_measure_groups_h_measure_group_fk FOREIGN KEY (h_measure_group_sk) REFERENCES public.h_measure_groups (h_measure_group_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_measure_groups_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_measure_group_sk);

-- ==============================
-- Hub: Measure Units
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_measure_units (
    h_measure_unit_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_measure_units_pk PRIMARY KEY (h_measure_unit_sk),
    CONSTRAINT h_measure_units_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_measure_unit_sk);

-- ==============================
-- Satellite: Measure Units
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_measure_units (
    h_measure_unit_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    name text NOT NULL,
    abbreviation text NOT NULL,
    CONSTRAINT s_measure_units_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_measure_units_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_measure_unit_sk);

-- ==============================
-- Link: MeasureUnits_ToID
-- ==============================

CREATE TABLE IF NOT EXISTS public.l_measure_convert_to_ids (
    l_measure_convert_to_ids_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_measure_unit_sk uuid NOT NULL,
    h_measure_convert_sk uuid NOT NULL,
    CONSTRAINT l_measure_convert_to_ids_pk PRIMARY KEY (l_measure_convert_to_ids_sk),
    CONSTRAINT l_measure_convert_to_ids_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_measure_convert_to_ids_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_measure_convert_to_ids_h_measure_convert_fk FOREIGN KEY (h_measure_convert_sk) REFERENCES public.h_measure_converts (h_measure_convert_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (l_measure_convert_to_ids_sk);

-- ==============================
-- Link: MeasureUnits_FromID
-- ==============================

CREATE TABLE IF NOT EXISTS public.l_measure_convert_from_ids (
    l_measure_convert_from_ids_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_measure_unit_sk uuid NOT NULL,
    h_measure_convert_sk uuid NOT NULL,
    CONSTRAINT l_measure_convert_from_ids_pk PRIMARY KEY (l_measure_convert_from_ids_sk),
    CONSTRAINT l_measure_convert_from_ids_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_measure_convert_from_ids_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_measure_convert_from_ids_h_measure_convert_fk FOREIGN KEY (h_measure_convert_sk) REFERENCES public.h_measure_converts (h_measure_convert_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (l_measure_convert_from_ids_sk);

-- ==============================
-- Link: MeasureUnits_MeasureGroups
-- ==============================

CREATE TABLE IF NOT EXISTS public.l_measure_units_measure_groups (
    l_measure_units_measure_groups_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_measure_unit_sk uuid NOT NULL,
    h_measure_group_sk uuid NOT NULL,
    CONSTRAINT l_measure_units_measure_groups_pk PRIMARY KEY (
        l_measure_units_measure_groups_sk
    ),
    CONSTRAINT l_measure_units_measure_groups_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_measure_units_measure_groups_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_measure_units_measure_groups_h_measure_group_fk FOREIGN KEY (h_measure_group_sk) REFERENCES public.h_measure_groups (h_measure_group_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_measure_units_measure_groups_sk
);


-- ==============================
-- Link: MeasureUnit_ObjectPropertyies
-- ==============================

CREATE TABLE IF NOT EXISTS public.l_measure_units_object_properties (
    l_measure_units_object_properties_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_measure_unit_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_measure_units_object_property_pk PRIMARY KEY (
        l_measure_units_object_properties_sk
    ),
    CONSTRAINT l_measure_units_object_property_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_measure_units_object_property_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_measure_units_object_property_h_object_property_descriptor_fk FOREIGN KEY (
        h_object_property_sk
    ) REFERENCES public.h_object_properties (
        h_object_property_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_measure_units_object_properties_sk
);

-- ==============================
-- Hub: Object Property Descriptor Nodes
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_object_property_descriptor_nodes (
    h_object_property_descriptor_node_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_object_property_descriptor_nodes_pk PRIMARY KEY (
        h_object_property_descriptor_node_sk
    ),
    CONSTRAINT h_object_property_descriptor_nodes_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_object_property_descriptor_node_sk
);

-- ==============================
-- Satellite: Object Property Descriptor Nodes
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_object_property_descriptor_nodes (
    h_object_property_descriptor_node_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    name text NOT NULL,
    description text NOT NULL,
    parent_id uuid NOT NULL,
    translated_id integer NOT NULL,
    CONSTRAINT s_object_property_descriptor_nodes_h_object_property_descriptor_node_fk FOREIGN KEY (
        h_object_property_descriptor_node_sk
    ) REFERENCES public.h_object_property_descriptor_nodes (
        h_object_property_descriptor_node_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_object_property_descriptor_nodes_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT s_object_property_descriptor_nodes_parent_id_fk FOREIGN KEY (parent_id) REFERENCES public.h_object_property_descriptor_nodes (
        h_object_property_descriptor_node_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    h_object_property_descriptor_node_sk
);

-- ==============================
-- Hub: Object Property Descriptors
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_object_property_descriptors (
    h_object_property_descriptor_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_object_property_descriptors_pk PRIMARY KEY (
        h_object_property_descriptor_sk
    ),
    CONSTRAINT h_object_property_descriptors_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_object_property_descriptor_sk
);

-- ==============================
-- Satellite: Object Property Descriptors
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_object_property_descriptors (
    h_object_property_descriptor_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    name text NOT NULL,
    description text,
    tag text NOT NULL,
    max_records int4 NOT NULL,
    save_history bool NOT NULL,
    default_value text NOT NULL,
    visible_for_scada text NOT NULL,
    date_created TIMESTAMP,
    date_modified TIMESTAMP,
    cdb_sync_date TIMESTAMP,
    cdb_version int4 NOT NULL,
    translated_id integer NOT NULL,
    CONSTRAINT s_object_property_descriptors_h_object_property_descriptor_fk FOREIGN KEY (
        h_object_property_descriptor_sk
    ) REFERENCES public.h_object_property_descriptors (
        h_object_property_descriptor_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_object_property_descriptors_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_object_property_descriptor_sk
);

-- ==============================
-- Link: ObjectProperties_ObjectPropertyDescriptors
-- ==============================

CREATE TABLE IF NOT EXISTS public.l_object_properties_object_property_descriptors (
    l_object_properties_object_property_descriptors_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_object_property_descriptor_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_object_properties_object_property_descriptors_pk PRIMARY KEY (
        l_object_properties_object_property_descriptors_sk
    ),
    CONSTRAINT l_object_properties_object_property_descriptors_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_properties_object_property_descriptors_h_object_prop_descriptor_fk FOREIGN KEY (
        h_object_property_descriptor_sk
    ) REFERENCES public.h_object_property_descriptors (
        h_object_property_descriptor_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_properties_object_property_descriptors_h_object_proper_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_object_properties_object_property_descriptors_sk
);

-- ==============================
-- Link: ObjectPropertyDescriptorNode_ObjectPropertyDescriptors
-- ==============================

CREATE TABLE IF NOT EXISTS public.l_object_property_descriptor_nodes_object_property_descriptors (
    l_object_property_descriptor_nodes_object_property_descriptors_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_object_property_descriptor_sk uuid NOT NULL,
    h_object_property_descriptor_node_sk uuid NOT NULL,
    CONSTRAINT l_object_property_descriptor_nodes_object_property_descriptors_pk PRIMARY KEY (
        l_object_property_descriptor_nodes_object_property_descriptors_sk
    ),
    CONSTRAINT l_object_property_descriptor_node_object_property_descriptors_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_property_descriptor_node_object_prop_descriptors_h_object_property_descriptor_fk FOREIGN KEY (
        h_object_property_descriptor_sk
    ) REFERENCES public.h_object_property_descriptors (
        h_object_property_descriptor_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_property_descriptor_node_object_property_descriptors_h_object_property_descriptor_node_fk FOREIGN KEY (
        h_object_property_descriptor_node_sk
    ) REFERENCES public.h_object_property_descriptor_nodes (
        h_object_property_descriptor_node_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_object_property_descriptor_nodes_object_property_descriptors_sk
);

-- ==============================
-- Hub: Object Group Descriptors
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_object_group_descriptors (
    h_object_group_descriptor_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_object_group_descriptors_pk PRIMARY KEY (h_object_group_descriptor_sk),
    CONSTRAINT h_object_group_descriptors_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_object_group_descriptor_sk);

-- ==============================
-- Satellite: Object Group Descriptors
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_object_group_descriptors (
    h_object_group_descriptor_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    name text NOT NULL,
    CONSTRAINT s_object_group_descriptors_h_object_group_descriptor_fk FOREIGN KEY (h_object_group_descriptor_sk) REFERENCES public.h_object_group_descriptors (h_object_group_descriptor_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_object_group_descriptors_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_object_group_descriptor_sk);

-- ==============================
-- Link: Property Types - Object Property Descriptors
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_property_types_object_property_descriptors (
    l_property_types_object_property_descriptors_uuid_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_property_types_sk uuid NOT NULL,
    h_object_property_descriptor_sk uuid NOT NULL,
    CONSTRAINT l_property_types_object_property_descriptors_pk PRIMARY KEY (
        l_property_types_object_property_descriptors_uuid_sk
    ),
    CONSTRAINT l_property_types_object_property_descriptors_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_property_types_object_property_descriptors_h_property_types_fk FOREIGN KEY (h_property_types_sk) REFERENCES public.h_property_types (h_property_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_property_types_object_property_descriptors_h_object_property_descriptor_fk FOREIGN KEY (
        h_object_property_descriptor_sk
    ) REFERENCES public.h_object_property_descriptors (
        h_object_property_descriptor_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_property_types_object_property_descriptors_uuid_sk
);

-- ==============================
-- Hub: Object Groups
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_object_groups (
    h_object_group_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_object_groups_pk PRIMARY KEY (h_object_group_sk),
    CONSTRAINT h_object_groups_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_object_group_sk);

-- ==============================
-- Link: ObjectGroupDescriptor_Objects
-- ==============================

CREATE TABLE IF NOT EXISTS public.l_object_group_descriptors_objects (
    l_object_group_descriptors_objects_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_object_group_descriptor_sk uuid NOT NULL,
    h_object_group_sk uuid NOT NULL,
    CONSTRAINT l_object_group_descriptors_objects_pk PRIMARY KEY (
        l_object_group_descriptors_objects_sk
    ),
    CONSTRAINT l_object_group_descriptors_objects_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_group_descriptors_objects_h_object_group_descriptor_fk FOREIGN KEY (h_object_group_descriptor_sk) REFERENCES public.h_object_group_descriptors (h_object_group_descriptor_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_group_descriptors_objects_h_object_group_fk FOREIGN KEY (h_object_group_sk) REFERENCES public.h_object_groups (h_object_group_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_object_group_descriptors_objects_sk
);



-- ==============================
-- Hub: Object Templates
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_object_templates (
    h_object_templates_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_object_templates_pk PRIMARY KEY (h_object_templates_sk),
    CONSTRAINT h_object_templates_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_object_templates_sk);

-- ==============================
-- Satellite: Object Templates
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_object_templates (
    h_object_templates_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    name text NOT NULL,
    tag_name text,
    parent_id uuid,
    description text,
    template_name text NOT NULL,
    from_template_name text NOT NULL,
    CONSTRAINT s_object_templates_h_object_templates_fk FOREIGN KEY (h_object_templates_sk) REFERENCES public.h_object_templates (h_object_templates_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_object_templates_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_object_templates_sk);

-- ==============================
-- Hub: PouUserDefinedItems
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_pou_user_defined_items (
    h_pou_user_defined_item_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_pou_user_defined_items_pk PRIMARY KEY (h_pou_user_defined_item_sk),
    CONSTRAINT h_pou_user_defined_items_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_pou_user_defined_item_sk);

-- ==============================
-- Satellite: PouUserDefinedItems
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_pou_user_defined_items (
    h_pou_user_defined_item_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    call_name text,
    body_type int4 NOT NULL,
    name text NOT NULL,
    description text NOT NULL,
    author text NOT NULL,
    xml_interface text NOT NULL,
    xml_body text NOT NULL,
    date_of_create TIMESTAMP NOT NULL,
    date_of_edit TIMESTAMP NOT NULL,
    CONSTRAINT s_pou_user_defined_items_h_pou_user_defined_item_fk FOREIGN KEY (h_pou_user_defined_item_sk) REFERENCES public.h_pou_user_defined_items (h_pou_user_defined_item_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_pou_user_defined_items_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_pou_user_defined_item_sk);

-- ==============================
-- Hub: PouUserItemsTree
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_pou_user_tree_items (
    h_pou_user_tree_item_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_pou_user_tree_item_pk PRIMARY KEY (h_pou_user_tree_item_sk),
    CONSTRAINT h_pou_user_tree_item_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_pou_user_tree_item_sk);

-- ==============================
-- Satellite: PouUserItemsTree
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_pou_user_tree_items (
    h_pou_user_tree_item_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    name text NOT NULL,
    description text NOT NULL,
    parent_id uuid NOT NULL,
    CONSTRAINT s_pou_user_tree_item_h_pou_user_tree_item_fk FOREIGN KEY (h_pou_user_tree_item_sk) REFERENCES public.h_pou_user_tree_items (h_pou_user_tree_item_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_pou_user_tree_item_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_pou_user_tree_item_sk);

-- ==============================
-- Link: PouUserItemsTree_PouUserDefinedItems
-- ==============================

CREATE TABLE IF NOT EXISTS public.l_pou_user_tree_items_pou_user_defined_items (
    l_pou_user_tree_items_pou_user_defined_items_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_pou_user_tree_item_sk uuid NOT NULL,
    h_pou_user_defined_item_sk uuid NOT NULL,
    CONSTRAINT l_pou_user_tree_items_pou_user_defined_items_pk PRIMARY KEY (
        l_pou_user_tree_items_pou_user_defined_items_sk
    ),
    CONSTRAINT l_pou_user_tree_items_pou_user_defined_items_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_pou_user_tree_items_pou_user_defined_items_h_pou_user_tree_item_fk FOREIGN KEY (h_pou_user_tree_item_sk) REFERENCES public.h_pou_user_tree_items (h_pou_user_tree_item_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_pou_user_tree_items_pou_user_defined_items_h_pou_user_defined_item_fk FOREIGN KEY (h_pou_user_defined_item_sk) REFERENCES public.h_pou_user_defined_items (h_pou_user_defined_item_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_pou_user_tree_items_pou_user_defined_items_sk
);

-- ==============================
-- Hub: Preset Chart Settings
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_preset_chart_settings (
    h_preset_chart_setting_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_preset_chart_setting_pk PRIMARY KEY (h_preset_chart_setting_sk),
    CONSTRAINT h_preset_chart_setting_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_preset_chart_setting_sk);

-- ==============================
-- Satellite: Preset Chart Settings
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_preset_chart_settings (
    h_preset_chart_setting_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    preset_chart_settings text NOT NULL,
    user_id uuid,
    CONSTRAINT s_preset_chart_settings_h_preset_chart_setting_fk FOREIGN KEY (h_preset_chart_setting_sk) REFERENCES public.h_preset_chart_settings (h_preset_chart_setting_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_preset_chart_settings_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_preset_chart_setting_sk);

-- ==============================
-- Hub: Object Rules
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_object_rules (
    h_object_rule_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_object_rules_pk PRIMARY KEY (h_object_rule_sk),
    CONSTRAINT h_object_rules_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_object_rule_sk);

-- ==============================
-- Satellite: Object Rules
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_object_rules (
    h_object_rule_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    pou_call_name text NOT NULL,
    comment text NOT NULL,
    enabled bool NOT NULL,
    run_level integer NOT NULL,
    from_template_name text NOT NULL,
    is_in_template bool NOT NULL,
    tag text NOT NULL,
    running_period bigint NOT NULL,
    CONSTRAINT s_object_rules_h_object_rule_fk FOREIGN KEY (h_object_rule_sk) REFERENCES public.h_object_rules (h_object_rule_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_object_rules_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_object_rule_sk);

-- ==============================
-- Hub: Objects
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_objects (
    h_object_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_objects_pk PRIMARY KEY (h_object_sk),
    CONSTRAINT h_objects_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_object_sk);

-- ==============================
-- Satellite: Objects
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_objects (
    h_object_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    name text NOT NULL,
    tag_name text NOT NULL,
    parent_id uuid NOT NULL,
    description text NOT NULL,
    template_name text NOT NULL,
    from_template_name text NOT NULL,
    date_created TIMESTAMP NOT NULL,
    date_modified TIMESTAMP NOT NULL,
    cdb_sync_date TIMESTAMP,
    cdb_version integer NOT NULL,
    cdb_id uuid,
    CONSTRAINT s_objects_h_object_fk FOREIGN KEY (h_object_sk) REFERENCES public.h_objects (h_object_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_objects_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_object_sk);

-- ==============================
-- Link: Object Rules – Running Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_object_rules_running_types (
    l_object_rules_running_types_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_object_rule_sk uuid NOT NULL,
    h_running_type_sk uuid NOT NULL,
    CONSTRAINT l_object_rules_running_types_pk PRIMARY KEY (
        l_object_rules_running_types_sk
    ),
    CONSTRAINT l_object_rules_running_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_rules_running_types_h_object_rule_fk FOREIGN KEY (h_object_rule_sk) REFERENCES public.h_object_rules (h_object_rule_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_rules_running_types_h_running_type_fk FOREIGN KEY (h_running_type_sk) REFERENCES public.h_running_types (h_running_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_object_rules_running_types_sk
);

-- ==============================
-- Link: Object Rules – Objects
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_object_rules_objects (
    l_object_rules_objects_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_object_rule_sk uuid NOT NULL,
    h_object_sk uuid NOT NULL,
    CONSTRAINT l_object_rules_objects_pk PRIMARY KEY (l_object_rules_objects_sk),
    CONSTRAINT l_object_rules_objects_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_rules_objects_h_object_rule_fk FOREIGN KEY (h_object_rule_sk) REFERENCES public.h_object_rules (h_object_rule_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_rules_objects_h_object_fk FOREIGN KEY (h_object_sk) REFERENCES public.h_objects (h_object_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (l_object_rules_objects_sk);

-- ==============================
-- Link: Object Rules – POU User Defined Items
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_object_rules_pou_user_defined_items (
    l_object_rules_pou_user_defined_items_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_object_rule_sk uuid NOT NULL,
    h_pou_user_defined_item_sk uuid NOT NULL,
    CONSTRAINT l_object_rules_pou_user_defined_items_pk PRIMARY KEY (
        l_object_rules_pou_user_defined_items_sk
    ),
    CONSTRAINT l_object_rules_pou_user_defined_items_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_rules_pou_user_defined_items_h_object_rule_fk FOREIGN KEY (h_object_rule_sk) REFERENCES public.h_object_rules (h_object_rule_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_rules_pou_user_defined_items_h_pou_user_defined_item_fk FOREIGN KEY (h_pou_user_defined_item_sk) REFERENCES public.h_pou_user_defined_items (h_pou_user_defined_item_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_object_rules_pou_user_defined_items_sk
);

-- ==============================
-- Link: Object Rules – Object Properties
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_object_rules_object_properties (
    l_object_rules_object_properties_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_object_rule_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    fbd_link_type integer NOT NULL,
    fbd_link_name text NOT NULL,
    CONSTRAINT l_object_rules_object_properties_pk PRIMARY KEY (
        l_object_rules_object_properties_sk
    ),
    CONSTRAINT l_object_rules_object_properties_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_rules_object_properties_h_object_rule_fk FOREIGN KEY (h_object_rule_sk) REFERENCES public.h_object_rules (h_object_rule_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_rules_object_properties_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_object_rules_object_properties_sk
);

-- ==============================
-- Link: Object Property Descriptor – Objects
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_object_properties_objects (
    l_object_properties_objects_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    h_object_sk uuid NOT NULL,
    CONSTRAINT l_object_properties_objects_pk PRIMARY KEY (
        l_object_properties_objects_sk
    ),
    CONSTRAINT l_object_properties_objects_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_properties_objects_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_properties_objects_h_object_fk FOREIGN KEY (h_object_sk) REFERENCES public.h_objects (h_object_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_object_properties_objects_sk
);

-- ==============================
-- Hub: User Log
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_user_logs (
    h_user_log_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_user_log_pk PRIMARY KEY (h_user_log_sk),
    CONSTRAINT h_user_log_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_user_log_sk);

-- ==============================
-- Satellite: User Log
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_user_logs (
    h_user_log_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    user_id uuid NOT NULL,
    date_datetime TIMESTAMP NOT NULL,
    action_description text NOT NULL,
    CONSTRAINT s_user_log_h_user_log_fk FOREIGN KEY (h_user_log_sk) REFERENCES public.h_user_logs (h_user_log_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_user_log_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_user_log_sk);

-- ==============================
-- Link: User Log - User Action Type
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_user_logs_user_action_types (
    l_user_logs_user_action_types_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_user_log_sk uuid NOT NULL,
    h_user_action_type_sk uuid NOT NULL,
    CONSTRAINT l_user_logs_user_action_types_pk PRIMARY KEY (
        l_user_logs_user_action_types_sk
    ),
    CONSTRAINT l_user_logs_user_action_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_user_logs_user_action_types_h_user_log_fk FOREIGN KEY (h_user_log_sk) REFERENCES public.h_user_logs (h_user_log_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_user_logs_user_action_types_h_user_action_type_fk FOREIGN KEY (h_user_action_type_sk) REFERENCES public.h_user_action_types (h_user_action_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_user_logs_user_action_types_sk
);

-- ==============================
-- Hub: TIK SCADA Log
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_tik_scada_logs (
    h_tik_scada_log_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_tik_scada_log_pk PRIMARY KEY (h_tik_scada_log_sk),
    CONSTRAINT h_tik_scada_log_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_tik_scada_log_sk);

-- ==============================
-- Satellite: TIK SCADA Log
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_tik_scada_logs (
    h_tik_scada_log_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    timestamp_datetime TIMESTAMP NOT NULL,
    message text NOT NULL,
    acked bool NOT NULL,
    acked_timestamp TIMESTAMP NOT NULL,
    source_path text NOT NULL,
    aggregate_path text NOT NULL,
    participant text,
    CONSTRAINT s_tik_scada_log_h_tik_scada_log_fk FOREIGN KEY (h_tik_scada_log_sk) REFERENCES public.h_tik_scada_logs (h_tik_scada_log_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_tik_scada_log_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_tik_scada_log_sk);

-- ==============================
-- Link: TIK SCADA Log - TIK SCADA Log Type
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_tik_scada_logs_tik_scada_log_types (
    l_tik_scada_logs_tik_scada_log_types_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_tik_scada_log_sk uuid NOT NULL,
    h_tik_scada_log_type_sk uuid NOT NULL,
    CONSTRAINT l_tik_scada_logs_tik_scada_log_types_pk PRIMARY KEY (
        l_tik_scada_logs_tik_scada_log_types_sk
    ),
    CONSTRAINT l_tik_scada_logs_tik_scada_log_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_tik_scada_logs_tik_scada_log_types_h_tik_scada_log_fk FOREIGN KEY (h_tik_scada_log_sk) REFERENCES public.h_tik_scada_logs (h_tik_scada_log_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_tik_scada_logs_tik_scada_log_types_h_tik_scada_log_types_fk FOREIGN KEY (h_tik_scada_log_type_sk) REFERENCES public.h_tik_scada_log_types (h_tik_scada_log_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_tik_scada_logs_tik_scada_log_types_sk
);

-- ==============================
-- Hub: TIK Expert Slices
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_tik_expert_slices (
    h_tik_expert_slice_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_tik_expert_slices_pk PRIMARY KEY (h_tik_expert_slice_sk),
    CONSTRAINT h_tik_expert_slices_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_tik_expert_slice_sk);

-- ==============================
-- Satellite: TIK Expert Slices
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_tik_expert_slices (
    h_tik_expert_slice_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    slice_date TIMESTAMP NOT NULL,
    numeric_x float NOT NULL,
    comment text NOT NULL,
    CONSTRAINT s_tik_expert_slices_h_tik_expert_slice_fk FOREIGN KEY (h_tik_expert_slice_sk) REFERENCES public.h_tik_expert_slices (h_tik_expert_slice_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_tik_expert_slices_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_tik_expert_slice_sk);

-- ==============================
-- Link: Object Property - TIK Expert Slice
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_object_properties_tik_expert_slices (
    l_object_properties_tik_expert_slices_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    h_tik_expert_slice_sk uuid NOT NULL,
    CONSTRAINT l_object_properties_tik_expert_slices_pk PRIMARY KEY (
        l_object_properties_tik_expert_slices_sk
    ),
    CONSTRAINT l_object_properties_tik_expert_slices_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_properties_tik_expert_slices_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_properties_tik_expert_slices_h_tik_expert_slice_fk FOREIGN KEY (h_tik_expert_slice_sk) REFERENCES public.h_tik_expert_slices (h_tik_expert_slice_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_object_properties_tik_expert_slices_sk
);

-- ==============================
-- Hub: Object Type Descriptors
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_object_type_descriptors (
    h_object_type_descriptor_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_object_type_descriptors_pk PRIMARY KEY (h_object_type_descriptor_sk),
    CONSTRAINT h_object_type_descriptors_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_object_type_descriptor_sk);

-- ==============================
-- Satellite: Object Type Descriptors
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_object_type_descriptors (
    h_object_type_descriptor_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    name text NOT NULL,
    tag text NOT NULL,
    parent_id uuid NOT NULL,
    description text NOT NULL,
    CONSTRAINT s_object_type_descriptors_h_object_type_descriptor_fk FOREIGN KEY (h_object_type_descriptor_sk) REFERENCES public.h_object_type_descriptors (h_object_type_descriptor_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_object_type_descriptors_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_object_type_descriptor_sk);

-- ==============================
-- Link: Object - Object Type Descriptors
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_objects_object_type_descriptors (
    l_objects_object_type_descriptors_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_object_sk uuid NOT NULL,
    h_object_type_descriptor_sk uuid NOT NULL,
    CONSTRAINT l_objects_object_type_descriptors_pk PRIMARY KEY (
        l_objects_object_type_descriptors_sk
    ),
    CONSTRAINT l_objects_object_type_descriptors_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_objects_object_type_descriptors_h_object_fk FOREIGN KEY (h_object_sk) REFERENCES public.h_objects (h_object_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_objects_object_type_descriptors_h_object_type_descriptor_fk FOREIGN KEY (h_object_type_descriptor_sk) REFERENCES public.h_object_type_descriptors (h_object_type_descriptor_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_objects_object_type_descriptors_sk
);

-- ==============================
-- Link: Image - Object Type Descriptors
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_images_object_type_descriptors (
    l_images_object_type_descriptors_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_image_sk uuid NOT NULL,
    h_object_type_descriptor_sk uuid NOT NULL,
    CONSTRAINT l_images_object_type_descriptors_pk PRIMARY KEY (
        l_images_object_type_descriptors_sk
    ),
    CONSTRAINT l_images_object_type_descriptors_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_images_object_type_descriptors_h_image_fk FOREIGN KEY (h_image_sk) REFERENCES public.h_images (h_image_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_images_object_type_descriptors_h_object_type_descriptor_fk FOREIGN KEY (h_object_type_descriptor_sk) REFERENCES public.h_object_type_descriptors (h_object_type_descriptor_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_images_object_type_descriptors_sk
);

-- ==============================
-- Hub: Plon Route Objects Lists
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_pion_route_objects_lists (
    h_pion_route_objects_list_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_pion_route_objects_lists_pk PRIMARY KEY (h_pion_route_objects_list_sk),
    CONSTRAINT h_pion_route_objects_lists_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_pion_route_objects_list_sk);

-- ==============================
-- Satellite: Plon Route Objects Lists
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_pion_route_objects_lists (
    h_pion_route_objects_list_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    name text NOT NULL,
    description text NOT NULL,
    xml_objects_list text NOT NULL,
    date_modified TIMESTAMP,
    date_modified_cbd TIMESTAMP,
    CONSTRAINT s_pion_route_objects_lists_h_pion_route_objects_list_fk FOREIGN KEY (h_pion_route_objects_list_sk) REFERENCES public.h_pion_route_objects_lists (h_pion_route_objects_list_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_pion_route_objects_lists_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_pion_route_objects_list_sk);

-- ==============================
-- Hub: User Defined Property Lists
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_user_defined_property_lists (
    h_user_defined_property_list_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_user_defined_property_lists_pk PRIMARY KEY (
        h_user_defined_property_list_sk
    ),
    CONSTRAINT h_user_defined_property_lists_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_user_defined_property_list_sk
);

-- ==============================
-- Satellite: User Defined Property Lists
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_user_defined_property_lists (
    h_user_defined_property_list_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    property_list_name text NOT NULL,
    description text NOT NULL,
    xml_properties_list text NOT NULL,
    CONSTRAINT s_user_defined_property_lists_h_user_defined_property_list_fk FOREIGN KEY (
        h_user_defined_property_list_sk
    ) REFERENCES public.h_user_defined_property_lists (
        h_user_defined_property_list_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_user_defined_property_lists_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_user_defined_property_list_sk
);

-- ==============================
-- Hub: User Defined Property Lists Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_user_defined_property_lists_types (
    h_user_defined_property_list_type_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_user_defined_property_lists_types_pk PRIMARY KEY (
        h_user_defined_property_list_type_sk
    ),
    CONSTRAINT h_user_defined_property_lists_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_user_defined_property_list_type_sk
);

-- ==============================
-- Satellite: User Defined Property Lists Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_user_defined_property_lists_types (
    h_user_defined_property_list_type_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    property_list_type_name text NOT NULL,
    description text NOT NULL,
    CONSTRAINT s_user_defined_property_lists_types_h_user_defined_property_list_type_fk FOREIGN KEY (
        h_user_defined_property_list_type_sk
    ) REFERENCES public.h_user_defined_property_lists_types (
        h_user_defined_property_list_type_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_user_defined_property_lists_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_user_defined_property_list_type_sk
);

-- ==============================
-- Link: User Defined Property Lists - User Defined Property Lists Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_user_defined_prop_lists_user_defined_prop_list_types (
    l_user_defined_property_lists_user_defined_property_lists_type_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_user_defined_property_list_sk uuid NOT NULL,
    h_user_defined_property_list_type_sk uuid NOT NULL,
    CONSTRAINT l_user_defined_prop_lists_user_defined_prop_list_types_pk PRIMARY KEY (
        l_user_defined_property_lists_user_defined_property_lists_type_sk
    ),
    CONSTRAINT l_user_defined_prop_lists_user_defined_prop_list_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_user_defined_property_lists_user_defined_prop_lists_types_h_user_defined_prop_list_fk FOREIGN KEY (
        h_user_defined_property_list_sk
    ) REFERENCES public.h_user_defined_property_lists (
        h_user_defined_property_list_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_user_defined_prop_lists_user_defined_prop_list_types_h_user_defined_property_list_type_fk FOREIGN KEY (
        h_user_defined_property_list_type_sk
    ) REFERENCES public.h_user_defined_property_lists_types (
        h_user_defined_property_list_type_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_user_defined_property_lists_user_defined_property_lists_type_sk
);

-- ==============================
-- Hub: User Defined Tiles Properties Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_user_defined_tiles_property_configs (
    h_user_defined_tiles_property_config_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    business_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_user_defined_tiles_property_configs_pk PRIMARY KEY (
        h_user_defined_tiles_property_config_sk
    ),
    CONSTRAINT h_user_defined_tiles_property_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_user_defined_tiles_property_config_sk
);

-- ==============================
-- Satellite: User Defined Tiles Properties Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_user_defined_tiles_property_configs (
    h_user_defined_tiles_property_config_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    name text NOT NULL,
    tiles_properties text NOT NULL,
    CONSTRAINT s_user_defined_tiles_property_configs_h_user_defined_tiles_property_config_fk FOREIGN KEY (
        h_user_defined_tiles_property_config_sk
    ) REFERENCES public.h_user_defined_tiles_property_configs (
        h_user_defined_tiles_property_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_user_defined_tiles_property_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_user_defined_tiles_property_config_sk
);

-- ==============================
-- Link: User Defined Property Lists - Objects
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_user_defined_property_lists_objects (
    l_user_defined_property_lists_objects_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_user_defined_property_list_sk uuid NOT NULL,
    h_object_sk uuid NOT NULL,
    CONSTRAINT l_user_defined_property_lists_objects_pk PRIMARY KEY (
        l_user_defined_property_lists_objects_sk
    ),
    CONSTRAINT l_user_defined_property_lists_objects_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_user_defined_property_lists_objects_h_user_defined_property_list_fk FOREIGN KEY (
        h_user_defined_property_list_sk
    ) REFERENCES public.h_user_defined_property_lists (
        h_user_defined_property_list_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_user_defined_property_lists_objects_h_object_fk FOREIGN KEY (h_object_sk) REFERENCES public.h_objects (h_object_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_user_defined_property_lists_objects_sk
);

-- ==============================
-- Link: User Defined Tiles Properties Configs - Objects
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_user_defined_tiles_property_configs_objects (
    l_user_defined_tiles_property_configs_objects_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_user_defined_tiles_property_config_sk uuid NOT NULL,
    h_object_sk uuid NOT NULL,
    CONSTRAINT l_user_defined_tiles_property_configs_objects_pk PRIMARY KEY (
        l_user_defined_tiles_property_configs_objects_sk
    ),
    CONSTRAINT l_user_defined_tiles_property_configs_objects_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_user_defined_tiles_property_configs_objects_h_user_defined_tiles_property_config_fk FOREIGN KEY (
        h_user_defined_tiles_property_config_sk
    ) REFERENCES public.h_user_defined_tiles_property_configs (
        h_user_defined_tiles_property_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_user_defined_tiles_property_configs_objects_h_object_fk FOREIGN KEY (h_object_sk) REFERENCES public.h_objects (h_object_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_user_defined_tiles_property_configs_objects_sk
);

-- ==============================
-- Hub: IO creyt Channel Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_io_creyt_channel_configs (
    h_io_creyt_channel_config_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_io_creyt_channel_configs_pk PRIMARY KEY (h_io_creyt_channel_config_sk),
    CONSTRAINT h_io_creyt_channel_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_creyt_channel_config_sk);

-- ==============================
-- Satellite: IO creyt Channel Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_creyt_channel_configs (
    h_io_creyt_channel_config_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    channel_num integer NOT NULL,
    min_raw float NOT NULL,
    max_raw float NOT NULL,
    min_eu float NOT NULL,
    max_eu float NOT NULL,
    CONSTRAINT s_io_creyt_channel_configs_h_io_creyt_channel_config_fk FOREIGN KEY (h_io_creyt_channel_config_sk) REFERENCES public.h_io_creyt_channel_configs (h_io_creyt_channel_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_io_creyt_channel_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_creyt_channel_config_sk);

-- ==============================
-- Hub: IO creyt Channel States
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_io_creyt_channel_states (
    h_io_creyt_channel_state_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_io_creyt_channel_states_pk PRIMARY KEY (h_io_creyt_channel_state_sk),
    CONSTRAINT h_io_creyt_channel_states_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_creyt_channel_state_sk);

-- ==============================
-- Satellite: IO creyt Channel States
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_creyt_channel_states (
    h_io_creyt_channel_state_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    channel_num integer NOT NULL,
    CONSTRAINT s_io_creyt_channel_states_h_io_creyt_channel_state_fk FOREIGN KEY (h_io_creyt_channel_state_sk) REFERENCES public.h_io_creyt_channel_states (h_io_creyt_channel_state_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_io_creyt_channel_states_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_creyt_channel_state_sk);

-- ==============================
-- Link: IO creyt Channel Configs - Measure Units
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_creyt_channel_configs_measure_units (
    l_io_creyt_channel_configs_measure_units_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_creyt_channel_config_sk uuid NOT NULL,
    h_measure_unit_sk uuid NOT NULL,
    CONSTRAINT l_io_creyt_channel_configs_measure_units_pk PRIMARY KEY (
        l_io_creyt_channel_configs_measure_units_sk
    ),
    CONSTRAINT l_io_creyt_channel_configs_measure_units_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_creyt_channel_configs_measure_units_h_io_creyt_channel_config_fk FOREIGN KEY (h_io_creyt_channel_config_sk) REFERENCES public.h_io_creyt_channel_configs (h_io_creyt_channel_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_creyt_channel_configs_measure_units_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_creyt_channel_configs_measure_units_sk
);

-- -- ==============================
-- -- Link: IO creyt Channel States - Measure Units
-- -- ==============================
-- CREATE TABLE IF NOT EXISTS public.l_io_creyt_channel_states_measure_units (
--     l_io_creyt_channel_states_measure_units_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
--     load_dttm TIMESTAMPTZ NOT NULL,
-- valid_from_dttm TIMESTAMPTZ NOT NULL,
--     valid_to_dttm TIMESTAMPTZ NULL,
--     active_flag bool DEFAULT true NOT NULL,
--     data_source_id uuid NOT NULL,
--    data_file_id uuid NOT NULL,
--     h_io_creyt_channel_state_sk uuid NOT NULL,
--     h_measure_unit_sk uuid NOT NULL,
--     CONSTRAINT l_io_creyt_channel_states_measure_units_pk PRIMARY KEY (
--         l_io_creyt_channel_states_measure_units_sk
--     ),
--     CONSTRAINT l_io_creyt_channel_states_measure_units_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
--     CONSTRAINT l_io_creyt_channel_states_measure_units_h_io_creyt_channel_state_fk FOREIGN KEY (h_io_creyt_channel_state_sk) REFERENCES public.h_io_creyt_channel_states (h_io_creyt_channel_state_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
--     CONSTRAINT l_io_creyt_channel_states_measure_units_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE
-- ) DISTRIBUTED BY (
--     l_io_creyt_channel_states_measure_units_sk
-- );

-- ==============================
-- Link: IO creyt Channel Configs - Object Properties
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_creyt_channel_configs_object_properties (
    l_io_creyt_channel_configs_object_properties_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_creyt_channel_config_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_io_creyt_channel_configs_object_properties_pk PRIMARY KEY (
        l_io_creyt_channel_configs_object_properties_sk
    ),
    CONSTRAINT l_io_creyt_channel_configs_object_properties_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_creyt_channel_configs_object_properties_h_io_creyt_channel_config_fk FOREIGN KEY (h_io_creyt_channel_config_sk) REFERENCES public.h_io_creyt_channel_configs (h_io_creyt_channel_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_creyt_channel_configs_object_properties_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_creyt_channel_configs_object_properties_sk
);

-- ==============================
-- Link: IO creyt Channel States - Object Properties
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_creyt_channel_states_object_properties (
    l_io_creyt_channel_states_object_properties_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_creyt_channel_state_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_io_creyt_channel_states_object_properties_pk PRIMARY KEY (
        l_io_creyt_channel_states_object_properties_sk
    ),
    CONSTRAINT l_io_creyt_channel_states_object_properties_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_creyt_channel_states_object_properties_h_io_creyt_channel_state_fk FOREIGN KEY (h_io_creyt_channel_state_sk) REFERENCES public.h_io_creyt_channel_states (h_io_creyt_channel_state_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_creyt_channel_states_object_properties_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_creyt_channel_states_object_properties_sk
);

-- ==============================
-- Link: IO creyt Channel Configs - IO Device Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_creyt_channel_configs_io_device_configs (
    l_io_creyt_channel_configs_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_creyt_channel_config_sk uuid NOT NULL,
    h_io_device_config_sk uuid NOT NULL,
    CONSTRAINT l_io_creyt_channel_configs_io_device_configs_pk PRIMARY KEY (
        l_io_creyt_channel_configs_io_device_configs_sk
    ),
    CONSTRAINT l_io_creyt_channel_configs_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_creyt_channel_configs_io_device_configs_h_io_creyt_channel_config_fk FOREIGN KEY (h_io_creyt_channel_config_sk) REFERENCES public.h_io_creyt_channel_configs (h_io_creyt_channel_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_creyt_channel_configs_io_device_configs_h_io_device_config_fk FOREIGN KEY (h_io_device_config_sk) REFERENCES public.h_io_device_configs (h_io_device_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_creyt_channel_configs_io_device_configs_sk
);

-- ==============================
-- Link: IO creyt Channel States - IO Device Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_creyt_channel_states_io_device_configs (
    l_io_creyt_channel_states_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_creyt_channel_state_sk uuid NOT NULL,
    h_io_device_config_sk uuid NOT NULL,
    CONSTRAINT l_io_creyt_channel_states_io_device_configs_pk PRIMARY KEY (
        l_io_creyt_channel_states_io_device_configs_sk
    ),
    CONSTRAINT l_io_creyt_channel_states_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_creyt_channel_states_io_device_configs_h_io_creyt_channel_state_fk FOREIGN KEY (h_io_creyt_channel_state_sk) REFERENCES public.h_io_creyt_channel_states (h_io_creyt_channel_state_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_creyt_channel_states_io_device_configs_h_io_device_config_fk FOREIGN KEY (h_io_device_config_sk) REFERENCES public.h_io_device_configs (h_io_device_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_creyt_channel_states_io_device_configs_sk
);

-- ==============================
-- Hub: IO creyt Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_io_creyt_configs (
    h_io_creyt_config_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_io_creyt_configs_pk PRIMARY KEY (h_io_creyt_config_sk),
    CONSTRAINT h_io_creyt_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_creyt_config_sk);

-- ==============================
-- Satellite: IO creyt Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_creyt_configs (
    h_io_creyt_config_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    primary_ip text NOT NULL,
    secondary_ip text NOT NULL,
    primary_modbus_port INTEGER NOT NULL,
    secondary_modbus_port INTEGER NOT NULL,
    primary_http_port INTEGER NOT NULL,
    secondary_http_port INTEGER NOT NULL,
    scan_time INTEGER NOT NULL,
    connect_retries INTEGER NOT NULL,
    samples_file_name text NOT NULL,
    is_sync_read_slave bool NOT NULL,
    sync_read_group_name text NOT NULL,
    detect_failures bool NOT NULL,
    detect_failures_length INTEGER NOT NULL,
    enabled_secondary_ip bool NOT NULL,
    num_block_samples INTEGER NOT NULL,
    CONSTRAINT s_io_creyt_configs_h_io_creyt_config_fk FOREIGN KEY (h_io_creyt_config_sk) REFERENCES public.h_io_creyt_configs (h_io_creyt_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_io_creyt_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_creyt_config_sk);

-- ==============================
-- Link: IO creyt Configs - IO Device Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_creyt_configs_io_device_configs (
    l_io_creyt_configs_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_creyt_config_sk uuid NOT NULL,
    h_io_device_config_sk uuid NOT NULL,
    CONSTRAINT l_io_creyt_configs_io_device_configs_pk PRIMARY KEY (
        l_io_creyt_configs_io_device_configs_sk
    ),
    CONSTRAINT l_io_creyt_configs_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_creyt_configs_io_device_configs_h_io_creyt_config_fk FOREIGN KEY (h_io_creyt_config_sk) REFERENCES public.h_io_creyt_configs (h_io_creyt_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_creyt_configs_io_device_configs_h_io_device_config_fk FOREIGN KEY (h_io_device_config_sk) REFERENCES public.h_io_device_configs (h_io_device_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_creyt_configs_io_device_configs_sk
);

-- ==============================
-- Hub: IO Creyt IMOPerTimes
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_io_creyt_im_oper_times (
    h_io_creyt_im_oper_time_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_io_creyt_im_oper_time_pk PRIMARY KEY (h_io_creyt_im_oper_time_sk),
    CONSTRAINT h_io_creyt_im_oper_times_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_creyt_im_oper_time_sk);

-- ==============================
-- Satellite: IO Creyt IMOPerTimes
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_creyt_im_oper_times (
    h_io_creyt_im_oper_time_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    CONSTRAINT s_io_creyt_im_oper_times_h_io_creyt_im_oper_times_fk FOREIGN KEY (h_io_creyt_im_oper_time_sk) REFERENCES public.h_io_creyt_im_oper_times (h_io_creyt_im_oper_time_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_io_creyt_im_oper_times_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_creyt_im_oper_time_sk);

-- ==============================
-- Link: IO Creyt IMOPerTimes - Object Properties
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_creyt_im_oper_times_object_properties (
    l_io_creyt_im_oper_times_object_properties_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_creyt_im_oper_time_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_io_creyt_im_oper_times_object_properties_pk PRIMARY KEY (
        l_io_creyt_im_oper_times_object_properties_sk
    ),
    CONSTRAINT l_io_creyt_im_oper_times_object_properties_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_creyt_im_oper_times_object_properties_h_io_creyt_im_oper_times_fk FOREIGN KEY (h_io_creyt_im_oper_time_sk) REFERENCES public.h_io_creyt_im_oper_times (h_io_creyt_im_oper_time_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_creyt_im_oper_times_object_properties_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_creyt_im_oper_times_object_properties_sk
);

-- ==============================
-- Link: IO Creyt IMOPerTimes - IO Device Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_creyt_im_oper_times_io_device_configs (
    l_io_creyt_im_oper_times_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_creyt_im_oper_time_sk uuid NOT NULL,
    h_io_device_config_sk uuid NOT NULL,
    CONSTRAINT l_io_creyt_im_oper_times_io_device_configs_pk PRIMARY KEY (
        l_io_creyt_im_oper_times_io_device_configs_sk
    ),
    CONSTRAINT l_io_creyt_im_oper_times_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_creyt_im_oper_times_io_device_configs_h_io_creyt_im_oper_times_fk FOREIGN KEY (h_io_creyt_im_oper_time_sk) REFERENCES public.h_io_creyt_im_oper_times (h_io_creyt_im_oper_time_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_creyt_im_oper_times_io_device_configs_h_io_device_config_fk FOREIGN KEY (h_io_device_config_sk) REFERENCES public.h_io_device_configs (h_io_device_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_creyt_im_oper_times_io_device_configs_sk
);

-- ==============================
-- Hub: IO Creyt Controller States
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_io_creyt_controller_states (
    h_io_creyt_controller_state_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_io_creyt_controller_states_pk PRIMARY KEY (
        h_io_creyt_controller_state_sk
    ),
    CONSTRAINT h_io_creyt_controller_states_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_io_creyt_controller_state_sk
);

-- ==============================
-- Satellite: IO Creyt Controller States
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_creyt_controller_states (
    h_io_creyt_controller_state_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    row_num integer NOT NULL,
    CONSTRAINT s_io_creyt_controller_states_h_io_creyt_controller_state_fk FOREIGN KEY (
        h_io_creyt_controller_state_sk
    ) REFERENCES public.h_io_creyt_controller_states (
        h_io_creyt_controller_state_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_io_creyt_controller_states_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_io_creyt_controller_state_sk
);

-- ==============================
-- Link: IO Creyt Controller States - Object Properties
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_creyt_controller_states_object_properties (
    l_io_creyt_controller_states_object_properties_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_creyt_controller_state_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_io_creyt_controller_states_object_properties_pk PRIMARY KEY (
        l_io_creyt_controller_states_object_properties_sk
    ),
    CONSTRAINT l_io_creyt_controller_states_object_properties_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_creyt_controller_states_object_properties_h_io_creyt_controller_state_fk FOREIGN KEY (
        h_io_creyt_controller_state_sk
    ) REFERENCES public.h_io_creyt_controller_states (
        h_io_creyt_controller_state_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_creyt_controller_states_object_properties_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_creyt_controller_states_object_properties_sk
);

-- ==============================
-- Link: IO Creyt Controller States - IO Device Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_creyt_controller_states_io_device_configs (
    l_io_creyt_controller_states_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_creyt_controller_state_sk uuid NOT NULL,
    h_io_device_config_sk uuid NOT NULL,
    CONSTRAINT l_io_creyt_controller_states_io_device_configs_pk PRIMARY KEY (
        l_io_creyt_controller_states_io_device_configs_sk
    ),
    CONSTRAINT l_io_creyt_controller_states_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_creyt_controller_states_io_device_configs_h_io_creyt_controller_state_fk FOREIGN KEY (
        h_io_creyt_controller_state_sk
    ) REFERENCES public.h_io_creyt_controller_states (
        h_io_creyt_controller_state_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_creyt_controller_states_io_device_configs_h_io_device_config_fk FOREIGN KEY (h_io_device_config_sk) REFERENCES public.h_io_device_configs (h_io_device_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_creyt_controller_states_io_device_configs_sk
);

-- ==============================
-- Link: IO Creyt Controller States - Opertime Properties
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_creyt_controller_states_oper_time_properties (
    l_io_creyt_controller_states_oper_time_properties_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_creyt_controller_state_sk uuid NOT NULL,
    h_opertime_properties_sk uuid NOT NULL,
    CONSTRAINT l_io_creyt_controller_states_oper_time_properties_pk PRIMARY KEY (
        l_io_creyt_controller_states_oper_time_properties_sk
    ),
    CONSTRAINT l_io_creyt_controller_states_oper_time_properties_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_creyt_controller_states_oper_time_properties_h_io_creyt_controller_state_fk FOREIGN KEY (
        h_io_creyt_controller_state_sk
    ) REFERENCES public.h_io_creyt_controller_states (
        h_io_creyt_controller_state_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_creyt_controller_states_oper_time_properties_h_opertime_properties_fk FOREIGN KEY (h_opertime_properties_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_creyt_controller_states_oper_time_properties_sk
);

-- ==============================
-- Hub: IO Creyt lcard Channel Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_io_lcard_channel_configs (
    h_io_lcard_channel_config_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_io_lcard_channel_configs_pk PRIMARY KEY (h_io_lcard_channel_config_sk),
    CONSTRAINT h_io_lcard_channel_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_lcard_channel_config_sk);

-- ==============================
-- Satellite: IO Creyt lcard Channel Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_lcard_channel_configs (
    h_io_lcard_channel_config_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    slot integer NOT NULL,
    channel_number integer NOT NULL,
    is_scaled bool NOT NULL,
    min_raw float NOT NULL,
    max_raw float NOT NULL,
    min_eu float NOT NULL,
    max_eu float NOT NULL,
    property_mask_id uuid NOT NULL,
    CONSTRAINT s_io_lcard_channel_configs_h_io_lcard_channel_config_fk FOREIGN KEY (h_io_lcard_channel_config_sk) REFERENCES public.h_io_lcard_channel_configs (h_io_lcard_channel_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_io_lcard_channel_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_lcard_channel_config_sk);

-- ==============================
-- Link: IO Creyt lcard Channel Configs - Measure Units
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_lcard_channel_configs_measure_units (
    l_io_lcard_channel_configs_measure_units_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_lcard_channel_config_sk uuid NOT NULL,
    h_measure_unit_sk uuid NOT NULL,
    CONSTRAINT l_io_lcard_channel_configs_measure_units_pk PRIMARY KEY (
        l_io_lcard_channel_configs_measure_units_sk
    ),
    CONSTRAINT l_io_lcard_channel_configs_measure_units_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_lcard_channel_configs_measure_units_h_io_lcard_channel_config_fk FOREIGN KEY (h_io_lcard_channel_config_sk) REFERENCES public.h_io_lcard_channel_configs (h_io_lcard_channel_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_lcard_channel_configs_measure_units_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_lcard_channel_configs_measure_units_sk
);

-- ==============================
-- Link: IO Creyt lcard Channel Configs - Object Properties
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_lcard_channel_configs_object_properties (
    l_io_lcard_channel_configs_object_properties_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_lcard_channel_config_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_io_lcard_channel_configs_object_properties_pk PRIMARY KEY (
        l_io_lcard_channel_configs_object_properties_sk
    ),
    CONSTRAINT l_io_lcard_channel_configs_object_properties_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_lcard_channel_configs_object_properties_h_io_lcard_channel_config_fk FOREIGN KEY (h_io_lcard_channel_config_sk) REFERENCES public.h_io_lcard_channel_configs (h_io_lcard_channel_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_lcard_channel_configs_object_properties_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_lcard_channel_configs_object_properties_sk
);

-- ==============================
-- Link: IO Creyt lcard Channel Configs - IO Device Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_lcard_channel_configs_io_device_configs (
    l_io_lcard_channel_configs_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_lcard_channel_config_sk uuid NOT NULL,
    h_io_device_config_sk uuid NOT NULL,
    CONSTRAINT l_io_lcard_channel_configs_io_device_configs_pk PRIMARY KEY (
        l_io_lcard_channel_configs_io_device_configs_sk
    ),
    CONSTRAINT l_io_lcard_channel_configs_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_lcard_channel_configs_io_device_configs_h_io_lcard_channel_config_fk FOREIGN KEY (h_io_lcard_channel_config_sk) REFERENCES public.h_io_lcard_channel_configs (h_io_lcard_channel_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_lcard_channel_configs_io_device_configs_h_io_device_config_fk FOREIGN KEY (h_io_device_config_sk) REFERENCES public.h_io_device_configs (h_io_device_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_lcard_channel_configs_io_device_configs_sk
);

-- ==============================
-- Hub: IO LCard Crate Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_io_lcard_crate_configs (
    h_io_lcard_crate_config_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_io_lcard_crate_configs_pk PRIMARY KEY (h_io_lcard_crate_config_sk),
    CONSTRAINT h_io_lcard_crate_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_lcard_crate_config_sk);

-- ==============================
-- Satellite: IO LCard Crate Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_lcard_crate_configs (
    h_io_lcard_crate_config_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    ip text NOT NULL,
    port integer NOT NULL,
    serial_number text NOT NULL,
    sampling_time integer NOT NULL,
    scan_time integer NOT NULL,
    CONSTRAINT s_io_lcard_crate_configs_h_io_lcard_crate_config_fk FOREIGN KEY (h_io_lcard_crate_config_sk) REFERENCES public.h_io_lcard_crate_configs (h_io_lcard_crate_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_io_lcard_crate_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_lcard_crate_config_sk);

-- ==============================
-- Link: IO LCard Crate Configs - IO Device Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_lcard_crate_configs_io_device_configs (
    l_io_lcard_crate_configs_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_lcard_crate_config_sk uuid NOT NULL,
    h_io_device_config_sk uuid NOT NULL,
    CONSTRAINT l_io_lcard_crate_configs_io_device_configs_pk PRIMARY KEY (
        l_io_lcard_crate_configs_io_device_configs_sk
    ),
    CONSTRAINT l_io_lcard_crate_configs_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_lcard_crate_configs_io_device_configs_h_io_lcard_crate_config_fk FOREIGN KEY (h_io_lcard_crate_config_sk) REFERENCES public.h_io_lcard_crate_configs (h_io_lcard_crate_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_lcard_crate_configs_io_device_configs_h_io_device_config_fk FOREIGN KEY (h_io_device_config_sk) REFERENCES public.h_io_device_configs (h_io_device_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_lcard_crate_configs_io_device_configs_sk
);

-- ==============================
-- Link: IO LCard Crate Configs - Crate Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_lcard_crate_configs_crate_types (
    l_io_lcard_crate_configs_crate_types_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_lcard_crate_config_sk uuid NOT NULL,
    h_crate_type_sk uuid NOT NULL,
    CONSTRAINT l_io_lcard_crate_configs_crate_types_pk PRIMARY KEY (
        l_io_lcard_crate_configs_crate_types_sk
    ),
    CONSTRAINT l_io_lcard_crate_configs_crate_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_lcard_crate_configs_crate_types_h_io_lcard_crate_config_fk FOREIGN KEY (h_io_lcard_crate_config_sk) REFERENCES public.h_io_lcard_crate_configs (h_io_lcard_crate_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_lcard_crate_configs_crate_types_h_crate_type_fk FOREIGN KEY (h_crate_type_sk) REFERENCES public.h_crate_types (h_crate_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_lcard_crate_configs_crate_types_sk
);

-- ==============================
-- Hub: IO LCard Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_io_lcard_configs (
    h_io_lcard_config_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_io_lcard_configs_pk PRIMARY KEY (h_io_lcard_config_sk),
    CONSTRAINT h_io_lcard_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_lcard_config_sk);

-- ==============================
-- Satellite: IO LCard Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_lcard_configs (
    h_io_lcard_config_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    virtual_slot integer NOT NULL,
    sampling_rate float NOT NULL,
    operation_mode integer NOT NULL,
    timer_sampling_time integer NOT NULL,
    timer_scan_time integer NOT NULL,
    sync_sampling_time integer NOT NULL,
    CONSTRAINT s_io_lcard_configs_h_io_lcard_config_fk FOREIGN KEY (h_io_lcard_config_sk) REFERENCES public.h_io_lcard_configs (h_io_lcard_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_io_lcard_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_lcard_config_sk);

-- ==============================
-- Link: IO LCard Configs - IO Device Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_lcard_configs_io_device_configs (
    l_io_lcard_configs_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_lcard_config_sk uuid NOT NULL,
    h_io_device_config_sk uuid NOT NULL,
    CONSTRAINT l_io_lcard_configs_io_device_configs_pk PRIMARY KEY (
        l_io_lcard_configs_io_device_configs_sk
    ),
    CONSTRAINT l_io_lcard_configs_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_lcard_configs_io_device_configs_h_io_lcard_config_fk FOREIGN KEY (h_io_lcard_config_sk) REFERENCES public.h_io_lcard_configs (h_io_lcard_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_lcard_configs_io_device_configs_h_io_device_config_fk FOREIGN KEY (h_io_device_config_sk) REFERENCES public.h_io_device_configs (h_io_device_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_lcard_configs_io_device_configs_sk
);

-- ==============================
-- Hub: IO LCard Crate Sync
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_io_lcard_sync_crates (
    h_io_lcard_sync_crate_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_io_lcard_sync_crate_pk PRIMARY KEY (h_io_lcard_sync_crate_sk),
    CONSTRAINT h_io_lcard_sync_crates_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_lcard_sync_crate_sk);

-- ==============================
-- Satellite: IO LCard Crate Sync
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_lcard_sync_crates (
    h_io_lcard_sync_crate_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    is_sync bool NOT NULL,
    is_leader bool NOT NULL,
    is_slave bool NOT NULL,
    leader_id uuid NOT NULL,
    CONSTRAINT s_io_lcard_sync_crates_h_io_lcard_sync_crates_fk FOREIGN KEY (h_io_lcard_sync_crate_sk) REFERENCES public.h_io_lcard_sync_crates (h_io_lcard_sync_crate_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_io_lcard_sync_crates_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_lcard_sync_crate_sk);

-- ==============================
-- Link: IO LCard Crate Sync - IO Device Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_lcard_sync_crates_io_device_configs (
    l_io_lcard_sync_crate_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_lcard_sync_crate_sk uuid NOT NULL,
    h_io_device_config_sk uuid NOT NULL,
    CONSTRAINT l_io_lcard_sync_crate_io_device_configs_pk PRIMARY KEY (
        l_io_lcard_sync_crate_io_device_configs_sk
    ),
    CONSTRAINT l_io_lcard_sync_crate_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_lcard_sync_crate_io_device_configs_h_io_lcard_sync_crate_fk FOREIGN KEY (h_io_lcard_sync_crate_sk) REFERENCES public.h_io_lcard_sync_crates (h_io_lcard_sync_crate_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_lcard_sync_crate_io_device_configs_h_io_device_config_fk FOREIGN KEY (h_io_device_config_sk) REFERENCES public.h_io_device_configs (h_io_device_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_lcard_sync_crate_io_device_configs_sk
);

-- ==============================
-- Hub: IO LCard Input Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_io_lcard_input_configs (
    h_io_lcard_input_config_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_io_lcard_input_configs_pk PRIMARY KEY (h_io_lcard_input_config_sk),
    CONSTRAINT h_io_lcard_input_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_lcard_input_config_sk);

-- ==============================
-- Satellite: IO LCard Input Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_lcard_input_configs (
    h_io_lcard_input_config_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    input_num integer NOT NULL,
    input_range integer NOT NULL,
    is_scaled bool NOT NULL,
    min_raw float NOT NULL,
    max_raw float NOT NULL,
    min_eu float NOT NULL,
    max_eu float NOT NULL,
    CONSTRAINT s_io_lcard_input_configs_h_io_lcard_input_config_fk FOREIGN KEY (h_io_lcard_input_config_sk) REFERENCES public.h_io_lcard_input_configs (h_io_lcard_input_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_io_lcard_input_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_lcard_input_config_sk);

-- ==============================
-- Link: IO LCard Input Configs - Measure Units
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_lcard_input_configs_measure_units (
    l_io_lcard_input_configs_measure_units_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_lcard_input_config_sk uuid NOT NULL,
    h_measure_unit_sk uuid NOT NULL,
    CONSTRAINT l_io_lcard_input_configs_measure_units_pk PRIMARY KEY (
        l_io_lcard_input_configs_measure_units_sk
    ),
    CONSTRAINT l_io_lcard_input_configs_measure_units_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_lcard_input_configs_measure_units_h_io_lcard_input_config_fk FOREIGN KEY (h_io_lcard_input_config_sk) REFERENCES public.h_io_lcard_input_configs (h_io_lcard_input_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_lcard_input_configs_measure_units_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_lcard_input_configs_measure_units_sk
);

-- ==============================
-- Link: IO LCard Input Configs - Object Properties
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_lcard_input_configs_object_properties (
    l_io_lcard_input_configs_object_properties_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_lcard_input_config_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_io_lcard_input_configs_object_properties_pk PRIMARY KEY (
        l_io_lcard_input_configs_object_properties_sk
    ),
    CONSTRAINT l_io_lcard_input_configs_object_properties_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_lcard_input_configs_object_properties_h_io_lcard_input_config_fk FOREIGN KEY (h_io_lcard_input_config_sk) REFERENCES public.h_io_lcard_input_configs (h_io_lcard_input_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_lcard_input_configs_object_properties_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_lcard_input_configs_object_properties_sk
);

-- ==============================
-- Link: IO LCard Input Configs - IO Device Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_lcard_input_configs_io_device_configs (
    l_io_lcard_input_configs_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_lcard_input_config_sk uuid NOT NULL,
    h_io_device_config_sk uuid NOT NULL,
    CONSTRAINT l_io_lcard_input_configs_io_device_configs_pk PRIMARY KEY (
        l_io_lcard_input_configs_io_device_configs_sk
    ),
    CONSTRAINT l_io_lcard_input_configs_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_lcard_input_configs_io_device_configs_h_io_lcard_input_config_fk FOREIGN KEY (h_io_lcard_input_config_sk) REFERENCES public.h_io_lcard_input_configs (h_io_lcard_input_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_lcard_input_configs_io_device_configs_h_io_device_config_fk FOREIGN KEY (h_io_device_config_sk) REFERENCES public.h_io_device_configs (h_io_device_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_lcard_input_configs_io_device_configs_sk
);

-- ==============================
-- Hub: IO Creyt Card Logic Input Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_io_lcard_logic_input_configs (
    h_io_lcard_logic_input_config_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_io_lcard_logic_input_configs_pk PRIMARY KEY (
        h_io_lcard_logic_input_config_sk
    ),
    CONSTRAINT h_io_lcard_logic_input_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_io_lcard_logic_input_config_sk
);

-- ==============================
-- Satellite: IO Creyt Card Logic Input Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_lcard_logic_input_configs (
    h_io_lcard_logic_input_config_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    input_number integer NOT NULL,
    input_range integer NOT NULL,
    is_scaled bool NOT NULL,
    min_raw float NOT NULL,
    max_raw float NOT NULL,
    min_eu float NOT NULL,
    max_eu float NOT NULL,
    CONSTRAINT s_io_lcard_logic_input_configs_h_io_lcard_logic_input_config_fk FOREIGN KEY (
        h_io_lcard_logic_input_config_sk
    ) REFERENCES public.h_io_lcard_logic_input_configs (
        h_io_lcard_logic_input_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_io_lcard_logic_input_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_io_lcard_logic_input_config_sk
);

-- ==============================
-- Link: IO Creyt Card Logic Input Configs - Timer Property ID
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_lcard_logic_input_configs_timer_property_ids (
    l_io_lcard_logic_input_configs_timer_property_ids_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_lcard_logic_input_config_sk uuid NOT NULL,
    h_timer_property_id_sk uuid NOT NULL,
    CONSTRAINT l_io_lcard_logic_input_configs_timer_property_ids_pk PRIMARY KEY (
        l_io_lcard_logic_input_configs_timer_property_ids_sk
    ),
    CONSTRAINT l_io_lcard_logic_input_configs_timer_property_ids_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_lcard_logic_input_configs_timer_property_ids_h_io_lcard_logic_input_config_fk FOREIGN KEY (
        h_io_lcard_logic_input_config_sk
    ) REFERENCES public.h_io_lcard_logic_input_configs (
        h_io_lcard_logic_input_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_lcard_logic_input_configs_timer_property_ids_h_timer_property_id_fk FOREIGN KEY (h_timer_property_id_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_lcard_logic_input_configs_timer_property_ids_sk
);

-- ==============================
-- Link: IO Creyt Card Logic Input Configs - Sync Property ID
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_lcard_logic_input_configs_sync_property_ids (
    l_io_lcard_logic_input_configs_sync_property_ids_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_lcard_logic_input_config_sk uuid NOT NULL,
    h_sync_property_id_sk uuid NOT NULL,
    CONSTRAINT l_io_lcard_logic_input_configs_sync_property_ids_pk PRIMARY KEY (
        l_io_lcard_logic_input_configs_sync_property_ids_sk
    ),
    CONSTRAINT l_io_lcard_logic_input_configs_sync_property_ids_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_lcard_logic_input_configs_sync_property_ids_h_io_lcard_logic_input_config_fk FOREIGN KEY (
        h_io_lcard_logic_input_config_sk
    ) REFERENCES public.h_io_lcard_logic_input_configs (
        h_io_lcard_logic_input_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_lcard_logic_input_configs_sync_property_ids_h_sync_property_id_fk FOREIGN KEY (h_sync_property_id_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_lcard_logic_input_configs_sync_property_ids_sk
);

-- ==============================
-- Link: IO Creyt Card Logic Input Configs - IO Device Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_lcard_logic_input_configs_io_device_configs (
    l_io_lcard_logic_input_configs_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_lcard_logic_input_config_sk uuid NOT NULL,
    h_io_device_config_sk uuid NOT NULL,
    CONSTRAINT l_io_lcard_logic_input_configs_io_device_configs_pk PRIMARY KEY (
        l_io_lcard_logic_input_configs_io_device_configs_sk
    ),
    CONSTRAINT l_io_lcard_logic_input_configs_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_lcard_logic_input_configs_io_device_configs_h_io_lcard_logic_input_config_fk FOREIGN KEY (
        h_io_lcard_logic_input_config_sk
    ) REFERENCES public.h_io_lcard_logic_input_configs (
        h_io_lcard_logic_input_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_lcard_logic_input_configs_io_device_configs_h_io_device_config_fk FOREIGN KEY (h_io_device_config_sk) REFERENCES public.h_io_device_configs (h_io_device_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_lcard_logic_input_configs_io_device_configs_sk
);

-- ==============================
-- Link: IO Creyt Card Logic Input Configs - L Card Logic Input Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_lcard_logic_input_configs_l_card_logic_input_types (
    l_io_lcard_logic_input_configs_l_card_logic_input_types_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_lcard_logic_input_config_sk uuid NOT NULL,
    h_l_card_logic_input_type_sk uuid NOT NULL,
    CONSTRAINT l_io_lcard_logic_input_configs_l_card_logic_input_types_pk PRIMARY KEY (
        l_io_lcard_logic_input_configs_l_card_logic_input_types_sk
    ),
    CONSTRAINT l_io_lcard_logic_input_configs_l_card_logic_input_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_lcard_logic_input_configs_l_card_logic_input_types_h_io_lcard_logic_input_config_fk FOREIGN KEY (
        h_io_lcard_logic_input_config_sk
    ) REFERENCES public.h_io_lcard_logic_input_configs (
        h_io_lcard_logic_input_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_lcard_logic_input_configs_l_card_logic_input_types_h_l_card_logic_input_type_fk FOREIGN KEY (h_l_card_logic_input_type_sk) REFERENCES public.h_l_card_logic_input_types (h_l_card_logic_input_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_lcard_logic_input_configs_l_card_logic_input_types_sk
);

-- ==============================
-- Hub: IO Creyt Card Module Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_io_lcard_module_configs (
    h_io_lcard_module_config_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_io_lcard_module_configs_pk PRIMARY KEY (h_io_lcard_module_config_sk),
    CONSTRAINT h_io_lcard_module_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_lcard_module_config_sk);

-- ==============================
-- Satellite: IO Creyt Card Module Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_lcard_module_configs (
    h_io_lcard_module_config_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    slot integer NOT NULL,
    frequency_divisor integer NOT NULL,
    CONSTRAINT s_io_lcard_module_configs_h_io_lcard_module_config_fk FOREIGN KEY (h_io_lcard_module_config_sk) REFERENCES public.h_io_lcard_module_configs (h_io_lcard_module_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_io_lcard_module_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_lcard_module_config_sk);

-- ==============================
-- Link: IO Creyt Card Module Configs - IO Device Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_lcard_module_configs_io_device_configs (
    l_io_lcard_module_configs_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_lcard_module_config_sk uuid NOT NULL,
    h_io_device_config_sk uuid NOT NULL,
    CONSTRAINT l_io_lcard_module_configs_io_device_configs_pk PRIMARY KEY (
        l_io_lcard_module_configs_io_device_configs_sk
    ),
    CONSTRAINT l_io_lcard_module_configs_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_lcard_module_configs_io_device_configs_h_io_lcard_module_config_fk FOREIGN KEY (h_io_lcard_module_config_sk) REFERENCES public.h_io_lcard_module_configs (h_io_lcard_module_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_lcard_module_configs_io_device_configs_h_io_device_config_fk FOREIGN KEY (h_io_device_config_sk) REFERENCES public.h_io_device_configs (h_io_device_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_lcard_module_configs_io_device_configs_sk
);
-- ==============================
-- Link: IO Creyt Card Module Configs - L Card Crate Module Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_lcard_module_configs_l_card_crate_module_types (
    l_io_lcard_module_configs_l_card_crate_module_types_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_lcard_module_config_sk uuid NOT NULL,
    h_l_card_crate_module_type_sk uuid NOT NULL,
    CONSTRAINT l_io_lcard_module_configs_l_card_crate_module_types_pk PRIMARY KEY (
        l_io_lcard_module_configs_l_card_crate_module_types_sk
    ),
    CONSTRAINT l_io_lcard_module_configs_l_card_crate_module_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_lcard_module_configs_l_card_crate_module_types_h_io_lcard_module_config_fk FOREIGN KEY (h_io_lcard_module_config_sk) REFERENCES public.h_io_lcard_module_configs (h_io_lcard_module_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_lcard_module_configs_l_card_crate_module_types_h_l_card_crate_module_type_fk FOREIGN KEY (h_l_card_crate_module_type_sk) REFERENCES public.h_l_card_crate_module_types (h_l_card_crate_module_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_lcard_module_configs_l_card_crate_module_types_sk
);

-- ==============================
-- Hub: IO Modbus TCP Register Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_io_modbus_tcp_register_configs (
    h_io_modbus_tcp_register_config_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_io_modbus_tcp_register_configs_pk PRIMARY KEY (
        h_io_modbus_tcp_register_config_sk
    ),
    CONSTRAINT h_io_modbus_tcp_register_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_io_modbus_tcp_register_config_sk
);

-- ==============================
-- Satellite: IO Modbus TCP Register Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_modbus_tcp_register_configs (
    h_io_modbus_tcp_register_config_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    register_address INTEGER NOT NULL,
    byte_order text NOT NULL,
    min_raw float NOT NULL,
    max_raw float NOT NULL,
    min_eu float NOT NULL,
    max_eu float NOT NULL,
    is_scaled bool NOT NULL,
    factor float NOT NULL,
    is_write bool NOT NULL,
    CONSTRAINT s_io_modbus_tcp_register_configs_h_io_modbus_tcp_register_config_fk FOREIGN KEY (
        h_io_modbus_tcp_register_config_sk
    ) REFERENCES public.h_io_modbus_tcp_register_configs (
        h_io_modbus_tcp_register_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_io_modbus_tcp_register_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_io_modbus_tcp_register_config_sk
);

-- ==============================
-- Link: IO Modbus TCP Register Configs - Write Property ID
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_modbus_tcp_register_configs_write_property_ids (
    l_io_modbus_tcp_register_configs_write_property_ids_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_modbus_tcp_register_config_sk uuid NOT NULL,
    h_write_property_id_sk uuid NOT NULL,
    CONSTRAINT l_io_modbus_tcp_register_configs_write_property_ids_pk PRIMARY KEY (
        l_io_modbus_tcp_register_configs_write_property_ids_sk
    ),
    CONSTRAINT l_io_modbus_tcp_register_configs_write_property_ids_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_modbus_tcp_register_configs_write_property_ids_h_io_modbus_tcp_register_config_fk FOREIGN KEY (
        h_io_modbus_tcp_register_config_sk
    ) REFERENCES public.h_io_modbus_tcp_register_configs (
        h_io_modbus_tcp_register_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_modbus_tcp_register_configs_write_property_ids_h_write_property_id_fk FOREIGN KEY (h_write_property_id_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_modbus_tcp_register_configs_write_property_ids_sk
);

-- ==============================
-- Link: IO Modbus TCP Register Configs - Object Properties
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_modbus_tcp_register_configs_object_properties (
    l_io_modbus_tcp_register_configs_object_properties_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_modbus_tcp_register_config_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_io_modbus_tcp_register_configs_object_properties_pk PRIMARY KEY (
        l_io_modbus_tcp_register_configs_object_properties_sk
    ),
    CONSTRAINT l_io_modbus_tcp_register_configs_object_properties_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_modbus_tcp_register_configs_object_properties_h_io_modbus_tcp_register_config_fk FOREIGN KEY (
        h_io_modbus_tcp_register_config_sk
    ) REFERENCES public.h_io_modbus_tcp_register_configs (
        h_io_modbus_tcp_register_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_modbus_tcp_register_configs_object_properties_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_modbus_tcp_register_configs_object_properties_sk
);

-- ==============================
-- Link: IO Modbus TCP Register Configs - IO Device Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_modbus_tcp_register_configs_io_device_configs (
    l_io_modbus_tcp_register_configs_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_modbus_tcp_register_config_sk uuid NOT NULL,
    h_io_device_config_sk uuid NOT NULL,
    CONSTRAINT l_io_modbus_tcp_register_configs_io_device_configs_pk PRIMARY KEY (
        l_io_modbus_tcp_register_configs_io_device_configs_sk
    ),
    CONSTRAINT l_io_modbus_tcp_register_configs_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_modbus_tcp_register_configs_io_device_configs_h_io_modbus_tcp_register_config_fk FOREIGN KEY (
        h_io_modbus_tcp_register_config_sk
    ) REFERENCES public.h_io_modbus_tcp_register_configs (
        h_io_modbus_tcp_register_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_modbus_tcp_register_configs_io_device_configs_h_io_device_config_fk FOREIGN KEY (h_io_device_config_sk) REFERENCES public.h_io_device_configs (h_io_device_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_modbus_tcp_register_configs_io_device_configs_sk
);

-- ==============================
-- Hub: IO Modbus TCP Bit Decompression Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_io_modbus_tcp_bit_decompression_configs (
    h_io_modbus_tcp_bit_decompression_config_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_io_modbus_tcp_bit_decompression_configs_pk PRIMARY KEY (
        h_io_modbus_tcp_bit_decompression_config_sk
    ),
    CONSTRAINT h_io_modbus_tcp_bit_decompression_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_io_modbus_tcp_bit_decompression_config_sk
);

-- ==============================
-- Link: IO Modbus TCP Register Configs - IO Modbus TCP Bit Decompression Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_modbus_tcp_reg_configs_io_modbus_tcp_bit_decomn_configs (
    l_io_modbus_tcp_reg_configs_io_modbus_tcp_bit_decomn_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_modbus_tcp_register_config_sk uuid NOT NULL,
    h_io_modbus_tcp_bit_decompression_config_sk uuid NOT NULL,
    CONSTRAINT l_io_modbus_tcp_reg_configs_io_modbus_tcp_bit_decomn_configs_pk PRIMARY KEY (
        l_io_modbus_tcp_reg_configs_io_modbus_tcp_bit_decomn_configs_sk
    ),
    CONSTRAINT l_io_modbus_tcp_reg_configs_io_modbus_tcp_bit_decomn_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_modbus_tcp_reg_configs_io_modbus_tcp_bit_decomn_configs_h_io_modbus_tcp_register_config_fk FOREIGN KEY (
        h_io_modbus_tcp_register_config_sk
    ) REFERENCES public.h_io_modbus_tcp_register_configs (
        h_io_modbus_tcp_register_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_modbus_tcp_reg_conf_io_modbus_tcp_bit_decomn_configs_h_io_modbus_tcp_bit_decompression_config_fk FOREIGN KEY (
        h_io_modbus_tcp_bit_decompression_config_sk
    ) REFERENCES public.h_io_modbus_tcp_bit_decompression_configs (
        h_io_modbus_tcp_bit_decompression_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_modbus_tcp_reg_configs_io_modbus_tcp_bit_decomn_configs_sk
);

-- ==============================
-- Link: IO Modbus TCP Register Configs - Measure Units
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_modbus_tcp_register_configs_measure_units (
    l_io_modbus_tcp_register_configs_measure_units_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_modbus_tcp_register_config_sk uuid NOT NULL,
    h_measure_unit_sk uuid NOT NULL,
    CONSTRAINT l_io_modbus_tcp_register_configs_measure_units_pk PRIMARY KEY (
        l_io_modbus_tcp_register_configs_measure_units_sk
    ),
    CONSTRAINT l_io_modbus_tcp_register_configs_measure_units_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_modbus_tcp_register_configs_measure_units_h_io_modbus_tcp_register_config_fk FOREIGN KEY (
        h_io_modbus_tcp_register_config_sk
    ) REFERENCES public.h_io_modbus_tcp_register_configs (
        h_io_modbus_tcp_register_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_modbus_tcp_register_configs_measure_units_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_modbus_tcp_register_configs_measure_units_sk
);

-- ==============================
-- Satellite: IO Modbus TCP Bit Decompression Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_modbus_tcp_bit_decompression_configs (
    h_io_modbus_tcp_bit_decompression_config_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    bit_number integer NOT NULL,
    is_write bool NOT NULL,
    mask_inversion bool NOT NULL,
    CONSTRAINT s_io_modbus_tcp_bit_decompression_configs_h_io_modbus_tcp_bit_decompression_config_fk FOREIGN KEY (
        h_io_modbus_tcp_bit_decompression_config_sk
    ) REFERENCES public.h_io_modbus_tcp_bit_decompression_configs (
        h_io_modbus_tcp_bit_decompression_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_io_modbus_tcp_bit_decompression_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_io_modbus_tcp_bit_decompression_config_sk
);

-- ==============================
-- Link: IO Modbus TCP Bit Decompression Configs - Write Property
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_modbus_tcp_bit_decompression_configs_write_properties (
    l_io_modbus_tcp_bit_decompression_configs_write_properties_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_modbus_tcp_bit_decompression_config_sk uuid NOT NULL,
    h_write_property_id_sk uuid NOT NULL,
    CONSTRAINT l_io_modbus_tcp_bit_decompression_configs_write_properties_pk PRIMARY KEY (
        l_io_modbus_tcp_bit_decompression_configs_write_properties_sk
    ),
    CONSTRAINT l_io_modbus_tcp_bit_decompression_configs_write_properties_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_modbus_tcp_bit_decompression_configs_write_properties_h_io_modbus_tcp_bit_decompression_config_fk FOREIGN KEY (
        h_io_modbus_tcp_bit_decompression_config_sk
    ) REFERENCES public.h_io_modbus_tcp_bit_decompression_configs (
        h_io_modbus_tcp_bit_decompression_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_modbus_tcp_bit_decompression_configs_write_properties_h_write_property_id_fk FOREIGN KEY (h_write_property_id_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_modbus_tcp_bit_decompression_configs_write_properties_sk
);

-- ==============================
-- Link: IO Modbus TCP Bit Decompression Configs - Object Properties
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_modbus_tcp_bit_decompression_configs_object_properties (
    l_io_modbus_tcp_bit_decompression_configs_object_properties_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_modbus_tcp_bit_decompression_config_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_io_modbus_tcp_bit_decompression_configs_object_properties_pk PRIMARY KEY (
        l_io_modbus_tcp_bit_decompression_configs_object_properties_sk
    ),
    CONSTRAINT l_io_modbus_tcp_bit_decompression_configs_object_properties_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_modbus_tcp_bit_decompression_configs_object_properties_h_io_modbus_tcp_bit_decompression_config_fk FOREIGN KEY (
        h_io_modbus_tcp_bit_decompression_config_sk
    ) REFERENCES public.h_io_modbus_tcp_bit_decompression_configs (
        h_io_modbus_tcp_bit_decompression_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_modbus_tcp_bit_decompression_configs_object_properties_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_modbus_tcp_bit_decompression_configs_object_properties_sk
);

-- ==============================
-- Link: IO Modbus TCP Bit Decompression Configs - IO Device Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_modbus_tcp_bit_decompression_configs_io_modbus_tcp_register_configs (
    l_io_modbus_tcp_bit_decompression_configs_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_modbus_tcp_bit_decompression_config_sk uuid NOT NULL,
    h_io_modbus_tcp_register_config_sk uuid NOT NULL,
    CONSTRAINT l_io_modbus_tcp_bit_decompression_configs_io_device_configs_pk PRIMARY KEY (
        l_io_modbus_tcp_bit_decompression_configs_io_device_configs_sk
    ),
    CONSTRAINT l_io_modbus_tcp_bit_decompression_configs_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_modbus_tcp_bit_decompression_configs_io_deviceconfigs_h_io_modbus_tcp_bit_decompression_config_fk FOREIGN KEY (
        h_io_modbus_tcp_bit_decompression_config_sk
    ) REFERENCES public.h_io_modbus_tcp_bit_decompression_configs (
        h_io_modbus_tcp_bit_decompression_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_modbus_tcp_bit_decompression_configs_h_io_modbus_tcp_register_config_fk FOREIGN KEY (
        h_io_modbus_tcp_register_config_sk
    ) REFERENCES public.h_io_modbus_tcp_register_configs (
        h_io_modbus_tcp_register_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_modbus_tcp_bit_decompression_configs_io_device_configs_sk
);

-- ==============================
-- Link: IO Modbus TCP Register Configs - Register Type Enum
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_modbus_tcp_register_configs_register_types (
    l_io_modbus_tcp_register_configs_register_types_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_modbus_tcp_register_config_sk uuid NOT NULL,
    h_register_type_sk uuid NOT NULL,
    CONSTRAINT l_io_modbus_tcp_register_configs_register_types_pk PRIMARY KEY (
        l_io_modbus_tcp_register_configs_register_types_sk
    ),
    CONSTRAINT l_io_modbus_tcp_register_configs_register_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_modbus_tcp_register_configs_register_types_h_io_modbus_tcp_register_config_fk FOREIGN KEY (
        h_io_modbus_tcp_register_config_sk
    ) REFERENCES public.h_io_modbus_tcp_register_configs (
        h_io_modbus_tcp_register_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_modbus_tcp_register_configs_register_types_h_register_type_fk FOREIGN KEY (h_register_type_sk) REFERENCES public.h_register_types (h_register_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_modbus_tcp_register_configs_register_types_sk
);

-- ==============================
-- Link: IO Modbus TCP Register Configs - Type Name
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_modbus_tcp_register_configs_data_type_names (
    l_io_modbus_tcp_register_configs_data_type_names_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_modbus_tcp_register_config_sk uuid NOT NULL,
    h_data_type_name_sk uuid NOT NULL,
    CONSTRAINT l_io_modbus_tcp_register_configs_data_type_names_pk PRIMARY KEY (
        l_io_modbus_tcp_register_configs_data_type_names_sk
    ),
    CONSTRAINT l_io_modbus_tcp_register_configs_data_type_names_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_modbus_tcp_register_configs_data_type_names_h_io_modbus_tcp_register_config_fk FOREIGN KEY (
        h_io_modbus_tcp_register_config_sk
    ) REFERENCES public.h_io_modbus_tcp_register_configs (
        h_io_modbus_tcp_register_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_modbus_tcp_register_configs_data_type_names_h_data_type_name_fk FOREIGN KEY (h_data_type_name_sk) REFERENCES public.h_data_type_names (h_data_type_name_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_modbus_tcp_register_configs_data_type_names_sk
);

-- ==============================
-- Hub: IO Modbus TCP Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_io_modbus_tcp_configs (
    h_io_modbus_tcp_config_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_io_modbus_tcp_configs_pk PRIMARY KEY (h_io_modbus_tcp_config_sk),
    CONSTRAINT h_io_modbus_tcp_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_modbus_tcp_config_sk);

-- ==============================
-- Satellite: IO Modbus TCP Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_modbus_tcp_configs (
    h_io_modbus_tcp_config_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    device_id integer NOT NULL,
    primary_ip text NOT NULL,
    secondary_ip text NOT NULL,
    primary_port integer NOT NULL,
    secondary_port integer NOT NULL,
    scan_time integer NOT NULL,
    coil_status_max integer NOT NULL,
    input_status_max integer NOT NULL,
    holding_register_max integer NOT NULL,
    input_register_max integer NOT NULL,
    connected_retries integer NOT NULL,
    secondary_enabled bool NOT NULL,
    connect_to_primary_available bool,
    sync_read_group_name text NOT NULL,
    serial_port_name_primary text NOT NULL,
    serial_port_name_secondary text NOT NULL,
    serial_baud_rate integer NOT NULL,
    serial_parity integer NOT NULL,
    serial_bit_count integer NOT NULL,
    serial_stop_bits integer NOT NULL,
    log_level integer NOT NULL,
    receive_timeout integer NOT NULL,
    write_timeout integer NOT NULL,
    period_write integer NOT NULL,
    CONSTRAINT s_io_modbus_tcp_configs_h_io_modbus_tcp_config_fk FOREIGN KEY (h_io_modbus_tcp_config_sk) REFERENCES public.h_io_modbus_tcp_configs (h_io_modbus_tcp_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_io_modbus_tcp_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_modbus_tcp_config_sk);

-- ==============================
-- Link: IO Modbus TCP Configs - IO Device Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_modbus_tcp_configs_io_device_configs (
    l_io_modbus_tcp_configs_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_modbus_tcp_config_sk uuid NOT NULL,
    h_io_device_config_sk uuid NOT NULL,
    CONSTRAINT l_io_modbus_tcp_configs_io_device_configs_pk PRIMARY KEY (
        l_io_modbus_tcp_configs_io_device_configs_sk
    ),
    CONSTRAINT l_io_modbus_tcp_configs_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_modbus_tcp_configs_io_device_configs_h_io_modbus_tcp_config_fk FOREIGN KEY (h_io_modbus_tcp_config_sk) REFERENCES public.h_io_modbus_tcp_configs (h_io_modbus_tcp_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_modbus_tcp_configs_io_device_configs_h_io_device_config_fk FOREIGN KEY (h_io_device_config_sk) REFERENCES public.h_io_device_configs (h_io_device_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_modbus_tcp_configs_io_device_configs_sk
);

-- ==============================
-- Link: IO Modbus TCP Configs - Device Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_modbus_tcp_configs_device_types (
    l_io_modbus_tcp_configs_device_types_sk uuid DEFAULT uuid_generate_v4() NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_modbus_tcp_config_sk uuid NOT NULL,
    h_device_type_sk uuid NOT NULL,
    CONSTRAINT l_io_modbus_tcp_configs_device_types_pk PRIMARY KEY (l_io_modbus_tcp_configs_device_types_sk),
    CONSTRAINT l_io_modbus_tcp_configs_device_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_modbus_tcp_configs_device_types_h_io_modbus_tcp_config_fk FOREIGN KEY (h_io_modbus_tcp_config_sk) REFERENCES public.h_io_modbus_tcp_configs (h_io_modbus_tcp_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_modbus_tcp_configs_device_types_h_device_type_fk FOREIGN KEY (h_device_type_sk) REFERENCES public.h_device_types (h_device_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (l_io_modbus_tcp_configs_device_types_sk);

-- ==============================
-- Link: IO Modbus TCP Configs - Interface Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_modbus_tcp_configs_interface_types (
    l_io_modbus_tcp_configs_interface_types_sk uuid DEFAULT uuid_generate_v4() NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_modbus_tcp_config_sk uuid NOT NULL,
    h_interface_type_sk uuid NOT NULL,
    CONSTRAINT l_io_modbus_tcp_configs_interface_types_pk PRIMARY KEY (l_io_modbus_tcp_configs_interface_types_sk),
    CONSTRAINT l_io_modbus_tcp_configs_interface_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_modbus_tcp_configs_interface_types_h_io_modbus_tcp_config_fk FOREIGN KEY (h_io_modbus_tcp_config_sk) REFERENCES public.h_io_modbus_tcp_configs (h_io_modbus_tcp_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_modbus_tcp_configs_interface_types_h_interface_type_fk FOREIGN KEY (h_interface_type_sk) REFERENCES public.h_interface_types (h_interface_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (l_io_modbus_tcp_configs_interface_types_sk);



-- ==============================
-- Hub: IO OPC DA Client Item Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_io_opc_da_client_item_configs (
    h_io_opc_da_client_item_config_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_io_opc_da_client_item_configs_pk PRIMARY KEY (
        h_io_opc_da_client_item_config_sk
    ),
    CONSTRAINT h_io_opc_da_client_item_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_io_opc_da_client_item_config_sk
);

-- ==============================
-- Satellite: IO OPC DA Client Item Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_opc_da_client_item_configs (
    h_io_opc_da_client_item_config_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    item_id text NOT NULL,
    group_name text NOT NULL,
    is_active bool NOT NULL,
    CONSTRAINT s_io_opc_da_client_item_configs_h_io_opc_da_client_item_config_fk FOREIGN KEY (
        h_io_opc_da_client_item_config_sk
    ) REFERENCES public.h_io_opc_da_client_item_configs (
        h_io_opc_da_client_item_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_io_opc_da_client_item_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_io_opc_da_client_item_config_sk
);

-- ==============================
-- Link: IO OPC DA Client Item Configs - Object Properties
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_opc_da_client_item_configs_object_properties (
    l_io_opc_da_client_item_configs_object_properties_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_opc_da_client_item_config_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_io_opc_da_client_item_configs_object_properties_pk PRIMARY KEY (
        l_io_opc_da_client_item_configs_object_properties_sk
    ),
    CONSTRAINT l_io_opc_da_client_item_configs_object_properties_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_opc_da_client_item_configs_object_properties_h_io_opc_da_client_item_config_fk FOREIGN KEY (
        h_io_opc_da_client_item_config_sk
    ) REFERENCES public.h_io_opc_da_client_item_configs (
        h_io_opc_da_client_item_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_opc_da_client_item_configs_object_properties_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_opc_da_client_item_configs_object_properties_sk
);

-- ==============================
-- Link: IO OPC DA Client Item Configs - IO Device Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_opc_da_client_item_configs_io_device_configs (
    l_io_opc_da_client_item_configs_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_opc_da_client_item_config_sk uuid NOT NULL,
    h_io_device_config_sk uuid NOT NULL,
    CONSTRAINT l_io_opc_da_client_item_configs_io_device_configs_pk PRIMARY KEY (
        l_io_opc_da_client_item_configs_io_device_configs_sk
    ),
    CONSTRAINT l_io_opc_da_client_item_configs_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_opc_da_client_item_configs_io_device_configs_h_io_opc_da_client_item_config_fk FOREIGN KEY (
        h_io_opc_da_client_item_config_sk
    ) REFERENCES public.h_io_opc_da_client_item_configs (
        h_io_opc_da_client_item_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_opc_da_client_item_configs_io_device_configs_h_io_device_config_fk FOREIGN KEY (h_io_device_config_sk) REFERENCES public.h_io_device_configs (h_io_device_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_opc_da_client_item_configs_io_device_configs_sk
);

-- ==============================
-- Link: IO OPC DA Client Item Configs - Data Type OPC DA
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_opc_da_client_item_configs_opc_da_data_types (
    l_io_opc_da_client_item_configs_opc_da_data_types_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_opc_da_client_item_config_sk uuid NOT NULL,
    h_opc_ua_data_type_sk uuid NOT NULL,
    CONSTRAINT l_io_opc_da_client_item_configs_opc_da_data_types_pk PRIMARY KEY (
        l_io_opc_da_client_item_configs_opc_da_data_types_sk
    ),
    CONSTRAINT l_io_opc_da_client_item_configs_opc_da_data_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_opc_da_client_item_configs_opc_da_data_types_h_io_opc_da_client_item_config_fk FOREIGN KEY (
        h_io_opc_da_client_item_config_sk
    ) REFERENCES public.h_io_opc_da_client_item_configs (
        h_io_opc_da_client_item_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_opc_da_client_item_configs_opc_da_data_types_h_data_type_fk FOREIGN KEY (h_opc_ua_data_type_sk) REFERENCES public.h_opc_ua_data_types (h_opc_ua_data_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_opc_da_client_item_configs_opc_da_data_types_sk
);

-- ==============================
-- Hub: IO OPC DA Client Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_io_opc_da_client_configs (
    h_io_opc_da_client_config_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_io_opc_da_client_configs_pk PRIMARY KEY (h_io_opc_da_client_config_sk),
    CONSTRAINT h_io_opc_da_client_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_opc_da_client_config_sk);

-- ==============================
-- Satellite: IO OPC DA Client Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_opc_da_client_configs (
    h_io_opc_da_client_config_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    ip text NOT NULL,
    server_name text NOT NULL,
    connect_retries integer NOT NULL,
    CONSTRAINT s_io_opc_da_client_configs_h_io_opc_da_client_config_fk FOREIGN KEY (h_io_opc_da_client_config_sk) REFERENCES public.h_io_opc_da_client_configs (h_io_opc_da_client_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_io_opc_da_client_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_opc_da_client_config_sk);

-- ==============================
-- Link: IO OPC DA Client Configs - IO Device Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_opc_da_client_configs_io_device_configs (
    l_io_opc_da_client_configs_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_opc_da_client_config_sk uuid NOT NULL,
    h_io_device_config_sk uuid NOT NULL,
    CONSTRAINT l_io_opc_da_client_configs_io_device_configs_pk PRIMARY KEY (
        l_io_opc_da_client_configs_io_device_configs_sk
    ),
    CONSTRAINT l_io_opc_da_client_configs_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_opc_da_client_configs_io_device_configs_h_io_opc_da_client_config_fk FOREIGN KEY (h_io_opc_da_client_config_sk) REFERENCES public.h_io_opc_da_client_configs (h_io_opc_da_client_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_opc_da_client_configs_io_device_configs_h_io_device_config_fk FOREIGN KEY (h_io_device_config_sk) REFERENCES public.h_io_device_configs (h_io_device_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_opc_da_client_configs_io_device_configs_sk
);

-- ==============================
-- Hub: IO OPC DA Client Group Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_io_opc_da_client_group_configs (
    h_io_opc_da_client_group_config_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_io_opc_da_client_group_configs_pk PRIMARY KEY (
        h_io_opc_da_client_group_config_sk
    ),
    CONSTRAINT h_io_opc_da_client_group_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_io_opc_da_client_group_config_sk
);

-- ==============================
-- Satellite: IO OPC DA Client Group Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_opc_da_client_group_configs (
    h_io_opc_da_client_group_config_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    name text NOT NULL,
    is_active bool NOT NULL,
    update_rate integer NOT NULL,
    CONSTRAINT s_io_opc_da_client_group_configs_h_io_opc_da_client_group_config_fk FOREIGN KEY (
        h_io_opc_da_client_group_config_sk
    ) REFERENCES public.h_io_opc_da_client_group_configs (
        h_io_opc_da_client_group_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_io_opc_da_client_group_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_io_opc_da_client_group_config_sk
);

-- ==============================
-- Link: IO OPC DA Client Group Configs - IO Device Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_opc_da_client_group_configs_io_device_configs (
    l_io_opc_da_client_group_configs_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_opc_da_client_group_config_sk uuid NOT NULL,
    h_io_device_config_sk uuid NOT NULL,
    CONSTRAINT l_io_opc_da_client_group_configs_io_device_configs_pk PRIMARY KEY (
        l_io_opc_da_client_group_configs_io_device_configs_sk
    ),
    CONSTRAINT l_io_opc_da_client_group_configs_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_opc_da_client_group_configs_io_device_configs_h_io_opc_da_client_group_config_fk FOREIGN KEY (
        h_io_opc_da_client_group_config_sk
    ) REFERENCES public.h_io_opc_da_client_group_configs (
        h_io_opc_da_client_group_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_opc_da_client_group_configs_io_device_configs_h_io_device_config_fk FOREIGN KEY (h_io_device_config_sk) REFERENCES public.h_io_device_configs (h_io_device_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_opc_da_client_group_configs_io_device_configs_sk
);

-- ==============================
-- Hub: IO OPC UA Client Transform Item Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_io_opc_ua_client_transform_item_configs (
    h_io_opc_ua_client_transform_item_config_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_io_opc_ua_client_transform_item_configs_pk PRIMARY KEY (
        h_io_opc_ua_client_transform_item_config_sk
    ),
    CONSTRAINT h_io_opc_ua_client_transform_item_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_io_opc_ua_client_transform_item_config_sk
);

-- ==============================
-- Satellite: IO OPC UA Client Transform Item Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_opc_ua_client_transform_item_configs (
    h_io_opc_ua_client_transform_item_config_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    full_path_name text NOT NULL,
    sample_item_id text NOT NULL,
    min_raw real NOT NULL,
    max_raw real NOT NULL,
    min_eu real NOT NULL,
    max_eu real NOT NULL,
    is_scale bool NOT NULL,
    factor real NOT NULL,
    CONSTRAINT s_io_opc_ua_client_transform_item_configs_h_io_opc_ua_client_transform_item_config_fk FOREIGN KEY (
        h_io_opc_ua_client_transform_item_config_sk
    ) REFERENCES public.h_io_opc_ua_client_transform_item_configs (
        h_io_opc_ua_client_transform_item_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_io_opc_ua_client_transform_item_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_io_opc_ua_client_transform_item_config_sk
);

-- ==============================
-- Link: IO OPC UA Client Transform Item Configs - Object Properties
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_opc_ua_client_transform_item_configs_object_properties (
    l_io_opc_ua_client_transform_item_configs_object_properties_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_opc_ua_client_transform_item_config_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_io_opc_ua_client_transform_item_configs_object_properties_pk PRIMARY KEY (
        l_io_opc_ua_client_transform_item_configs_object_properties_sk
    ),
    CONSTRAINT l_io_opc_ua_client_transform_item_configs_object_properties_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_opc_ua_client_transform_item_configs_object_properties_h_io_opc_ua_client_transform_item_config_fk FOREIGN KEY (
        h_io_opc_ua_client_transform_item_config_sk
    ) REFERENCES public.h_io_opc_ua_client_transform_item_configs (
        h_io_opc_ua_client_transform_item_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_opc_ua_client_transform_item_configs_object_properties_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_opc_ua_client_transform_item_configs_object_properties_sk
);

-- ==============================
-- Link: IO OPC UA Client Transform Item Configs - IO Device Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_opc_ua_client_transform_item_configs_io_device_configs (
    l_io_opc_ua_client_transform_item_configs_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_opc_ua_client_transform_item_config_sk uuid NOT NULL,
    h_io_device_config_sk uuid NOT NULL,
    CONSTRAINT l_io_opc_ua_client_transform_item_configs_io_device_configs_pk PRIMARY KEY (
        l_io_opc_ua_client_transform_item_configs_io_device_configs_sk
    ),
    CONSTRAINT l_io_opc_ua_client_transform_item_configs_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_opc_ua_client_transform_item_configs_io_device_configs_h_io_opc_ua_client_transform_item_config_fk FOREIGN KEY (
        h_io_opc_ua_client_transform_item_config_sk
    ) REFERENCES public.h_io_opc_ua_client_transform_item_configs (
        h_io_opc_ua_client_transform_item_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_opc_ua_client_transform_item_configs_io_deviceconfigs_h_io_device_config_fk FOREIGN KEY (h_io_device_config_sk) REFERENCES public.h_io_device_configs (h_io_device_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_opc_ua_client_transform_item_configs_io_device_configs_sk
);

-- ==============================
-- Hub: IO OPC UA Client Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_io_opc_ua_client_configs (
    h_io_opc_ua_client_config_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_io_opc_ua_client_configs_pk PRIMARY KEY (h_io_opc_ua_client_config_sk),
    CONSTRAINT h_io_opc_ua_client_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_opc_ua_client_config_sk);

-- ==============================
-- Satellite: IO OPC UA Client Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_opc_ua_client_configs (
    h_io_opc_ua_client_config_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    ip text NOT NULL,
    server_name text NOT NULL,
    connect_retries integer NOT NULL,
    authentication_id integer NOT NULL,
    user_name text,
    password text,
    certificate_file_path text,
    private_key_file_path text,
    num_block_samples integer NOT NULL,
    num_group_sample integer NOT NULL,
    CONSTRAINT s_io_opc_ua_client_configs_h_io_opc_ua_client_config_fk FOREIGN KEY (h_io_opc_ua_client_config_sk) REFERENCES public.h_io_opc_ua_client_configs (h_io_opc_ua_client_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_io_opc_ua_client_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_opc_ua_client_config_sk);

-- ==============================
-- Link: IO OPC UA Client Configs - IO Device Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_opc_ua_client_configs_io_device_configs (
    l_io_opc_ua_client_configs_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_opc_ua_client_config_sk uuid NOT NULL,
    h_io_device_config_sk uuid NOT NULL,
    CONSTRAINT l_io_opc_ua_client_configs_io_device_configs_pk PRIMARY KEY (
        l_io_opc_ua_client_configs_io_device_configs_sk
    ),
    CONSTRAINT l_io_opc_ua_client_configs_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_opc_ua_client_configs_io_device_configs_h_io_opc_ua_client_config_fk FOREIGN KEY (h_io_opc_ua_client_config_sk) REFERENCES public.h_io_opc_ua_client_configs (h_io_opc_ua_client_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_opc_ua_client_configs_io_device_configs_h_io_device_config_fk FOREIGN KEY (h_io_device_config_sk) REFERENCES public.h_io_device_configs (h_io_device_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_opc_ua_client_configs_io_device_configs_sk
);

-- ==============================
-- Hub: IO OPC UA Client Group Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_io_opc_ua_client_group_configs (
    h_io_opc_ua_client_group_config_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_io_opc_ua_client_group_configs_pk PRIMARY KEY (
        h_io_opc_ua_client_group_config_sk
    ),
    CONSTRAINT h_io_opc_ua_client_group_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_io_opc_ua_client_group_config_sk
);

-- ==============================
-- Satellite: IO OPC UA Client Group Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_opc_ua_client_group_configs (
    h_io_opc_ua_client_group_config_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    name text NOT NULL,
    is_active bool NOT NULL,
    update_rate integer NOT NULL,
    is_subscription bool NOT NULL,
    CONSTRAINT s_io_opc_ua_client_group_configs_h_io_opc_ua_client_group_config_fk FOREIGN KEY (
        h_io_opc_ua_client_group_config_sk
    ) REFERENCES public.h_io_opc_ua_client_group_configs (
        h_io_opc_ua_client_group_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_io_opc_ua_client_group_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_io_opc_ua_client_group_config_sk
);

-- ==============================
-- Link: IO OPC UA Client Group Configs - IO Device Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_opc_ua_client_group_configs_io_device_configs (
    l_io_opc_ua_client_group_configs_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_opc_ua_client_group_config_sk uuid NOT NULL,
    h_io_device_config_sk uuid NOT NULL,
    CONSTRAINT l_io_opc_ua_client_group_configs_io_device_configs_pk PRIMARY KEY (
        l_io_opc_ua_client_group_configs_io_device_configs_sk
    ),
    CONSTRAINT l_io_opc_ua_client_group_configs_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_opc_ua_client_group_configs_io_device_configs_h_io_opc_ua_client_group_config_fk FOREIGN KEY (
        h_io_opc_ua_client_group_config_sk
    ) REFERENCES public.h_io_opc_ua_client_group_configs (
        h_io_opc_ua_client_group_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_opc_ua_client_group_configs_io_device_configs_h_io_device_config_fk FOREIGN KEY (h_io_device_config_sk) REFERENCES public.h_io_device_configs (h_io_device_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_opc_ua_client_group_configs_io_device_configs_sk
);

-- ==============================
-- Hub: IO OPC UA Client Item Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_io_opc_ua_client_item_configs (
    h_io_opc_ua_client_item_config_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    CONSTRAINT h_io_opc_ua_client_item_configs_pk PRIMARY KEY (
        h_io_opc_ua_client_item_config_sk
    ),
    CONSTRAINT h_io_opc_ua_client_item_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_io_opc_ua_client_item_config_sk
);

-- ==============================
-- Satellite: IO OPC UA Client Item Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_opc_ua_client_item_configs (
    h_io_opc_ua_client_item_config_sk uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    item_id text NOT NULL,
    group_name text NOT NULL,
    is_active bool NOT NULL,
    full_path_name text,
    to_server bool NOT NULL,
    CONSTRAINT s_io_opc_ua_client_item_configs_h_io_opc_ua_client_item_config_fk FOREIGN KEY (
        h_io_opc_ua_client_item_config_sk
    ) REFERENCES public.h_io_opc_ua_client_item_configs (
        h_io_opc_ua_client_item_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_io_opc_ua_client_item_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_io_opc_ua_client_item_config_sk
);

-- ==============================
-- Link: IO OPC UA Client Item Configs - Object Properties
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_opc_ua_client_item_configs_object_properties (
    l_io_opc_ua_client_item_configs_object_properties_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_opc_ua_client_item_config_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_io_opc_ua_client_item_configs_object_properties_pk PRIMARY KEY (
        l_io_opc_ua_client_item_configs_object_properties_sk
    ),
    CONSTRAINT l_io_opc_ua_client_item_configs_object_properties_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_opc_ua_client_item_configs_object_properties_h_io_opc_ua_client_item_config_fk FOREIGN KEY (
        h_io_opc_ua_client_item_config_sk
    ) REFERENCES public.h_io_opc_ua_client_item_configs (
        h_io_opc_ua_client_item_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_opc_ua_client_item_configs_object_properties_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_opc_ua_client_item_configs_object_properties_sk
);

-- ==============================
-- Link: IO OPC UA Client Item Configs - IO Device Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_opc_ua_client_item_configs_io_device_configs (
    l_io_opc_ua_client_item_configs_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_opc_ua_client_item_config_sk uuid NOT NULL,
    h_io_device_config_sk uuid NOT NULL,
    CONSTRAINT l_io_opc_ua_client_item_configs_io_device_configs_pk PRIMARY KEY (
        l_io_opc_ua_client_item_configs_io_device_configs_sk
    ),
    CONSTRAINT l_io_opc_ua_client_item_configs_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_opc_ua_client_item_configs_io_device_configs_h_io_opc_ua_client_item_config_fk FOREIGN KEY (
        h_io_opc_ua_client_item_config_sk
    ) REFERENCES public.h_io_opc_ua_client_item_configs (
        h_io_opc_ua_client_item_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_opc_ua_client_item_configs_io_device_configs_h_io_device_config_fk FOREIGN KEY (h_io_device_config_sk) REFERENCES public.h_io_device_configs (h_io_device_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_opc_ua_client_item_configs_io_device_configs_sk
);

-- ==============================
-- Link: IO OPC UA Client Item Configs - Data Type Server
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_opc_ua_client_item_configs_data_type_servers (
    l_io_opc_ua_client_item_configs_data_type_server_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_opc_ua_client_item_config_sk uuid NOT NULL,
    h_data_type_server_sk uuid NOT NULL,
    CONSTRAINT l_io_opc_ua_client_item_configs_data_type_server_pk PRIMARY KEY (
        l_io_opc_ua_client_item_configs_data_type_server_sk
    ),
    CONSTRAINT l_io_opc_ua_client_item_configs_data_type_servers_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_opc_ua_client_item_configs_data_type_servers_h_io_opc_ua_client_item_config_fk FOREIGN KEY (
        h_io_opc_ua_client_item_config_sk
    ) REFERENCES public.h_io_opc_ua_client_item_configs (
        h_io_opc_ua_client_item_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_opc_ua_client_item_configs_data_type_servers_h_data_type_servers_fk FOREIGN KEY (h_data_type_server_sk) REFERENCES public.h_data_type_servers (h_data_type_server_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_opc_ua_client_item_configs_data_type_server_sk
);

-- ==============================
-- Link: IO OPC UA Client Item Configs - Data Type OPC UA
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_opc_ua_client_item_configs_opc_ua_data_types (
    l_io_opc_ua_client_item_configs_opc_ua_data_types_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL,
    valid_from_dttm TIMESTAMPTZ NOT NULL,
    valid_to_dttm TIMESTAMPTZ NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    h_io_opc_ua_client_item_config_sk uuid NOT NULL,
    h_opc_ua_data_type_sk uuid NOT NULL,
    CONSTRAINT l_io_opc_ua_client_item_configs_opc_ua_data_types_pk PRIMARY KEY (
        l_io_opc_ua_client_item_configs_opc_ua_data_types_sk
    ),
    CONSTRAINT l_io_opc_ua_client_item_configs_opc_ua_data_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_opc_ua_client_item_configs_opc_ua_data_types_h_io_opc_ua_client_item_config_fk FOREIGN KEY (
        h_io_opc_ua_client_item_config_sk
    ) REFERENCES public.h_io_opc_ua_client_item_configs (
        h_io_opc_ua_client_item_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_opc_ua_client_item_configs_opc_ua_data_types_h_opc_ua_data_type_fk FOREIGN KEY (h_opc_ua_data_type_sk) REFERENCES public.h_opc_ua_data_types (h_opc_ua_data_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_opc_ua_client_item_configs_opc_ua_data_types_sk
);




-- ==============================
-- Staging tables
-- ==============================

-- stg_measure_groups
CREATE TABLE IF NOT EXISTS public.stg_measure_groups (
    id uuid,
    name text,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_aggregate_notification_configs
CREATE TABLE IF NOT EXISTS public.stg_aggregate_notification_configs (
    id uuid,
    config_id uuid,
    email_address uuid,
    aggregate uuid,
    send_is_enabled integer,
    count_repeats integer,
    min_defect_level integer,
    resend_is_enabled integer,
    resend_time_span integer,
    notification_language integer,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_emailaddress_sk uuid NOT NULL,
    emailaddress_sk uuid NOT NULL,
    l_hub_aggregate_sk uuid NOT NULL,
    aggregate_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_aggregates
CREATE TABLE IF NOT EXISTS public.stg_aggregates (
    id uuid,
    aggregatepath text,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_annotation
CREATE TABLE IF NOT EXISTS public.stg_annotation (
    id integer,
    annotationid uuid,
    namegraphics text,
    fullpath text,
    property text,
    selectedinterval integer,
    annotationtype text,
    userlogin text,
    datecreate TIMESTAMP,
    jsondata json,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_bearings
CREATE TABLE IF NOT EXISTS public.stg_bearings (
    bearingid uuid,
    number text,
    outerrace_d numeric,
    innerrace_d numeric,
    rollingelement_d numeric,
    rollingelement_count integer,
    contactangle numeric,
    bpfi numeric,
    bpfo numeric,
    bsf numeric,
    ftf numeric,
    servicelife integer,
    datecreated TIMESTAMP,
    datemodified TIMESTAMP,
    cdbsyncdate TIMESTAMP,
    cdbversion integer,
    cdbid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_diagnostic_alarms_v2
CREATE TABLE IF NOT EXISTS public.stg_diagnostic_alarms (
    diagalarmid uuid,
    propertyid uuid,
    diagid uuid,
    diagtagname text,
    alarmstate integer,
    date TIMESTAMP,
    confirmed bool,
    comment text,
    defectstate integer,
    defectname text,
    defectdetails text,
    recommendation text,
    priority integer,
    groupname text,
    defecttype integer,
    hash_diag_alarm_sk uuid NOT NULL,
    hash_sat_diag_alarm_diff uuid NOT NULL,
    l_diag_alarm_propertyid_sk uuid NOT NULL,
    propertyid_sk uuid NOT NULL,
    l_diag_alarm_alarmstate_sk uuid NOT NULL,
    alarmstate_sk uuid NOT NULL,
    l_diag_alarm_diag_data_sk uuid NOT NULL,
    diag_data_sk uuid NOT NULL,
    hash_diag_data_sk uuid NOT NULL,
    hash_sat_diag_data_diff uuid NOT NULL,
    l_diag_data_defectstate_sk uuid NOT NULL,
    defectstate_sk uuid NOT NULL,
    l_diag_data_defecttype_sk uuid NOT NULL,
    defecttype_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_email_addresses
CREATE TABLE IF NOT EXISTS public.stg_email_addresses (
    id uuid,
    emailaddress text,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_io_creyt_channel_configs
CREATE TABLE IF NOT EXISTS public.stg_io_creyt_channel_configs (
    configid uuid,
    channelnum integer,
    propertyid uuid,
    minraw double precision,
    maxraw double precision,
    mineu double precision,
    maxeu double precision,
    measureunitid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_configid_sk uuid NOT NULL,
    configid_sk uuid NOT NULL,
    l_hub_propertyid_sk uuid NOT NULL,
    propertyid_sk uuid NOT NULL,
    l_hub_measureunitid_sk uuid NOT NULL,
    measureunitid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_io_creyt_channel_configs_v5
CREATE TABLE IF NOT EXISTS public.stg_io_creyt_channel_configs_v5 (
    configid uuid,
    channelnum integer,
    propertyid uuid,
    minraw double precision,
    maxraw double precision,
    mineu double precision,
    maxeu double precision,
    measureunitid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_configid_sk uuid NOT NULL,
    configid_sk uuid NOT NULL,
    l_hub_propertyid_sk uuid NOT NULL,
    propertyid_sk uuid NOT NULL,
    l_hub_measureunitid_sk uuid NOT NULL,
    measureunitid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_io_creyt_channel_states
CREATE TABLE IF NOT EXISTS public.stg_io_creyt_channel_states (
    configid uuid,
    channelnum integer,
    propertyid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_configid_sk uuid NOT NULL,
    configid_sk uuid NOT NULL,
    l_hub_propertyid_sk uuid NOT NULL,
    propertyid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_io_creyt_channel_states_v5
CREATE TABLE IF NOT EXISTS public.stg_io_creyt_channel_states_v5 (
    configid uuid,
    channelnum integer,
    propertyid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_configid_sk uuid NOT NULL,
    configid_sk uuid NOT NULL,
    l_hub_propertyid_sk uuid NOT NULL,
    propertyid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_io_creyt_configs
CREATE TABLE IF NOT EXISTS public.stg_io_creyt_configs (
    configid uuid,
    primaryip text,
    secondaryip text,
    primarymodbusport integer,
    secondarymodbusport integer,
    primaryhttpport integer,
    secondaryhttpport integer,
    scantime integer,
    connectretries integer,
    samplesfilename text,
    issyncreadslave bool,
    syncreadgroupname text,
    detectfailures bool,
    detectfailurelength integer,
    enabledsecondaryip bool,
    numblocksamples integer,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_configid_sk uuid NOT NULL,
    configid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_io_creyt_configs_v5
CREATE TABLE IF NOT EXISTS public.stg_io_creyt_configs_v5 (
    configid uuid,
    primaryip text,
    secondaryip text,
    primarymodbusport integer,
    secondarymodbusport integer,
    primaryhttpport integer,
    secondaryhttpport integer,
    scantime integer,
    connectretries integer,
    samplesfilename text,
    issyncreadslave bool,
    syncreadgroupname text,
    detectfailures bool,
    detectfailurelength integer,
    enabledsecondaryip bool,
    numblocksamples integer,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_configid_sk uuid NOT NULL,
    configid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_io_creyt_controller_states
CREATE TABLE IF NOT EXISTS public.stg_io_creyt_controller_states (
    configid uuid,
    rownum integer,
    opertimepropertyid uuid,
    statepropertyid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_configid_sk uuid NOT NULL,
    configid_sk uuid NOT NULL,
    l_hub_opertimepropertyid_sk uuid NOT NULL,
    opertimepropertyid_sk uuid NOT NULL,
    l_hub_statepropertyid_sk uuid NOT NULL,
    statepropertyid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_io_creyt_controller_states_v5
CREATE TABLE IF NOT EXISTS public.stg_io_creyt_controller_states_v5 (
    configid uuid,
    rownum integer,
    opertimepropertyid uuid,
    statepropertyid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_configid_sk uuid NOT NULL,
    configid_sk uuid NOT NULL,
    l_hub_opertimepropertyid_sk uuid NOT NULL,
    opertimepropertyid_sk uuid NOT NULL,
    l_hub_statepropertyid_sk uuid NOT NULL,
    statepropertyid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_io_creyt_im_oper_times
CREATE TABLE IF NOT EXISTS public.stg_io_creyt_im_oper_times (
    configid uuid,
    propertyid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_configid_sk uuid NOT NULL,
    configid_sk uuid NOT NULL,
    l_hub_propertyid_sk uuid NOT NULL,
    propertyid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_io_creyt_im_oper_times_v5
CREATE TABLE IF NOT EXISTS public.stg_io_creyt_im_oper_times_v5 (
    configid uuid,
    propertyid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_configid_sk uuid NOT NULL,
    configid_sk uuid NOT NULL,
    l_hub_propertyid_sk uuid NOT NULL,
    propertyid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_io_device_config_nodes
CREATE TABLE IF NOT EXISTS public.stg_io_device_config_nodes (
    id uuid,
    name text,
    parentnodeid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_io_device_configs
CREATE TABLE IF NOT EXISTS public.stg_io_device_configs (
    id uuid,
    name text,
    typeid uuid,
    confignodeid uuid,
    enabled bool,
    setnumber integer,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_typeid_sk uuid NOT NULL,
    typeid_sk uuid NOT NULL,
    l_hub_confignodeid_sk uuid NOT NULL,
    confignodeid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_io_l_card_channel_configs
CREATE TABLE IF NOT EXISTS public.stg_io_l_card_channel_configs (
    configid uuid,
    slot integer,
    channelnumber integer,
    propertyid uuid,
    isscaled bool,
    minraw numeric,
    maxraw numeric,
    mineu numeric,
    maxeu numeric,
    measureunitid uuid,
    propertymarkid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_configid_sk uuid NOT NULL,
    configid_sk uuid NOT NULL,
    l_hub_propertyid_sk uuid NOT NULL,
    propertyid_sk uuid NOT NULL,
    l_hub_measureunitid_sk uuid NOT NULL,
    measureunitid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_io_l_card_configs
CREATE TABLE IF NOT EXISTS public.stg_io_l_card_configs (
    configid uuid,
    virtualslot integer,
    samplingrate double precision,
    operationmode integer,
    timersamplingtime integer,
    timerscantime integer,
    syncsamplingtime integer,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_configid_sk uuid NOT NULL,
    configid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_io_l_card_crate_configs
CREATE TABLE IF NOT EXISTS public.stg_io_l_card_crate_configs (
    configid uuid,
    ip text,
    port integer,
    cratetype integer,
    serialnumber text,
    samplingtime integer,
    scantime integer,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_configid_sk uuid NOT NULL,
    configid_sk uuid NOT NULL,
    l_hub_cratetype_sk uuid NOT NULL,
    cratetype_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_io_l_card_crate_sync
CREATE TABLE IF NOT EXISTS public.stg_io_l_card_crate_sync (
    configid uuid,
    issync bool,
    isleader bool,
    isslave bool,
    leaderid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_configid_sk uuid NOT NULL,
    configid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_io_l_card_input_configs
CREATE TABLE IF NOT EXISTS public.stg_io_l_card_input_configs (
    configid uuid,
    inputnum integer,
    propertyid uuid,
    inputrange integer,
    isscale bool,
    minraw double precision,
    maxraw double precision,
    mineu double precision,
    maxeu double precision,
    measureunitid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_configid_sk uuid NOT NULL,
    configid_sk uuid NOT NULL,
    l_hub_propertyid_sk uuid NOT NULL,
    propertyid_sk uuid NOT NULL,
    l_hub_measureunitid_sk uuid NOT NULL,
    measureunitid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_io_l_card_logic_input_configs
CREATE TABLE IF NOT EXISTS public.stg_io_l_card_logic_input_configs (
    configid uuid,
    inputnumber integer,
    inputtype integer,
    timerpropertyid uuid,
    syncpropertyid uuid,
    inputrange integer,
    isscale bool,
    minraw double precision,
    maxraw double precision,
    mineu double precision,
    maxeu double precision,
    measureunitid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_configid_sk uuid NOT NULL,
    configid_sk uuid NOT NULL,
    l_hub_inputtype_sk uuid NOT NULL,
    inputtype_sk uuid NOT NULL,
    l_hub_timerpropertyid_sk uuid NOT NULL,
    timerpropertyid_sk uuid NOT NULL,
    l_hub_syncpropertyid_sk uuid NOT NULL,
    syncpropertyid_sk uuid NOT NULL,
    l_hub_measureunitid_sk uuid NOT NULL,
    measureunitid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_io_l_card_module_configs
CREATE TABLE IF NOT EXISTS public.stg_io_l_card_module_configs (
    configid uuid,
    slot integer,
    moduletype integer,
    frequencydivisor integer,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_configid_sk uuid NOT NULL,
    configid_sk uuid NOT NULL,
    l_hub_moduletype_sk uuid NOT NULL,
    moduletype_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_io_modbus_tcp_configs
CREATE TABLE IF NOT EXISTS public.stg_io_modbus_tcp_configs (
    configid uuid,
    deviceid integer,
    primaryip text,
    secondaryip text,
    primaryport integer,
    secondaryport integer,
    scantime integer,
    coilstatusmax integer,
    inputstatusmax integer,
    holdingregistermax integer,
    inputregistermax integer,
    connectretries integer,
    secondaryenabled bool,
    devicetype integer,
    connecttoptimaryifavailable bool,
    syncreadgroupname text,
    interfacetype integer,
    serial_portname_primary text,
    serial_portname_secondary text,
    serial_baudrate integer,
    serial_parity integer,
    serial_bitcount integer,
    serial_stopbits integer,
    loglevel integer,
    receivetimeout integer,
    writetimeout integer,
    periodwrite integer,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_configid_sk uuid NOT NULL,
    configid_sk uuid NOT NULL,
    l_hub_devicetype_sk uuid NOT NULL,
    devicetype_sk uuid NOT NULL,
    l_hub_interfacetype_sk uuid NOT NULL,
    interfacetype_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_io_modbus_tcp_register_configs
CREATE TABLE IF NOT EXISTS public.stg_io_modbus_tcp_register_configs (
    configid uuid,
    registertype integer,
    registeraddress integer,
    datatype integer,
    byteorder text,
    propertyid uuid,
    measureunitid uuid,
    minraw double precision,
    maxraw double precision,
    mineu double precision,
    maxeu double precision,
    isscale bool,
    factor double precision,
    id uuid,
    iswrite bool,
    writepropertyid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_configid_sk uuid NOT NULL,
    configid_sk uuid NOT NULL,
    l_hub_registertype_sk uuid NOT NULL,
    registertype_sk uuid NOT NULL,
    l_hub_datatype_sk uuid,
    datatype_sk uuid,
    l_hub_propertyid_sk uuid NOT NULL,
    propertyid_sk uuid NOT NULL,
    l_hub_measureunitid_sk uuid NOT NULL,
    measureunitid_sk uuid NOT NULL,
    l_hub_writepropertyid_sk uuid NOT NULL,
    writepropertyid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_io_modbus_tcp_register_configs_tmp
CREATE TABLE IF NOT EXISTS public.stg_io_modbus_tcp_register_configs_tmp (
    configid uuid,
    registertype integer,
    registeraddress integer,
    datatype integer,
    byteorder text,
    propertyid uuid,
    measureunitid uuid,
    minraw double precision,
    maxraw double precision,
    mineu double precision,
    maxeu double precision,
    isscale bool,
    factor double precision,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_configid_sk uuid NOT NULL,
    configid_sk uuid NOT NULL,
    l_hub_registertype_sk uuid NOT NULL,
    registertype_sk uuid NOT NULL,
    l_hub_datatype_sk uuid NOT NULL,
    datatype_sk uuid NOT NULL,
    l_hub_propertyid_sk uuid NOT NULL,
    propertyid_sk uuid NOT NULL,
    l_hub_measureunitid_sk uuid NOT NULL,
    measureunitid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_io_modbus_tcp_register_bit_decompression_configs
CREATE TABLE IF NOT EXISTS public.stg_io_modbus_tcp_register_bit_decompression_configs (
    id uuid,
    registerid uuid,
    propertyid uuid,
    bitnumber integer,
    iswrite bool,
    writepropertyid uuid,
    makeinversion bool,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_registerid_sk uuid NOT NULL,
    registerid_sk uuid NOT NULL,
    l_hub_propertyid_sk uuid NOT NULL,
    propertyid_sk uuid NOT NULL,
    l_hub_writepropertyid_sk uuid NOT NULL,
    writepropertyid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_io_opc_da_client_configs
CREATE TABLE IF NOT EXISTS public.stg_io_opc_da_client_configs (
    configid uuid,
    ip text,
    servername text,
    connectretries integer,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_configid_sk uuid NOT NULL,
    configid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_io_opc_da_client_group_configs
CREATE TABLE IF NOT EXISTS public.stg_io_opc_da_client_group_configs (
    configid uuid,
    name text,
    isactive bool,
    updaterate integer,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_configid_sk uuid NOT NULL,
    configid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_io_opc_da_client_item_configs
CREATE TABLE IF NOT EXISTS public.stg_io_opc_da_client_item_configs (
    configid uuid,
    propertyid uuid,
    itemid text,
    groupname text,
    isactive bool,
    datatype integer,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_configid_sk uuid NOT NULL,
    configid_sk uuid NOT NULL,
    l_hub_propertyid_sk uuid NOT NULL,
    propertyid_sk uuid NOT NULL,
    l_hub_datatype_sk uuid NOT NULL,
    datatype_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_io_opc_ua_client_configs
CREATE TABLE IF NOT EXISTS public.stg_io_opc_ua_client_configs (
    configid uuid,
    ip text,
    servername text,
    connectretries integer,
    authenticationid integer,
    username text,
    password text,
    certificatefilepath text,
    privatekeyfilepath text,
    numblocksamples integer,
    numgroupsample integer,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_configid_sk uuid NOT NULL,
    configid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_io_opc_ua_client_group_configs
CREATE TABLE IF NOT EXISTS public.stg_io_opc_ua_client_group_configs (
    configid uuid,
    name text,
    isactive bool,
    updaterate integer,
    issubscription bool,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_configid_sk uuid NOT NULL,
    configid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_io_opc_ua_client_item_configs
CREATE TABLE IF NOT EXISTS public.stg_io_opc_ua_client_item_configs (
    configid uuid,
    propertyid uuid,
    itemid text,
    groupname text,
    isactive bool,
    datatype integer,
    fullpathname text,
    toserver bool,
    datatypeserver integer,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_configid_sk uuid NOT NULL,
    configid_sk uuid NOT NULL,
    l_hub_propertyid_sk uuid NOT NULL,
    propertyid_sk uuid NOT NULL,
    l_hub_datatype_sk uuid NOT NULL,
    datatype_sk uuid NOT NULL,
    l_hub_datatypeserver_sk uuid NOT NULL,
    datatypeserver_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_io_opc_ua_client_transform_item_configs
CREATE TABLE IF NOT EXISTS public.stg_io_opc_ua_client_transform_item_configs (
    configid uuid,
    propertyid uuid,
    fullpathname text,
    sampleitemid text,
    freqitemid text,
    minraw double precision,
    maxraw double precision,
    mineu double precision,
    maxeu double precision,
    isscale bool,
    factor double precision,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_configid_sk uuid NOT NULL,
    configid_sk uuid NOT NULL,
    l_hub_propertyid_sk uuid NOT NULL,
    propertyid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_images
CREATE TABLE IF NOT EXISTS public.stg_images (
    id uuid,
    imagedata bytea,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_measure_convert
CREATE TABLE IF NOT EXISTS public.stg_measure_convert (
    fromid uuid,
    toid uuid,
    converttype integer,
    factor double precision,
    formula text,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_fromid_sk uuid NOT NULL,
    fromid_sk uuid NOT NULL,
    l_hub_toid_sk uuid NOT NULL,
    toid_sk uuid NOT NULL,
    l_hub_converttype_sk uuid NOT NULL,
    converttype_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_measure_units
CREATE TABLE IF NOT EXISTS public.stg_measure_units (
    id uuid,
    name text,
    abbreviation text,
    measuregroupid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_measuregroupid_sk uuid NOT NULL,
    measuregroupid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_model_template_tree_nodes
CREATE TABLE IF NOT EXISTS public.stg_model_template_tree_nodes (
    id uuid,
    parentid uuid,
    name text,
    tagname text,
    description text,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_model_templates
CREATE TABLE IF NOT EXISTS public.stg_model_templates (
    id uuid,
    ownernodeid uuid,
    name text,
    tagname text,
    description text,
    basetemplatename text,
    datecreated TIMESTAMP,
    datemodified TIMESTAMP,
    cdbsyncdate TIMESTAMP,
    cdbversion integer,
    cdbid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_ownernodeid_sk uuid NOT NULL,
    ownernodeid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_object_data_values_diagnostic_array_live_v2
CREATE TABLE IF NOT EXISTS public.stg_object_data_values_diagnostic_array_live_v2 (
    valueid uuid,
    diagid uuid,
    tagname text,
    defectstate integer,
    defectname text,
    defectdetails text,
    recomendation text,
    priority integer,
    groupname text,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_diagid_sk uuid NOT NULL,
    diagid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_object_data_values_diagnostic_array_v2
CREATE TABLE IF NOT EXISTS public.stg_object_data_values_diagnostic_array_v2 (
    valueid uuid,
    diagid uuid,
    tagname text,
    defectstate integer,
    defectname text,
    defectdetails text,
    recomendation text,
    priority integer,
    groupname text,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_diagid_sk uuid NOT NULL,
    diagid_sk uuid NOT NULL,
    l_hub_defectstate_sk uuid NOT NULL,
    defectstate_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);




-- stg_object_group_descriptors
CREATE TABLE IF NOT EXISTS public.stg_object_group_descriptors (
    groupdescriptorid uuid,
    name text,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_object_groups
CREATE TABLE IF NOT EXISTS public.stg_object_groups (
    id uuid,
    objectid uuid,
    groupdescriptorid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_objectid_sk uuid NOT NULL,
    objectid_sk uuid NOT NULL,
    l_hub_groupdescriptorid_sk uuid NOT NULL,
    groupdescriptorid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_object_properties
CREATE TABLE IF NOT EXISTS public.stg_object_properties (
    propertyid uuid,
    objectid uuid,
    propertydescriptorid uuid,
    isalias bool,
    measureunitid uuid,
    maxrecords integer,
    fromtemplatename text,
    tocopy bool,
    savehistory bool,
    storagetype integer,
    visibleforscada bool,
    isvaluesreplicable bool,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_objectid_sk uuid NOT NULL,
    objectid_sk uuid NOT NULL,
    l_hub_propertydescriptorid_sk uuid NOT NULL,
    propertydescriptorid_sk uuid NOT NULL,
    l_hub_measureunitid_sk uuid NOT NULL,
    measureunitid_sk uuid NOT NULL,
    l_hub_storagetype_sk uuid NOT NULL,
    storagetype_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_object_property_descriptor_nodes
CREATE TABLE IF NOT EXISTS public.stg_object_property_descriptor_nodes (
    id uuid,
    name text,
    description text,
    parentnodeid uuid,
    translateid integer,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_object_property_descriptors
CREATE TABLE IF NOT EXISTS public.stg_object_property_descriptors (
    propertydescriptorid uuid,
    name text,
    description text,
    tag text,
    parentnodeid uuid,
    propertytype integer,
    measureunitid uuid,
    maxrecords integer,
    savehistory bool,
    defaultvalue text,
    visibleforscada bool,
    datecreated TIMESTAMP,
    datemodified TIMESTAMP,
    cdbsyncdate TIMESTAMP,
    cdbversion integer,
    translateid integer,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_parentnodeid_sk uuid NOT NULL,
    parentnodeid_sk uuid NOT NULL,
    l_hub_propertytype_sk uuid NOT NULL,
    propertytype_sk uuid NOT NULL,
    l_hub_measureunitid_sk uuid NOT NULL,
    measureunitid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_object_rules
CREATE TABLE IF NOT EXISTS public.stg_object_rules (
    objectruleid uuid,
    objectid uuid,
    poucallname text,
    comment text,
    enabled bool,
    runlevel integer,
    fromtemplatename text,
    isintemplate bool,
    tag text,
    runningtype integer,
    runningperiod bigint,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_objectid_sk uuid NOT NULL,
    objectid_sk uuid NOT NULL,
    l_hub_runningtype_sk uuid NOT NULL,
    runningtype_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_object_templates
CREATE TABLE IF NOT EXISTS public.stg_object_templates (
    id uuid,
    name text,
    tagname text,
    parentid uuid,
    description text,
    templatename text,
    fromtemplatename text,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_object_type_descriptors
CREATE TABLE IF NOT EXISTS public.stg_object_type_descriptors (
    typedescriptorid uuid,
    name text,
    description text,
    tag text,
    parentid uuid,
    iconid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_iconid_sk uuid NOT NULL,
    iconid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_object_types
CREATE TABLE IF NOT EXISTS public.stg_object_types (
    objectid uuid,
    typedescriptorid uuid,
    fromtemplatename text,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_typedescriptorid_sk uuid NOT NULL,
    typedescriptorid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_objects
CREATE TABLE IF NOT EXISTS public.stg_objects (
    id uuid,
    name text,
    tagname text,
    parentid uuid,
    description text,
    templatename text,
    fromtemplatename text,
    datecreated TIMESTAMP,
    datemodified TIMESTAMP,
    cdbsyncdate TIMESTAMP,
    cdbversion integer,
    cdbid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_pion_route_objects_lists
CREATE TABLE IF NOT EXISTS public.stg_pion_route_objects_lists (
    id uuid,
    name text,
    description text,
    xmlobjectslist text,
    datemodified TIMESTAMP,
    datemodifiedcbd TIMESTAMP,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_pou_user_defined_items
CREATE TABLE IF NOT EXISTS public.stg_pou_user_defined_items (
    callname text,
    bodytype integer,
    name text,
    description text,
    author text,
    categoryid uuid,
    xmlinterface text,
    xmlbody text,
    dateofcreate TIMESTAMP,
    dateofedit TIMESTAMP,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_categoryid_sk uuid NOT NULL,
    categoryid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_pou_user_items_tree
CREATE TABLE IF NOT EXISTS public.stg_pou_user_items_tree (
    id uuid,
    name text,
    description text,
    parentid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_preset_chart_settings
CREATE TABLE IF NOT EXISTS public.stg_preset_chart_settings (
    id uuid,
    userid uuid,
    presetchartsettings text,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_tik_expert_slices
CREATE TABLE IF NOT EXISTS public.stg_tik_expert_slices (
    propertyid uuid,
    slicedate TIMESTAMP,
    numericx double precision,
    comment text,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_propertyid_sk uuid NOT NULL,
    propertyid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_tik_scada_log
CREATE TABLE IF NOT EXISTS public.stg_tik_scada_log (
    id uuid,
    type integer,
    timestamp TIMESTAMP,
    message text,
    acked bool,
    ackedtimestamp TIMESTAMP,
    sourcepath text,
    aggregatepath text,
    participant text,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_type_sk uuid NOT NULL,
    type_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_user_defined_property_lists
CREATE TABLE IF NOT EXISTS public.stg_user_defined_property_lists (
    propertylistid uuid,
    propertylistname text,
    description text,
    xmlpropertieslist text,
    propertylisttypeid uuid,
    objectid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_propertylisttypeid_sk uuid NOT NULL,
    propertylisttypeid_sk uuid NOT NULL,
    l_hub_objectid_sk uuid NOT NULL,
    objectid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_user_defined_property_lists_types
CREATE TABLE IF NOT EXISTS public.stg_user_defined_property_lists_types (
    propertylisttypeid uuid,
    propertylisttypename text,
    description text,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_user_defined_tiles_properties_configs
CREATE TABLE IF NOT EXISTS public.stg_user_defined_tiles_properties_configs (
    id uuid,
    name text,
    tilesproperties text,
    objectid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_objectid_sk uuid NOT NULL,
    objectid_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

-- stg_user_log
CREATE TABLE IF NOT EXISTS public.stg_user_log (
    id uuid,
    userid uuid,
    date TIMESTAMP,
    actiontype integer,
    actiondescription text,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    l_hub_actiontype_sk uuid NOT NULL,
    actiontype_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);


CREATE TABLE IF NOT EXISTS public.stg_main_object_data_values_nonhist (
    id uuid NOT NULL,
    measureunitid uuid NOT NULL,
    propertyid uuid NOT NULL,
    datatypeid uuid NOT NULL,
    date TIMESTAMP NOT NULL,
    value bytea,
    quality integer,
    comment text, 
    hash_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    h_data_type_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);


CREATE TABLE IF NOT EXISTS public.stg_any_object_data_values_hist (
    id uuid NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    value bytea,
    datatypeid uuid NOT NULL,
    hash_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    h_data_type_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);


CREATE TABLE IF NOT EXISTS public.stg_double_object_data_values_hist (
    id uuid NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    value bytea,
    datatypeid uuid NOT NULL,
    hash_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    h_data_type_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS public.stg_fh_double_object_data_values_hist (
    id uuid NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    value bytea,
    datatypeid uuid NOT NULL,
    hash_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    h_data_type_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    data_file_id uuid NOT NULL,
    main_db_id uuid NOT NULL,
    load_dttm TIMESTAMPTZ NOT NULL
);


-- ==============================
-- Data Initialization
-- ==============================

DO $$
DECLARE
    -- Переменные для временных меток и флагов
    load_datetime       TIMESTAMP := NOW();
    valid_from_datetime TIMESTAMP := NOW();
    active_flag_true    BOOL      := TRUE;
    default_source_id UUID      := '00000000-0000-0000-0000-000000000000'::UUID;
    default_file_id   UUID      := '00000000-0000-0000-0000-000000000000'::UUID;
    default_main_db_id   UUID      := '00000000-0000-0000-0000-000000000000'::UUID;

    -- 1. Defect Types
    type_1_uuid UUID;
    type_2_uuid UUID;

    -- 2. Alarm States
    state_0_uuid UUID;
    state_1_uuid UUID;
    state_2_uuid UUID;
    state_3_uuid UUID;

    -- 3. Defect States
    dstate_0_uuid UUID;
    dstate_1_uuid UUID;
    dstate_2_uuid UUID;
    dstate_3_uuid UUID;
    dstate_4_uuid UUID;
    dstate_5_uuid UUID;

    -- 4. Config Types
    cfg_1_uuid  UUID;
    cfg_2_uuid  UUID;
    cfg_3_uuid  UUID;
    cfg_4_uuid  UUID;
    cfg_5_uuid  UUID;
    cfg_6_uuid  UUID;
    cfg_7_uuid  UUID;
    cfg_8_uuid  UUID;
    cfg_9_uuid  UUID;
    cfg_10_uuid UUID;
    cfg_11_uuid UUID;
    cfg_12_uuid UUID;
    cfg_13_uuid UUID;

    -- 5. Index Type Data Records
    idx_0_uuid UUID;
    idx_1_uuid UUID;
    idx_2_uuid UUID;

    -- 6. OPC DA Data Types
    opc_0_uuid  UUID;
    opc_11_uuid UUID;
    opc_3_uuid  UUID;
    opc_20_uuid UUID;
    opc_5_uuid  UUID;
    opc_8_uuid  UUID;
    opc_7_uuid  UUID;

    -- 7. OPC UA Data Types
    ua_0_uuid  UUID;
    ua_11_uuid UUID;
    ua_3_uuid  UUID;
    ua_20_uuid UUID;
    ua_5_uuid  UUID;
    ua_8_uuid  UUID;
    ua_7_uuid  UUID;
    ua_30_uuid UUID;
    ua_31_uuid UUID;
    ua_32_uuid UUID;
    ua_33_uuid UUID;
    ua_34_uuid UUID;
    ua_35_uuid UUID;

    -- 8. Data Type Servers (0-15)
    srv_0_uuid  UUID;
    srv_1_uuid  UUID;
    srv_2_uuid  UUID;
    srv_3_uuid  UUID;
    srv_4_uuid  UUID;
    srv_5_uuid  UUID;
    srv_6_uuid  UUID;
    srv_7_uuid  UUID;
    srv_8_uuid  UUID;
    srv_9_uuid  UUID;
    srv_10_uuid UUID;
    srv_11_uuid UUID;
    srv_12_uuid UUID;
    srv_13_uuid UUID;
    srv_14_uuid UUID;
    srv_15_uuid UUID;

    -- 9. TIK SCADA Log Types (0-8)
    log_0_uuid UUID;
    log_1_uuid UUID;
    log_2_uuid UUID;
    log_3_uuid UUID;
    log_4_uuid UUID;
    log_5_uuid UUID;
    log_6_uuid UUID;
    log_7_uuid UUID;
    log_8_uuid UUID;
    log_9_uuid UUID;
    log_10_uuid UUID;
    log_11_uuid UUID;

    -- 10. User Action Types (0-19)
    act_0_uuid  UUID;
    act_1_uuid  UUID;
    act_2_uuid  UUID;
    act_3_uuid  UUID;
    act_4_uuid  UUID;
    act_5_uuid  UUID;
    act_6_uuid  UUID;
    act_7_uuid  UUID;
    act_8_uuid  UUID;
    act_9_uuid  UUID;
    act_10_uuid UUID;
    act_11_uuid UUID;
    act_12_uuid UUID;
    act_13_uuid UUID;
    act_14_uuid UUID;
    act_15_uuid UUID;
    act_16_uuid UUID;
    act_17_uuid UUID;
    act_18_uuid UUID;
    act_19_uuid UUID;

    -- 11. Running Types
    run_1_uuid UUID;
    run_2_uuid UUID;

    -- 12. Crate Types
    crate_0_uuid UUID;

    -- 13. L Card Logic Input Types
    lcard_in_0_uuid UUID;
    lcard_in_1_uuid UUID;
    lcard_in_2_uuid UUID;
    lcard_in_3_uuid UUID;

    -- 14. L Card Crate Module Types
    lcard_mod_0_uuid UUID;
    lcard_mod_1_uuid UUID;
    lcard_mod_2_uuid UUID;
    lcard_mod_3_uuid UUID;
    lcard_mod_4_uuid UUID;

    -- 15. Register Types
    reg_1_uuid UUID;
    reg_2_uuid UUID;
    reg_3_uuid UUID;
    reg_4_uuid UUID;

    -- 16. Type Names
    tn_0_uuid UUID;
    tn_1_uuid UUID;
    tn_2_uuid UUID;
    tn_3_uuid UUID;
    tn_4_uuid UUID;
    tn_5_uuid UUID;
    tn_6_uuid UUID;
    tn_7_uuid UUID;
    tn_8_uuid UUID;

    -- 17. Data Types (UUID-based IDs)
    dt_1_uuid  UUID;
    dt_2_uuid  UUID;
    dt_3_uuid  UUID;
    dt_4_uuid  UUID;
    dt_5_uuid  UUID;
    dt_6_uuid  UUID;
    dt_7_uuid  UUID;
    dt_8_uuid  UUID;
    dt_9_uuid  UUID;
    dt_10_uuid UUID;
    dt_11_uuid UUID;
    dt_12_uuid UUID;
    dt_13_uuid UUID;
    dt_14_uuid UUID;
    dt_15_uuid UUID;
    dt_16_uuid UUID;
    dt_17_uuid UUID;
    dt_18_uuid UUID;
    dt_19_uuid UUID;
    dt_20_uuid UUID;
    dt_21_uuid UUID;

    id_1  TEXT := '37beb486-d0dc-4c9d-9e50-419632f63616';
    id_2  TEXT := 'cff1de41-ed0a-4fe7-a003-2b9107d44f0e';
    id_3  TEXT := '20da2798-2340-45e9-8b75-29cfe4f99901';
    id_4  TEXT := '4fc8e8d4-cfdb-46db-9e5f-41cdb9f7ef33';
    id_5  TEXT := '34a809aa-9c72-4baa-b892-00d4cba7d7a4';
    id_6  TEXT := 'c0be4ae3-3a1b-4a63-9fcc-04dbdde6a30c';
    id_7  TEXT := 'e940b7ee-aba1-4447-b940-1686bb70551b';
    id_8  TEXT := '969420b8-fd94-4476-b06e-ed7408d42a61';
    id_9  TEXT := '0232db10-59e8-4840-bbe6-e8749eae690f';
    id_10 TEXT := '962df766-9d5e-44ad-be1b-1768d3b90d25';
    id_11 TEXT := '7cf98a0e-e55b-4d00-bd15-479cf05a5ddd';
    id_12 TEXT := '6bc397f1-26ac-44b5-9f55-bbaf84475285';
    id_13 TEXT := '8796c1a5-542e-4e8b-beb6-0b8b18ac0a18';
    id_14 TEXT := '86da3e91-6e11-445f-afdf-72e25bcafb0c';
    id_15 TEXT := '399f26c3-f102-4146-a8de-f44c9441a872';
    id_16 TEXT := 'c573609b-47c2-4836-88b9-aa595a2b5fdb';
    id_17 TEXT := '2017db6e-9629-49ff-8c2f-04c0f8ee135e';
    id_18 TEXT := 'fd18a4a9-6b08-46e0-a99a-fb69c56c2dee';
    id_19 TEXT := 'f7b05632-247b-47db-89a9-1e80ac388fb2';
    id_20 TEXT := 'a1b3c4d5-e6f7-8901-2345-67890abcdeff';
    id_21 TEXT := 'c00de691-45df-49f9-a94b-442b63d5904c';

    -- 18. Spectrum Types
    spec_0_uuid UUID;
    spec_1_uuid UUID;
    spec_2_uuid UUID;
    spec_3_uuid UUID;

    -- 19. Property Types
    prop_0_uuid  UUID;
    prop_1_uuid  UUID;
    prop_2_uuid  UUID;
    prop_3_uuid  UUID;
    prop_4_uuid  UUID;
    prop_5_uuid  UUID;
    prop_6_uuid  UUID;
    prop_7_uuid  UUID;
    prop_8_uuid  UUID;
    prop_9_uuid  UUID;
    prop_10_uuid UUID;
    prop_11_uuid UUID;
    prop_12_uuid UUID;
    prop_13_uuid UUID;
    prop_14_uuid UUID;
    prop_15_uuid UUID;
    prop_16_uuid UUID;

    -- 20. Storage Types
    stor_0_uuid UUID;
    stor_1_uuid UUID;

    -- 21. Convert Types
    conv_0_uuid UUID;
    conv_1_uuid UUID;

    -- 22. Device Types
    dev_null_uuid UUID;
    dev_0_uuid    UUID;
    dev_1_uuid    UUID;

    -- 23. Interface Types
    iface_1_uuid UUID;
    iface_2_uuid UUID;
BEGIN
    -- ========================================================================
    -- 1. Defect Types
    -- ========================================================================
    type_1_uuid := uuid_in(md5('h_diagnostic_defect_types|1')::cstring);
    type_2_uuid := uuid_in(md5('h_diagnostic_defect_types|0')::cstring);

    INSERT INTO public.h_diagnostic_defect_types (h_diagnostic_defect_type_sk, load_dttm, data_source_id, data_file_id, main_db_id)
    VALUES (type_1_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (type_2_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id);

    INSERT INTO public.s_diagnostic_defect_types (h_diagnostic_defect_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, data_file_id, main_db_id, type_id, description, description_ru)
    VALUES (type_1_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_main_db_id, default_file_id, 1, 'Triggering', 'Срабатывающий'),
           (type_2_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_main_db_id, default_file_id, 0, 'Floating', 'Плавающий');


    -- ========================================================================
    -- 2. Alarm States
    -- ========================================================================
    state_0_uuid := uuid_in(md5('h_diagnostic_alarm_states|0')::cstring);
    state_1_uuid := uuid_in(md5('h_diagnostic_alarm_states|1')::cstring);
    state_2_uuid := uuid_in(md5('h_diagnostic_alarm_states|2')::cstring);
    state_3_uuid := uuid_in(md5('h_diagnostic_alarm_states|3')::cstring);

    INSERT INTO public.h_diagnostic_alarm_states (h_diagnostic_alarm_state_sk, load_dttm, data_source_id, data_file_id, main_db_id)
    VALUES (state_0_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (state_1_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (state_2_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (state_3_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id);

    INSERT INTO public.s_diagnostic_alarm_states (h_diagnostic_alarm_state_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, data_file_id, main_db_id, state_id, description, description_ru)
    VALUES (state_0_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_main_db_id, default_file_id, 0, 'Normal', 'Норма'),
           (state_1_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_main_db_id, default_file_id, 1, 'Armed', 'Взведен (есть аларм)'),
           (state_2_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_main_db_id, default_file_id, 2, 'Acked', 'Квитирован'),
           (state_3_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_main_db_id, default_file_id, 3, 'Repaired', 'Исправлен');


    -- ========================================================================
    -- 3. Defect States
    -- ========================================================================
    dstate_0_uuid := uuid_in(md5('h_diagnostic_defect_states|0')::cstring);
    dstate_1_uuid := uuid_in(md5('h_diagnostic_defect_states|1')::cstring);
    dstate_2_uuid := uuid_in(md5('h_diagnostic_defect_states|2')::cstring);
    dstate_3_uuid := uuid_in(md5('h_diagnostic_defect_states|3')::cstring);
    dstate_4_uuid := uuid_in(md5('h_diagnostic_defect_states|4')::cstring);
    dstate_5_uuid := uuid_in(md5('h_diagnostic_defect_states|5')::cstring);

    INSERT INTO public.h_diagnostic_defect_states (h_diagnostic_defect_state_sk, load_dttm, data_source_id, data_file_id, main_db_id)
    VALUES (dstate_0_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (dstate_1_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (dstate_2_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (dstate_3_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (dstate_4_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (dstate_5_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id);

    INSERT INTO public.s_diagnostic_defect_states (h_diagnostic_defect_state_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, data_file_id, main_db_id, state_id, description, description_ru)
    VALUES (dstate_0_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 0, 'Unknown', 'Неизвестно'),
           (dstate_1_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 1, 'NotProcessed', 'Не обработано'),
           (dstate_2_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 2, 'Normal', 'Норма'),
           (dstate_3_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 3, 'Low', 'Низкий'),
           (dstate_4_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 4, 'Medium', 'Средний'),
           (dstate_5_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 5, 'High', 'Высокий');


    -- ========================================================================
    -- 4. Config Types
    -- ========================================================================
    cfg_1_uuid  := uuid_in(md5('h_config_types|be3db707-8534-4804-a757-e036536783ec')::cstring);
    cfg_2_uuid  := uuid_in(md5('h_config_types|69ebf4a6-365a-4bc6-8b63-6e54394b222d')::cstring);
    cfg_3_uuid  := uuid_in(md5('h_config_types|924af965-2251-4050-9b0d-cad8de981bda')::cstring);
    cfg_4_uuid  := uuid_in(md5('h_config_types|d7bebe88-9a87-4e4b-b2e2-946dd7620d99')::cstring);
    cfg_5_uuid  := uuid_in(md5('h_config_types|c44c65e1-86e7-4d09-9f42-4c1657c761b2')::cstring);
    cfg_6_uuid  := uuid_in(md5('h_config_types|4340de8b-b523-41a3-811c-e53ef4c4abcd')::cstring);
    cfg_7_uuid  := uuid_in(md5('h_config_types|fa638913-f8f4-4f1b-87bb-8a101f6536ef')::cstring);


    INSERT INTO public.h_config_types (h_config_type_sk, load_dttm, data_source_id, data_file_id, main_db_id)
    VALUES (cfg_1_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (cfg_2_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (cfg_3_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (cfg_4_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (cfg_5_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (cfg_6_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (cfg_7_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id);

    INSERT INTO public.s_config_types (h_config_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, data_file_id, main_db_id, type_id, description)
    VALUES (cfg_1_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 'be3db707-8534-4804-a757-e036536783ec'::uuid, 'CREYT'),
           (cfg_2_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, '69ebf4a6-365a-4bc6-8b63-6e54394b222d'::uuid, 'CREYT V5'),
           (cfg_3_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, '924af965-2251-4050-9b0d-cad8de981bda'::uuid, 'Modbus TCP'),
           (cfg_4_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 'd7bebe88-9a87-4e4b-b2e2-946dd7620d99'::uuid, 'OPC DA Client'),
           (cfg_5_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 'c44c65e1-86e7-4d09-9f42-4c1657c761b2'::uuid, 'OPC UA Client'),
           (cfg_6_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, '4340de8b-b523-41a3-811c-e53ef4c4abcd'::uuid, 'L-lcard'),
           (cfg_7_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 'fa638913-f8f4-4f1b-87bb-8a101f6536ef'::uuid, 'L-lcard Crate');



    -- ========================================================================
    -- 5. Index Type Data Records
    -- ========================================================================
    idx_0_uuid := uuid_in(md5('h_index_type_data_records|0')::cstring);
    idx_1_uuid := uuid_in(md5('h_index_type_data_records|1')::cstring);
    idx_2_uuid := uuid_in(md5('h_index_type_data_records|2')::cstring);

    INSERT INTO public.h_index_type_data_records (h_index_type_data_record_sk, load_dttm, data_source_id, data_file_id, main_db_id)
    VALUES (idx_0_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (idx_1_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (idx_2_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id);

    INSERT INTO public.s_index_type_data_records (h_index_type_data_record_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, data_file_id, main_db_id, index_type_id, description, description_ru)
    VALUES (idx_0_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 0, 'Temperature', 'Температура'),
           (idx_1_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 1, 'Battery Voltage', 'Напряжение батареи'),
           (idx_2_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 2, 'Serial Number', 'Серийный номер');


    -- ========================================================================
    -- 6. OPC DA Data Types
    -- ========================================================================
    opc_0_uuid  := uuid_in(md5('h_opc_da_data_types|0')::cstring);
    opc_11_uuid := uuid_in(md5('h_opc_da_data_types|11')::cstring);
    opc_3_uuid  := uuid_in(md5('h_opc_da_data_types|3')::cstring);
    opc_20_uuid := uuid_in(md5('h_opc_da_data_types|20')::cstring);
    opc_5_uuid  := uuid_in(md5('h_opc_da_data_types|5')::cstring);
    opc_8_uuid  := uuid_in(md5('h_opc_da_data_types|8')::cstring);
    opc_7_uuid  := uuid_in(md5('h_opc_da_data_types|7')::cstring);

    INSERT INTO public.h_opc_da_data_types (h_opc_da_data_type_sk, load_dttm, data_source_id, data_file_id, main_db_id)
    VALUES (opc_0_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (opc_11_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (opc_3_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (opc_20_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (opc_5_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (opc_8_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (opc_7_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id);

    INSERT INTO public.s_opc_da_data_types (h_opc_da_data_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, data_file_id, main_db_id, data_type_id, description, description_ru)
    VALUES (opc_0_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 0, 'Empty', 'Пусто'),
           (opc_11_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 11, 'bool', 'Логический'),
           (opc_3_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 3, 'Integer', 'Целое число'),
           (opc_20_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 20, 'Long', 'Длинное целое'),
           (opc_5_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 5, 'Double', 'Число с плавающей точкой'),
           (opc_8_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 8, 'String', 'Строка'),
           (opc_7_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 7, 'DateTime', 'Дата и время');


    -- ========================================================================
    -- 7. OPC UA Data Types
    -- ========================================================================
    ua_0_uuid  := uuid_in(md5('h_opc_ua_data_types|0')::cstring);
    ua_11_uuid := uuid_in(md5('h_opc_ua_data_types|11')::cstring);
    ua_3_uuid  := uuid_in(md5('h_opc_ua_data_types|3')::cstring);
    ua_20_uuid := uuid_in(md5('h_opc_ua_data_types|20')::cstring);
    ua_5_uuid  := uuid_in(md5('h_opc_ua_data_types|5')::cstring);
    ua_8_uuid  := uuid_in(md5('h_opc_ua_data_types|8')::cstring);
    ua_7_uuid  := uuid_in(md5('h_opc_ua_data_types|7')::cstring);
    ua_30_uuid := uuid_in(md5('h_opc_ua_data_types|30')::cstring);
    ua_31_uuid := uuid_in(md5('h_opc_ua_data_types|31')::cstring);
    ua_32_uuid := uuid_in(md5('h_opc_ua_data_types|32')::cstring);
    ua_33_uuid := uuid_in(md5('h_opc_ua_data_types|33')::cstring);
    ua_34_uuid := uuid_in(md5('h_opc_ua_data_types|34')::cstring);
    ua_35_uuid := uuid_in(md5('h_opc_ua_data_types|35')::cstring);

    INSERT INTO public.h_opc_ua_data_types (h_opc_ua_data_type_sk, load_dttm, data_source_id, data_file_id, main_db_id)
    VALUES (ua_0_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (ua_11_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (ua_3_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (ua_20_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (ua_5_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (ua_8_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (ua_7_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (ua_30_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (ua_31_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (ua_32_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (ua_33_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (ua_34_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (ua_35_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id);

    INSERT INTO public.s_opc_ua_data_types (h_opc_ua_data_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, data_file_id, main_db_id, data_type_id, description, description_ru)
    VALUES (ua_0_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 0, 'Empty', 'Пусто'),
           (ua_11_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 11, 'bool', 'Логический'),
           (ua_3_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 3, 'Integer', 'Целое число'),
           (ua_20_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 20, 'Long', 'Длинное целое'),
           (ua_5_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 5, 'Double', 'Число с плавающей точкой'),
           (ua_8_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 8, 'String', 'Строка'),
           (ua_7_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 7, 'DateTime', 'Дата и время'),
           (ua_30_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 30, 'Duration', 'Продолжительность'),
           (ua_31_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 31, 'Integer Array', 'Массив целых чисел'),
           (ua_32_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 32, 'Float Array', 'Массив чисел с плавающей точкой'),
           (ua_33_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 33, 'Float Multi-Dimensional Array', 'Многомерный массив чисел с плавающей точкой'),
           (ua_34_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 34, 'Data Sample', 'Образец данных'),
           (ua_35_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 35, 'Diagnostic Array', 'Массив диагностических данных');


    -- ========================================================================
    -- 8. Data Type Servers
    -- ========================================================================
    srv_0_uuid  := uuid_in(md5('h_data_type_servers|0')::cstring);
    srv_1_uuid  := uuid_in(md5('h_data_type_servers|1')::cstring);
    srv_2_uuid  := uuid_in(md5('h_data_type_servers|2')::cstring);
    srv_3_uuid  := uuid_in(md5('h_data_type_servers|3')::cstring);
    srv_4_uuid  := uuid_in(md5('h_data_type_servers|4')::cstring);
    srv_5_uuid  := uuid_in(md5('h_data_type_servers|5')::cstring);
    srv_6_uuid  := uuid_in(md5('h_data_type_servers|6')::cstring);
    srv_7_uuid  := uuid_in(md5('h_data_type_servers|7')::cstring);
    srv_8_uuid  := uuid_in(md5('h_data_type_servers|8')::cstring);
    srv_9_uuid  := uuid_in(md5('h_data_type_servers|9')::cstring);
    srv_10_uuid := uuid_in(md5('h_data_type_servers|10')::cstring);
    srv_11_uuid := uuid_in(md5('h_data_type_servers|11')::cstring);
    srv_12_uuid := uuid_in(md5('h_data_type_servers|12')::cstring);
    srv_13_uuid := uuid_in(md5('h_data_type_servers|13')::cstring);
    srv_14_uuid := uuid_in(md5('h_data_type_servers|14')::cstring);
    srv_15_uuid := uuid_in(md5('h_data_type_servers|15')::cstring);

    INSERT INTO public.h_data_type_servers (h_data_type_server_sk, load_dttm, data_source_id, data_file_id, main_db_id)
    VALUES (srv_0_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (srv_1_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (srv_2_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (srv_3_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (srv_4_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (srv_5_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (srv_6_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (srv_7_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (srv_8_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (srv_9_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (srv_10_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (srv_11_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (srv_12_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (srv_13_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (srv_14_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (srv_15_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id);

    INSERT INTO public.s_data_type_servers (h_data_type_server_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, data_file_id, main_db_id, data_type_id, description, description_ru)
    VALUES (srv_0_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 0, 'Null', 'Пусто (Null)'),
           (srv_1_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 1, 'bool', 'Логический'),
           (srv_2_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 2, 'SByte', 'Целое со знаком (8 бит)'),
           (srv_3_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 3, 'Byte', 'Байт (без знака)'),
           (srv_4_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 4, 'Int16', 'Целое 16-битное со знаком'),
           (srv_5_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 5, 'UInt16', 'Целое 16-битное без знака'),
           (srv_6_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 6, 'Int32', 'Целое 32-битное со знаком'),
           (srv_7_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 7, 'UInt32', 'Целое 32-битное без знака'),
           (srv_8_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 8, 'Int64', 'Целое 64-битное со знаком'),
           (srv_9_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 9, 'UInt64', 'Целое 64-битное без знака'),
           (srv_10_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 10, 'Float', 'Число с плавающей точкой (32 бит)'),
           (srv_11_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 11, 'Double', 'Число с плавающей точкой (64 бит)'),
           (srv_12_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 12, 'String', 'Строка'),
           (srv_13_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 13, 'DateTime', 'Дата и время'),
           (srv_14_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 14, 'ByteString', 'Массив байтов'),
           (srv_15_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 15, 'Double', 'Число с плавающей точкой (64 бит)');


    -- ========================================================================
    -- 9. TIK SCADA Log Types
    -- ========================================================================
    log_0_uuid  := uuid_in(md5('h_tik_scada_log_types|0')::cstring);
    log_1_uuid  := uuid_in(md5('h_tik_scada_log_types|1')::cstring);
    log_2_uuid  := uuid_in(md5('h_tik_scada_log_types|2')::cstring);
    log_3_uuid  := uuid_in(md5('h_tik_scada_log_types|3')::cstring);
    log_4_uuid  := uuid_in(md5('h_tik_scada_log_types|4')::cstring);
    log_5_uuid  := uuid_in(md5('h_tik_scada_log_types|5')::cstring);
    log_6_uuid  := uuid_in(md5('h_tik_scada_log_types|6')::cstring);
    log_7_uuid  := uuid_in(md5('h_tik_scada_log_types|7')::cstring);
    log_8_uuid  := uuid_in(md5('h_tik_scada_log_types|8')::cstring);
    log_9_uuid  := uuid_in(md5('h_tik_scada_log_types|9')::cstring);
    log_10_uuid := uuid_in(md5('h_tik_scada_log_types|10')::cstring);
    log_11_uuid := uuid_in(md5('h_tik_scada_log_types|11')::cstring);

    INSERT INTO public.h_tik_scada_log_types (h_tik_scada_log_type_sk, load_dttm, data_source_id, data_file_id, main_db_id)
    VALUES 
        (log_0_uuid,  load_datetime, default_source_id, default_file_id, default_main_db_id),
        (log_1_uuid,  load_datetime, default_source_id, default_file_id, default_main_db_id),
        (log_2_uuid,  load_datetime, default_source_id, default_file_id, default_main_db_id),
        (log_3_uuid,  load_datetime, default_source_id, default_file_id, default_main_db_id),
        (log_4_uuid,  load_datetime, default_source_id, default_file_id, default_main_db_id),
        (log_5_uuid,  load_datetime, default_source_id, default_file_id, default_main_db_id),
        (log_6_uuid,  load_datetime, default_source_id, default_file_id, default_main_db_id),
        (log_7_uuid,  load_datetime, default_source_id, default_file_id, default_main_db_id),
        (log_8_uuid,  load_datetime, default_source_id, default_file_id, default_main_db_id),
        (log_9_uuid,  load_datetime, default_source_id, default_file_id, default_main_db_id),
        (log_10_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
        (log_11_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id);

    INSERT INTO public.s_tik_scada_log_types (h_tik_scada_log_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, data_file_id, main_db_id, log_type_id, description, description_ru)
    VALUES 
        (log_0_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 0,  'Authentication',          'Аутентификация'),
        (log_1_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 1,  'Access Token Management', 'Управление токенами доступа'),
        (log_2_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 2,  'Key Management',          'Управление ключами'),
        (log_3_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 3,  'User Management',         'Управление пользователями'),
        (log_4_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 4,  'Role & Permission Mgmt',  'Управление ролями и правами'),
        (log_5_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 5,  'Project Management',      'Управление проектами'),
        (log_6_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 6,  'Connection Management',   'Управление подключениями'),
        (log_7_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 7,  'Certificate Management',  'Управление сертификатами'),
        (log_8_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 8,  'Database Management',     'Управление базами данных'),
        (log_9_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 9,  'Password Management',     'Управление паролями'),
        (log_10_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 10, 'System Operations',       'Системные операции'),
        (log_11_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 11, 'Other Events',            'Прочие события');

    -- ========================================================================
    -- 10. User Action Types
    -- ========================================================================
    act_0_uuid  := uuid_in(md5('h_user_action_types|0')::cstring);
    act_1_uuid  := uuid_in(md5('h_user_action_types|1')::cstring);
    act_2_uuid  := uuid_in(md5('h_user_action_types|2')::cstring);
    act_3_uuid  := uuid_in(md5('h_user_action_types|3')::cstring);
    act_4_uuid  := uuid_in(md5('h_user_action_types|4')::cstring);
    act_5_uuid  := uuid_in(md5('h_user_action_types|5')::cstring);
    act_6_uuid  := uuid_in(md5('h_user_action_types|6')::cstring);
    act_7_uuid  := uuid_in(md5('h_user_action_types|7')::cstring);
    act_8_uuid  := uuid_in(md5('h_user_action_types|8')::cstring);
    act_9_uuid  := uuid_in(md5('h_user_action_types|9')::cstring);
    act_10_uuid := uuid_in(md5('h_user_action_types|10')::cstring);
    act_11_uuid := uuid_in(md5('h_user_action_types|11')::cstring);
    act_12_uuid := uuid_in(md5('h_user_action_types|12')::cstring);
    act_13_uuid := uuid_in(md5('h_user_action_types|13')::cstring);
    act_14_uuid := uuid_in(md5('h_user_action_types|14')::cstring);
    act_15_uuid := uuid_in(md5('h_user_action_types|15')::cstring);
    act_16_uuid := uuid_in(md5('h_user_action_types|16')::cstring);
    act_17_uuid := uuid_in(md5('h_user_action_types|17')::cstring);
    act_18_uuid := uuid_in(md5('h_user_action_types|18')::cstring);
    act_19_uuid := uuid_in(md5('h_user_action_types|19')::cstring);

    INSERT INTO public.h_user_action_types (h_user_action_type_sk, load_dttm, data_source_id, data_file_id, main_db_id)
    VALUES (act_0_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (act_1_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (act_2_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (act_3_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (act_4_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (act_5_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (act_6_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (act_7_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (act_8_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (act_9_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (act_10_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (act_11_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (act_12_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (act_13_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (act_14_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (act_15_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (act_16_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (act_17_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (act_18_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (act_19_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id);

    INSERT INTO public.s_user_action_types (h_user_action_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, data_file_id, main_db_id, action_type_id, description, description_ru)
    VALUES (act_0_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 0, 'Model Added', 'Добавлена модель'),
           (act_1_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 1, 'Model Removed', 'Удалена модель'),
           (act_2_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 2, 'Model Renamed', 'Переименована модель'),
           (act_3_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 3, 'Template Added', 'Добавлен шаблон'),
           (act_4_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 4, 'Template Removed', 'Удалён шаблон'),
           (act_5_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 5, 'Template Renamed', 'Переименован шаблон'),
           (act_6_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 6, 'Property Descriptor Added', 'Добавлено описание свойства'),
           (act_7_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 7, 'Property Descriptor Removed', 'Удалено описание свойства'),
           (act_8_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 8, 'Property Descriptor Edited', 'Изменено описание свойства'),
           (act_9_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 9, 'Type Descriptor Added', 'Добавлено описание типа'),
           (act_10_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 10, 'Type Descriptor Removed', 'Удалено описание типа'),
           (act_11_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 11, 'Property Added', 'Добавлено свойство'),
           (act_12_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 12, 'Property Removed', 'Удалено свойство'),
           (act_13_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 13, 'Type Added', 'Добавлен тип'),
           (act_14_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 14, 'Type Removed', 'Удалён тип'),
           (act_15_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 15, 'Rule Added', 'Добавлено правило'),
           (act_16_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 16, 'Rule Removed', 'Удалено правило'),
           (act_17_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 17, 'Bearing Added', 'Добавлен подшипник'),
           (act_18_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 18, 'Bearing Removed', 'Удалён подшипник'),
           (act_19_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 19, 'Bearing Edited', 'Изменён подшипник');


    -- ========================================================================
    -- 11. Running Types
    -- ========================================================================
    run_1_uuid := uuid_in(md5('h_running_types|1')::cstring);
    run_2_uuid := uuid_in(md5('h_running_types|2')::cstring);

    INSERT INTO public.h_running_types (h_running_type_sk, load_dttm, data_source_id, data_file_id, main_db_id)
    VALUES (run_1_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (run_2_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id);

    INSERT INTO public.s_running_types (h_running_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, data_file_id, main_db_id, type_id, description, description_ru)
    VALUES (run_1_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 1, 'Level', 'По уровню'),
           (run_2_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 2, 'Period', 'По периоду');


    -- ========================================================================
    -- 12. Crate Types
    -- ========================================================================
    crate_0_uuid := uuid_in(md5('h_crate_types|0')::cstring);

    INSERT INTO public.h_crate_types (h_crate_type_sk, load_dttm, data_source_id, data_file_id, main_db_id)
    VALUES (crate_0_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id);

    INSERT INTO public.s_crate_types (h_crate_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, data_file_id, main_db_id, type_id, description, description_ru)
    VALUES (crate_0_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 0, 'LTR_EU_2', 'LTR_EU_2');


    -- ========================================================================
    -- 13. L Card Logic Input Types
    -- ========================================================================
    lcard_in_0_uuid := uuid_in(md5('h_l_card_logic_input_types|0')::cstring);
    lcard_in_1_uuid := uuid_in(md5('h_l_card_logic_input_types|1')::cstring);
    lcard_in_2_uuid := uuid_in(md5('h_l_card_logic_input_types|2')::cstring);
    lcard_in_3_uuid := uuid_in(md5('h_l_card_logic_input_types|3')::cstring);

    INSERT INTO public.h_l_card_logic_input_types (h_l_card_logic_input_type_sk, load_dttm, data_source_id, data_file_id, main_db_id)
    VALUES (lcard_in_0_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (lcard_in_1_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (lcard_in_2_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (lcard_in_3_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id);

    INSERT INTO public.s_l_card_logic_input_types (h_l_card_logic_input_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, data_file_id, main_db_id, type_id, description, description_ru)
    VALUES (lcard_in_0_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 0, 'DIFFERENTIAL', 'Дифференциальный'),
           (lcard_in_1_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 1, 'SINGLE_ENDED', 'Односторонний'),
           (lcard_in_2_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 2, 'DISCR_IN_BIT', 'Дискр.вход (бит)'),
           (lcard_in_3_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 3, 'DISCR_OUT_BIT', 'Дискр.выход (бит)');


    -- ========================================================================
    -- 14. L Card Crate Module Types
    -- ========================================================================
    lcard_mod_0_uuid := uuid_in(md5('h_l_card_crate_module_types|0')::cstring);
    lcard_mod_1_uuid := uuid_in(md5('h_l_card_crate_module_types|1')::cstring);
    lcard_mod_2_uuid := uuid_in(md5('h_l_card_crate_module_types|2')::cstring);
    lcard_mod_3_uuid := uuid_in(md5('h_l_card_crate_module_types|3')::cstring);
    lcard_mod_4_uuid := uuid_in(md5('h_l_card_crate_module_types|4')::cstring);

    INSERT INTO public.h_l_card_crate_module_types (h_l_card_crate_module_type_sk, load_dttm, data_source_id, data_file_id, main_db_id)
    VALUES (lcard_mod_0_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (lcard_mod_1_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (lcard_mod_2_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (lcard_mod_3_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (lcard_mod_4_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id);

    INSERT INTO public.s_l_card_crate_module_types (h_l_card_crate_module_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, data_file_id, main_db_id, type_id, description, description_ru)
    VALUES (lcard_mod_0_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 0, 'LTR25', 'LTR25'),
           (lcard_mod_1_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 1, 'LTR27', 'LTR27'),
           (lcard_mod_2_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 2, 'LTR24', 'LTR24'),
           (lcard_mod_3_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 3, 'LTR11_DIFF', 'LTR11 Дифференциальный (16 каналов)'),
           (lcard_mod_4_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 4, 'LTR11_GND', 'LTR11 С общей землей (32 канала)');


    -- ========================================================================
    -- 15. Register Types
    -- ========================================================================
    reg_1_uuid := uuid_in(md5('h_register_types|1')::cstring);
    reg_2_uuid := uuid_in(md5('h_register_types|2')::cstring);
    reg_3_uuid := uuid_in(md5('h_register_types|3')::cstring);
    reg_4_uuid := uuid_in(md5('h_register_types|4')::cstring);

    INSERT INTO public.h_register_types (h_register_type_sk, load_dttm, data_source_id, data_file_id, main_db_id)
    VALUES (reg_1_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (reg_2_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (reg_3_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (reg_4_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id);

    INSERT INTO public.s_register_types (h_register_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, data_file_id, main_db_id, type_id, description, description_ru)
    VALUES (reg_1_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 1, 'CoilStatus', 'Состояние катушки'),
           (reg_2_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 2, 'InputStatus', 'Состояние входа'),
           (reg_3_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 3, 'HoldingRegister', 'Регистр хранения'),
           (reg_4_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 4, 'InputRegister', 'Входной регистр');


    -- ========================================================================
    -- 16. Type Names
    -- ========================================================================
    tn_0_uuid := uuid_in(md5('h_data_type_names|0')::cstring);
    tn_1_uuid := uuid_in(md5('h_data_type_names|1')::cstring);
    tn_2_uuid := uuid_in(md5('h_data_type_names|2')::cstring);
    tn_3_uuid := uuid_in(md5('h_data_type_names|3')::cstring);
    tn_4_uuid := uuid_in(md5('h_data_type_names|4')::cstring);
    tn_5_uuid := uuid_in(md5('h_data_type_names|5')::cstring);
    tn_6_uuid := uuid_in(md5('h_data_type_names|6')::cstring);
    tn_7_uuid := uuid_in(md5('h_data_type_names|7')::cstring);
    tn_8_uuid := uuid_in(md5('h_data_type_names|8')::cstring);

    INSERT INTO public.h_data_type_names (h_data_type_name_sk, load_dttm, data_source_id, data_file_id, main_db_id)
    VALUES (tn_0_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (tn_1_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (tn_2_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (tn_3_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (tn_4_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (tn_5_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (tn_6_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (tn_7_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (tn_8_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id);

    INSERT INTO public.s_data_type_names (h_data_type_name_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, data_file_id, main_db_id, type_id, description, description_ru)
    VALUES (tn_0_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 0, 'Int8', 'Целое 8-битное со знаком'),
           (tn_1_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 1, 'UInt8', 'Целое 8-битное без знака'),
           (tn_2_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 2, 'Int16', 'Целое 16-битное со знаком'),
           (tn_3_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 3, 'UInt16', 'Целое 16-битное без знака'),
           (tn_4_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 4, 'Int32', 'Целое 32-битное со знаком'),
           (tn_5_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 5, 'UInt32', 'Целое 32-битное без знака'),
           (tn_6_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 6, 'Float32', 'Число с плавающей точкой 32 бита'),
           (tn_7_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 7, 'Float64', 'Число с плавающей точкой 64 бита'),
           (tn_8_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 8, 'bool', 'Логическое значение');


    -- ========================================================================
    -- 17. Data Types
    -- ========================================================================
    dt_1_uuid  := uuid_in(md5('h_data_types|' || id_1)::cstring);
    dt_2_uuid  := uuid_in(md5('h_data_types|' || id_2)::cstring);
    dt_3_uuid  := uuid_in(md5('h_data_types|' || id_3)::cstring);
    dt_4_uuid  := uuid_in(md5('h_data_types|' || id_4)::cstring);
    dt_5_uuid  := uuid_in(md5('h_data_types|' || id_5)::cstring);
    dt_6_uuid  := uuid_in(md5('h_data_types|' || id_6)::cstring);
    dt_7_uuid  := uuid_in(md5('h_data_types|' || id_7)::cstring);
    dt_8_uuid  := uuid_in(md5('h_data_types|' || id_8)::cstring);
    dt_9_uuid  := uuid_in(md5('h_data_types|' || id_9)::cstring);
    dt_10_uuid := uuid_in(md5('h_data_types|' || id_10)::cstring);
    dt_11_uuid := uuid_in(md5('h_data_types|' || id_11)::cstring);
    dt_12_uuid := uuid_in(md5('h_data_types|' || id_12)::cstring);
    dt_13_uuid := uuid_in(md5('h_data_types|' || id_13)::cstring);
    dt_14_uuid := uuid_in(md5('h_data_types|' || id_14)::cstring);
    dt_15_uuid := uuid_in(md5('h_data_types|' || id_15)::cstring);
    dt_16_uuid := uuid_in(md5('h_data_types|' || id_16)::cstring);
    dt_17_uuid := uuid_in(md5('h_data_types|' || id_17)::cstring);
    dt_18_uuid := uuid_in(md5('h_data_types|' || id_18)::cstring);
    dt_19_uuid := uuid_in(md5('h_data_types|' || id_19)::cstring);
    dt_20_uuid := uuid_in(md5('h_data_types|' || id_20)::cstring);
    dt_21_uuid := uuid_in(md5('h_data_types|' || id_21)::cstring);

    INSERT INTO public.h_data_types (h_data_type_sk, load_dttm, data_source_id, data_file_id, main_db_id)
    VALUES (dt_1_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (dt_2_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (dt_3_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (dt_4_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (dt_5_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (dt_6_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (dt_7_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (dt_8_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (dt_9_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (dt_10_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (dt_11_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (dt_12_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (dt_13_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (dt_14_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (dt_15_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (dt_16_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (dt_17_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (dt_18_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (dt_19_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (dt_20_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (dt_21_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id);

    INSERT INTO public.s_data_types (h_data_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, data_file_id, main_db_id, type_id, description, description_ru)
    VALUES (dt_1_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, id_1::uuid, 'INT_ARRAY', 'Целочисленный массив'),
           (dt_2_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, id_2::uuid, 'INT32', 'Целое 32-битное со знаком'),
           (dt_3_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, id_3::uuid, 'FLOAT64', 'Число с плавающей точкой 64 бита'),
           (dt_4_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, id_4::uuid, 'CREYT', 'Тип данных CREYT'),
           (dt_5_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, id_5::uuid, 'LCARD', 'Тип данных LCARD'),
           (dt_6_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, id_6::uuid, 'BODE', 'Тип данных BODE'),
           (dt_7_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, id_7::uuid, 'bool', 'Логическое значение'),
           (dt_8_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, id_8::uuid, 'DATA_SAMPLE', 'Образец данных'),
           (dt_9_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, id_9::uuid, 'DATETIME', 'Дата и время'),
           (dt_10_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, id_10::uuid, 'DATETIME_ARRAY', 'Массив дат и времени'),
           (dt_11_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, id_11::uuid, 'DIAGNOSTIC_ARRAY', 'Массив диагностических данных'),
           (dt_12_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, id_12::uuid, 'DURATION', 'Продолжительность'),
           (dt_13_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, id_13::uuid, 'FILE', 'Файл'),
           (dt_14_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, id_14::uuid, 'FLOAT_ARRAY', 'Массив чисел с плавающей точкой'),
           (dt_15_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, id_15::uuid, 'INT64', 'Целое 64-битное со знаком'),
           (dt_16_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, id_16::uuid, 'SIEMENS_SAMPLE', 'Образец данных Siemens'),
           (dt_17_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, id_17::uuid, 'SPECTRUM', 'Спектр'),
           (dt_18_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, id_18::uuid, 'STRING', 'Строка'),
           (dt_19_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, id_19::uuid, 'PION', 'Тип данных PION'),
           (dt_20_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, id_20::uuid, 'STRING_ARRAY', 'Массив строк'),
           (dt_21_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, id_21::uuid, 'TIKEXPERT', 'Тип данных TIKEXPERT');


    -- ========================================================================
    -- 18. Spectrum Types
    -- ========================================================================
    spec_0_uuid := uuid_in(md5('h_spectrum_types|0')::cstring);
    spec_1_uuid := uuid_in(md5('h_spectrum_types|1')::cstring);
    spec_2_uuid := uuid_in(md5('h_spectrum_types|2')::cstring);
    spec_3_uuid := uuid_in(md5('h_spectrum_types|3')::cstring);

    INSERT INTO public.h_spectrum_types (h_spectrum_type_sk, load_dttm, data_source_id, data_file_id, main_db_id)
    VALUES (spec_0_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (spec_1_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (spec_2_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (spec_3_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id);

    INSERT INTO public.s_spectrum_types (h_spectrum_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, data_file_id, main_db_id, type_id, description, description_ru)
    VALUES (spec_0_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 0, 'Acceleration', 'Спектр ускорения'),
           (spec_1_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 1, 'Velocity', 'Спектр скорости'),
           (spec_2_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 2, 'Displacement', 'Спектр перемещения'),
           (spec_3_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 3, 'Envelope', 'Спектр огибающей');


    -- ========================================================================
    -- 19. Property Types
    -- ========================================================================
    prop_0_uuid  := uuid_in(md5('h_property_types|0')::cstring);
    prop_1_uuid  := uuid_in(md5('h_property_types|1')::cstring);
    prop_2_uuid  := uuid_in(md5('h_property_types|2')::cstring);
    prop_3_uuid  := uuid_in(md5('h_property_types|3')::cstring);
    prop_4_uuid  := uuid_in(md5('h_property_types|4')::cstring);
    prop_5_uuid  := uuid_in(md5('h_property_types|5')::cstring);
    prop_6_uuid  := uuid_in(md5('h_property_types|6')::cstring);
    prop_7_uuid  := uuid_in(md5('h_property_types|7')::cstring);
    prop_8_uuid  := uuid_in(md5('h_property_types|8')::cstring);
    prop_9_uuid  := uuid_in(md5('h_property_types|9')::cstring);
    prop_10_uuid := uuid_in(md5('h_property_types|10')::cstring);
    prop_11_uuid := uuid_in(md5('h_property_types|11')::cstring);
    prop_12_uuid := uuid_in(md5('h_property_types|12')::cstring);
    prop_13_uuid := uuid_in(md5('h_property_types|13')::cstring);
    prop_14_uuid := uuid_in(md5('h_property_types|14')::cstring);
    prop_15_uuid := uuid_in(md5('h_property_types|15')::cstring);
    prop_16_uuid := uuid_in(md5('h_property_types|16')::cstring);

    INSERT INTO public.h_property_types (h_property_type_sk, load_dttm, data_source_id, data_file_id, main_db_id)
    VALUES (prop_0_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (prop_1_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (prop_2_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (prop_3_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (prop_4_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (prop_5_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (prop_6_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (prop_7_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (prop_8_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (prop_9_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (prop_10_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (prop_11_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (prop_12_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (prop_13_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (prop_14_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (prop_15_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (prop_16_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id);

    INSERT INTO public.s_property_types (h_property_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, data_file_id, main_db_id, type_id, description, description_ru)
    VALUES (prop_0_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 0, 'Unknown', 'Неизвестный'),
           (prop_1_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 1, 'bool (bool)', 'Логический (bool)'),
           (prop_2_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 2, 'Integer (int)', 'Целочисленный (int)'),
           (prop_3_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 3, 'Long', 'Длинное целое (long)'),
           (prop_4_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 4, 'Double', 'Число с плавающей точкой (double)'),
           (prop_5_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 5, 'String', 'Строка (string)'),
           (prop_6_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 6, 'DateTime', 'Дата и время'),
           (prop_7_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 7, 'Duration', 'Продолжительность'),
           (prop_8_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 8, 'Data Sample', 'Образец данных'),
           (prop_9_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 9, 'Integer Array', 'Массив целых чисел'),
           (prop_10_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 10, 'Float Array', 'Массив чисел с плавающей точкой'),
           (prop_11_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 11, 'Diagnostic Array', 'Массив диагностических данных'),
           (prop_12_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 12, 'File', 'Файл'),
           (prop_13_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 13, 'Float Multi-Dimensional Array', 'Многомерный массив чисел с плавающей точкой'),
           (prop_14_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 14, 'DateTime Array', 'Массив дат и времени'),
           (prop_15_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 15, 'Spectrum', 'Спектр'),
           (prop_16_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 16, 'Bode', 'Боде (Bode)');


    -- ========================================================================
    -- 20. Storage Types
    -- ========================================================================
    stor_0_uuid := uuid_in(md5('h_storage_types|0')::cstring);
    stor_1_uuid := uuid_in(md5('h_storage_types|1')::cstring);

    INSERT INTO public.h_storage_types (h_storage_type_sk, load_dttm, data_source_id, data_file_id, main_db_id)
    VALUES (stor_0_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (stor_1_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id);

    INSERT INTO public.s_storage_types (h_storage_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, data_file_id, main_db_id, storage_type_id, description, description_ru)
    VALUES (stor_0_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 0, 'Database Storage', 'Хранение в базе данных'),
           (stor_1_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 1, 'File System Storage', 'Хранение в файловой системе');


    -- ========================================================================
    -- 21. Convert Types
    -- ========================================================================
    conv_0_uuid := uuid_in(md5('h_convert_types|0')::cstring);
    conv_1_uuid := uuid_in(md5('h_convert_types|1')::cstring);

    INSERT INTO public.h_convert_types (h_convert_type_sk, load_dttm, data_source_id, data_file_id, main_db_id)
    VALUES (conv_0_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (conv_1_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id);

    INSERT INTO public.s_convert_types (h_convert_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, data_file_id, main_db_id, convert_type_id, description, description_ru)
    VALUES (conv_0_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 0, 'Factor', 'Коэффициент'),
           (conv_1_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 1, 'Function', 'Функция');


    -- ========================================================================
    -- 22. Device Types
    -- ========================================================================
    dev_null_uuid := uuid_in(md5('h_device_types|null')::cstring);
    dev_0_uuid    := uuid_in(md5('h_device_types|0')::cstring);
    dev_1_uuid    := uuid_in(md5('h_device_types|1')::cstring);

    INSERT INTO public.h_device_types (h_device_type_sk, load_dttm, data_source_id, data_file_id, main_db_id)
    VALUES (dev_null_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (dev_0_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (dev_1_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id);

    INSERT INTO public.s_device_types (h_device_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, data_file_id, main_db_id, type_id, description, description_ru)
    VALUES (dev_null_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, null, 'None', 'Нет данных'),
           (dev_0_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 0, 'Usual', 'Обычное'),
           (dev_1_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 1, 'Creyt', 'Крейт');


    -- ========================================================================
    -- 23. Interface Types
    -- ========================================================================
    iface_1_uuid := uuid_in(md5('h_interface_types|1')::cstring);
    iface_2_uuid := uuid_in(md5('h_interface_types|2')::cstring);

    INSERT INTO public.h_interface_types (h_interface_type_sk, load_dttm, data_source_id, data_file_id, main_db_id)
    VALUES (iface_1_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id),
           (iface_2_uuid, load_datetime, default_source_id, default_file_id, default_main_db_id);

    INSERT INTO public.s_interface_types (h_interface_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, data_file_id, main_db_id, type_id, description, description_ru)
    VALUES (iface_1_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 1, 'Modbus TCP', 'Modbus TCP'),
           (iface_2_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, default_file_id, default_main_db_id, 2, 'Modbus RTU (Serial)', 'Modbus RTU (Последовательный)');
END;
$$;