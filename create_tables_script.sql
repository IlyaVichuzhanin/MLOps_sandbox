-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ==============================
-- Record Source Referenceks
-- ==============================

CREATE TABLE IF NOT EXISTS public.data_catalogue (
    id SERIAL UNIQUE NOT NULL,
    data_source_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000'::uuid,
    main_db_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000'::uuid,
    main_db_name TEXT NOT NULL DEFAULT 'Unknown',
    file_name TEXT,           -- разрешить NULL (не применимо к заглушке)
    file_size INTEGER,        -- разрешить NULL
    file_hash TEXT CHECK (LENGTH("file_hash") = 64),          -- разрешить NULL
    last_update TIMESTAMP,    -- разрешить NULL
    upload_date_time TIMESTAMP, -- разрешить NULL
    created_dttm TIMESTAMP DEFAULT NOW() NOT NULL,
    hdfs_storage_path TEXT,   -- разрешить NULL
    hdfs_full_path TEXT,      -- разрешить NULL
    data_type TEXT,           -- разрешить NULL
    data_format TEXT,         -- разрешить NULL
    is_uploaded_to_dwh BOOL DEFAULT FALSE NOT NULL,
    CONSTRAINT data_catalogue_pk PRIMARY KEY (data_source_id)
) DISTRIBUTED BY (data_source_id);

-- ==============================
-- Diagnostic Defect Types
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_diagnostic_defect_types (
    h_diagnostic_defect_type_sk uuid DEFAULT uuid_generate_v4() NOT NULL,
    load_dttm timestamp DEFAULT NOW() NOT NULL,
    data_source_id uuid DEFAULT '00000000-0000-0000-0000-000000000000'::uuid NOT NULL,
    CONSTRAINT h_diagnostic_defect_types_pk PRIMARY KEY (h_diagnostic_defect_type_sk),
    CONSTRAINT h_diagnostic_defect_types_data_catalogue_fk 
        FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_diagnostic_defect_type_sk);

CREATE TABLE IF NOT EXISTS public.s_diagnostic_defect_types (
    h_diagnostic_defect_type_sk uuid NOT NULL,
    load_dttm timestamp DEFAULT NOW() NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp DEFAULT NOW() NOT NULL,
    data_source_id uuid DEFAULT '00000000-0000-0000-0000-000000000000'::uuid NOT NULL,
    CONSTRAINT h_diagnostic_alarm_states_pk PRIMARY KEY (h_diagnostic_alarm_state_sk),
    CONSTRAINT h_diagnostic_alarm_states_data_catalogue_fk 
        FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_diagnostic_alarm_state_sk);

CREATE TABLE IF NOT EXISTS public.s_diagnostic_alarm_states (
    h_diagnostic_alarm_state_sk uuid NOT NULL,
    load_dttm timestamp DEFAULT NOW() NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp DEFAULT NOW() NOT NULL,
    data_source_id uuid DEFAULT '00000000-0000-0000-0000-000000000000'::uuid NOT NULL,
    CONSTRAINT h_diagnostic_defect_states_pk PRIMARY KEY (h_diagnostic_defect_state_sk),
    CONSTRAINT h_diagnostic_defect_states_data_catalogue_fk 
        FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_diagnostic_defect_state_sk);

CREATE TABLE IF NOT EXISTS public.s_diagnostic_defect_states (
    h_diagnostic_defect_state_sk uuid NOT NULL,
    load_dttm timestamp DEFAULT NOW() NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp DEFAULT NOW() NOT NULL,
    data_source_id uuid DEFAULT '00000000-0000-0000-0000-000000000000'::uuid NOT NULL,
    CONSTRAINT h_config_types_pk PRIMARY KEY (h_config_type_sk),
    CONSTRAINT h_config_types_data_catalogue_fk 
        FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_config_type_sk);

CREATE TABLE IF NOT EXISTS public.s_config_types (
    h_config_type_sk uuid NOT NULL,
    load_dttm timestamp DEFAULT NOW() NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp DEFAULT NOW() NOT NULL,
    data_source_id uuid DEFAULT '00000000-0000-0000-0000-000000000000'::uuid NOT NULL,
    CONSTRAINT h_index_type_data_record_pk PRIMARY KEY (h_index_type_data_record_sk),
    CONSTRAINT h_index_type_data_record_data_catalogue_fk 
        FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_index_type_data_record_sk);

CREATE TABLE IF NOT EXISTS public.s_index_type_data_records (
    h_index_type_data_record_sk uuid NOT NULL,
    load_dttm timestamp DEFAULT NOW() NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp DEFAULT NOW() NOT NULL,
    data_source_id uuid DEFAULT '00000000-0000-0000-0000-000000000000'::uuid NOT NULL,
    CONSTRAINT h_opc_da_data_type_pk PRIMARY KEY (h_opc_da_data_type_sk),
    CONSTRAINT h_opc_da_data_type_data_catalogue_fk 
        FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_opc_da_data_type_sk);

CREATE TABLE IF NOT EXISTS public.s_opc_da_data_types (
    h_opc_da_data_type_sk uuid NOT NULL,
    load_dttm timestamp DEFAULT NOW() NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp DEFAULT NOW() NOT NULL,
    data_source_id uuid DEFAULT '00000000-0000-0000-0000-000000000000'::uuid NOT NULL,
    CONSTRAINT h_opc_ua_data_type_pk PRIMARY KEY (h_opc_ua_data_type_sk),
    CONSTRAINT h_opc_ua_data_type_data_catalogue_fk 
        FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_opc_ua_data_type_sk);

CREATE TABLE IF NOT EXISTS public.s_opc_ua_data_types (
    h_opc_ua_data_type_sk uuid NOT NULL,
    load_dttm timestamp DEFAULT NOW() NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp DEFAULT NOW() NOT NULL,
    data_source_id uuid DEFAULT '00000000-0000-0000-0000-000000000000'::uuid NOT NULL,
    CONSTRAINT h_data_type_servers_pk PRIMARY KEY (h_data_type_server_sk),
    CONSTRAINT h_data_type_servers_data_catalogue_fk 
        FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_data_type_server_sk);

CREATE TABLE IF NOT EXISTS public.s_data_type_servers (
    h_data_type_server_sk uuid NOT NULL,
    load_dttm timestamp DEFAULT NOW() NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp DEFAULT NOW() NOT NULL,
    data_source_id uuid DEFAULT '00000000-0000-0000-0000-000000000000'::uuid NOT NULL,
    CONSTRAINT h_tik_scada_log_types_pk PRIMARY KEY (h_tik_scada_log_type_sk),
    CONSTRAINT h_tik_scada_log_types_data_catalogue_fk 
        FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_tik_scada_log_type_sk);

CREATE TABLE IF NOT EXISTS public.s_tik_scada_log_types (
    h_tik_scada_log_type_sk uuid NOT NULL,
    load_dttm timestamp DEFAULT NOW() NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp DEFAULT NOW() NOT NULL,
    data_source_id uuid DEFAULT '00000000-0000-0000-0000-000000000000'::uuid NOT NULL,
    CONSTRAINT h_user_action_type_pk PRIMARY KEY (h_user_action_type_sk),
    CONSTRAINT h_user_action_type_data_catalogue_fk 
        FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_user_action_type_sk);

CREATE TABLE IF NOT EXISTS public.s_user_action_types (
    h_user_action_type_sk uuid NOT NULL,
    load_dttm timestamp DEFAULT NOW() NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_running_types_pk PRIMARY KEY (h_running_type_sk),
    CONSTRAINT h_running_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_running_type_sk);

-- ==============================
-- Satellite: Running Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_running_types (
    h_running_type_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_crate_types_pk PRIMARY KEY (h_crate_type_sk),
    CONSTRAINT h_crate_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_crate_type_sk);

-- ==============================
-- Satellite: Crate Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_crate_types (
    h_crate_type_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_l_card_logic_input_types_pk PRIMARY KEY (h_l_card_logic_input_type_sk),
    CONSTRAINT h_l_card_logic_input_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_l_card_logic_input_type_sk);

-- ==============================
-- Satellite: L Card Logic Input Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_l_card_logic_input_types (
    h_l_card_logic_input_type_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_l_card_crate_module_types_pk PRIMARY KEY (h_l_card_crate_module_type_sk),
    CONSTRAINT h_l_card_crate_module_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_l_card_crate_module_type_sk);

-- ==============================
-- Satellite: L Card Crate Module Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_l_card_crate_module_types (
    h_l_card_crate_module_type_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_register_types_pk PRIMARY KEY (h_register_type_sk),
    CONSTRAINT h_register_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_register_type_sk);

-- ==============================
-- Satellite: Register Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_register_types (
    h_register_type_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    type_id integer NOT NULL,
    description text NOT NULL,
    description_ru text NOT NULL,
    CONSTRAINT s_register_types_h_register_type_fk FOREIGN KEY (h_register_type_sk) REFERENCES public.h_register_types (h_register_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_register_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_register_type_sk);

-- ==============================
-- Hub: Type Names
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_type_names (
    h_type_name_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_type_names_pk PRIMARY KEY (h_type_name_sk),
    CONSTRAINT h_type_names_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_type_name_sk);

-- ==============================
-- Satellite: Type Names
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_type_names (
    h_type_name_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    type_id integer NOT NULL,
    description text NOT NULL,
    description_ru text NOT NULL,
    CONSTRAINT s_type_names_h_type_name_fk FOREIGN KEY (h_type_name_sk) REFERENCES public.h_type_names (h_type_name_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_type_names_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_type_name_sk);

-- ==============================
-- Hub: Data Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_data_types (
    h_data_type_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_data_types_pk PRIMARY KEY (h_data_type_sk),
    CONSTRAINT h_data_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_data_type_sk);

-- ==============================
-- Satellite: Data Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_data_types (
    h_data_type_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_spectrum_types_pk PRIMARY KEY (h_spectrum_type_sk),
    CONSTRAINT h_spectrum_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_spectrum_type_sk);

-- ==============================
-- Satellite: Spectrum Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_spectrum_types (
    h_spectrum_type_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp DEFAULT NOW() NOT NULL,
    data_source_id uuid DEFAULT '00000000-0000-0000-0000-000000000000'::uuid NOT NULL,
    CONSTRAINT h_property_types_pk PRIMARY KEY (h_property_type_sk),
    CONSTRAINT h_property_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_property_type_sk);

-- ==============================
-- Satellite: Property Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_property_types (
    h_property_type_sk uuid NOT NULL,
    load_dttm timestamp DEFAULT NOW() NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp DEFAULT NOW() NOT NULL,
    data_source_id uuid DEFAULT '00000000-0000-0000-0000-000000000000'::uuid NOT NULL,
    CONSTRAINT h_storage_types_pk PRIMARY KEY (h_storage_type_sk),
    CONSTRAINT h_storage_types_data_catalogue_fk 
        FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_storage_type_sk);

-- ==============================
-- Satellite: Storage Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_storage_types (
    h_storage_type_sk uuid NOT NULL,
    load_dttm timestamp DEFAULT NOW() NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_convert_types_pk PRIMARY KEY (h_convert_type_sk),
    CONSTRAINT h_convert_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_convert_type_sk);

-- ==============================
-- Satellite: Convert Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_convert_types (
    h_convert_type_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    convert_type_id int4 NOT NULL,
    description varchar NOT NULL,
    description_ru varchar NOT NULL,
    CONSTRAINT s_convert_types_h_convert_type_fk FOREIGN KEY (h_convert_type_sk) REFERENCES public.h_convert_types (h_convert_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_convert_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_convert_type_sk);

-- ==============================
-- Aggregate Configs (Hub)
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_aggregate_configs (
    h_aggregate_config_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp DEFAULT NOW() NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_aggregate_configs_pk PRIMARY KEY (h_aggregate_config_sk),
    CONSTRAINT h_aggregate_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_aggregate_config_sk);

-- ==============================
-- Aggregate Notification Configs (Hub)
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_aggregate_notification_configs (
    h_aggregate_notification_config_sk uuid NOT NULL,
    load_dttm timestamp DEFAULT NOW() NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    aggregate_id uuid NOT NULL,
    CONSTRAINT h_aggregates_pk PRIMARY KEY (h_aggregate_sk),
    CONSTRAINT h_aggregates_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_aggregate_sk);

-- ==============================
-- Satellite: Aggregates
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_aggregates (
    h_aggregate_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_annotations_pk PRIMARY KEY (h_annotation_sk),
    CONSTRAINT h_annotations_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_annotation_sk);

-- ==============================
-- Satellite: Annotations
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_annotations (
    h_annotation_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    name_graphics text NOT NULL,
    full_path text NOT NULL,
    property text NOT NULL,
    selected_interval int4 NOT NULL,
    annotation_type text NOT NULL,
    user_login text NOT NULL,
    date_create timestamp NOT NULL,
    json_annotation_settings json NOT NULL,
    CONSTRAINT s_annotations_h_annotations_fk FOREIGN KEY (h_annotation_sk) REFERENCES public.h_annotations (h_annotation_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_annotations_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_annotation_sk);

-- ==============================
-- Hub: Bearings
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_bearings (
    h_bearing_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_bearings_pk PRIMARY KEY (h_bearing_sk),
    CONSTRAINT h_bearings_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_bearing_sk);

-- ==============================
-- Satellite: Bearings
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_bearings (
    h_bearing_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    date_created timestamp,
    date_modified timestamp,
    cdb_sync_date timestamp,
    cdb_version int4 NOT NULL,
    cdb_id uuid NOT NULL,
    CONSTRAINT s_bearings_h_bearing_sk_fk FOREIGN KEY (h_bearing_sk) REFERENCES public.h_bearings (h_bearing_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_bearings_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_bearing_sk);

-- ==============================
-- Hub: DatabaseIds
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_database_ids (
    h_database_id_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    database_id_id text NOT NULL,
    main_db_id text NOT NULL,
    data_type int4 NOT NULL,
    CONSTRAINT h_database_ids_pk PRIMARY KEY (h_database_id_sk),
    CONSTRAINT h_database_ids_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_database_id_sk);

-- ==============================
-- Satellite: DatabaseIds
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_database_ids (
    h_database_id_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_diagnostic_alarms_pk PRIMARY KEY (h_diagnostic_alarm_sk),
    CONSTRAINT h_diagnostic_alarms_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_diagnostic_alarm_sk);

-- ==============================
-- Satellite: Diagnostic Alarms
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_diagnostic_alarms (
    h_diagnostic_alarm_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    date_datetime timestamp NOT NULL,
    confirmed boolean NOT NULL,
    comment text NOT NULL,
    CONSTRAINT s_diagnostic_alarms_h_diagnostic_alarm_fk FOREIGN KEY (h_diagnostic_alarm_sk) REFERENCES public.h_diagnostic_alarms (h_diagnostic_alarm_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_diagnostic_alarms_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_diagnostic_alarm_sk);

-- ==============================
-- Hub: Diags
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_diags (
    h_diag_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_diags_pk PRIMARY KEY (h_diag_sk),
    CONSTRAINT h_diags_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_diag_sk);

-- ==============================
-- Satellite: Diags
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_diags (
    h_diag_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    diag_tag_name text NOT NULL,
    defect_name text NOT NULL,
    defect_details text NOT NULL,
    recommendation text NOT NULL,
    priority int4 NOT NULL,
    group_name text NOT NULL,
    CONSTRAINT s_diags_h_diag_fk FOREIGN KEY (h_diag_sk) REFERENCES public.h_diags (h_diag_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_diags_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_diag_sk);

-- ==============================
-- Link: DiagnosticDefectState_Diag
-- ==============================

CREATE TABLE IF NOT EXISTS public.l_diagnostic_defect_states_diags (
    l_diagnostic_defect_states_diags_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_diag_sk uuid NOT NULL,
    h_diagnostic_defect_state_sk uuid NOT NULL,
    CONSTRAINT l_diagnostic_defect_states_diags_pk PRIMARY KEY (
        l_diagnostic_defect_states_diags_sk
    ),
    CONSTRAINT l_diagnostic_defect_states_diags_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_diagnostic_defect_states_diags_h_diag_fk FOREIGN KEY (h_diag_sk) REFERENCES public.h_diags (h_diag_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_diagnostic_defect_states_diags_h_diagnostic_defect_state_fk FOREIGN KEY (h_diagnostic_defect_state_sk) REFERENCES public.h_diagnostic_defect_states (h_diagnostic_defect_state_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_diagnostic_defect_states_diags_sk
);

-- ==============================
-- Link: Diags_DiagnosticAlarm
-- ==============================

CREATE TABLE IF NOT EXISTS public.l_diags_diagnostic_alarms (
    l_diags_diagnostic_alarms_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_diagnostic_alarm_sk uuid NOT NULL,
    h_diag_sk uuid NOT NULL,
    CONSTRAINT l_diags_diagnostic_alarms_pk PRIMARY KEY (l_diags_diagnostic_alarms_sk),
    CONSTRAINT l_diags_diagnostic_alarms_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_diags_diagnostic_alarms_h_diagnostic_alarm_fk FOREIGN KEY (h_diagnostic_alarm_sk) REFERENCES public.h_diagnostic_alarms (h_diagnostic_alarm_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_diags_diagnostic_alarms_h_diag_fk FOREIGN KEY (h_diag_sk) REFERENCES public.h_diags (h_diag_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (l_diags_diagnostic_alarms_sk);

-- ==============================
-- Link: DiagnosticAlarmState_DiagnosticAlarm
-- ==============================

CREATE TABLE IF NOT EXISTS public.l_diagnostic_alarm_states_diagnostic_alarms (
    l_diagnostic_alarm_states_diagnostic_alarms_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_object_properties_pk PRIMARY KEY (h_object_property_sk),
    CONSTRAINT h_object_properties_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_object_property_sk);

-- ==============================
-- Satellite: Object Properties
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_object_properties (
    h_object_property_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    is_alias bool NOT NULL,
    max_records int4 NOT NULL,
    from_template_name text NOT NULL,
    to_copy bool NOT NULL,
    save_history bool NOT NULL,
    storage_type int4 NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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

CREATE TABLE IF NOT EXISTS public.l_diagnostic_defect_types_diags (
    l_diagnostic_defect_types_diags_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_diag_sk uuid NOT NULL,
    h_diagnostic_defect_type_sk uuid NOT NULL,
    CONSTRAINT l_diagnostic_defect_types_diags_pk PRIMARY KEY (
        l_diagnostic_defect_types_diags_sk
    ),
    CONSTRAINT l_diagnostic_defect_types_diags_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_diagnostic_defect_types_diags_h_diag_fk FOREIGN KEY (h_diag_sk) REFERENCES public.h_diags (h_diag_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_diagnostic_defect_types_diags_h_diagnostic_defect_type_fk FOREIGN KEY (h_diagnostic_defect_type_sk) REFERENCES public.h_diagnostic_defect_types (h_diagnostic_defect_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_diagnostic_defect_types_diags_sk
);

-- ==============================
-- Link: Object Properties - Storage Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_object_properties_storage_types (
    l_object_properties_storage_types_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_images_pk PRIMARY KEY (h_image_sk),
    CONSTRAINT h_images_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_image_sk);

-- ==============================
-- Satellite: Images
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_images (
    h_image_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_io_device_config_nodes_pk PRIMARY KEY (h_io_device_config_node_sk),
    CONSTRAINT h_io_device_config_nodes_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_device_config_node_sk);

-- ==============================
-- Satellite: IO Device Config Nodes
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_io_device_config_nodes (
    h_io_device_config_node_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_io_device_configs_pk PRIMARY KEY (h_io_device_config_sk),
    CONSTRAINT h_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_device_config_sk);

-- ==============================
-- Satellite: IO Device Configs
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_io_device_configs (
    h_io_device_config_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    name text NOT NULL,
    enabled int4 NOT NULL,
    set_number int4 NOT NULL,
    CONSTRAINT s_io_device_configs_h_io_device_config_fk FOREIGN KEY (h_io_device_config_sk) REFERENCES public.h_io_device_configs (h_io_device_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_device_config_sk);

-- ==============================
-- Link: ConfigTypes_io_device_configs
-- ==============================

CREATE TABLE IF NOT EXISTS public.l_config_types_io_device_configs (
    l_config_types_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_model_templates_pk PRIMARY KEY (h_model_template_sk),
    CONSTRAINT h_model_templates_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_model_template_sk);

-- ==============================
-- Satellite: Model Templates
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_model_templates (
    h_model_template_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    name text NOT NULL,
    tag_name text NOT NULL,
    description text NOT NULL,
    base_template_name text NOT NULL,
    date_created timestamp,
    date_modified timestamp,
    cdb_sync_date timestamp,
    cdb_version int4 NOT NULL,
    cdb_id uuid NOT NULL,
    CONSTRAINT s_model_templates_h_model_template_fk FOREIGN KEY (h_model_template_sk) REFERENCES public.h_model_templates (h_model_template_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_model_templates_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_model_template_sk);

-- ==============================
-- Hub: Model Template Tree Nodes
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_model_template_tree_nodes (
    h_model_template_tree_node_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_model_template_tree_nodes_pk PRIMARY KEY (h_model_template_tree_node_sk),
    CONSTRAINT h_model_template_tree_nodes_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_model_template_tree_node_sk);

-- ==============================
-- Satellite: Model Template Tree Nodes
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_model_template_tree_nodes (
    h_model_template_tree_node_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_measure_convert_pk PRIMARY KEY (h_measure_convert_sk),
    CONSTRAINT h_measure_convert_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_measure_convert_sk);

-- ==============================
-- Satellite: Measure Convert
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_measure_converts (
    h_measure_convert_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_measure_groups_pk PRIMARY KEY (h_measure_group_sk),
    CONSTRAINT h_measure_groups_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_measure_group_sk);

-- ==============================
-- Satellite: Measure Groups
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_measure_groups (
    h_measure_group_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_measure_units_pk PRIMARY KEY (h_measure_unit_sk),
    CONSTRAINT h_measure_units_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_measure_unit_sk);

-- ==============================
-- Satellite: Measure Units
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_measure_units (
    h_measure_unit_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    name text NOT NULL,
    abbreviation text NOT NULL,
    CONSTRAINT s_measure_units_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_measure_units_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_measure_unit_sk);

-- ==============================
-- Link: MeasureUnits_ToID
-- ==============================

CREATE TABLE IF NOT EXISTS public.l_measure_units_to_ids (
    l_measure_units_to_ids_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_measure_unit_sk uuid NOT NULL,
    h_measure_convert_sk uuid NOT NULL,
    CONSTRAINT l_measure_units_to_ids_pk PRIMARY KEY (l_measure_units_to_ids_sk),
    CONSTRAINT l_measure_units_to_ids_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_measure_units_to_ids_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_measure_units_to_ids_h_measure_convert_fk FOREIGN KEY (h_measure_convert_sk) REFERENCES public.h_measure_converts (h_measure_convert_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (l_measure_units_to_ids_sk);

-- ==============================
-- Link: MeasureUnits_FromID
-- ==============================

CREATE TABLE IF NOT EXISTS public.l_measure_units_from_ids (
    l_measure_units_from_ids_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_measure_unit_sk uuid NOT NULL,
    h_measure_convert_sk uuid NOT NULL,
    CONSTRAINT l_measure_units_from_ids_pk PRIMARY KEY (l_measure_units_from_ids_sk),
    CONSTRAINT l_measure_units_from_ids_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_measure_units_from_ids_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_measure_units_from_ids_h_measure_convert_fk FOREIGN KEY (h_measure_convert_sk) REFERENCES public.h_measure_converts (h_measure_convert_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (l_measure_units_from_ids_sk);

-- ==============================
-- Link: MeasureUnits_MeasureGroups
-- ==============================

CREATE TABLE IF NOT EXISTS public.l_measure_units_measure_groups (
    l_measure_units_measure_groups_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
-- Hub: Object Property Descriptor Nodes
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_object_property_descriptor_nodes (
    h_object_property_descriptor_node_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    name text NOT NULL,
    description text NOT NULL,
    parent_id uuid NOT NULL,
    translated_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    name text NOT NULL,
    description text,
    tag text NOT NULL,
    max_records int4 NOT NULL,
    save_history bool NOT NULL,
    default_value text NOT NULL,
    visible_for_scada text NOT NULL,
    date_created timestamp,
    date_modified timestamp,
    cdb_sync_date timestamp,
    cdb_version int4 NOT NULL,
    translated_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_object_group_descriptors_pk PRIMARY KEY (h_object_group_descriptor_sk),
    CONSTRAINT h_object_group_descriptors_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_object_group_descriptor_sk);

-- ==============================
-- Satellite: Object Group Descriptors
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_object_group_descriptors (
    h_object_group_descriptor_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_object_groups_pk PRIMARY KEY (h_object_group_sk),
    CONSTRAINT h_object_groups_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_object_group_sk);

-- ==============================
-- Link: ObjectGroupDescriptor_Objects
-- ==============================

CREATE TABLE IF NOT EXISTS public.l_object_group_descriptors_objects (
    l_object_group_descriptors_objects_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
-- Link: MeasureUnit_ObjectPropertyDescriptors
-- ==============================

CREATE TABLE IF NOT EXISTS public.l_measure_units_object_property_descriptors (
    l_measure_units_object_property_descriptors_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_measure_unit_sk uuid NOT NULL,
    h_object_property_descriptor_sk uuid NOT NULL,
    CONSTRAINT l_measure_units_object_property_descriptors_pk PRIMARY KEY (
        l_measure_units_object_property_descriptors_sk
    ),
    CONSTRAINT l_measure_units_object_property_descriptors_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_measure_units_object_property_descriptors_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_measure_units_object_property_descriptors_h_object_property_descriptor_fk FOREIGN KEY (
        h_object_property_descriptor_sk
    ) REFERENCES public.h_object_property_descriptors (
        h_object_property_descriptor_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_measure_units_object_property_descriptors_sk
);

-- ==============================
-- Hub: Object Templates
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_object_templates (
    h_object_templates_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_object_templates_pk PRIMARY KEY (h_object_templates_sk),
    CONSTRAINT h_object_templates_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_object_templates_sk);

-- ==============================
-- Satellite: Object Templates
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_object_templates (
    h_object_templates_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_pou_user_defined_items_pk PRIMARY KEY (h_pou_user_defined_item_sk),
    CONSTRAINT h_pou_user_defined_items_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_pou_user_defined_item_sk);

-- ==============================
-- Satellite: PouUserDefinedItems
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_pou_user_defined_items (
    h_pou_user_defined_item_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    call_name text,
    body_type int4 NOT NULL,
    name text NOT NULL,
    description text NOT NULL,
    author text NOT NULL,
    xml_interface text NOT NULL,
    xml_body text NOT NULL,
    date_of_create timestamp NOT NULL,
    date_of_edit timestamp NOT NULL,
    CONSTRAINT s_pou_user_defined_items_h_pou_user_defined_item_fk FOREIGN KEY (h_pou_user_defined_item_sk) REFERENCES public.h_pou_user_defined_items (h_pou_user_defined_item_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_pou_user_defined_items_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_pou_user_defined_item_sk);

-- ==============================
-- Hub: PouUserItemsTree
-- ==============================

CREATE TABLE IF NOT EXISTS public.h_pou_user_tree_items (
    h_pou_user_tree_item_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_pou_user_tree_item_pk PRIMARY KEY (h_pou_user_tree_item_sk),
    CONSTRAINT h_pou_user_tree_item_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_pou_user_tree_item_sk);

-- ==============================
-- Satellite: PouUserItemsTree
-- ==============================

CREATE TABLE IF NOT EXISTS public.s_pou_user_tree_items (
    h_pou_user_tree_item_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_preset_chart_setting_pk PRIMARY KEY (h_preset_chart_setting_sk),
    CONSTRAINT h_preset_chart_setting_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_preset_chart_setting_sk);

-- ==============================
-- Satellite: Preset Chart Settings
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_preset_chart_settings (
    h_preset_chart_setting_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_object_rules_pk PRIMARY KEY (h_object_rule_sk),
    CONSTRAINT h_object_rules_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_object_rule_sk);

-- ==============================
-- Satellite: Object Rules
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_object_rules (
    h_object_rule_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    pou_call_name text NOT NULL,
    comment text NOT NULL,
    enabled boolean NOT NULL,
    run_level integer NOT NULL,
    from_template_name text NOT NULL,
    is_in_template boolean NOT NULL,
    tag text NOT NULL,
    running_type integer NOT NULL,
    running_period integer NOT NULL,
    CONSTRAINT s_object_rules_h_object_rule_fk FOREIGN KEY (h_object_rule_sk) REFERENCES public.h_object_rules (h_object_rule_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_object_rules_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_object_rule_sk);

-- ==============================
-- Hub: Objects
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_objects (
    h_object_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_objects_pk PRIMARY KEY (h_object_sk),
    CONSTRAINT h_objects_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_object_sk);

-- ==============================
-- Satellite: Objects
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_objects (
    h_object_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    name text NOT NULL,
    tag_name text NOT NULL,
    parent_id uuid NOT NULL,
    description text NOT NULL,
    template_name text NOT NULL,
    from_template_name text NOT NULL,
    date_created timestamp NOT NULL,
    date_modified timestamp NOT NULL,
    cdb_sync_date timestamp NOT NULL,
    cdb_version integer NOT NULL,
    cdb_id_sk uuid NOT NULL,
    CONSTRAINT s_objects_h_object_fk FOREIGN KEY (h_object_sk) REFERENCES public.h_objects (h_object_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_objects_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_object_sk);

-- ==============================
-- Link: Object Rules – Running Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_object_rules_running_types (
    l_object_rules_running_types_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
CREATE TABLE IF NOT EXISTS public.l_object_property_descriptors_objects (
    l_object_property_descriptors_objects_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    h_object_sk uuid NOT NULL,
    CONSTRAINT l_object_property_descriptors_objects_pk PRIMARY KEY (
        l_object_property_descriptors_objects_sk
    ),
    CONSTRAINT l_object_property_descriptors_objects_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_property_descriptors_objects_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_property_descriptors_objects_h_object_fk FOREIGN KEY (h_object_sk) REFERENCES public.h_objects (h_object_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_object_property_descriptors_objects_sk
);

-- ==============================
-- Hub: User Log
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_user_logs (
    h_user_log_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_user_log_pk PRIMARY KEY (h_user_log_sk),
    CONSTRAINT h_user_log_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_user_log_sk);

-- ==============================
-- Satellite: User Log
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_user_logs (
    h_user_log_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    user_id uuid NOT NULL,
    date_datetime timestamp NOT NULL,
    action_description text NOT NULL,
    CONSTRAINT s_user_log_h_user_log_fk FOREIGN KEY (h_user_log_sk) REFERENCES public.h_user_logs (h_user_log_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_user_log_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_user_log_sk);

-- ==============================
-- Link: User Log - User Action Type
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_user_logs_user_action_types (
    l_user_logs_user_action_types_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_tik_scada_log_pk PRIMARY KEY (h_tik_scada_log_sk),
    CONSTRAINT h_tik_scada_log_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_tik_scada_log_sk);

-- ==============================
-- Satellite: TIK SCADA Log
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_tik_scada_logs (
    h_tik_scada_log_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    timestamp_datetime timestamp NOT NULL,
    message text NOT NULL,
    acked boolean NOT NULL,
    acked_timestamp timestamp NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_tik_expert_slices_pk PRIMARY KEY (h_tik_expert_slice_sk),
    CONSTRAINT h_tik_expert_slices_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_tik_expert_slice_sk);

-- ==============================
-- Satellite: TIK Expert Slices
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_tik_expert_slices (
    h_tik_expert_slice_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    slice_date timestamp NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_object_type_descriptors_pk PRIMARY KEY (h_object_type_descriptor_sk),
    CONSTRAINT h_object_type_descriptors_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_object_type_descriptor_sk);

-- ==============================
-- Satellite: Object Type Descriptors
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_object_type_descriptors (
    h_object_type_descriptor_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_object_sk uuid NOT NULL,
    h_object_type_descriptor_sk uuid NOT NULL,
    from_template_name text NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_pion_route_objects_lists_pk PRIMARY KEY (h_pion_route_objects_list_sk),
    CONSTRAINT h_pion_route_objects_lists_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_pion_route_objects_list_sk);

-- ==============================
-- Satellite: Plon Route Objects Lists
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_pion_route_objects_lists (
    h_pion_route_objects_list_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    name text NOT NULL,
    description text NOT NULL,
    xml_objects_list text NOT NULL,
    date_modified timestamp NOT NULL,
    date_modified_cbd timestamp NOT NULL,
    CONSTRAINT s_pion_route_objects_lists_h_pion_route_objects_list_fk FOREIGN KEY (h_pion_route_objects_list_sk) REFERENCES public.h_pion_route_objects_lists (h_pion_route_objects_list_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_pion_route_objects_lists_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_pion_route_objects_list_sk);

-- ==============================
-- Hub: User Defined Property Lists
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_user_defined_property_lists (
    h_user_defined_property_list_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_io_creyt_channel_configs_pk PRIMARY KEY (h_io_creyt_channel_config_sk),
    CONSTRAINT h_io_creyt_channel_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_creyt_channel_config_sk);

-- ==============================
-- Satellite: IO creyt Channel Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_creyt_channel_configs (
    h_io_creyt_channel_config_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_io_creyt_channel_states_pk PRIMARY KEY (h_io_creyt_channel_state_sk),
    CONSTRAINT h_io_creyt_channel_states_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_creyt_channel_state_sk);

-- ==============================
-- Satellite: IO creyt Channel States
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_creyt_channel_states (
    h_io_creyt_channel_state_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
--     load_dttm timestamp NOT NULL,
--     data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_io_creyt_configs_pk PRIMARY KEY (h_io_creyt_config_sk),
    CONSTRAINT h_io_creyt_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_creyt_config_sk);

-- ==============================
-- Satellite: IO creyt Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_creyt_configs (
    h_io_creyt_config_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    is_sync_read_slave boolean NOT NULL,
    sync_read_group_name text NOT NULL,
    detect_failures boolean NOT NULL,
    detect_failure_length INTEGER NOT NULL,
    enabled_secondary_ip boolean NOT NULL,
    num_block_samples INTEGER NOT NULL,
    CONSTRAINT s_io_creyt_configs_h_io_creyt_config_fk FOREIGN KEY (h_io_creyt_config_sk) REFERENCES public.h_io_creyt_configs (h_io_creyt_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_io_creyt_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_creyt_config_sk);

-- ==============================
-- Link: IO creyt Configs - IO Device Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_creyt_configs_io_device_configs (
    l_io_creyt_configs_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_io_creyt_im_oper_time_pk PRIMARY KEY (h_io_creyt_im_oper_time_sk),
    CONSTRAINT h_io_creyt_im_oper_times_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_creyt_im_oper_time_sk);

-- ==============================
-- Satellite: IO Creyt IMOPerTimes
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_creyt_im_oper_times (
    h_io_creyt_im_oper_time_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    CONSTRAINT s_io_creyt_im_oper_times_h_io_creyt_im_oper_times_fk FOREIGN KEY (h_io_creyt_im_oper_time_sk) REFERENCES public.h_io_creyt_im_oper_times (h_io_creyt_im_oper_time_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_io_creyt_im_oper_times_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_creyt_im_oper_time_sk);

-- ==============================
-- Link: IO Creyt IMOPerTimes - Object Properties
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_creyt_im_oper_times_object_properties (
    l_io_creyt_im_oper_times_object_properties_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_io_lcard_channel_configs_pk PRIMARY KEY (h_io_lcard_channel_config_sk),
    CONSTRAINT h_io_lcard_channel_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_lcard_channel_config_sk);

-- ==============================
-- Satellite: IO Creyt lcard Channel Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_lcard_channel_configs (
    h_io_lcard_channel_config_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    slot integer NOT NULL,
    channel_number integer NOT NULL,
    is_scaled boolean NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_io_lcard_crate_configs_pk PRIMARY KEY (h_io_lcard_crate_config_sk),
    CONSTRAINT h_io_lcard_crate_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_lcard_crate_config_sk);

-- ==============================
-- Satellite: IO LCard Crate Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_lcard_crate_configs (
    h_io_lcard_crate_config_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_io_lcard_configs_pk PRIMARY KEY (h_io_lcard_config_sk),
    CONSTRAINT h_io_lcard_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_lcard_config_sk);

-- ==============================
-- Satellite: IO LCard Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_lcard_configs (
    h_io_lcard_config_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_io_lcard_sync_crate_pk PRIMARY KEY (h_io_lcard_sync_crate_sk),
    CONSTRAINT h_io_lcard_sync_crates_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_lcard_sync_crate_sk);

-- ==============================
-- Satellite: IO LCard Crate Sync
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_lcard_sync_crates (
    h_io_lcard_sync_crate_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    is_sync boolean NOT NULL,
    is_leader boolean NOT NULL,
    is_slave boolean NOT NULL,
    leader_id uuid NOT NULL,
    CONSTRAINT s_io_lcard_sync_crates_h_io_lcard_sync_crates_fk FOREIGN KEY (h_io_lcard_sync_crate_sk) REFERENCES public.h_io_lcard_sync_crates (h_io_lcard_sync_crate_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_io_lcard_sync_crates_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_lcard_sync_crate_sk);

-- ==============================
-- Link: IO LCard Crate Sync - IO Device Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_lcard_sync_crates_io_device_configs (
    l_io_lcard_sync_crate_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_io_lcard_input_configs_pk PRIMARY KEY (h_io_lcard_input_config_sk),
    CONSTRAINT h_io_lcard_input_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_lcard_input_config_sk);

-- ==============================
-- Satellite: IO LCard Input Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_lcard_input_configs (
    h_io_lcard_input_config_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    input_num integer NOT NULL,
    input_range integer NOT NULL,
    is_scaled boolean NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    input_number integer NOT NULL,
    input_range integer NOT NULL,
    is_scaled boolean NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_io_lcard_module_configs_pk PRIMARY KEY (h_io_lcard_module_config_sk),
    CONSTRAINT h_io_lcard_module_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_lcard_module_config_sk);

-- ==============================
-- Satellite: IO Creyt Card Module Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_lcard_module_configs (
    h_io_lcard_module_config_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_io_lcard_module_config_sk uuid NOT NULL,
    h_io_device_config_sk uuid NOT NULL,
    CONSTRAINT l_io_lcard_module_configs_io_device_configs_pk PRIMARY KEY (
        l_io_lcard_module_configs_io_device_configs_sk
    ),
    CONSTRAINT l_io_lcard_module_configs_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_lcard_module_configs_io_device_configs_h_io_lcard_module_config_fk FOREIGN KEY (h_io_lcard_module_config_sk) REFERENCES public.h_io_lcard_module_configs (h_io_lcard_module_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_lcard_module_configs_io_device_configs_h_io_device_config_fk FOREIGN KEY (h_io_device_config_sk) REFERENCES public.h_io_device_configs (h_io_device_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_lcard_module_configs_io_device_configs
);
-- ==============================
-- Link: IO Creyt Card Module Configs - L Card Crate Module Types
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_lcard_module_configs_l_card_crate_module_types (
    l_io_lcard_module_configs_l_card_crate_module_types_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    register_address INTEGER NOT NULL,
    byte_order text NOT NULL,
    min_raw float NOT NULL,
    max_raw float NOT NULL,
    min_eu float NOT NULL,
    max_eu float NOT NULL,
    is_scaled boolean NOT NULL,
    factor float NOT NULL,
    is_write boolean NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    register_bit integer NOT NULL,
    bit_number integer NOT NULL,
    is_write boolean NOT NULL,
    write_property_id uuid NOT NULL,
    mask_inversion boolean NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
CREATE TABLE IF NOT EXISTS public.l_io_modbus_tcp_bit_decompression_configs_io_device_configs (
    l_io_modbus_tcp_bit_decompression_configs_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_io_modbus_tcp_bit_decompression_config_sk uuid NOT NULL,
    h_io_device_config_sk uuid NOT NULL,
    CONSTRAINT l_io_modbus_tcp_bit_decompression_configs_io_device_configs_pk PRIMARY KEY (
        l_io_modbus_tcp_bit_decompression_configs_io_device_configs_sk
    ),
    CONSTRAINT l_io_modbus_tcp_bit_decompression_configs_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_modbus_tcp_bit_decompression_configs_io_deviceconfigs_h_io_modbus_tcp_bit_decompression_config_fk FOREIGN KEY (
        h_io_modbus_tcp_bit_decompression_config_sk
    ) REFERENCES public.h_io_modbus_tcp_bit_decompression_configs (
        h_io_modbus_tcp_bit_decompression_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_modbus_tcp_bit_decompression_configs_io_device_configs_h_io_device_config_fk FOREIGN KEY (h_io_device_config_sk) REFERENCES public.h_io_device_configs (h_io_device_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_modbus_tcp_bit_decompression_configs_io_device_configs_sk
);

-- ==============================
-- Link: IO Modbus TCP Register Configs - Register Type Enum
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_modbus_tcp_register_configs_register_types (
    l_io_modbus_tcp_register_configs_register_types_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
CREATE TABLE IF NOT EXISTS public.l_io_modbus_tcp_register_configs_type_names (
    l_io_modbus_tcp_register_configs_type_names_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_io_modbus_tcp_register_config_sk uuid NOT NULL,
    h_type_name_sk uuid NOT NULL,
    CONSTRAINT l_io_modbus_tcp_register_configs_type_names_pk PRIMARY KEY (
        l_io_modbus_tcp_register_configs_type_names_sk
    ),
    CONSTRAINT l_io_modbus_tcp_register_configs_type_names_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_modbus_tcp_register_configs_type_names_h_io_modbus_tcp_register_config_fk FOREIGN KEY (
        h_io_modbus_tcp_register_config_sk
    ) REFERENCES public.h_io_modbus_tcp_register_configs (
        h_io_modbus_tcp_register_config_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_modbus_tcp_register_configs_type_names_h_type_name_fk FOREIGN KEY (h_type_name_sk) REFERENCES public.h_type_names (h_type_name_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_modbus_tcp_register_configs_type_names_sk
);

-- ==============================
-- Hub: IO Modbus TCP Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_io_modbus_tcp_configs (
    h_io_modbus_tcp_config_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_io_modbus_tcp_configs_pk PRIMARY KEY (h_io_modbus_tcp_config_sk),
    CONSTRAINT h_io_modbus_tcp_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_modbus_tcp_config_sk);

-- ==============================
-- Satellite: IO Modbus TCP Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_modbus_tcp_configs (
    h_io_modbus_tcp_config_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    device_id integer NOT NULL,
    primary_ip text NOT NULL,
    secondary_ip text NOT NULL,
    primary_port integer NOT NULL,
    secondary_port integer NOT NULL,
    scan_time integer NOT NULL,
    input_status_max integer NOT NULL,
    holding_register_max integer NOT NULL,
    input_register_max integer NOT NULL,
    connected_retries integer NOT NULL,
    secondary_enabled boolean NOT NULL,
    device_type integer NOT NULL,
    connect_to_primary_available integer NOT NULL,
    sync_read_group_name text NOT NULL,
    interface_type integer NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
-- Hub: IO MQTT Dev EUI Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_io_mqtt_dev_eui_configs (
    h_io_mqtt_dev_eui_config_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_io_mqtt_dev_eui_configs_pk PRIMARY KEY (h_io_mqtt_dev_eui_config_sk),
    CONSTRAINT h_io_mqtt_dev_eui_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_mqtt_dev_eui_config_sk);

-- ==============================
-- Satellite: IO MQTT Dev EUI Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_mqtt_dev_eui_configs (
    h_io_mqtt_dev_eui_config_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    topic text NOT NULL,
    dev_eui text NOT NULL,
    parser integer NOT NULL,
    version integer NOT NULL,
    CONSTRAINT s_io_mqtt_dev_eui_configs_h_io_mqtt_dev_eui_config_fk FOREIGN KEY (h_io_mqtt_dev_eui_config_sk) REFERENCES public.h_io_mqtt_dev_eui_configs (h_io_mqtt_dev_eui_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_io_mqtt_dev_eui_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_mqtt_dev_eui_config_sk);

-- ==============================
-- Link: IO MQTT Dev EUI Configs - Measure Units
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_mqtt_dev_eui_configs_measure_units (
    l_io_mqtt_dev_eui_configs_measure_units_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_io_mqtt_dev_eui_config_sk uuid NOT NULL,
    h_measure_unit_sk uuid NOT NULL,
    CONSTRAINT l_io_mqtt_dev_eui_configs_measure_units_pk PRIMARY KEY (
        l_io_mqtt_dev_eui_configs_measure_units_sk
    ),
    CONSTRAINT l_io_mqtt_dev_eui_configs_measure_units_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_mqtt_dev_eui_configs_measure_units_h_io_mqtt_dev_eui_config_fk FOREIGN KEY (h_io_mqtt_dev_eui_config_sk) REFERENCES public.h_io_mqtt_dev_eui_configs (h_io_mqtt_dev_eui_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_mqtt_dev_eui_configs_measure_units_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_mqtt_dev_eui_configs_measure_units_sk
);

-- ==============================
-- Link: IO MQTT Dev EUI Configs - Object Properties
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_mqtt_dev_eui_configs_object_properties (
    l_io_mqtt_dev_eui_configs_object_properties_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_io_mqtt_dev_eui_config_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_io_mqtt_dev_eui_configs_object_properties_pk PRIMARY KEY (
        l_io_mqtt_dev_eui_configs_object_properties_sk
    ),
    CONSTRAINT l_io_mqtt_dev_eui_configs_object_properties_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_mqtt_dev_eui_configs_object_properties_h_io_mqtt_dev_eui_config_fk FOREIGN KEY (h_io_mqtt_dev_eui_config_sk) REFERENCES public.h_io_mqtt_dev_eui_configs (h_io_mqtt_dev_eui_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_mqtt_dev_eui_configs_object_properties_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_mqtt_dev_eui_configs_object_properties_sk
);

-- ==============================
-- Link: IO MQTT Dev EUI Configs - IO Device Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_mqtt_dev_eui_configs_io_device_configs (
    l_io_mqtt_dev_eui_configs_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_io_mqtt_dev_eui_config_sk uuid NOT NULL,
    h_io_device_config_sk uuid NOT NULL,
    CONSTRAINT l_io_mqtt_dev_eui_configs_io_device_configs_pk PRIMARY KEY (
        l_io_mqtt_dev_eui_configs_io_device_configs_sk
    ),
    CONSTRAINT l_io_mqtt_dev_eui_configs_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_mqtt_dev_eui_configs_io_device_configs_h_io_mqtt_dev_eui_config_fk FOREIGN KEY (h_io_mqtt_dev_eui_config_sk) REFERENCES public.h_io_mqtt_dev_eui_configs (h_io_mqtt_dev_eui_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_mqtt_dev_eui_configs_io_device_configs_h_io_device_config_fk FOREIGN KEY (h_io_device_config_sk) REFERENCES public.h_io_device_configs (h_io_device_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_mqtt_dev_eui_configs_io_device_configs_sk
);

-- ==============================
-- Link: IO MQTT Dev EUI Configs - Index Type Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_mqtt_dev_eui_configs_index_type_data_records (
    l_io_mqtt_dev_eui_configs_index_type_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_io_mqtt_dev_eui_config_sk uuid NOT NULL,
    h_index_type_data_record_sk uuid NOT NULL,
    CONSTRAINT l_io_mqtt_dev_eui_configs_index_type_data_records_pk PRIMARY KEY (
        l_io_mqtt_dev_eui_configs_index_type_data_records_sk
    ),
    CONSTRAINT l_io_mqtt_dev_eui_configs_index_type_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_mqtt_dev_eui_configs_index_type_data_records_h_io_mqtt_dev_eui_config_fk FOREIGN KEY (h_io_mqtt_dev_eui_config_sk) REFERENCES public.h_io_mqtt_dev_eui_configs (h_io_mqtt_dev_eui_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_mqtt_dev_eui_configs_index_type_data_records_h_index_type_data_record_fk FOREIGN KEY (h_index_type_data_record_sk) REFERENCES public.h_index_type_data_records (h_index_type_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_mqtt_dev_eui_configs_index_type_data_records_sk
);

-- ==============================
-- Hub: IO MQTT Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_io_mqtt_configs (
    h_io_mqtt_config_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_io_mqtt_configs_pk PRIMARY KEY (h_io_mqtt_config_sk),
    CONSTRAINT h_io_mqtt_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_mqtt_config_sk);

-- ==============================
-- Satellite: IO MQTT Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_mqtt_configs (
    h_io_mqtt_config_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    address text NOT NULL,
    client_id text NOT NULL,
    login text NOT NULL,
    password text NOT NULL,
    tls boolean NOT NULL,
    clean_session boolean NOT NULL,
    CONSTRAINT s_io_mqtt_configs_h_io_mqtt_config_fk FOREIGN KEY (h_io_mqtt_config_sk) REFERENCES public.h_io_mqtt_configs (h_io_mqtt_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_io_mqtt_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_mqtt_config_sk);

-- ==============================
-- Link: IO MQTT Configs - IO Device Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_mqtt_configs_io_device_configs (
    l_io_mqtt_configs_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_io_mqtt_config_sk uuid NOT NULL,
    h_io_device_config_sk uuid NOT NULL,
    CONSTRAINT l_io_mqtt_configs_io_device_configs_pk PRIMARY KEY (
        l_io_mqtt_configs_io_device_configs_sk
    ),
    CONSTRAINT l_io_mqtt_configs_io_device_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_io_mqtt_configs_io_device_configs_h_io_mqtt_config_fk FOREIGN KEY (h_io_mqtt_config_sk) REFERENCES public.h_io_mqtt_configs (h_io_mqtt_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_io_mqtt_configs_io_device_configs_h_io_device_config_fk FOREIGN KEY (h_io_device_config_sk) REFERENCES public.h_io_device_configs (h_io_device_config_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_io_mqtt_configs_io_device_configs_sk
);

-- ==============================
-- Hub: IO OPC DA Client Item Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_io_opc_da_client_item_configs (
    h_io_opc_da_client_item_config_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    item_id text NOT NULL,
    group_name text NOT NULL,
    is_active boolean NOT NULL,
    data_type integer NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_io_opc_da_client_configs_pk PRIMARY KEY (h_io_opc_da_client_config_sk),
    CONSTRAINT h_io_opc_da_client_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_opc_da_client_config_sk);

-- ==============================
-- Satellite: IO OPC DA Client Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_opc_da_client_configs (
    h_io_opc_da_client_config_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    name text NOT NULL,
    is_active boolean NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    full_path_name text NOT NULL,
    sample_item_id text NOT NULL,
    min_raw real NOT NULL,
    max_raw real NOT NULL,
    min_eu real NOT NULL,
    max_eu real NOT NULL,
    is_scale boolean NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_io_opc_ua_client_configs_pk PRIMARY KEY (h_io_opc_ua_client_config_sk),
    CONSTRAINT h_io_opc_ua_client_configs_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_io_opc_ua_client_config_sk);

-- ==============================
-- Satellite: IO OPC UA Client Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_io_opc_ua_client_configs (
    h_io_opc_ua_client_config_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    name text NOT NULL,
    is_active boolean NOT NULL,
    update_rate integer NOT NULL,
    is_subscription boolean NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    item_id text NOT NULL,
    group_name text NOT NULL,
    is_active boolean NOT NULL,
    data_type integer NOT NULL,
    full_path_name text,
    to_server boolean NOT NULL,
    data_type_server integer NOT NULL,
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
-- Link: IO OPC UA Client Item Configs - IO Device Configs
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_io_opc_ua_client_item_configs_io_device_configs (
    l_io_opc_ua_client_item_configs_io_device_configs_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
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
-- Hub: Object Data Values
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_object_data_values (
    h_object_data_value_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_object_data_values_pk PRIMARY KEY (h_object_data_value_sk),
    CONSTRAINT h_object_data_values_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_object_data_value_sk);

-- ==============================
-- Satellite: Object Data Values
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_object_data_values (
    h_object_data_value_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    date_time timestamp NOT NULL,
    quality integer NOT NULL,
    comment text NOT NULL,
    value bytea NOT NULL,
    CONSTRAINT s_object_data_values_h_object_data_value_fk FOREIGN KEY (h_object_data_value_sk) REFERENCES public.h_object_data_values (h_object_data_value_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_object_data_values_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_object_data_value_sk);

-- ==============================
-- Link: Object Data Values - Measure Units
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_object_data_values_measure_units (
    l_object_data_values_measure_units_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_object_data_value_sk uuid NOT NULL,
    h_measure_unit_sk uuid NOT NULL,
    CONSTRAINT l_object_data_values_measure_units_pk PRIMARY KEY (
        l_object_data_values_measure_units_sk
    ),
    CONSTRAINT l_object_data_values_measure_units_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_data_values_measure_units_h_object_data_value_fk FOREIGN KEY (h_object_data_value_sk) REFERENCES public.h_object_data_values (h_object_data_value_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_data_values_measure_units_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_object_data_values_measure_units_sk
);

-- ==============================
-- Link: Object Data Values - Object Properties
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_object_data_values_object_properties (
    l_object_data_values_object_properties_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_object_data_value_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_object_data_values_object_properties_pk PRIMARY KEY (
        l_object_data_values_object_properties_sk
    ),
    CONSTRAINT l_object_data_values_object_properties_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_data_values_object_properties_h_object_data_value_fk FOREIGN KEY (h_object_data_value_sk) REFERENCES public.h_object_data_values (h_object_data_value_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_data_values_object_properties_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_object_data_values_object_properties_sk
);

-- ==============================
-- Link: Object Data Values - Data Type
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_object_data_values_data_types (
    l_object_data_values_data_types_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_object_data_value_sk uuid NOT NULL,
    h_data_type_sk uuid NOT NULL,
    CONSTRAINT l_object_data_values_data_types_pk PRIMARY KEY (
        l_object_data_values_data_types_sk
    ),
    CONSTRAINT l_object_data_values_data_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_data_values_data_types_h_object_data_value_fk FOREIGN KEY (h_object_data_value_sk) REFERENCES public.h_object_data_values (h_object_data_value_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_data_values_data_types_h_data_type_fk FOREIGN KEY (h_data_type_sk) REFERENCES public.h_data_types (h_data_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_object_data_values_data_types_sk
);

-- ==============================
-- Hub: Any Double Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_any_double_data_records (
    h_any_double_data_record_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_any_double_data_record_pk PRIMARY KEY (h_any_double_data_record_sk),
    CONSTRAINT h_any_double_data_record_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_any_double_data_record_sk);

-- ==============================
-- Satellite: Any Double Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_any_double_data_records (
    h_any_double_data_record_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    date_time timestamp NOT NULL,
    value bytea NOT NULL,
    CONSTRAINT s_any_double_data_record_h_any_double_data_record_fk FOREIGN KEY (h_any_double_data_record_sk) REFERENCES public.h_any_double_data_records (h_any_double_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_any_double_data_record_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_any_double_data_record_sk);

-- ==============================
-- Link: Any Double Data - Data Type
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_any_double_data_records_data_types (
    l_any_double_data_records_data_types_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_any_double_data_record_sk uuid NOT NULL,
    h_data_type_sk uuid NOT NULL,
    CONSTRAINT l_any_double_data_records_data_types_pk PRIMARY KEY (
        l_any_double_data_records_data_types_sk
    ),
    CONSTRAINT l_any_double_data_records_data_types_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_any_double_data_records_data_types_h_any_double_data_record_fk FOREIGN KEY (h_any_double_data_record_sk) REFERENCES public.h_any_double_data_records (h_any_double_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_any_double_data_records_data_types_h_data_type_fk FOREIGN KEY (h_data_type_sk) REFERENCES public.h_data_types (h_data_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_any_double_data_records_data_types_sk
);

-- ==============================
-- Link: Any Double Data - Object Properties
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_any_double_data_records_object_properties (
    l_any_double_data_records_object_properties_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_any_double_data_record_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_any_double_data_records_object_properties_pk PRIMARY KEY (
        l_any_double_data_records_object_properties_sk
    ),
    CONSTRAINT l_any_double_data_records_object_properties_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_any_double_data_records_object_properties_h_any_double_data_record_fk FOREIGN KEY (h_any_double_data_record_sk) REFERENCES public.h_any_double_data_records (h_any_double_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_any_double_data_records_object_properties_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_any_double_data_records_object_properties_sk
);

-- ==============================
-- Hub: Bode Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_bode_data_records (
    h_bode_data_record_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_bode_data_record_pk PRIMARY KEY (h_bode_data_record_sk),
    CONSTRAINT h_bode_data_record_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_bode_data_record_sk);

-- ==============================
-- Satellite: Bode Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_bode_data_records (
    h_bode_data_record_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    -- Поля из схемы
    DataTime timestamp NOT NULL,
    Quality integer NOT NULL,
    Comment text NOT NULL,
    MagnitudeValue double precision NOT NULL,
    PhaseValue double precision NOT NULL,
    TurnoverFrequencyValue double precision NOT NULL,
    ComparisonPosition integer NOT NULL,
    isDeleted boolean NOT NULL,
    CONSTRAINT s_bode_data_record_h_bode_data_record_fk FOREIGN KEY (h_bode_data_record_sk) REFERENCES public.h_bode_data_records (h_bode_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_bode_data_record_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_bode_data_record_sk);

-- ==============================
-- Link: Measure Units - Bode Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_measure_units_bode_data_records (
    l_measure_units_bode_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_bode_data_record_sk uuid NOT NULL,
    h_measure_unit_sk uuid NOT NULL,
    CONSTRAINT l_measure_units_bode_data_records_pk PRIMARY KEY (
        l_measure_units_bode_data_records_sk
    ),
    CONSTRAINT l_measure_units_bode_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_measure_units_bode_data_records_h_bode_data_record_fk FOREIGN KEY (h_bode_data_record_sk) REFERENCES public.h_bode_data_records (h_bode_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_measure_units_bode_data_records_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_measure_units_bode_data_records_sk
);

-- ==============================
-- Link: Object Properties - Bode Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_object_properties_bode_data_records (
    l_object_properties_bode_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_bode_data_record_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_object_properties_bode_data_records_pk PRIMARY KEY (
        l_object_properties_bode_data_records_sk
    ),
    CONSTRAINT l_object_properties_bode_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_properties_bode_data_records_h_bode_data_record_fk FOREIGN KEY (h_bode_data_record_sk) REFERENCES public.h_bode_data_records (h_bode_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_properties_bode_data_records_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_object_properties_bode_data_records_sk
);

-- ==============================
-- Hub: Boolean Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_boolean_data_records (
    h_boolean_data_record_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_boolean_data_record_pk PRIMARY KEY (h_boolean_data_record_sk),
    CONSTRAINT h_boolean_data_record_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_boolean_data_record_sk);

-- ==============================
-- Satellite: Boolean Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_boolean_data_records (
    h_boolean_data_record_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    -- Поля из схемы
    DataTime timestamp NOT NULL,
    Quality integer NOT NULL,
    Comment text NOT NULL,
    Value boolean NOT NULL,
    isDeleted boolean NOT NULL,
    CONSTRAINT s_boolean_data_record_h_boolean_data_record_fk FOREIGN KEY (h_boolean_data_record_sk) REFERENCES public.h_boolean_data_records (h_boolean_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_boolean_data_record_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_boolean_data_record_sk);

-- ==============================
-- Link: Measure Units - Boolean Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_measure_units_boolean_data_records (
    l_measure_units_boolean_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_boolean_data_record_sk uuid NOT NULL,
    h_measure_unit_sk uuid NOT NULL,
    CONSTRAINT l_measure_units_boolean_data_records_pk PRIMARY KEY (
        l_measure_units_boolean_data_records_sk
    ),
    CONSTRAINT l_measure_units_boolean_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_measure_units_boolean_data_records_h_boolean_data_record_fk FOREIGN KEY (h_boolean_data_record_sk) REFERENCES public.h_boolean_data_records (h_boolean_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_measure_units_boolean_data_records_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_measure_units_boolean_data_records_sk
);

-- ==============================
-- Link: Object Properties - Boolean Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_object_properties_boolean_data_records (
    l_object_properties_boolean_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_boolean_data_record_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_object_properties_boolean_data_records_pk PRIMARY KEY (
        l_object_properties_boolean_data_records_sk
    ),
    CONSTRAINT l_object_properties_boolean_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_properties_boolean_data_records_h_boolean_data_record_fk FOREIGN KEY (h_boolean_data_record_sk) REFERENCES public.h_boolean_data_records (h_boolean_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_properties_boolean_data_records_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_object_properties_boolean_data_records_sk
);

-- ==============================
-- Hub: Date Sample Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_date_sample_data_records (
    h_date_sample_data_record_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_date_sample_data_record_pk PRIMARY KEY (h_date_sample_data_record_sk),
    CONSTRAINT h_date_sample_data_record_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_date_sample_data_record_sk);

-- ==============================
-- Satellite: Date Sample Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_date_sample_data_records (
    h_date_sample_data_record_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    -- Поля из схемы
    DataID uuid NOT NULL,
    DateTime timestamp NOT NULL,
    Quality integer NOT NULL,
    Comment text NOT NULL,
    SampleRate double precision NOT NULL,
    RawValues double precision[] NOT NULL, -- Используем массив для хранения нескольких значений
    isDeleted boolean NOT NULL,
    CONSTRAINT s_date_sample_data_record_h_date_sample_data_record_fk
        FOREIGN KEY (h_date_sample_data_record_sk) REFERENCES public.h_date_sample_data_records(h_date_sample_data_record_sk)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_date_sample_data_record_data_catalogue_fk
        FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_date_sample_data_record_sk);

-- ==============================
-- Link: Measure Units - Date Sample Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_measure_units_date_sample_data_records (
    l_measure_units_date_sample_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_date_sample_data_record_sk uuid NOT NULL,
    h_measure_unit_sk uuid NOT NULL,
    CONSTRAINT l_measure_units_date_sample_data_records_pk PRIMARY KEY (
        l_measure_units_date_sample_data_records_sk
    ),
    CONSTRAINT l_measure_units_date_sample_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_measure_units_date_sample_data_records_h_date_sample_data_record_fk FOREIGN KEY (h_date_sample_data_record_sk) REFERENCES public.h_date_sample_data_records (h_date_sample_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_measure_units_date_sample_data_records_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_measure_units_date_sample_data_records_sk
);

-- ==============================
-- Link: Object Properties - Date Sample Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_object_properties_date_sample_data_records (
    l_object_properties_date_sample_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_date_sample_data_record_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_object_properties_date_sample_data_records_pk PRIMARY KEY (
        l_object_properties_date_sample_data_records_sk
    ),
    CONSTRAINT l_object_properties_date_sample_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_properties_date_sample_data_records_h_date_sample_data_record_fk FOREIGN KEY (h_date_sample_data_record_sk) REFERENCES public.h_date_sample_data_records (h_date_sample_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_properties_date_sample_data_records_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_object_properties_date_sample_data_records_sk
);

-- ==============================
-- Hub: Date Time Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_date_time_data_records (
    h_date_time_data_record_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_date_time_data_record_pk PRIMARY KEY (h_date_time_data_record_sk),
    CONSTRAINT h_date_time_data_record_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_date_time_data_record_sk);

-- ==============================
-- Satellite: Date Time Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_date_time_data_records (
    h_date_time_data_record_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    -- Поля из схемы
    DataID uuid NOT NULL,
    DateTime timestamp NOT NULL,
    Quality integer NOT NULL,
    Comment text NOT NULL,
    Value timestamp NOT NULL, -- В PostgreSQL тип TIMESTAMP используется для хранения даты и времени.
    isDeleted boolean NOT NULL,
    CONSTRAINT s_date_time_data_record_h_date_time_data_record_fk FOREIGN KEY (h_date_time_data_record_sk) REFERENCES public.h_date_time_data_records (h_date_time_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_date_time_data_record_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_date_time_data_record_sk);

-- ==============================
-- Link: Measure Units - Date Time Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_measure_units_date_time_data_records (
    l_measure_units_date_time_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_date_time_data_record_sk uuid NOT NULL,
    h_measure_unit_sk uuid NOT NULL,
    CONSTRAINT l_measure_units_date_time_data_records_pk PRIMARY KEY (
        l_measure_units_date_time_data_records_sk
    ),
    CONSTRAINT l_measure_units_date_time_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_measure_units_date_time_data_records_h_date_time_data_record_fk FOREIGN KEY (h_date_time_data_record_sk) REFERENCES public.h_date_time_data_records (h_date_time_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_measure_units_date_time_data_records_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_measure_units_date_time_data_records_sk
);

-- ==============================
-- Link: Object Properties - Date Time Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_object_properties_date_time_data_records (
    l_object_properties_date_time_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_date_time_data_record_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_object_properties_date_time_data_records_pk PRIMARY KEY (
        l_object_properties_date_time_data_records_sk
    ),
    CONSTRAINT l_object_properties_date_time_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_properties_date_time_data_records_h_date_time_data_record_fk FOREIGN KEY (h_date_time_data_record_sk) REFERENCES public.h_date_time_data_records (h_date_time_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_properties_date_time_data_records_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_object_properties_date_time_data_records_sk
);

-- ==============================
-- Hub: Date Time Array Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_date_time_array_data_records (
    h_date_time_array_data_record_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_date_time_array_data_record_pk PRIMARY KEY (
        h_date_time_array_data_record_sk
    ),
    CONSTRAINT h_date_time_array_data_record_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_date_time_array_data_record_sk
);

-- ==============================
-- Satellite: Date Time Array Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_date_time_array_data_records (
    h_date_time_array_data_record_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    -- Поля из схемы
    DataID uuid NOT NULL,
    DateTime timestamp NOT NULL,
    Quality integer NOT NULL,
    Comment text NOT NULL,
    Value timestamp[] NOT NULL, -- Используем массив для хранения нескольких значений даты и времени
    isDeleted boolean NOT NULL,
    CONSTRAINT s_date_time_array_data_record_h_date_time_array_data_record_fk
        FOREIGN KEY (h_date_time_array_data_record_sk) REFERENCES public.h_date_time_array_data_records(h_date_time_array_data_record_sk)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_date_time_array_data_record_data_catalogue_fk
        FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_date_time_array_data_record_sk);

-- ==============================
-- Link: Measure Units - Date Time Array Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_measure_units_date_time_array_data_records (
    l_measure_units_date_time_array_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_date_time_array_data_record_sk uuid NOT NULL,
    h_measure_unit_sk uuid NOT NULL,
    CONSTRAINT l_measure_units_date_time_array_data_records_pk PRIMARY KEY (
        l_measure_units_date_time_array_data_records_sk
    ),
    CONSTRAINT l_measure_units_date_time_array_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_measure_units_date_time_array_data_records_h_date_time_array_data_record_fk FOREIGN KEY (
        h_date_time_array_data_record_sk
    ) REFERENCES public.h_date_time_array_data_records (
        h_date_time_array_data_record_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_measure_units_date_time_array_data_records_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_measure_units_date_time_array_data_records_sk
);

-- ==============================
-- Link: Object Properties - Date Time Array Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_object_properties_date_time_array_data_records (
    l_object_properties_date_time_array_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_date_time_array_data_record_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_object_properties_date_time_array_data_records_pk PRIMARY KEY (
        l_object_properties_date_time_array_data_records_sk
    ),
    CONSTRAINT l_object_properties_date_time_array_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_properties_date_time_array_data_records_h_date_time_array_data_record_fk FOREIGN KEY (
        h_date_time_array_data_record_sk
    ) REFERENCES public.h_date_time_array_data_records (
        h_date_time_array_data_record_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_properties_date_time_array_data_records_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_object_properties_date_time_array_data_records_sk
);

-- ==============================
-- Hub: Diagnostic Array Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_diagnostic_array_data_records (
    h_diagnostic_array_data_record_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_diagnostic_array_data_record_pk PRIMARY KEY (
        h_diagnostic_array_data_record_sk
    ),
    CONSTRAINT h_diagnostic_array_data_record_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_diagnostic_array_data_record_sk
);

-- ==============================
-- Satellite: Diagnostic Array Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_diagnostic_array_data_records (
    h_diagnostic_array_data_record_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    -- Поля из схемы
    DataID uuid NOT NULL,
    DateTime timestamp NOT NULL,
    Quality integer NOT NULL,
    Comment text NOT NULL,
    Value text[] NOT NULL, -- Используем массив текста для хранения диагностики
    isDeleted boolean NOT NULL,
    CONSTRAINT s_diagnostic_array_data_record_h_diagnostic_array_data_record_fk
        FOREIGN KEY (h_diagnostic_array_data_record_sk) REFERENCES public.h_diagnostic_array_data_records(h_diagnostic_array_data_record_sk)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_diagnostic_array_data_record_data_catalogue_fk
        FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_diagnostic_array_data_record_sk);

-- ==============================
-- Link: Measure Units - Diagnostic Array Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_measure_units_diagnostic_array_data_records (
    l_measure_units_diagnostic_array_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_diagnostic_array_data_record_sk uuid NOT NULL,
    h_measure_unit_sk uuid NOT NULL,
    CONSTRAINT l_measure_units_diagnostic_array_data_records_pk PRIMARY KEY (
        l_measure_units_diagnostic_array_data_records_sk
    ),
    CONSTRAINT l_measure_units_diagnostic_array_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_measure_units_diagnostic_array_data_records_h_diagnostic_array_data_record_fk FOREIGN KEY (
        h_diagnostic_array_data_record_sk
    ) REFERENCES public.h_diagnostic_array_data_records (
        h_diagnostic_array_data_record_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_measure_units_diagnostic_array_data_records_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_measure_units_diagnostic_array_data_records_sk
);

-- ==============================
-- Link: Object Properties - Diagnostic Array Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_object_properties_diagnostic_array_data_records (
    l_object_properties_diagnostic_array_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_diagnostic_array_data_record_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_object_properties_diagnostic_array_data_records_pk PRIMARY KEY (
        l_object_properties_diagnostic_array_data_records_sk
    ),
    CONSTRAINT l_object_properties_diagnostic_array_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_properties_diagnostic_array_data_records_h_diagnostic_array_data_record_fk FOREIGN KEY (
        h_diagnostic_array_data_record_sk
    ) REFERENCES public.h_diagnostic_array_data_records (
        h_diagnostic_array_data_record_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_properties_diagnostic_array_data_records_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_object_properties_diagnostic_array_data_records_sk
);

-- ==============================
-- Hub: Duration Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_duration_data_records (
    h_duration_data_record_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_duration_data_record_pk PRIMARY KEY (h_duration_data_record_sk),
    CONSTRAINT h_duration_data_record_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_duration_data_record_sk);

-- ==============================
-- Satellite: Duration Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_duration_data_records (
    h_duration_data_record_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    -- Поля из схемы
    DataID uuid NOT NULL,
    DateTime timestamp NOT NULL,
    Quality integer NOT NULL,
    Comment text NOT NULL,
    Value interval NOT NULL, -- Тип INTERVAL используется для хранения временных промежутков.
    isDeleted boolean NOT NULL,
    CONSTRAINT s_duration_data_record_h_duration_data_record_fk FOREIGN KEY (h_duration_data_record_sk) REFERENCES public.h_duration_data_records (h_duration_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_duration_data_record_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_duration_data_record_sk);

-- ==============================
-- Link: Measure Units - Duration Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_measure_units_duration_data_records (
    l_measure_units_duration_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_duration_data_record_sk uuid NOT NULL,
    h_measure_unit_sk uuid NOT NULL,
    CONSTRAINT l_measure_units_duration_data_records_pk PRIMARY KEY (
        l_measure_units_duration_data_records_sk
    ),
    CONSTRAINT l_measure_units_duration_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_measure_units_duration_data_records_h_duration_data_record_fk FOREIGN KEY (h_duration_data_record_sk) REFERENCES public.h_duration_data_records (h_duration_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_measure_units_duration_data_records_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_measure_units_duration_data_records_sk
);

-- ==============================
-- Link: Object Properties - Duration Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_object_properties_duration_data_records (
    l_object_properties_duration_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_duration_data_record_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_object_properties_duration_data_records_pk PRIMARY KEY (
        l_object_properties_duration_data_records_sk
    ),
    CONSTRAINT l_object_properties_duration_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_properties_duration_data_records_h_duration_data_record_fk FOREIGN KEY (h_duration_data_record_sk) REFERENCES public.h_duration_data_records (h_duration_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_properties_duration_data_records_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_object_properties_duration_data_records_sk
);

-- ==============================
-- Hub: File Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_file_data_records (
    h_file_data_record_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_file_data_record_pk PRIMARY KEY (h_file_data_record_sk),
    CONSTRAINT h_file_data_record_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_file_data_record_sk);

-- ==============================
-- Satellite: File Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_file_data_records (
    h_file_data_record_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    -- Поля из схемы
    DataID uuid NOT NULL,
    DateTime timestamp NOT NULL,
    Quality integer NOT NULL,
    Comment text NOT NULL,
    Value text NOT NULL, -- В схеме указано TEXT, что подходит для хранения пути к файлу.
    FilePath text NOT NULL,
    File BYTEA NOT NULL, -- BLOB → в PostgreSQL это bytea
    Checksum text NOT NULL,
    isDeleted boolean NOT NULL,
    CONSTRAINT s_file_data_record_h_file_data_record_fk FOREIGN KEY (h_file_data_record_sk) REFERENCES public.h_file_data_records (h_file_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_file_data_record_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_file_data_record_sk);

-- ==============================
-- Link: Measure Units - File Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_measure_units_file_data_records (
    l_measure_units_file_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_file_data_record_sk uuid NOT NULL,
    h_measure_unit_sk uuid NOT NULL,
    CONSTRAINT l_measure_units_file_data_records_pk PRIMARY KEY (
        l_measure_units_file_data_records_sk
    ),
    CONSTRAINT l_measure_units_file_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_measure_units_file_data_records_h_file_data_record_fk FOREIGN KEY (h_file_data_record_sk) REFERENCES public.h_file_data_records (h_file_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_measure_units_file_data_records_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_measure_units_file_data_records_sk
);

-- ==============================
-- Link: Object Properties - File Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_object_properties_file_data_records (
    l_object_properties_file_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_file_data_record_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_object_properties_file_data_records_pk PRIMARY KEY (
        l_object_properties_file_data_records_sk
    ),
    CONSTRAINT l_object_properties_file_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_properties_file_data_records_h_file_data_record_fk FOREIGN KEY (h_file_data_record_sk) REFERENCES public.h_file_data_records (h_file_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_properties_file_data_records_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_object_properties_file_data_records_sk
);

-- ==============================
-- Hub: Float64 Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_float64_data_records (
    h_float64_data_record_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_float64_data_record_pk PRIMARY KEY (h_float64_data_record_sk),
    CONSTRAINT h_float64_data_record_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_float64_data_record_sk);

-- ==============================
-- Satellite: Float64 Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_float64_data_records (
    h_float64_data_record_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    -- Поля из схемы
    DataID uuid NOT NULL,
    DateTime timestamp NOT NULL,
    Quality integer NOT NULL,
    Comment text NOT NULL,
    Value double precision NOT NULL,
    isDeleted boolean NOT NULL,
    CONSTRAINT s_float64_data_record_h_float64_data_record_fk FOREIGN KEY (h_float64_data_record_sk) REFERENCES public.h_float64_data_records (h_float64_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_float64_data_record_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_float64_data_record_sk);

-- ==============================
-- Link: Measure Units - Float64 Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_measure_units_float64_data_records (
    l_measure_units_float64_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_float64_data_record_sk uuid NOT NULL,
    h_measure_unit_sk uuid NOT NULL,
    CONSTRAINT l_measure_units_float64_data_records_pk PRIMARY KEY (
        l_measure_units_float64_data_records_sk
    ),
    CONSTRAINT l_measure_units_float64_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_measure_units_float64_data_records_h_float64_data_record_fk FOREIGN KEY (h_float64_data_record_sk) REFERENCES public.h_float64_data_records (h_float64_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_measure_units_float64_data_records_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_measure_units_float64_data_records_sk
);

-- ==============================
-- Link: Object Properties - Float64 Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_object_properties_float64_data_records (
    l_object_properties_float64_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_float64_data_record_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_object_properties_float64_data_records_pk PRIMARY KEY (
        l_object_properties_float64_data_records_sk
    ),
    CONSTRAINT l_object_properties_float64_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_properties_float64_data_records_h_float64_data_record_fk FOREIGN KEY (h_float64_data_record_sk) REFERENCES public.h_float64_data_records (h_float64_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_properties_float64_data_records_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_object_properties_float64_data_records_sk
);

-- ==============================
-- Hub: Float Array Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_float_array_data_records (
    h_float_array_data_record_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_float_array_data_record_pk PRIMARY KEY (h_float_array_data_record_sk),
    CONSTRAINT h_float_array_data_record_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_float_array_data_record_sk);

-- ==============================
-- Satellite: Float Array Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_float_array_data_records (
    h_float_array_data_record_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    -- Поля из схемы
    DataID uuid NOT NULL,
    DateTime timestamp NOT NULL,
    Quality integer NOT NULL,
    Comment text NOT NULL,
    Value double precision[] NOT NULL, -- Используем массив для хранения нескольких значений
    isDeleted boolean NOT NULL,
    CONSTRAINT s_float_array_data_record_h_float_array_data_record_fk
        FOREIGN KEY (h_float_array_data_record_sk) REFERENCES public.h_float_array_data_records(h_float_array_data_record_sk)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_float_array_data_record_data_catalogue_fk
        FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_float_array_data_record_sk);

-- ==============================
-- Link: Measure Units - Float Array Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_measure_units_float_array_data_records (
    l_measure_units_float_array_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_float_array_data_record_sk uuid NOT NULL,
    h_measure_unit_sk uuid NOT NULL,
    CONSTRAINT l_measure_units_float_array_data_records_pk PRIMARY KEY (
        l_measure_units_float_array_data_records_sk
    ),
    CONSTRAINT l_measure_units_float_array_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_measure_units_float_array_data_records_h_float_array_data_record_fk FOREIGN KEY (h_float_array_data_record_sk) REFERENCES public.h_float_array_data_records (h_float_array_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_measure_units_float_array_data_records_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_measure_units_float_array_data_records_sk
);

-- ==============================
-- Link: Object Properties - Float Array Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_object_properties_float_array_data_records (
    l_object_properties_float_array_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_float_array_data_record_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_object_properties_float_array_data_records_pk PRIMARY KEY (
        l_object_properties_float_array_data_records_sk
    ),
    CONSTRAINT l_object_properties_float_array_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_properties_float_array_data_records_h_float_array_data_record_fk FOREIGN KEY (h_float_array_data_record_sk) REFERENCES public.h_float_array_data_records (h_float_array_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_properties_float_array_data_records_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_object_properties_float_array_data_records_sk
);

-- ==============================
-- Hub: Int32 Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_int32_data_records (
    h_int32_data_record_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_int32_data_record_pk PRIMARY KEY (h_int32_data_record_sk),
    CONSTRAINT h_int32_data_record_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_int32_data_record_sk);

-- ==============================
-- Satellite: Int32 Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_int32_data_records (
    h_int32_data_record_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    -- Поля из схемы
    DataID uuid NOT NULL,
    DateTime timestamp NOT NULL,
    Quality integer NOT NULL,
    Comment text NOT NULL,
    Value integer NOT NULL,
    isDeleted boolean NOT NULL,
    CONSTRAINT s_int32_data_record_h_int32_data_record_fk FOREIGN KEY (h_int32_data_record_sk) REFERENCES public.h_int32_data_records (h_int32_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_int32_data_record_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_int32_data_record_sk);

-- ==============================
-- Link: Measure Units - Int32 Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_measure_units_int32_data_records (
    l_measure_units_int32_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_int32_data_record_sk uuid NOT NULL,
    h_measure_unit_sk uuid NOT NULL,
    CONSTRAINT l_measure_units_int32_data_records_pk PRIMARY KEY (
        l_measure_units_int32_data_records_sk
    ),
    CONSTRAINT l_measure_units_int32_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_measure_units_int32_data_records_h_int32_data_record_fk FOREIGN KEY (h_int32_data_record_sk) REFERENCES public.h_int32_data_records (h_int32_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_measure_units_int32_data_records_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_measure_units_int32_data_records_sk
);

-- ==============================
-- Link: Object Properties - Int32 Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_object_properties_int32_data_records (
    l_object_properties_int32_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_int32_data_record_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_object_properties_int32_data_records_pk PRIMARY KEY (
        l_object_properties_int32_data_records_sk
    ),
    CONSTRAINT l_object_properties_int32_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_properties_int32_data_records_h_int32_data_record_fk FOREIGN KEY (h_int32_data_record_sk) REFERENCES public.h_int32_data_records (h_int32_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_properties_int32_data_records_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_object_properties_int32_data_records_sk
);

-- ==============================
-- Hub: Int64 Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_int64_data_records (
    h_int64_data_record_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_int64_data_record_pk PRIMARY KEY (h_int64_data_record_sk),
    CONSTRAINT h_int64_data_record_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_int64_data_record_sk);

-- ==============================
-- Satellite: Int64 Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_int64_data_records (
    h_int64_data_record_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    -- Поля из схемы
    DataID uuid NOT NULL,
    DateTime timestamp NOT NULL,
    Quality integer NOT NULL,
    Comment text NOT NULL,
    Value bigint NOT NULL, -- В PostgreSQL тип BIGINT используется для 64-битных целых чисел.
    isDeleted boolean NOT NULL,
    CONSTRAINT s_int64_data_record_h_int64_data_record_fk FOREIGN KEY (h_int64_data_record_sk) REFERENCES public.h_int64_data_records (h_int64_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_int64_data_record_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_int64_data_record_sk);

-- ==============================
-- Link: Measure Units - Int64 Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_measure_units_int64_data_records (
    l_measure_units_int64_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_int64_data_record_sk uuid NOT NULL,
    h_measure_unit_sk uuid NOT NULL,
    CONSTRAINT l_measure_units_int64_data_records_pk PRIMARY KEY (
        l_measure_units_int64_data_records_sk
    ),
    CONSTRAINT l_measure_units_int64_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_measure_units_int64_data_records_h_int64_data_record_fk FOREIGN KEY (h_int64_data_record_sk) REFERENCES public.h_int64_data_records (h_int64_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_measure_units_int64_data_records_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_measure_units_int64_data_records_sk
);

-- ==============================
-- Link: Object Properties - Int64 Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_object_properties_int64_data_records (
    l_object_properties_int64_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_int64_data_record_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_object_properties_int64_data_records_pk PRIMARY KEY (
        l_object_properties_int64_data_records_sk
    ),
    CONSTRAINT l_object_properties_int64_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_properties_int64_data_records_h_int64_data_record_fk FOREIGN KEY (h_int64_data_record_sk) REFERENCES public.h_int64_data_records (h_int64_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_properties_int64_data_records_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_object_properties_int64_data_records_sk
);

-- ==============================
-- Hub: Int Array Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_int_array_data_records (
    h_int_array_data_record_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_int_array_data_record_pk PRIMARY KEY (h_int_array_data_record_sk),
    CONSTRAINT h_int_array_data_record_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_int_array_data_record_sk);

-- ==============================
-- Satellite: Int Array Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_int_array_data_records (
    h_int_array_data_record_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    -- Поля из схемы
    DataID_sk uuid NOT NULL,
    DateTime timestamp NOT NULL,
    Quality integer NOT NULL,
    Comment text NOT NULL,
    Value int[] NOT NULL, -- Используем массив для хранения нескольких целых чисел
    isDeleted boolean NOT NULL,
    CONSTRAINT s_int_array_data_record_h_int_array_data_record_fk
        FOREIGN KEY (h_int_array_data_record_sk) REFERENCES public.h_int_array_data_records(h_int_array_data_record_sk)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_int_array_data_record_data_catalogue_fk
        FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_int_array_data_record_sk);

-- ==============================
-- Link: Measure Units - Int Array Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_measure_units_int_array_data_records (
    l_measure_units_int_array_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_int_array_data_record_sk uuid NOT NULL,
    h_measure_unit_sk uuid NOT NULL,
    CONSTRAINT l_measure_units_int_array_data_records_pk PRIMARY KEY (
        l_measure_units_int_array_data_records_sk
    ),
    CONSTRAINT l_measure_units_int_array_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_measure_units_int_array_data_records_h_int_array_data_record_fk FOREIGN KEY (h_int_array_data_record_sk) REFERENCES public.h_int_array_data_records (h_int_array_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_measure_units_int_array_data_records_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_measure_units_int_array_data_records_sk
);

-- ==============================
-- Link: Object Properties - Int Array Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_object_properties_int_array_data_records (
    l_object_properties_int_array_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_int_array_data_record_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_object_properties_int_array_data_records_pk PRIMARY KEY (
        l_object_properties_int_array_data_records_sk
    ),
    CONSTRAINT l_object_properties_int_array_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_properties_int_array_data_records_h_int_array_data_record_fk FOREIGN KEY (h_int_array_data_record_sk) REFERENCES public.h_int_array_data_records (h_int_array_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_properties_int_array_data_records_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_object_properties_int_array_data_records_sk
);

-- ==============================
-- Hub: Siemens Sample Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_siemens_sample_data_records (
    h_siemens_sample_data_record_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_siemens_sample_data_record_pk PRIMARY KEY (
        h_siemens_sample_data_record_sk
    ),
    CONSTRAINT h_siemens_sample_data_record_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (
    h_siemens_sample_data_record_sk
);

-- ==============================
-- Satellite: Siemens Sample Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_siemens_sample_data_records (
    h_siemens_sample_data_record_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    -- Поля из схемы
    DataID uuid NOT NULL,
    DateTime timestamp NOT NULL,
    Quality integer NOT NULL,
    Comment text NOT NULL,
    SampleRate double precision NOT NULL,
    RawValues double precision[] NOT NULL, -- Используем массив для хранения нескольких значений
    isDeleted boolean NOT NULL,
    CONSTRAINT s_siemens_sample_data_record_h_siemens_sample_data_record_fk
        FOREIGN KEY (h_siemens_sample_data_record_sk) REFERENCES public.h_siemens_sample_data_records(h_siemens_sample_data_record_sk)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_siemens_sample_data_record_data_catalogue_fk
        FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_siemens_sample_data_record_sk);

-- ==============================
-- Link: Measure Units - Siemens Sample Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_measure_units_siemens_sample_data_record (
    l_measure_units_siemens_sample_data_record_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_siemens_sample_data_record_sk uuid NOT NULL,
    h_measure_unit_sk uuid NOT NULL,
    CONSTRAINT l_measure_units_siemens_sample_data_record_pk PRIMARY KEY (
        l_measure_units_siemens_sample_data_record_sk
    ),
    CONSTRAINT l_measure_units_siemens_sample_data_record_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_measure_units_siemens_sample_data_record_h_siemens_sample_data_record_fk FOREIGN KEY (
        h_siemens_sample_data_record_sk
    ) REFERENCES public.h_siemens_sample_data_records (
        h_siemens_sample_data_record_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_measure_units_siemens_sample_data_record_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_measure_units_siemens_sample_data_record_sk
);

-- ==============================
-- Link: Object Properties - Siemens Sample Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_object_properties_siemens_sample_data_records (
    l_object_properties_siemens_sample_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_siemens_sample_data_record_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_object_properties_siemens_sample_data_records_pk PRIMARY KEY (
        l_object_properties_siemens_sample_data_records_sk
    ),
    CONSTRAINT l_object_properties_siemens_sample_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_properties_siemens_sample_data_records_h_siemens_sample_data_record_fk FOREIGN KEY (
        h_siemens_sample_data_record_sk
    ) REFERENCES public.h_siemens_sample_data_records (
        h_siemens_sample_data_record_sk
    ) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_properties_siemens_sample_data_records_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_object_properties_siemens_sample_data_records_sk
);

-- ==============================
-- Hub: Spectrum Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_spectrum_data_records (
    h_spectrum_data_record_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_spectrum_data_record_pk PRIMARY KEY (h_spectrum_data_record_sk),
    CONSTRAINT h_spectrum_data_record_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_spectrum_data_record_sk);

-- ==============================
-- Satellite: Spectrum Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_spectrum_data_records (
    h_spectrum_data_record_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    -- Поля из схемы
    DataID uuid NOT NULL,
    DateTime timestamp NOT NULL,
    Quality integer NOT NULL,
    Comment text NOT NULL,
    SampleRate double precision NOT NULL,
    Multiplier double precision NOT NULL,
    RawValues double precision[] NOT NULL, -- Используем массив для хранения нескольких значений
    isDeleted boolean NOT NULL,
    CONSTRAINT s_spectrum_data_record_h_spectrum_data_record_fk
        FOREIGN KEY (h_spectrum_data_record_sk) REFERENCES public.h_spectrum_data_records(h_spectrum_data_record_sk)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_spectrum_data_record_data_catalogue_fk
        FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue(data_source_id)
) DISTRIBUTED BY (h_spectrum_data_record_sk);

-- ==============================
-- Link: Spectrum Types - Spectrum Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_spectrum_types_spectrum_data_records (
    l_spectrum_types_spectrum_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_spectrum_data_record_sk uuid NOT NULL,
    h_spectrum_type_sk uuid NOT NULL,
    CONSTRAINT l_spectrum_types_spectrum_data_records_pk PRIMARY KEY (
        l_spectrum_types_spectrum_data_records_sk
    ),
    CONSTRAINT l_spectrum_types_spectrum_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_spectrum_types_spectrum_data_records_h_spectrum_data_record_fk FOREIGN KEY (h_spectrum_data_record_sk) REFERENCES public.h_spectrum_data_records (h_spectrum_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_spectrum_types_spectrum_data_records_h_spectrum_type_fk FOREIGN KEY (h_spectrum_type_sk) REFERENCES public.h_spectrum_types (h_spectrum_type_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_spectrum_types_spectrum_data_records_sk
);

-- ==============================
-- Link: Measure Units - Spectrum Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_measure_units_spectrum_data_records (
    l_measure_units_spectrum_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_spectrum_data_record_sk uuid NOT NULL,
    h_measure_unit_sk uuid NOT NULL,
    CONSTRAINT l_measure_units_spectrum_data_records_pk PRIMARY KEY (
        l_measure_units_spectrum_data_records_sk
    ),
    CONSTRAINT l_measure_units_spectrum_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_measure_units_spectrum_data_records_h_spectrum_data_record_fk FOREIGN KEY (h_spectrum_data_record_sk) REFERENCES public.h_spectrum_data_records (h_spectrum_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_measure_units_spectrum_data_records_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_measure_units_spectrum_data_records_sk
);

-- ==============================
-- Link: Object Properties - Spectrum Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_object_properties_spectrum_data_records (
    l_object_properties_spectrum_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_spectrum_data_record_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_object_properties_spectrum_data_records_pk PRIMARY KEY (
        l_object_properties_spectrum_data_records_sk
    ),
    CONSTRAINT l_object_properties_spectrum_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_properties_spectrum_data_records_h_spectrum_data_record_fk FOREIGN KEY (h_spectrum_data_record_sk) REFERENCES public.h_spectrum_data_records (h_spectrum_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_properties_spectrum_data_records_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_object_properties_spectrum_data_records_sk
);

-- ==============================
-- Hub: String Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.h_string_data_records (
    h_string_data_record_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    CONSTRAINT h_string_data_record_pk PRIMARY KEY (h_string_data_record_sk),
    CONSTRAINT h_string_data_record_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_string_data_record_sk);

-- ==============================
-- Satellite: String Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.s_string_data_records (
    h_string_data_record_sk uuid NOT NULL,
    load_dttm timestamp NOT NULL,
    valid_from_dttm timestamp NOT NULL,
    valid_to_dttm timestamp NULL,
    active_flag bool DEFAULT true NOT NULL,
    data_source_id uuid NOT NULL,
    hash_sat_diff uuid NOT NULL,
    -- Поля из схемы
    DataID uuid NOT NULL,
    DateTime timestamp NOT NULL,
    Quality integer NOT NULL,
    Comment text NOT NULL,
    Value text NOT NULL, -- В схеме указано TEXT, что подходит для строковых данных.
    isDeleted boolean NOT NULL,
    CONSTRAINT s_string_data_record_h_string_data_record_fk FOREIGN KEY (h_string_data_record_sk) REFERENCES public.h_string_data_records (h_string_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT s_string_data_record_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id)
) DISTRIBUTED BY (h_string_data_record_sk);

-- ==============================
-- Link: Measure Units - String Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_measure_units_string_data_records (
    l_measure_units_string_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_string_data_record_sk uuid NOT NULL,
    h_measure_unit_sk uuid NOT NULL,
    CONSTRAINT l_measure_units_string_data_records_pk PRIMARY KEY (
        l_measure_units_string_data_records_sk
    ),
    CONSTRAINT l_measure_units_string_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_measure_units_string_data_records_h_string_data_record_fk FOREIGN KEY (h_string_data_record_sk) REFERENCES public.h_string_data_records (h_string_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_measure_units_string_data_records_h_measure_unit_fk FOREIGN KEY (h_measure_unit_sk) REFERENCES public.h_measure_units (h_measure_unit_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_measure_units_string_data_records_sk
);

-- ==============================
-- Link: Object Properties - String Data
-- ==============================
CREATE TABLE IF NOT EXISTS public.l_object_properties_string_data_records (
    l_object_properties_string_data_records_sk uuid DEFAULT uuid_generate_v4 () NOT NULL,
    load_dttm timestamp NOT NULL,
    data_source_id uuid NOT NULL,
    h_string_data_record_sk uuid NOT NULL,
    h_object_property_sk uuid NOT NULL,
    CONSTRAINT l_object_properties_string_data_records_pk PRIMARY KEY (
        l_object_properties_string_data_records_sk
    ),
    CONSTRAINT l_object_properties_string_data_records_data_catalogue_fk FOREIGN KEY (data_source_id) REFERENCES public.data_catalogue (data_source_id),
    CONSTRAINT l_object_properties_string_data_records_h_string_data_record_fk FOREIGN KEY (h_string_data_record_sk) REFERENCES public.h_string_data_records (h_string_data_record_sk) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT l_object_properties_string_data_records_h_object_properties_fk FOREIGN KEY (h_object_property_sk) REFERENCES public.h_object_properties (h_object_property_sk) ON DELETE RESTRICT ON UPDATE CASCADE
) DISTRIBUTED BY (
    l_object_properties_string_data_records_sk
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
    load_dttm timestamp NOT NULL
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
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_aggregates
CREATE TABLE IF NOT EXISTS public.stg_aggregates (
    id uuid,
    aggregatepath text,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_annotation
CREATE TABLE IF NOT EXISTS public.stg_annotation (
    id integer,
    annotationid uuid,
    namegraphics text,
    fullpath text,
    property text,
    selectedinterval text,
    annotationtype text,
    userlogin text,
    datecreate text,
    jsondata text,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
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
    datecreated timestamp,
    datemodified timestamp,
    cdbsyncdate timestamp,
    cdbversion integer,
    cdbid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_diagnostic_alarm_history_v2
CREATE TABLE IF NOT EXISTS public.stg_diagnostic_alarm_history_v2 (
    diagalarmid uuid,
    propertyid uuid,
    diagid uuid,
    diagtagname text,
    alarmstate integer,
    date timestamp,
    confirmed boolean,
    comment text,
    defectstate integer,
    defectname text,
    defectdetails text,
    recommendation text,
    priority integer,
    groupname text,
    defecttype integer,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_diagnostic_alarms_v2
CREATE TABLE IF NOT EXISTS public.stg_diagnostic_alarms_v2 (
    diagalarmid uuid,
    propertyid uuid,
    diagid uuid,
    diagtagname text,
    alarmstate integer,
    date timestamp,
    confirmed boolean,
    comment text,
    defectstate integer,
    defectname text,
    defectdetails text,
    recommendation text,
    priority integer,
    groupname text,
    defecttype integer,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_email_addresses
CREATE TABLE IF NOT EXISTS public.stg_email_addresses (
    id uuid,
    emailaddress text,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
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
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_io_creyt_channel_configs
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
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_io_creyt_channel_states
CREATE TABLE IF NOT EXISTS public.stg_io_creyt_channel_states (
    configid uuid,
    channelnum integer,
    propertyid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_io_creyt_channel_states_v5
CREATE TABLE IF NOT EXISTS public.stg_io_creyt_channel_states_v5 (
    configid uuid,
    channelnum integer,
    propertyid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
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
    issyncreadslave boolean,
    syncreadgroupname text,
    detectfailures boolean,
    detectfailurelength integer,
    enabledsecondaryip boolean,
    numblocksamples integer,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
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
    issyncreadslave boolean,
    syncreadgroupname text,
    detectfailures boolean,
    detectfailurelength integer,
    enabledsecondaryip boolean,
    numblocksamples integer,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_io_creyt_controller_states
CREATE TABLE IF NOT EXISTS public.stg_io_creyt_controller_states (
    configid uuid,
    rownum integer,
    opertimepropertyid uuid,
    statepropertyid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_io_creyt_controller_states_v5
CREATE TABLE IF NOT EXISTS public.stg_io_creyt_controller_states_v5 (
    configid uuid,
    rownum integer,
    opertimepropertyid uuid,
    statepropertyid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_io_creyt_im_oper_times
CREATE TABLE IF NOT EXISTS public.stg_io_creyt_im_oper_times (
    configid uuid,
    propertyid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_io_creyt_im_oper_times_v5
CREATE TABLE IF NOT EXISTS public.stg_io_creyt_im_oper_times_v5 (
    configid uuid,
    propertyid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_io_device_config_nodes
CREATE TABLE IF NOT EXISTS public.stg_io_device_config_nodes (
    id uuid,
    name text,
    parentnodeid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_io_device_configs
CREATE TABLE IF NOT EXISTS public.stg_io_device_configs (
    id uuid,
    name text,
    typeid uuid,
    confignodeid uuid,
    enabled boolean,
    setnumber integer,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_io_l_card_channel_configs
CREATE TABLE IF NOT EXISTS public.stg_io_l_card_channel_configs (
    configid uuid,
    slot integer,
    channelnumber integer,
    propertyid uuid,
    isscaled integer,
    minraw numeric,
    maxraw numeric,
    mineu numeric,
    maxeu numeric,
    measureunitid uuid,
    propertymarkid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
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
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
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
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_io_l_card_crate_sync
CREATE TABLE IF NOT EXISTS public.stg_io_l_card_crate_sync (
    configid uuid,
    issync integer,
    isleader integer,
    isslave integer,
    leaderid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_io_l_card_input_configs
CREATE TABLE IF NOT EXISTS public.stg_io_l_card_input_configs (
    configid uuid,
    inputnum integer,
    propertyid uuid,
    inputrange integer,
    isscale boolean,
    minraw double precision,
    maxraw double precision,
    mineu double precision,
    maxeu double precision,
    measureunitid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_io_l_card_logic_input_configs
CREATE TABLE IF NOT EXISTS public.stg_io_l_card_logic_input_configs (
    configid uuid,
    inputnumber integer,
    inputtype integer,
    timerpropertyid uuid,
    syncpropertyid uuid,
    inputrange integer,
    isscale boolean,
    minraw double precision,
    maxraw double precision,
    mineu double precision,
    maxeu double precision,
    measureunitid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_io_l_card_module_configs
CREATE TABLE IF NOT EXISTS public.stg_io_l_card_module_configs (
    configid uuid,
    slot integer,
    moduletype integer,
    frequencydivisor integer,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_io_mqtt_configs
CREATE TABLE IF NOT EXISTS public.stg_io_mqtt_configs (
    configid uuid,
    address text,
    clientid text,
    login text,
    password text,
    tls boolean,
    cleansession boolean,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_io_mqtt_dev_eui_configs
CREATE TABLE IF NOT EXISTS public.stg_io_mqtt_dev_eui_configs (
    configid uuid,
    topic text,
    deveui text,
    version integer,
    parser integer,
    indextypedata integer,
    propertyid uuid,
    measureunitid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
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
    secondaryenabled boolean,
    devicetype integer,
    connecttoptimaryifavailable boolean,
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
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
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
    isscale boolean,
    factor double precision,
    id uuid,
    iswrite boolean,
    writepropertyid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
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
    isscale boolean,
    factor double precision,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_io_modbus_tcp_register_bit_decompression_configs
CREATE TABLE IF NOT EXISTS public.stg_io_modbus_tcp_register_bit_decompression_configs (
    id uuid,
    registerid uuid,
    propertyid uuid,
    bitnumber integer,
    iswrite boolean,
    writepropertyid uuid,
    makeinversion boolean,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_io_opc_da_client_configs
CREATE TABLE IF NOT EXISTS public.stg_io_opc_da_client_configs (
    configid uuid,
    ip text,
    servername text,
    connectretries integer,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_io_opc_da_client_group_configs
CREATE TABLE IF NOT EXISTS public.stg_io_opc_da_client_group_configs (
    configid uuid,
    name text,
    isactive boolean,
    updaterate integer,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_io_opc_da_client_item_configs
CREATE TABLE IF NOT EXISTS public.stg_io_opc_da_client_item_configs (
    configid uuid,
    propertyid uuid,
    itemid text,
    groupname text,
    isactive boolean,
    datatype integer,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
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
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_io_opc_ua_client_group_configs
CREATE TABLE IF NOT EXISTS public.stg_io_opc_ua_client_group_configs (
    configid uuid,
    name text,
    isactive boolean,
    updaterate integer,
    issubscription boolean,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_io_opc_ua_client_item_configs
CREATE TABLE IF NOT EXISTS public.stg_io_opc_ua_client_item_configs (
    configid uuid,
    propertyid uuid,
    itemid text,
    groupname text,
    isactive boolean,
    datatype integer,
    fullpathname text,
    toserver boolean,
    datatypeserver integer,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
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
    isscale boolean,
    factor double precision,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_images
CREATE TABLE IF NOT EXISTS public.stg_images (
    id uuid,
    imagedata bytea,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
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
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_measure_units
CREATE TABLE IF NOT EXISTS public.stg_measure_units (
    id uuid,
    name text,
    abbreviation text,
    measuregroupid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
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
    load_dttm timestamp NOT NULL
);

-- stg_model_templates
CREATE TABLE IF NOT EXISTS public.stg_model_templates (
    id uuid,
    ownernodeid uuid,
    name text,
    tagname text,
    description text,
    basetemplatename text,
    datecreated timestamp,
    datemodified timestamp,
    cdbsyncdate timestamp,
    cdbversion integer,
    cdbid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
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
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
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
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_object_data_values_live_v2
CREATE TABLE IF NOT EXISTS public.stg_object_data_values_live_v2 (
    id uuid,
    propertyid uuid,
    measureunitid uuid,
    datatypeid uuid,
    date timestamp,
    quality integer,
    comment text,
    value bytea,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_object_data_values_common_snapshot
CREATE TABLE IF NOT EXISTS public.stg_object_data_values_common_snapshot (
    id uuid,
    propertyid uuid,
    measureunitid uuid,
    datatypeid uuid,
    date timestamp,
    quality integer,
    comment text,
    value bytea,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_object_data_values_common_snapshot_save
CREATE TABLE IF NOT EXISTS public.stg_object_data_values_common_snapshot_save (
    id uuid,
    propertyid uuid,
    measureunitid uuid,
    datatypeid uuid,
    date timestamp,
    quality integer,
    comment text,
    value bytea,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_object_data_values_history_snapshot
CREATE TABLE IF NOT EXISTS public.stg_object_data_values_history_snapshot (
    id uuid,
    propertyid uuid,
    measureunitid uuid,
    datatypeid uuid,
    date timestamp,
    quality integer,
    comment text,
    value bytea,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_object_data_values_snapshot
CREATE TABLE IF NOT EXISTS public.stg_object_data_values_snapshot (
    id uuid,
    propertyid uuid,
    measureunitid uuid,
    datatypeid uuid,
    date timestamp,
    quality integer,
    comment text,
    value bytea,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_object_data_values_snapshot_single
CREATE TABLE IF NOT EXISTS public.stg_object_data_values_snapshot_single (
    id uuid,
    propertyid uuid,
    measureunitid uuid,
    datatypeid uuid,
    date timestamp,
    quality integer,
    comment text,
    value bytea,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_object_data_values_v2
CREATE TABLE IF NOT EXISTS public.stg_object_data_values_v2 (
    id uuid,
    propertyid uuid,
    measureunitid uuid,
    datatypeid uuid,
    date timestamp,
    quality integer,
    comment text,
    value bytea,
    isdeleted boolean,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_object_group_descriptors
CREATE TABLE IF NOT EXISTS public.stg_object_group_descriptors (
    groupdescriptorid uuid,
    name text,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_object_groups
CREATE TABLE IF NOT EXISTS public.stg_object_groups (
    id uuid,
    objectid uuid,
    groupdescriptorid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_object_properties
CREATE TABLE IF NOT EXISTS public.stg_object_properties (
    propertyid uuid,
    objectid uuid,
    propertydescriptorid uuid,
    isalias boolean,
    measureunitid uuid,
    maxrecords integer,
    fromtemplatename text,
    tocopy boolean,
    savehistory boolean,
    storagetype integer,
    visibleforscada boolean,
    isvaluesreplicable boolean,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
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
    load_dttm timestamp NOT NULL
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
    savehistory integer,
    defaultvalue text,
    visibleforscada integer,
    datecreated timestamp,
    datemodified timestamp,
    cdbsyncdate timestamp,
    cdbversion integer,
    translateid integer,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_object_rules
CREATE TABLE IF NOT EXISTS public.stg_object_rules (
    objectruleid uuid,
    objectid uuid,
    poucallname text,
    comment text,
    enabled boolean,
    runlevel integer,
    fromtemplatename text,
    isintemplate boolean,
    tag text,
    runningtype integer,
    runningperiod bigint,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
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
    load_dttm timestamp NOT NULL
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
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_object_types
CREATE TABLE IF NOT EXISTS public.stg_object_types (
    objectid uuid,
    typedescriptorid uuid,
    fromtemplatename text,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
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
    datecreated timestamp,
    datemodified timestamp,
    cdbsyncdate timestamp,
    cdbversion integer,
    cdbid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_pion_route_objects_lists
CREATE TABLE IF NOT EXISTS public.stg_pion_route_objects_lists (
    id uuid,
    name text,
    description text,
    xmlobjectslist text,
    datemodified timestamp,
    datemodifiedcbd timestamp,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
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
    dateofcreate timestamp,
    dateofedit timestamp,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
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
    load_dttm timestamp NOT NULL
);

-- stg_preset_chart_settings
CREATE TABLE IF NOT EXISTS public.stg_preset_chart_settings (
    id uuid,
    userid uuid,
    presetchartsettings text,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_tik_expert_slices
CREATE TABLE IF NOT EXISTS public.stg_tik_expert_slices (
    propertyid uuid,
    slicedate timestamp,
    numericx double precision,
    comment text,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_tik_scada_log
CREATE TABLE IF NOT EXISTS public.stg_tik_scada_log (
    id uuid,
    type integer,
    timestamp timestamp,
    message text,
    acked boolean,
    ackedtimestamp timestamp,
    sourcepath text,
    aggregatepath text,
    participant text,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
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
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_user_defined_property_lists_types
CREATE TABLE IF NOT EXISTS public.stg_user_defined_property_lists_types (
    propertylisttypeid uuid,
    propertylisttypename text,
    description text,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_user_defined_tiles_properties_configs
CREATE TABLE IF NOT EXISTS public.stg_user_defined_tiles_properties_configs (
    id uuid,
    name text,
    tilesproperties text,
    objectid uuid,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- stg_user_log
CREATE TABLE IF NOT EXISTS public.stg_user_log (
    id uuid,
    userid uuid,
    date timestamp,
    actiontype integer,
    actiondescription text,
    hash_sat_diff uuid NOT NULL,
    hash_sk uuid NOT NULL,
    data_source_id uuid NOT NULL,
    load_dttm timestamp NOT NULL
);

-- ==============================
-- Data Initialization
-- ==============================

DO $$
DECLARE
    load_datetime timestamp;
    default_source_id uuid ;
    valid_from_datetime timestamp;
    active_flag_true bool;

    -- Diagnostic Defect Types
    DiagnosticDefectType_type_1_uuid uuid;
    DiagnosticDefectType_type_2_uuid uuid;

    -- Alarm States
    diagnostic_alarm_state_1_uuid uuid;
    diagnostic_alarm_state_2_uuid uuid;
    diagnostic_alarm_state_3_uuid uuid;
    diagnostic_alarm_state_4_uuid uuid;

    -- Defect States
    diagnostic_defect_state_1_uuid uuid;
    diagnostic_defect_state_2_uuid uuid;
    diagnostic_defect_state_3_uuid uuid;
    diagnostic_defect_state_4_uuid uuid;
    diagnostic_defect_state_5_uuid uuid;
    diagnostic_defect_state_6_uuid uuid;

    -- Config Types (h_config_type UUIDs)
    config_type_1_h uuid;
    config_type_2_h uuid;
    config_type_3_h uuid;
    config_type_4_h uuid;
    config_type_5_h uuid;
    config_type_6_h uuid;
    config_type_7_h uuid;
    config_type_8_h uuid;
    config_type_9_h uuid;
    config_type_10_h uuid;
    config_type_11_h uuid;
    config_type_12_h uuid;
    config_type_13_h uuid;

        -- Index Type Data
    index_type_temp_uuid uuid;
    index_type_batt_uuid uuid;
    index_type_serial_uuid uuid;


    -- OPC DA Data Types
    opc_empty_uuid uuid;
    opc_bool_uuid uuid;
    opc_int_uuid uuid;
    opc_long_uuid uuid;
    opc_double_uuid uuid;
    opc_string_uuid uuid;
    opc_datetime_uuid uuid;

        -- OPC UA Data Types
    ua_empty_uuid uuid;
    ua_bool_uuid uuid;
    ua_int_uuid uuid;
    ua_long_uuid uuid;
    ua_double_uuid uuid;
    ua_string_uuid uuid;
    ua_datetime_uuid uuid;
    ua_duration_uuid uuid;
    ua_int_array_uuid uuid;
    ua_float_array_uuid uuid;
    ua_float_multidim_array_uuid uuid;
    ua_data_sample_uuid uuid;
    ua_diagnostic_array_uuid uuid;


    -- Server Data Types (0 to 15)
    srv_type_0_uuid  uuid;
    srv_type_1_uuid  uuid;
    srv_type_2_uuid  uuid;
    srv_type_3_uuid  uuid;
    srv_type_4_uuid  uuid;
    srv_type_5_uuid  uuid;
    srv_type_6_uuid  uuid;
    srv_type_7_uuid  uuid;
    srv_type_8_uuid  uuid;
    srv_type_9_uuid  uuid;
    srv_type_10_uuid uuid;
    srv_type_11_uuid uuid;
    srv_type_12_uuid uuid;
    srv_type_13_uuid uuid;
    srv_type_14_uuid uuid;
    srv_type_15_uuid uuid;

    -- TIK SCADA Log Types
    log_info_uuid uuid;
    log_warning_uuid uuid;
    log_error_uuid uuid;
    log_fatal_uuid uuid;
    log_setpoint_uuid uuid;
    log_value_changed_uuid uuid;
    log_setpoint2_uuid uuid;
    log_alarm_uuid uuid;
    log_script_error_uuid uuid;

        -- User Action Types (0 to 19)
    action_0_uuid  uuid;
    action_1_uuid  uuid;
    action_2_uuid  uuid;
    action_3_uuid  uuid;
    action_4_uuid  uuid;
    action_5_uuid  uuid;
    action_6_uuid  uuid;
    action_7_uuid  uuid;
    action_8_uuid  uuid;
    action_9_uuid  uuid;
    action_10_uuid uuid;
    action_11_uuid uuid;
    action_12_uuid uuid;
    action_13_uuid uuid;
    action_14_uuid uuid;
    action_15_uuid uuid;
    action_16_uuid uuid;
    action_17_uuid uuid;
    action_18_uuid uuid;
    action_19_uuid uuid;


        -- Running Types
    running_type_level_uuid uuid;
    running_type_period_uuid uuid;

        -- Crate Types
    crate_type_ltr_eu_2_uuid uuid;

        -- L Card Logic Input Types
    l_card_logic_input_type_differential_uuid uuid;
    l_card_logic_input_type_single_ended_uuid uuid;
    l_card_logic_input_type_discr_in_bit_uuid uuid;
    l_card_logic_input_type_discr_out_bit_uuid uuid;


        -- L Card Crate Module Types
    l_card_crate_module_type_ltr25_uuid uuid;
    l_card_crate_module_type_ltr27_uuid uuid;
    l_card_crate_module_type_ltr24_uuid uuid;
    l_card_crate_module_type_ltr11_diff_uuid uuid;
    l_card_crate_module_type_ltr11_gnd_uuid uuid;


        -- Register Types
    register_type_coil_status_uuid uuid;
    register_type_input_status_uuid uuid;
    register_type_holding_register_uuid uuid;
    register_type_input_register_uuid uuid;


        -- Type Names
    type_name_int8_uuid uuid;
    type_name_uint8_uuid uuid;
    type_name_int16_uuid uuid;
    type_name_uint16_uuid uuid;
    type_name_int32_uuid uuid;
    type_name_uint32_uuid uuid;
    type_name_float32_uuid uuid;
    type_name_float64_uuid uuid;
    type_name_boolean_uuid uuid;


    -- Data Types
    data_type_int_array_uuid uuid := '37BEB486-D0DC-4C9D-9E50-419632F63616'::uuid;
    data_type_int32_uuid uuid := 'CFF1DE41-ED0A-4FE7-A003-2B9107D44F0E'::uuid;
    data_type_float64_uuid uuid := '20DA2798-2340-45E9-8B75-29CFE4F99901'::uuid;
    data_type_creyt_uuid uuid := '4FC8E8D4-CFDB-46DB-9E5F-41CDB9F7EF33'::uuid;
    data_type_lcard_uuid uuid := '34A809AA-9C72-4BAA-B892-00D4CBA7D7A4'::uuid;
    data_type_bode_uuid uuid := 'C0BE4AE3-3A1B-4A63-9FCC-04DBDDE6A30C'::uuid;
    data_type_boolean_uuid uuid := 'E940B7EE-ABA1-4447-B940-1686BB70551B'::uuid;
    data_type_data_sample_uuid uuid := '969420b8-fd94-4476-b06e-ed7408d42a61'::uuid;
    data_type_datetime_uuid uuid := '0232DB10-59E8-4840-BBE6-E8749EAE690F'::uuid;
    data_type_datetime_array_uuid uuid := '962df766-9d5e-44ad-be1b-1768d3b90d25'::uuid;
    data_type_diagnostic_array_uuid uuid := '7CF98A0E-E55B-4D00-BD15-479CF05A5DDD'::uuid;
    data_type_duration_uuid uuid := '6BC397F1-26AC-44B5-9F55-BBAF84475285'::uuid;
    data_type_file_uuid uuid := '8796C1A5-542E-4E8B-BEB6-0B8B18AC0A18'::uuid;
    data_type_float_array_uuid uuid := '86DA3E91-6E11-445F-AFDF-72E25BCAFB0C'::uuid;
    data_type_int64_uuid uuid := '399F26C3-F102-4146-A8DE-F44C9441A872'::uuid;
    data_type_siemens_sample_uuid uuid := 'C573609B-47C2-4836-88B9-AA595A2B5FDB'::uuid;
    data_type_spectrum_uuid uuid := '2017DB6E-9629-49FF-8C2F-04C0F8EE135E'::uuid;
    data_type_string_uuid uuid := 'FD18A4A9-6B08-46E0-A99A-FB69C56C2DEE'::uuid;
    data_type_pion_uuid uuid := 'F7B05632-247B-47DB-89A9-1E80AC388FB2'::uuid;
    data_type_string_array_uuid uuid := 'A1B3C4D5-E6F7-8901-2345-67890ABCDEFF'::uuid;
    data_type_tikexpert_uuid uuid := 'C00DE691-45DF-49F9-A94B-442B63D5904C'::uuid;


    -- UUIDs for Spectrum Types
    spectrum_acceleration_uuid uuid;
    spectrum_velocity_uuid uuid;
    spectrum_displacement_uuid uuid;
    spectrum_envelope_uuid uuid;

        -- Property Type UUIDs
    property_type_unknown_uuid uuid;
    property_type_boolean_uuid uuid;
    property_type_integer_uuid uuid;
    property_type_long_uuid uuid;
    property_type_double_uuid uuid;
    property_type_string_uuid uuid;
    property_type_datetime_uuid uuid;
    property_type_duration_uuid uuid;
    property_type_datasample_uuid uuid;
    property_type_intarray_uuid uuid;
    property_type_floatarray_uuid uuid;
    property_type_diagnosticarray_uuid uuid;
    property_type_file_uuid uuid;
    property_type_floatmultidimarray_uuid uuid;
    property_type_datetimearray_uuid uuid;
    property_type_spectrum_uuid uuid;
    property_type_bode_uuid uuid;

        -- Storage Type UUIDs
    storage_type_db_uuid uuid;
    storage_type_file_uuid uuid;

    -- Convert Type UUIDs
    convert_type_factor_uuid uuid;
    convert_type_function_uuid uuid;

BEGIN
    load_datetime := NOW();
    default_source_id := '00000000-0000-0000-0000-000000000000'::uuid;
    valid_from_datetime := NOW();
    active_flag_true := true;

    -- Generate UUIDs for Diagnostic Defect Types
    DiagnosticDefectType_type_1_uuid := uuid_generate_v4();
    DiagnosticDefectType_type_2_uuid := uuid_generate_v4();


    INSERT INTO public.data_catalogue (data_source_id, main_db_name, main_db_id, is_uploaded_to_dwh)
    VALUES ('00000000-0000-0000-0000-000000000000'::uuid,
        'Unknown',
        '00000000-0000-0000-0000-000000000000'::uuid,
        true
    )
    ON CONFLICT (data_source_id) DO NOTHING;

    -- Insert Defect Types
    INSERT INTO public.h_diagnostic_defect_types (h_diagnostic_defect_type_sk, load_dttm, data_source_id) 
    VALUES 
        (DiagnosticDefectType_type_1_uuid, load_datetime, default_source_id),
        (DiagnosticDefectType_type_2_uuid, load_datetime, default_source_id);

    INSERT INTO public.s_diagnostic_defect_types (h_diagnostic_defect_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, type_id, description, description_ru) 
    VALUES 
        (DiagnosticDefectType_type_1_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 1, 'Triggering', 'Срабатывающий'),
        (DiagnosticDefectType_type_2_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 0, 'Floating', 'Плавающий');

    -- Generate UUIDs for Alarm States
    diagnostic_alarm_state_1_uuid := uuid_generate_v4();
    diagnostic_alarm_state_2_uuid := uuid_generate_v4();
    diagnostic_alarm_state_3_uuid := uuid_generate_v4();
    diagnostic_alarm_state_4_uuid := uuid_generate_v4();

    -- Insert Alarm States
    INSERT INTO public.h_diagnostic_alarm_states (h_diagnostic_alarm_state_sk, load_dttm, data_source_id) 
    VALUES 
        (diagnostic_alarm_state_1_uuid, load_datetime, default_source_id),
        (diagnostic_alarm_state_2_uuid, load_datetime, default_source_id),
        (diagnostic_alarm_state_3_uuid, load_datetime, default_source_id),
        (diagnostic_alarm_state_4_uuid, load_datetime, default_source_id);

    INSERT INTO public.s_diagnostic_alarm_states (h_diagnostic_alarm_state_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, state_id, description, description_ru) 
    VALUES 
        (diagnostic_alarm_state_1_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 0, 'Normal', 'Норма'),
        (diagnostic_alarm_state_2_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 1, 'Armed', 'Взведен (есть аларм)'),
        (diagnostic_alarm_state_3_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 2, 'Acked', 'Квитирован'),
        (diagnostic_alarm_state_4_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 3, 'Repaired', 'Исправлен');

    -- Generate UUIDs for Defect States
    diagnostic_defect_state_1_uuid := uuid_generate_v4();
    diagnostic_defect_state_2_uuid := uuid_generate_v4();
    diagnostic_defect_state_3_uuid := uuid_generate_v4();
    diagnostic_defect_state_4_uuid := uuid_generate_v4();
    diagnostic_defect_state_5_uuid := uuid_generate_v4();
    diagnostic_defect_state_6_uuid := uuid_generate_v4();

    -- Insert Defect States
    INSERT INTO public.h_diagnostic_defect_states (h_diagnostic_defect_state_sk, load_dttm, data_source_id) 
    VALUES 
        (diagnostic_defect_state_1_uuid, load_datetime, default_source_id),
        (diagnostic_defect_state_2_uuid, load_datetime, default_source_id),
        (diagnostic_defect_state_3_uuid, load_datetime, default_source_id),
        (diagnostic_defect_state_4_uuid, load_datetime, default_source_id),
        (diagnostic_defect_state_5_uuid, load_datetime, default_source_id),
        (diagnostic_defect_state_6_uuid, load_datetime, default_source_id);

    INSERT INTO public.s_diagnostic_defect_states (h_diagnostic_defect_state_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, state_id, description, description_ru) 
    VALUES 
        (diagnostic_defect_state_1_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 0, 'Unknown', 'Неизвестно'),
        (diagnostic_defect_state_2_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 1, 'NotProcessed', 'Не обработано'),
        (diagnostic_defect_state_3_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 2, 'Normal', 'Норма'),
        (diagnostic_defect_state_4_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 3, 'Low', 'Низкий'),
        (diagnostic_defect_state_5_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 4, 'Medium', 'Средний'),
        (diagnostic_defect_state_6_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 5, 'High', 'Высокий');

    -- Generate h_config_type UUIDs for Config Types
    config_type_1_h := uuid_generate_v4();
    config_type_2_h := uuid_generate_v4();
    config_type_3_h := uuid_generate_v4();
    config_type_4_h := uuid_generate_v4();
    config_type_5_h := uuid_generate_v4();
    config_type_6_h := uuid_generate_v4();
    config_type_7_h := uuid_generate_v4();
    config_type_8_h := uuid_generate_v4();
    config_type_9_h := uuid_generate_v4();
    config_type_10_h := uuid_generate_v4();
    config_type_11_h := uuid_generate_v4();
    config_type_12_h := uuid_generate_v4();
    config_type_13_h := uuid_generate_v4();

    -- Insert Config Types (Hub)
    INSERT INTO public.h_config_types (h_config_type_sk, load_dttm, data_source_id)
    VALUES
        (config_type_1_h, load_datetime, default_source_id),
        (config_type_2_h, load_datetime, default_source_id),
        (config_type_3_h, load_datetime, default_source_id),
        (config_type_4_h, load_datetime, default_source_id),
        (config_type_5_h, load_datetime, default_source_id),
        (config_type_6_h, load_datetime, default_source_id),
        (config_type_7_h, load_datetime, default_source_id),
        (config_type_8_h, load_datetime, default_source_id),
        (config_type_9_h, load_datetime, default_source_id),
        (config_type_10_h, load_datetime, default_source_id),
        (config_type_11_h, load_datetime, default_source_id),
        (config_type_12_h, load_datetime, default_source_id),
        (config_type_13_h, load_datetime, default_source_id);

    -- Insert Config Types (Satellite)
    INSERT INTO public.s_config_types (h_config_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, type_id, description
    )
    VALUES
        (config_type_1_h, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 'BE3DB707-8534-4804-A757-E036536783EC'::uuid, 'CREYT'),
        (config_type_2_h, load_datetime, valid_from_datetime, active_flag_true, default_source_id, '69EBF4A6-365A-4BC6-8B63-6E54394B222D'::uuid, 'CREYT V5'),
        (config_type_3_h, load_datetime, valid_from_datetime, active_flag_true, default_source_id, '924AF965-2251-4050-9B0D-CAD8DE981BDA'::uuid, 'Modbus TCP'),
        (config_type_4_h, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 'D7BEBE88-9A87-4E4b-B2E2-946DD7620D99'::uuid, 'OPC DA Client'),
        (config_type_5_h, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 'c44c65e1-86e7-4d09-9f42-4c1657c761b2'::uuid, 'OPC UA Client'),
        (config_type_6_h, load_datetime, valid_from_datetime, active_flag_true, default_source_id, '4e37e276-87f3-4cfb-b9e1-aeb4c3f93dc8'::uuid, 'TIK-241.1'),
        (config_type_7_h, load_datetime, valid_from_datetime, active_flag_true, default_source_id, '4340DE8B-B523-41A3-811C-E53EF4C4ABCD'::uuid, 'L-lcard'),
        (config_type_8_h, load_datetime, valid_from_datetime, active_flag_true, default_source_id, '06122236-713D-4E73-92F7-DE9C9941E508'::uuid, 'Pumpjack'),
        (config_type_9_h, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 'FA638913-F8F4-4F1B-87BB-8A101F6536EF'::uuid, 'L-lcard Crate'),
        (config_type_10_h, load_datetime, valid_from_datetime, active_flag_true, default_source_id, '480B11EE-BB77-4D7F-A5D4-CF864C9956E8'::uuid, 'Siemens'),
        (config_type_11_h, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 'D8832C34-60EF-4CDE-9A4F-8782506060E2'::uuid, 'TIK-REG'),
        (config_type_12_h, load_datetime, valid_from_datetime, active_flag_true, default_source_id, '6EA0A535-515B-4340-9932-C9708F30CE59'::uuid, 'MQTT'),
        (config_type_13_h, load_datetime, valid_from_datetime, active_flag_true, default_source_id, '5C28C781-2CE8-4B25-85E2-87451889E623'::uuid, 'CSV');


    -- Generate UUIDs for Index Type Data
    index_type_temp_uuid := uuid_generate_v4();
    index_type_batt_uuid := uuid_generate_v4();
    index_type_serial_uuid := uuid_generate_v4();

    -- Insert into Hub
    INSERT INTO public.h_index_type_data_records (h_index_type_data_record_sk, load_dttm, data_source_id)
    VALUES
        (index_type_temp_uuid, load_datetime, default_source_id),
        (index_type_batt_uuid, load_datetime, default_source_id),
        (index_type_serial_uuid, load_datetime, default_source_id);

    -- Insert into Satellite (with English and Russian descriptions)
    INSERT INTO public.s_index_type_data_records (h_index_type_data_record_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, index_type_id, description, description_ru
    )
    VALUES
        (index_type_temp_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 0, 'Temperature (°C)', 'Температура (°C)'),
        (index_type_batt_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 1, 'Battery Voltage (V)', 'Напряжение батареи (В)'),
        (index_type_serial_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 2, 'Serial Number', 'Серийный номер');

        -- Generate UUIDs for OPC DA Data Types
    opc_empty_uuid      := uuid_generate_v4();
    opc_bool_uuid       := uuid_generate_v4();
    opc_int_uuid        := uuid_generate_v4();
    opc_long_uuid       := uuid_generate_v4();
    opc_double_uuid     := uuid_generate_v4();
    opc_string_uuid     := uuid_generate_v4();
    opc_datetime_uuid   := uuid_generate_v4();

    -- Insert into Hub
    INSERT INTO public.h_opc_da_data_types (h_opc_da_data_type_sk, load_dttm, data_source_id)
    VALUES
        (opc_empty_uuid,      load_datetime, default_source_id),
        (opc_bool_uuid,       load_datetime, default_source_id),
        (opc_int_uuid,        load_datetime, default_source_id),
        (opc_long_uuid,       load_datetime, default_source_id),
        (opc_double_uuid,     load_datetime, default_source_id),
        (opc_string_uuid,     load_datetime, default_source_id),
        (opc_datetime_uuid,   load_datetime, default_source_id);

    -- Insert into Satellite
    INSERT INTO public.s_opc_da_data_types (h_opc_da_data_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, data_type_id, description, description_ru
    )
    VALUES
        (opc_empty_uuid,      load_datetime, valid_from_datetime, active_flag_true, default_source_id, 0,  'Empty',           'Пусто'),
        (opc_bool_uuid,       load_datetime, valid_from_datetime, active_flag_true, default_source_id, 11, 'Boolean',          'Логический'),
        (opc_int_uuid,        load_datetime, valid_from_datetime, active_flag_true, default_source_id, 3,  'Integer',          'Целое число'),
        (opc_long_uuid,       load_datetime, valid_from_datetime, active_flag_true, default_source_id, 20, 'Long',             'Длинное целое'),
        (opc_double_uuid,     load_datetime, valid_from_datetime, active_flag_true, default_source_id, 5,  'Double',           'Число с плавающей точкой'),
        (opc_string_uuid,     load_datetime, valid_from_datetime, active_flag_true, default_source_id, 8,  'String',           'Строка'),
        (opc_datetime_uuid,   load_datetime, valid_from_datetime, active_flag_true, default_source_id, 7,  'DateTime',         'Дата и время');

        -- Generate UUIDs for OPC UA Data Types
    ua_empty_uuid                   := uuid_generate_v4();
    ua_bool_uuid                    := uuid_generate_v4();
    ua_int_uuid                     := uuid_generate_v4();
    ua_long_uuid                    := uuid_generate_v4();
    ua_double_uuid                  := uuid_generate_v4();
    ua_string_uuid                  := uuid_generate_v4();
    ua_datetime_uuid                := uuid_generate_v4();
    ua_duration_uuid                := uuid_generate_v4();
    ua_int_array_uuid               := uuid_generate_v4();
    ua_float_array_uuid             := uuid_generate_v4();
    ua_float_multidim_array_uuid    := uuid_generate_v4();
    ua_data_sample_uuid             := uuid_generate_v4();
    ua_diagnostic_array_uuid        := uuid_generate_v4();

    -- Insert into Hub
    INSERT INTO public.h_opc_ua_data_types (h_opc_ua_data_type_sk, load_dttm, data_source_id)
    VALUES
        (ua_empty_uuid,                   load_datetime, default_source_id),
        (ua_bool_uuid,                    load_datetime, default_source_id),
        (ua_int_uuid,                     load_datetime, default_source_id),
        (ua_long_uuid,                    load_datetime, default_source_id),
        (ua_double_uuid,                  load_datetime, default_source_id),
        (ua_string_uuid,                  load_datetime, default_source_id),
        (ua_datetime_uuid,                load_datetime, default_source_id),
        (ua_duration_uuid,                load_datetime, default_source_id),
        (ua_int_array_uuid,               load_datetime, default_source_id),
        (ua_float_array_uuid,             load_datetime, default_source_id),
        (ua_float_multidim_array_uuid,    load_datetime, default_source_id),
        (ua_data_sample_uuid,             load_datetime, default_source_id),
        (ua_diagnostic_array_uuid,        load_datetime, default_source_id);

    -- Insert into Satellite
    INSERT INTO public.s_opc_ua_data_types (h_opc_ua_data_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, data_type_id, description, description_ru
    )
    VALUES
        (ua_empty_uuid,                   load_datetime, valid_from_datetime, active_flag_true, default_source_id, 0,   'Empty',                    'Пусто'),
        (ua_bool_uuid,                    load_datetime, valid_from_datetime, active_flag_true, default_source_id, 11,  'Boolean',                   'Логический'),
        (ua_int_uuid,                     load_datetime, valid_from_datetime, active_flag_true, default_source_id, 3,   'Integer',                   'Целое число'),
        (ua_long_uuid,                    load_datetime, valid_from_datetime, active_flag_true, default_source_id, 20,  'Long',                      'Длинное целое'),
        (ua_double_uuid,                  load_datetime, valid_from_datetime, active_flag_true, default_source_id, 5,   'Double',                    'Число с плавающей точкой'),
        (ua_string_uuid,                  load_datetime, valid_from_datetime, active_flag_true, default_source_id, 8,   'String',                    'Строка'),
        (ua_datetime_uuid,                load_datetime, valid_from_datetime, active_flag_true, default_source_id, 7,   'DateTime',                  'Дата и время'),
        (ua_duration_uuid,                load_datetime, valid_from_datetime, active_flag_true, default_source_id, 30,  'Duration',                  'Продолжительность'),
        (ua_int_array_uuid,               load_datetime, valid_from_datetime, active_flag_true, default_source_id, 31, 'Integer Array',             'Массив целых чисел'),
        (ua_float_array_uuid,             load_datetime, valid_from_datetime, active_flag_true, default_source_id, 32, 'Float Array',               'Массив чисел с плавающей точкой'),
        (ua_float_multidim_array_uuid,    load_datetime, valid_from_datetime, active_flag_true, default_source_id, 33, 'Float Multi-Dimensional Array', 'Многомерный массив чисел с плавающей точкой'),
        (ua_data_sample_uuid,             load_datetime, valid_from_datetime, active_flag_true, default_source_id, 34, 'Data Sample',               'Образец данных'),
        (ua_diagnostic_array_uuid,        load_datetime, valid_from_datetime, active_flag_true, default_source_id, 35, 'Diagnostic Array',          'Массив диагностических данных');


    -- Generate UUIDs for Server Data Types
    srv_type_0_uuid  := uuid_generate_v4();
    srv_type_1_uuid  := uuid_generate_v4();
    srv_type_2_uuid  := uuid_generate_v4();
    srv_type_3_uuid  := uuid_generate_v4();
    srv_type_4_uuid  := uuid_generate_v4();
    srv_type_5_uuid  := uuid_generate_v4();
    srv_type_6_uuid  := uuid_generate_v4();
    srv_type_7_uuid  := uuid_generate_v4();
    srv_type_8_uuid  := uuid_generate_v4();
    srv_type_9_uuid  := uuid_generate_v4();
    srv_type_10_uuid := uuid_generate_v4();
    srv_type_11_uuid := uuid_generate_v4();
    srv_type_12_uuid := uuid_generate_v4();
    srv_type_13_uuid := uuid_generate_v4();
    srv_type_14_uuid := uuid_generate_v4();
    srv_type_15_uuid := uuid_generate_v4();

    -- Insert into Hub
    INSERT INTO public.h_data_type_servers (h_data_type_server_sk, load_dttm, data_source_id)
    VALUES
        (srv_type_0_uuid,  load_datetime, default_source_id),
        (srv_type_1_uuid,  load_datetime, default_source_id),
        (srv_type_2_uuid,  load_datetime, default_source_id),
        (srv_type_3_uuid,  load_datetime, default_source_id),
        (srv_type_4_uuid,  load_datetime, default_source_id),
        (srv_type_5_uuid,  load_datetime, default_source_id),
        (srv_type_6_uuid,  load_datetime, default_source_id),
        (srv_type_7_uuid,  load_datetime, default_source_id),
        (srv_type_8_uuid,  load_datetime, default_source_id),
        (srv_type_9_uuid,  load_datetime, default_source_id),
        (srv_type_10_uuid, load_datetime, default_source_id),
        (srv_type_11_uuid, load_datetime, default_source_id),
        (srv_type_12_uuid, load_datetime, default_source_id),
        (srv_type_13_uuid, load_datetime, default_source_id),
        (srv_type_14_uuid, load_datetime, default_source_id),
        (srv_type_15_uuid, load_datetime, default_source_id);

    -- Insert into Satellite
    INSERT INTO public.s_data_type_servers (h_data_type_server_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, data_type_id, description, description_ru
    )
    VALUES
        (srv_type_0_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, 0,  'Null',                      'Пусто (Null)'),
        (srv_type_1_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, 1,  'Boolean',                   'Логический'),
        (srv_type_2_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, 2,  'SByte',                     'Целое со знаком (8 бит)'),
        (srv_type_3_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, 3,  'Byte',                      'Байт (без знака)'),
        (srv_type_4_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, 4,  'Int16',                     'Целое 16-битное со знаком'),
        (srv_type_5_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, 5,  'UInt16',                    'Целое 16-битное без знака'),
        (srv_type_6_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, 6,  'Int32',                     'Целое 32-битное со знаком'),
        (srv_type_7_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, 7,  'UInt32',                    'Целое 32-битное без знака'),
        (srv_type_8_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, 8,  'Int64',                     'Целое 64-битное со знаком'),
        (srv_type_9_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, 9,  'UInt64',                    'Целое 64-битное без знака'),
        (srv_type_10_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 10, 'Float',                     'Число с плавающей точкой (32 бит)'),
        (srv_type_11_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 11, 'Double',                    'Число с плавающей точкой (64 бит)'),
        (srv_type_12_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 12, 'String',                    'Строка'),
        (srv_type_13_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 13, 'DateTime',                  'Дата и время'),
        (srv_type_14_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 14, 'ByteString',                'Массив байтов'),
        (srv_type_15_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 15, 'Double',                    'Число с плавающей точкой (64 бит)');  -- как указано


    -- Generate UUIDs
    log_info_uuid           := uuid_generate_v4();
    log_warning_uuid        := uuid_generate_v4();
    log_error_uuid          := uuid_generate_v4();
    log_fatal_uuid          := uuid_generate_v4();
    log_setpoint_uuid       := uuid_generate_v4();
    log_value_changed_uuid  := uuid_generate_v4();
    log_setpoint2_uuid      := uuid_generate_v4();
    log_alarm_uuid          := uuid_generate_v4();
    log_script_error_uuid   := uuid_generate_v4();

    -- Insert into Hub
    INSERT INTO public.h_tik_scada_log_types (h_tik_scada_log_type_sk, load_dttm, data_source_id)
    VALUES
        (log_info_uuid,          load_datetime, default_source_id),
        (log_warning_uuid,       load_datetime, default_source_id),
        (log_error_uuid,         load_datetime, default_source_id),
        (log_fatal_uuid,         load_datetime, default_source_id),
        (log_setpoint_uuid,      load_datetime, default_source_id),
        (log_value_changed_uuid, load_datetime, default_source_id),
        (log_setpoint2_uuid,     load_datetime, default_source_id),
        (log_alarm_uuid,         load_datetime, default_source_id),
        (log_script_error_uuid,  load_datetime, default_source_id);

    -- Insert into Satellite
    INSERT INTO public.s_tik_scada_log_types (h_tik_scada_log_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, log_type_id, description, description_ru
    )
    VALUES
        (log_info_uuid,          load_datetime, valid_from_datetime, active_flag_true, default_source_id, 0, 'Info',              'Информация'),
        (log_warning_uuid,       load_datetime, valid_from_datetime, active_flag_true, default_source_id, 1, 'Warning',           'Предупреждение'),
        (log_error_uuid,         load_datetime, valid_from_datetime, active_flag_true, default_source_id, 2, 'Error',             'Ошибка'),
        (log_fatal_uuid,         load_datetime, valid_from_datetime, active_flag_true, default_source_id, 3, 'Fatal',             'Критическая ошибка'),
        (log_setpoint_uuid,      load_datetime, valid_from_datetime, active_flag_true, default_source_id, 4, 'Setpoint',          'Уставка'),
        (log_value_changed_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 5, 'Value Changed',     'Изменённое значение'),
        (log_setpoint2_uuid,     load_datetime, valid_from_datetime, active_flag_true, default_source_id, 6, 'Setpoint 2',        'Вторая уставка'),
        (log_alarm_uuid,         load_datetime, valid_from_datetime, active_flag_true, default_source_id, 7, 'Alarm',             'Тревога'),
        (log_script_error_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, 8, 'Script Error',      'Ошибка скрипта');

    -- Generate UUIDs
    action_0_uuid  := uuid_generate_v4();
    action_1_uuid  := uuid_generate_v4();
    action_2_uuid  := uuid_generate_v4();
    action_3_uuid  := uuid_generate_v4();
    action_4_uuid  := uuid_generate_v4();
    action_5_uuid  := uuid_generate_v4();
    action_6_uuid  := uuid_generate_v4();
    action_7_uuid  := uuid_generate_v4();
    action_8_uuid  := uuid_generate_v4();
    action_9_uuid  := uuid_generate_v4();
    action_10_uuid := uuid_generate_v4();
    action_11_uuid := uuid_generate_v4();
    action_12_uuid := uuid_generate_v4();
    action_13_uuid := uuid_generate_v4();
    action_14_uuid := uuid_generate_v4();
    action_15_uuid := uuid_generate_v4();
    action_16_uuid := uuid_generate_v4();
    action_17_uuid := uuid_generate_v4();
    action_18_uuid := uuid_generate_v4();
    action_19_uuid := uuid_generate_v4();

    -- Insert into Hub
    INSERT INTO public.h_user_action_types (h_user_action_type_sk, load_dttm, data_source_id)
    VALUES
        (action_0_uuid,  load_datetime, default_source_id),
        (action_1_uuid,  load_datetime, default_source_id),
        (action_2_uuid,  load_datetime, default_source_id),
        (action_3_uuid,  load_datetime, default_source_id),
        (action_4_uuid,  load_datetime, default_source_id),
        (action_5_uuid,  load_datetime, default_source_id),
        (action_6_uuid,  load_datetime, default_source_id),
        (action_7_uuid,  load_datetime, default_source_id),
        (action_8_uuid,  load_datetime, default_source_id),
        (action_9_uuid,  load_datetime, default_source_id),
        (action_10_uuid, load_datetime, default_source_id),
        (action_11_uuid, load_datetime, default_source_id),
        (action_12_uuid, load_datetime, default_source_id),
        (action_13_uuid, load_datetime, default_source_id),
        (action_14_uuid, load_datetime, default_source_id),
        (action_15_uuid, load_datetime, default_source_id),
        (action_16_uuid, load_datetime, default_source_id),
        (action_17_uuid, load_datetime, default_source_id),
        (action_18_uuid, load_datetime, default_source_id),
        (action_19_uuid, load_datetime, default_source_id);

    -- Insert into Satellite
    INSERT INTO public.s_user_action_types (h_user_action_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, action_type_id, description, description_ru
    )
    VALUES
        (action_0_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, 0,  'Model Added',               'Добавлена модель'),
        (action_1_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, 1,  'Model Removed',             'Удалена модель'),
        (action_2_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, 2,  'Model Renamed',             'Переименована модель'),
        (action_3_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, 3,  'Template Added',            'Добавлен шаблон'),
        (action_4_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, 4,  'Template Removed',          'Удалён шаблон'),
        (action_5_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, 5,  'Template Renamed',          'Переименован шаблон'),
        (action_6_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, 6,  'Property Descriptor Added', 'Добавлено описание свойства'),
        (action_7_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, 7,  'Property Descriptor Removed','Удалено описание свойства'),
        (action_8_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, 8,  'Property Descriptor Edited', 'Изменено описание свойства'),
        (action_9_uuid,  load_datetime, valid_from_datetime, active_flag_true, default_source_id, 9,  'Type Descriptor Added',     'Добавлено описание типа'),
        (action_10_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 10, 'Type Descriptor Removed',   'Удалено описание типа'),
        (action_11_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 11, 'Property Added',            'Добавлено свойство'),
        (action_12_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 12, 'Property Removed',          'Удалено свойство'),
        (action_13_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 13, 'Type Added',                'Добавлен тип'),
        (action_14_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 14, 'Type Removed',              'Удалён тип'),
        (action_15_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 15, 'Rule Added',                'Добавлено правило'),
        (action_16_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 16, 'Rule Removed',              'Удалено правило'),
        (action_17_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 17, 'Bearing Added',             'Добавлен подшипник'),
        (action_18_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 18, 'Bearing Removed',           'Удалён подшипник'),
        (action_19_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 19, 'Bearing Edited',            'Изменён подшипник');


    running_type_level_uuid := uuid_generate_v4();
    running_type_period_uuid := uuid_generate_v4();

    -- Insert into Hub
    INSERT INTO public.h_running_types (h_running_type_sk, load_dttm, data_source_id)
    VALUES
        (running_type_level_uuid, load_datetime, default_source_id),
        (running_type_period_uuid, load_datetime, default_source_id);

    -- Insert into Satellite
    INSERT INTO public.s_running_types (h_running_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, type_id, description, description_ru
    )
    VALUES
        (running_type_level_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 1, 'Level', 'По уровню'),
        (running_type_period_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 2, 'Period', 'По периоду');


        -- Generate UUID for Crate Type
    crate_type_ltr_eu_2_uuid := uuid_generate_v4();

    -- Insert into Hub
    INSERT INTO public.h_crate_types (h_crate_type_sk, load_dttm, data_source_id)
    VALUES
        (crate_type_ltr_eu_2_uuid, load_datetime, default_source_id);

    -- Insert into Satellite
    INSERT INTO public.s_crate_types (h_crate_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, type_id, description, description_ru
    )
    VALUES
        (crate_type_ltr_eu_2_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 0, 'LTR_EU_2', 'LTR_EU_2');


        -- Generate UUIDs for L Card Logic Input Types
    l_card_logic_input_type_differential_uuid := uuid_generate_v4();
    l_card_logic_input_type_single_ended_uuid := uuid_generate_v4();
    l_card_logic_input_type_discr_in_bit_uuid := uuid_generate_v4();
    l_card_logic_input_type_discr_out_bit_uuid := uuid_generate_v4();

    -- Insert into Hub
    INSERT INTO public.h_l_card_logic_input_types (h_l_card_logic_input_type_sk, load_dttm, data_source_id)
    VALUES
        (l_card_logic_input_type_differential_uuid, load_datetime, default_source_id),
        (l_card_logic_input_type_single_ended_uuid, load_datetime, default_source_id),
        (l_card_logic_input_type_discr_in_bit_uuid, load_datetime, default_source_id),
        (l_card_logic_input_type_discr_out_bit_uuid, load_datetime, default_source_id);

    -- Insert into Satellite
    INSERT INTO public.s_l_card_logic_input_types (h_l_card_logic_input_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, type_id, description, description_ru
    )
    VALUES
        (l_card_logic_input_type_differential_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 0, 'DIFFERENTIAL', 'Дифференциальный'),
        (l_card_logic_input_type_single_ended_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 1, 'SINGLE_ENDED', 'Односторонний'),
        (l_card_logic_input_type_discr_in_bit_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 2, 'DISCR_IN_BIT', 'Дискр.вход (бит)'),
        (l_card_logic_input_type_discr_out_bit_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 3, 'DISCR_OUT_BIT', 'Дискр.выход (бит)');


    -- Generate UUIDs for L Card Crate Module Types
    l_card_crate_module_type_ltr25_uuid := uuid_generate_v4();
    l_card_crate_module_type_ltr27_uuid := uuid_generate_v4();
    l_card_crate_module_type_ltr24_uuid := uuid_generate_v4();
    l_card_crate_module_type_ltr11_diff_uuid := uuid_generate_v4();
    l_card_crate_module_type_ltr11_gnd_uuid := uuid_generate_v4();

    -- Insert into Hub
    INSERT INTO public.h_l_card_crate_module_types (h_l_card_crate_module_type_sk, load_dttm, data_source_id)
    VALUES
        (l_card_crate_module_type_ltr25_uuid, load_datetime, default_source_id),
        (l_card_crate_module_type_ltr27_uuid, load_datetime, default_source_id),
        (l_card_crate_module_type_ltr24_uuid, load_datetime, default_source_id),
        (l_card_crate_module_type_ltr11_diff_uuid, load_datetime, default_source_id),
        (l_card_crate_module_type_ltr11_gnd_uuid, load_datetime, default_source_id);

    -- Insert into Satellite
    INSERT INTO public.s_l_card_crate_module_types (h_l_card_crate_module_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, type_id, description, description_ru
    )
    VALUES
        (l_card_crate_module_type_ltr25_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 0, 'LTR25', 'LTR25'),
        (l_card_crate_module_type_ltr27_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 1, 'LTR27', 'LTR27'),
        (l_card_crate_module_type_ltr24_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 2, 'LTR24', 'LTR24'),
        (l_card_crate_module_type_ltr11_diff_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 3, 'LTR11_DIFF', 'LTR11 Дифференциальный (16 каналов)'),
        (l_card_crate_module_type_ltr11_gnd_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 4, 'LTR11_GND', 'LTR11 С общей землей (32 канала)');


    -- Generate UUIDs for Register Types
    register_type_coil_status_uuid := uuid_generate_v4();
    register_type_input_status_uuid := uuid_generate_v4();
    register_type_holding_register_uuid := uuid_generate_v4();
    register_type_input_register_uuid := uuid_generate_v4();

    -- Insert into Hub
    INSERT INTO public.h_register_types (h_register_type_sk, load_dttm, data_source_id)
    VALUES
        (register_type_coil_status_uuid, load_datetime, default_source_id),
        (register_type_input_status_uuid, load_datetime, default_source_id),
        (register_type_holding_register_uuid, load_datetime, default_source_id),
        (register_type_input_register_uuid, load_datetime, default_source_id);

    -- Insert into Satellite
    INSERT INTO public.s_register_types (h_register_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, type_id, description, description_ru
    )
    VALUES
        (register_type_coil_status_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 1, 'CoilStatus', 'Состояние катушки'),
        (register_type_input_status_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 2, 'InputStatus', 'Состояние входа'),
        (register_type_holding_register_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 3, 'HoldingRegister', 'Регистр хранения'),
        (register_type_input_register_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 4, 'InputRegister', 'Входной регистр');


    -- Generate UUIDs for Type Names
    type_name_int8_uuid := uuid_generate_v4();
    type_name_uint8_uuid := uuid_generate_v4();
    type_name_int16_uuid := uuid_generate_v4();
    type_name_uint16_uuid := uuid_generate_v4();
    type_name_int32_uuid := uuid_generate_v4();
    type_name_uint32_uuid := uuid_generate_v4();
    type_name_float32_uuid := uuid_generate_v4();
    type_name_float64_uuid := uuid_generate_v4();
    type_name_boolean_uuid := uuid_generate_v4();

    -- Insert into Hub
    INSERT INTO public.h_type_names (h_type_name_sk, load_dttm, data_source_id)
    VALUES
        (type_name_int8_uuid, load_datetime, default_source_id),
        (type_name_uint8_uuid, load_datetime, default_source_id),
        (type_name_int16_uuid, load_datetime, default_source_id),
        (type_name_uint16_uuid, load_datetime, default_source_id),
        (type_name_int32_uuid, load_datetime, default_source_id),
        (type_name_uint32_uuid, load_datetime, default_source_id),
        (type_name_float32_uuid, load_datetime, default_source_id),
        (type_name_float64_uuid, load_datetime, default_source_id),
        (type_name_boolean_uuid, load_datetime, default_source_id);

    -- Insert into Satellite
    INSERT INTO public.s_type_names (h_type_name_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, type_id, description, description_ru
    )
    VALUES
        (type_name_int8_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 0, 'Int8', 'Целое 8-битное со знаком'),
        (type_name_uint8_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 1, 'UInt8', 'Целое 8-битное без знака'),
        (type_name_int16_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 2, 'Int16', 'Целое 16-битное со знаком'),
        (type_name_uint16_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 3, 'UInt16', 'Целое 16-битное без знака'),
        (type_name_int32_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 4, 'Int32', 'Целое 32-битное со знаком'),
        (type_name_uint32_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 5, 'UInt32', 'Целое 32-битное без знака'),
        (type_name_float32_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 6, 'Float32', 'Число с плавающей точкой 32 бита'),
        (type_name_float64_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 7, 'Float64', 'Число с плавающей точкой 64 бита'),
        (type_name_boolean_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 8, 'Boolean', 'Логическое значение');

    -- Insert into Hub
    INSERT INTO public.h_data_types (h_data_type_sk, load_dttm, data_source_id)
    VALUES
        (data_type_int_array_uuid, load_datetime, default_source_id),
        (data_type_int32_uuid, load_datetime, default_source_id),
        (data_type_float64_uuid, load_datetime, default_source_id),
        (data_type_creyt_uuid, load_datetime, default_source_id),
        (data_type_lcard_uuid, load_datetime, default_source_id),
        (data_type_bode_uuid, load_datetime, default_source_id),
        (data_type_boolean_uuid, load_datetime, default_source_id),
        (data_type_data_sample_uuid, load_datetime, default_source_id),
        (data_type_datetime_uuid, load_datetime, default_source_id),
        (data_type_datetime_array_uuid, load_datetime, default_source_id),
        (data_type_diagnostic_array_uuid, load_datetime, default_source_id),
        (data_type_duration_uuid, load_datetime, default_source_id),
        (data_type_file_uuid, load_datetime, default_source_id),
        (data_type_float_array_uuid, load_datetime, default_source_id),
        (data_type_int64_uuid, load_datetime, default_source_id),
        (data_type_siemens_sample_uuid, load_datetime, default_source_id),
        (data_type_spectrum_uuid, load_datetime, default_source_id),
        (data_type_string_uuid, load_datetime, default_source_id),
        (data_type_pion_uuid, load_datetime, default_source_id),
        (data_type_string_array_uuid, load_datetime, default_source_id),
        (data_type_tikexpert_uuid, load_datetime, default_source_id);

    -- Insert into Satellite
    INSERT INTO public.s_data_types (h_data_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, type_id, description, description_ru
    )
    VALUES
        (data_type_int_array_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, '37BEB486-D0DC-4C9D-9E50-419632F63616'::uuid, 'INT_ARRAY', 'Целочисленный массив'),
        (data_type_int32_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 'CFF1DE41-ED0A-4FE7-A003-2B9107D44F0E'::uuid, 'INT32', 'Целое 32-битное со знаком'),
        (data_type_float64_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, '20DA2798-2340-45E9-8B75-29CFE4F99901'::uuid, 'FLOAT64', 'Число с плавающей точкой 64 бита'),
        (data_type_creyt_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, '4FC8E8D4-CFDB-46DB-9E5F-41CDB9F7EF33'::uuid, 'CREYT', 'Тип данных CREYT'),
        (data_type_lcard_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, '34A809AA-9C72-4BAA-B892-00D4CBA7D7A4'::uuid, 'LCARD', 'Тип данных LCARD'),
        (data_type_bode_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 'C0BE4AE3-3A1B-4A63-9FCC-04DBDDE6A30C'::uuid, 'BODE', 'Тип данных BODE'),
        (data_type_boolean_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 'E940B7EE-ABA1-4447-B940-1686BB70551B'::uuid, 'BOOLEAN', 'Логическое значение'),
        (data_type_data_sample_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, '969420b8-fd94-4476-b06e-ed7408d42a61'::uuid, 'DATA_SAMPLE', 'Образец данных'),
        (data_type_datetime_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, '0232DB10-59E8-4840-BBE6-E8749EAE690F'::uuid, 'DATETIME', 'Дата и время'),
        (data_type_datetime_array_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, '962df766-9d5e-44ad-be1b-1768d3b90d25'::uuid, 'DATETIME_ARRAY', 'Массив дат и времени'),
        (data_type_diagnostic_array_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, '7CF98A0E-E55B-4D00-BD15-479CF05A5DDD'::uuid, 'DIAGNOSTIC_ARRAY', 'Массив диагностических данных'),
        (data_type_duration_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, '6BC397F1-26AC-44B5-9F55-BBAF84475285'::uuid, 'DURATION', 'Продолжительность'),
        (data_type_file_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, '8796C1A5-542E-4E8B-BEB6-0B8B18AC0A18'::uuid, 'FILE', 'Файл'),
        (data_type_float_array_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, '86DA3E91-6E11-445F-AFDF-72E25BCAFB0C'::uuid, 'FLOAT_ARRAY', 'Массив чисел с плавающей точкой'),
        (data_type_int64_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, '399F26C3-F102-4146-A8DE-F44C9441A872'::uuid, 'INT64', 'Целое 64-битное со знаком'),
        (data_type_siemens_sample_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 'C573609B-47C2-4836-88B9-AA595A2B5FDB'::uuid, 'SIEMENS_SAMPLE', 'Образец данных Siemens'),
        (data_type_spectrum_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, '2017DB6E-9629-49FF-8C2F-04C0F8EE135E'::uuid, 'SPECTRUM', 'Спектр'),
        (data_type_string_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 'FD18A4A9-6B08-46E0-A99A-FB69C56C2DEE'::uuid, 'STRING', 'Строка'),
        (data_type_pion_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 'F7B05632-247B-47DB-89A9-1E80AC388FB2'::uuid, 'PION', 'Тип данных PION'),
        (data_type_string_array_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 'A1B3C4D5-E6F7-8901-2345-67890ABCDEFF'::uuid, 'STRING_ARRAY', 'Массив строк'),
        (data_type_tikexpert_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 'C00DE691-45DF-49F9-A94B-442B63D5904C'::uuid, 'TIKEXPERT', 'Тип данных TIKEXPERT');

    -- Generate UUIDs
    spectrum_acceleration_uuid := uuid_generate_v4();
    spectrum_velocity_uuid      := uuid_generate_v4();
    spectrum_displacement_uuid  := uuid_generate_v4();
    spectrum_envelope_uuid := uuid_generate_v4();

    -- Insert into Hub
    INSERT INTO public.h_spectrum_types (h_spectrum_type_sk, load_dttm, data_source_id)
    VALUES
        (spectrum_acceleration_uuid, load_datetime, default_source_id),
        (spectrum_velocity_uuid, load_datetime, default_source_id),
        (spectrum_displacement_uuid, load_datetime, default_source_id),
        (spectrum_envelope_uuid, load_datetime, default_source_id);

    -- Insert into Satellite
    INSERT INTO public.s_spectrum_types (h_spectrum_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id,
        type_id, description, description_ru)
    VALUES
        (spectrum_acceleration_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 0, 'Acceleration', 'Спектр ускорения'),
        (spectrum_velocity_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 1, 'Velocity', 'Спектр скорости'),
        (spectrum_displacement_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 2, 'Displacement', 'Спектр перемещения'),
        (spectrum_envelope_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 3, 'Envelope', 'Спектр огибающей');

    -- Generate UUIDs for Property Types
    property_type_unknown_uuid := uuid_generate_v4();
    property_type_boolean_uuid := uuid_generate_v4();
    property_type_integer_uuid := uuid_generate_v4();
    property_type_long_uuid := uuid_generate_v4();
    property_type_double_uuid := uuid_generate_v4();
    property_type_string_uuid := uuid_generate_v4();
    property_type_datetime_uuid := uuid_generate_v4();
    property_type_duration_uuid := uuid_generate_v4();
    property_type_datasample_uuid := uuid_generate_v4();
    property_type_intarray_uuid := uuid_generate_v4();
    property_type_floatarray_uuid := uuid_generate_v4();
    property_type_diagnosticarray_uuid := uuid_generate_v4();
    property_type_file_uuid := uuid_generate_v4();
    property_type_floatmultidimarray_uuid := uuid_generate_v4();
    property_type_datetimearray_uuid := uuid_generate_v4();
    property_type_spectrum_uuid := uuid_generate_v4();
    property_type_bode_uuid := uuid_generate_v4();
    
    -- Insert into Hub
    INSERT INTO public.h_property_types (h_property_type_sk, load_dttm, data_source_id)
    VALUES
    (property_type_unknown_uuid, load_datetime, default_source_id),
    (property_type_boolean_uuid, load_datetime, default_source_id),
    (property_type_integer_uuid, load_datetime, default_source_id),
    (property_type_long_uuid, load_datetime, default_source_id),
    (property_type_double_uuid, load_datetime, default_source_id),
    (property_type_string_uuid, load_datetime, default_source_id),
    (property_type_datetime_uuid, load_datetime, default_source_id),
    (property_type_duration_uuid, load_datetime, default_source_id),
    (property_type_datasample_uuid, load_datetime, default_source_id),
    (property_type_intarray_uuid, load_datetime, default_source_id),
    (property_type_floatarray_uuid, load_datetime, default_source_id),
    (property_type_diagnosticarray_uuid, load_datetime, default_source_id),
    (property_type_file_uuid, load_datetime, default_source_id),
    (property_type_floatmultidimarray_uuid, load_datetime, default_source_id),
    (property_type_datetimearray_uuid, load_datetime, default_source_id),
    (property_type_spectrum_uuid, load_datetime, default_source_id),
    (property_type_bode_uuid, load_datetime, default_source_id);
    
    -- Insert into Satellite with descriptions
    INSERT INTO public.s_property_types (h_property_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, type_id, description, description_ru
    )
    VALUES
    (property_type_unknown_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 0, 'Unknown', 'Неизвестный'),
    (property_type_boolean_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 1, 'Boolean (bool)', 'Логический (bool)'),
    (property_type_integer_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 2, 'Integer (int)', 'Целочисленный (int)'),
    (property_type_long_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 3, 'Long', 'Длинное целое (long)'),
    (property_type_double_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 4, 'Double', 'Число с плавающей точкой (double)'),
    (property_type_string_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 5, 'String', 'Строка (string)'),
    (property_type_datetime_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 6, 'DateTime', 'Дата и время'),
    (property_type_duration_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 7, 'Duration', 'Продолжительность'),
    (property_type_datasample_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 8, 'Data Sample', 'Образец данных'),
    (property_type_intarray_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 9, 'Integer Array', 'Массив целых чисел'),
    (property_type_floatarray_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 10, 'Float Array', 'Массив чисел с плавающей точкой'),
    (property_type_diagnosticarray_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 11, 'Diagnostic Array', 'Массив диагностических данных'),
    (property_type_file_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 12, 'File', 'Файл'),
    (property_type_floatmultidimarray_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 13, 'Float Multi-Dimensional Array', 'Многомерный массив чисел с плавающей точкой'),
    (property_type_datetimearray_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 14, 'DateTime Array', 'Массив дат и времени'),
    (property_type_spectrum_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 15, 'Spectrum', 'Спектр'),
    (property_type_bode_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 16, 'Bode', 'Боде (Bode)');


        -- Generate UUIDs for Storage Types
    storage_type_db_uuid := uuid_generate_v4();
    storage_type_file_uuid := uuid_generate_v4();
    
    -- Insert into Hub
    INSERT INTO public.h_storage_types (h_storage_type_sk, load_dttm, data_source_id)
    VALUES
        (storage_type_db_uuid, load_datetime, default_source_id),
        (storage_type_file_uuid, load_datetime, default_source_id);
    
    -- Insert into Satellite
    INSERT INTO public.s_storage_types (
        h_storage_type_sk, load_dttm, valid_from_dttm, active_flag, 
        data_source_id, storage_type_id, description, description_ru
    )
    VALUES
        (storage_type_db_uuid, load_datetime, valid_from_datetime, active_flag_true, 
         default_source_id, 0, 'Database Storage', 'Хранение в базе данных'),
        (storage_type_file_uuid, load_datetime, valid_from_datetime, active_flag_true, 
         default_source_id, 1, 'File System Storage', 'Хранение в файловой системе');
    
        -- Generate UUIDs for Convert Types
    convert_type_factor_uuid := uuid_generate_v4();
    convert_type_function_uuid := uuid_generate_v4();

    -- Insert into Hub
    INSERT INTO public.h_convert_types (h_convert_type_sk, load_dttm, data_source_id)
    VALUES
    (convert_type_factor_uuid, load_datetime, default_source_id),
    (convert_type_function_uuid, load_datetime, default_source_id);

    -- Insert into Satellite
    INSERT INTO public.s_convert_types (h_convert_type_sk, load_dttm, valid_from_dttm, active_flag, data_source_id, convert_type_id, description, description_ru
    )
    VALUES
    (convert_type_factor_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 0, 'Factor', 'Коэффициент'),
    (convert_type_function_uuid, load_datetime, valid_from_datetime, active_flag_true, default_source_id, 1, 'Function', 'Функция');

END $$;