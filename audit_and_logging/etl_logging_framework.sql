-- ===================================================================
-- ETL LOGGING FRAMEWORK
-- Provides `etl_logs` schema, tables and helper functions used by
-- `schema_validation.sql`, `file_output_logging.sql` and other scripts.
-- ===================================================================

CREATE SCHEMA IF NOT EXISTS etl_logs;

-- ETL execution log
CREATE TABLE IF NOT EXISTS etl_logs.etl_execution_log (
    log_id SERIAL PRIMARY KEY,
    execution_id UUID NOT NULL,
    procedure_name TEXT,
    step_name TEXT,
    step_order INT,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(50), -- RUNNING, COMPLETED, FAILED
    rows_processed BIGINT DEFAULT 0,
    rows_inserted BIGINT DEFAULT 0,
    rows_updated BIGINT DEFAULT 0,
    rows_deleted BIGINT DEFAULT 0,
    duration_seconds DOUBLE PRECISION,
    error_message TEXT,
    metadata JSONB
);

-- Data quality / validation issues
CREATE TABLE IF NOT EXISTS etl_logs.data_quality_log (
    quality_id SERIAL PRIMARY KEY,
    execution_id UUID,
    table_name TEXT,
    column_name TEXT,
    issue_type TEXT,
    severity_level VARCHAR(20), -- LOW, MEDIUM, HIGH, CRITICAL
    affected_rows BIGINT,
    issue_description TEXT,
    sample_values TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Schema validation log (for structural mismatches)
CREATE TABLE IF NOT EXISTS etl_logs.schema_validation_log (
    schema_validation_id SERIAL PRIMARY KEY,
    execution_id UUID,
    source_table TEXT,
    target_table TEXT,
    validation_type TEXT,
    validation_status TEXT,
    expected_value TEXT,
    actual_value TEXT,
    issue_description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Performance metrics
CREATE TABLE IF NOT EXISTS etl_logs.performance_metrics (
    metric_id SERIAL PRIMARY KEY,
    execution_id UUID,
    table_name TEXT,
    operation_type TEXT,
    duration_ms BIGINT,
    rows_affected BIGINT,
    start_time TIMESTAMP,
    end_time TIMESTAMP
);

-- ===== Helper functions used by validation and export scripts =====

-- Start an ETL step and return log_id
CREATE OR REPLACE FUNCTION etl_logs.log_etl_step_start(
    p_execution_id UUID,
    p_procedure_name TEXT,
    p_step_name TEXT,
    p_step_order INT,
    p_metadata JSONB DEFAULT NULL
) RETURNS INTEGER AS $$
DECLARE
    v_log_id INTEGER;
BEGIN
    INSERT INTO etl_logs.etl_execution_log (
        execution_id, procedure_name, step_name, step_order, start_time, status, metadata
    ) VALUES (
        p_execution_id, p_procedure_name, p_step_name, p_step_order, CURRENT_TIMESTAMP, 'RUNNING', p_metadata
    ) RETURNING log_id INTO v_log_id;

    RETURN v_log_id;
END;
$$ LANGUAGE plpgsql;

-- Mark a step complete (or failed) and update counts
CREATE OR REPLACE FUNCTION etl_logs.log_etl_step_complete(
    p_log_id INTEGER,
    p_status VARCHAR,
    p_rows_processed BIGINT DEFAULT 0,
    p_rows_inserted BIGINT DEFAULT 0,
    p_rows_updated BIGINT DEFAULT 0,
    p_rows_deleted BIGINT DEFAULT 0
) RETURNS VOID AS $$
BEGIN
    UPDATE etl_logs.etl_execution_log
    SET status = p_status,
        rows_processed = p_rows_processed,
        rows_inserted = p_rows_inserted,
        rows_updated = p_rows_updated,
        rows_deleted = p_rows_deleted,
        end_time = CURRENT_TIMESTAMP,
        duration_seconds = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - start_time))
    WHERE log_id = p_log_id;
END;
$$ LANGUAGE plpgsql;

-- Insert a data quality issue
CREATE OR REPLACE FUNCTION etl_logs.log_data_quality_issue(
    p_execution_id UUID,
    p_table_name TEXT,
    p_column_name TEXT,
    p_issue_type TEXT,
    p_issue_description TEXT,
    p_affected_rows BIGINT DEFAULT 0,
    p_sample_values TEXT DEFAULT NULL,
    p_severity_level VARCHAR DEFAULT 'MEDIUM'
) RETURNS VOID AS $$
BEGIN
    INSERT INTO etl_logs.data_quality_log (
        execution_id, table_name, column_name, issue_type, issue_description, affected_rows, sample_values, severity_level
    ) VALUES (
        p_execution_id, p_table_name, p_column_name, p_issue_type, p_issue_description, p_affected_rows, p_sample_values, p_severity_level
    );
END;
$$ LANGUAGE plpgsql;

-- Validate table existence
CREATE OR REPLACE FUNCTION etl_logs.validate_table_exists(
    p_execution_id UUID,
    p_schema_name TEXT,
    p_table_name TEXT
) RETURNS BOOLEAN AS $$
DECLARE
    v_exists BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = COALESCE(p_schema_name, 'public')
          AND table_name = p_table_name
    ) INTO v_exists;

    IF NOT v_exists THEN
        PERFORM etl_logs.log_data_quality_issue(p_execution_id, p_table_name, NULL, 'TABLE_MISSING', 'Required table missing', 0, NULL, 'CRITICAL');
    END IF;

    RETURN v_exists;
END;
$$ LANGUAGE plpgsql;

-- Validate column schema (basic check: existence and data type contains expected)
CREATE OR REPLACE FUNCTION etl_logs.validate_column_schema(
    p_execution_id UUID,
    p_schema_name TEXT,
    p_table_name TEXT,
    p_column_name TEXT,
    p_expected_data_type TEXT
) RETURNS BOOLEAN AS $$
DECLARE
    v_found BOOLEAN := FALSE;
    v_actual_data_type TEXT;
BEGIN
    SELECT data_type || COALESCE('('||character_maximum_length||')','') INTO v_actual_data_type
    FROM information_schema.columns
    WHERE table_schema = COALESCE(p_schema_name, 'public')
      AND table_name = p_table_name
      AND column_name = p_column_name
    LIMIT 1;

    IF v_actual_data_type IS NOT NULL THEN
        v_found := TRUE;
        IF p_expected_data_type IS NOT NULL AND p_expected_data_type <> '' THEN
            IF POSITION(LOWER(p_expected_data_type) IN LOWER(v_actual_data_type)) = 0 THEN
                PERFORM etl_logs.log_data_quality_issue(p_execution_id, p_table_name, p_column_name, 'TYPE_MISMATCH', format('Expected %s but found %s', p_expected_data_type, v_actual_data_type), 0, NULL, 'HIGH');
                RETURN FALSE;
            END IF;
        END IF;
    ELSE
        PERFORM etl_logs.log_data_quality_issue(p_execution_id, p_table_name, p_column_name, 'COLUMN_MISSING', 'Required column missing', 0, NULL, 'CRITICAL');
    END IF;

    RETURN v_found;
END;
$$ LANGUAGE plpgsql;

-- Count NULL violations for a column
CREATE OR REPLACE FUNCTION etl_logs.validate_null_constraints(
    p_execution_id UUID,
    p_schema_name TEXT,
    p_table_name TEXT,
    p_column_name TEXT
) RETURNS BIGINT AS $$
DECLARE
    v_sql TEXT;
    v_count BIGINT := 0;
BEGIN
    v_sql := format('SELECT COUNT(*) FROM %I.%I WHERE %I IS NULL', COALESCE(p_schema_name,'public'), p_table_name, p_column_name);
    EXECUTE v_sql INTO v_count;

    IF v_count > 0 THEN
        PERFORM etl_logs.log_data_quality_issue(p_execution_id, p_table_name, p_column_name, 'NULL_VIOLATION', format('%s NULL values found', v_count), v_count, NULL, 'HIGH');
    END IF;

    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

-- Check duplicates for a column
CREATE OR REPLACE FUNCTION etl_logs.validate_duplicates(
    p_execution_id UUID,
    p_schema_name TEXT,
    p_table_name TEXT,
    p_column_name TEXT
) RETURNS BIGINT AS $$
DECLARE
    v_sql TEXT;
    v_count BIGINT := 0;
BEGIN
    v_sql := format('SELECT COUNT(*) FROM (SELECT %I, COUNT(*) AS c FROM %I.%I GROUP BY %I HAVING COUNT(*) > 1) t', p_column_name, COALESCE(p_schema_name,'public'), p_table_name, p_column_name);
    EXECUTE v_sql INTO v_count;

    IF v_count > 0 THEN
        PERFORM etl_logs.log_data_quality_issue(p_execution_id, p_table_name, p_column_name, 'DUPLICATES', format('%s duplicate values found', v_count), v_count, NULL, 'HIGH');
    END IF;

    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

-- Generate a compact ETL summary JSONB
CREATE OR REPLACE FUNCTION etl_logs.generate_etl_summary(p_execution_id UUID) RETURNS JSONB AS $$
DECLARE
    v_total_steps INT;
    v_successful INT;
    v_failed INT;
    v_total_rows BIGINT;
    v_total_duration DOUBLE PRECISION;
BEGIN
    SELECT COUNT(*) FILTER (WHERE execution_id = p_execution_id) INTO v_total_steps FROM etl_logs.etl_execution_log;
    SELECT COUNT(*) FILTER (WHERE execution_id = p_execution_id AND status = 'COMPLETED') INTO v_successful FROM etl_logs.etl_execution_log;
    SELECT COUNT(*) FILTER (WHERE execution_id = p_execution_id AND status = 'FAILED') INTO v_failed FROM etl_logs.etl_execution_log;
    SELECT COALESCE(SUM(rows_processed),0) INTO v_total_rows FROM etl_logs.etl_execution_log WHERE execution_id = p_execution_id;
    SELECT COALESCE(SUM(duration_seconds),0) INTO v_total_duration FROM etl_logs.etl_execution_log WHERE execution_id = p_execution_id;

    RETURN jsonb_build_object(
        'total_steps', COALESCE(v_total_steps,0),
        'successful_steps', COALESCE(v_successful,0),
        'failed_steps', COALESCE(v_failed,0),
        'total_rows_processed', v_total_rows,
        'total_duration_seconds', v_total_duration
    );
END;
$$ LANGUAGE plpgsql;

-- Indexes for quick lookups
CREATE INDEX IF NOT EXISTS idx_etl_execution_execution_id ON etl_logs.etl_execution_log(execution_id);
CREATE INDEX IF NOT EXISTS idx_data_quality_execution_id ON etl_logs.data_quality_log(execution_id);
CREATE INDEX IF NOT EXISTS idx_schema_validation_execution_id ON etl_logs.schema_validation_log(execution_id);

RAISE NOTICE 'ETL Logging Framework installed successfully.';
