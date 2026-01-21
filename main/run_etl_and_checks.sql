-- Orchestrator: Run ETL steps, validations, exports, and audit maintenance
-- Usage: run this in `psql` or include in your pipeline.

-- Stop on first error when running via psql
\set ON_ERROR_STOP on

-- === Load supporting frameworks and schemas (idempotent) ===
\i etl_logging_framework.sql
\i healthcare_audit_framework.sql
\i file_output_logging.sql

-- Optionally ensure OLTP and star DDL are created (uncomment to run)
-- \i OLTP_schema/OLTP_schema_ddl.sql
-- \i star_schema/star_schema.sql

-- === Orchestration logic ===
DO $$
DECLARE
    v_execution_id UUID := gen_random_uuid();
    v_log_id INTEGER;
    v_validation_exec UUID;
BEGIN
    -- Start orchestrator entry
    v_log_id := etl_logs.log_etl_step_start(v_execution_id, 'orchestrator', 'start_orchestration', 1);

    -- 1) ETL: placeholder for actual ETL work (extract/transform/load)
    -- Replace the PERFORM below with your real ETL procedure call, e.g.
    -- PERFORM etl_jobs.load_star_from_oltp(v_execution_id);
    PERFORM etl_logs.log_data_quality_issue(v_execution_id, 'orchestrator', NULL, 'ETL_PLACEHOLDER', 'Replace with real ETL procedure', 0, NULL, 'LOW');

    PERFORM etl_logs.log_etl_step_complete(v_log_id, 'COMPLETED', 0, 0, 0, 0);

    -- 2) Schema Validation (this procedure creates its own execution_id)
    RAISE NOTICE 'Running schema validation...';
    CALL validate_healthcare_schemas();

    -- Capture the most recent validation execution id for export/analysis
    SELECT execution_id INTO v_validation_exec
    FROM etl_logs.etl_execution_log
    WHERE procedure_name = 'validate_healthcare_schemas'
    ORDER BY start_time DESC
    LIMIT 1;

    -- 3) Export logs/reports for orchestrator and validation runs
    RAISE NOTICE 'Exporting ETL logs and validation reports...';
    -- Export for main orchestrator execution
    CALL etl_exports.export_logs_to_files(v_execution_id, ARRAY['LOGS','SUMMARY','PERFORMANCE','QUALITY_REPORT'], '/tmp/etl_exports');

    -- Also export validation run logs if present
    IF v_validation_exec IS NOT NULL THEN
        CALL etl_exports.export_logs_to_files(v_validation_exec, ARRAY['LOGS','SUMMARY','QUALITY_REPORT'], '/tmp/etl_exports');
    END IF;

    -- 4) Audit maintenance: archive old audit records (optional)
    PERFORM archive_audit_records();

    RAISE NOTICE 'Orchestration completed. Execution id: %', v_execution_id;
END $$;

-- Helpful notes:
-- - Edit this file to replace the ETL placeholder with your real ETL procedure(s).
-- - If running on Windows, update the output directory path passed to export procedures.
-- - To include DDL deployments, uncomment the `\i` lines above.
