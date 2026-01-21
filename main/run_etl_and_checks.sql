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

    -- 1) ETL: run the instrumented ETL procedure and pass orchestrator execution_id
    RAISE NOTICE 'Running ETL (execution id: %)...', v_execution_id;
    CALL run_healthcare_etl(v_execution_id);

    PERFORM etl_logs.log_etl_step_complete(v_log_id, 'COMPLETED', 0, 0, 0, 0);

    -- 2) Schema Validation: pass same execution_id so logs are linked
    RAISE NOTICE 'Running schema validation (linked to ETL execution id: %)...', v_execution_id;
    CALL validate_healthcare_schemas(v_execution_id);

    -- 3) Export logs/reports for the linked execution
    RAISE NOTICE 'Exporting ETL logs and validation reports (execution id: %)...', v_execution_id;
    CALL etl_exports.export_logs_to_files(v_execution_id, ARRAY['LOGS','SUMMARY','PERFORMANCE','QUALITY_REPORT'], '/tmp/etl_exports');

    -- 4) Audit maintenance: archive old audit records (optional)
    PERFORM archive_audit_records();

    RAISE NOTICE 'Orchestration completed. Execution id: %', v_execution_id;
END $$;

-- Helpful notes:
-- - Edit this file to replace the ETL placeholder with your real ETL procedure(s).
-- - If running on Windows, update the output directory path passed to export procedures.
-- - To include DDL deployments, uncomment the `\i` lines above.
