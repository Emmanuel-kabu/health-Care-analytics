**User Guide**

This guide explains how to run the ETL, run schema validation, view logs, and export reports for this repository.

**Quick Start**
- **Load frameworks**: run the logging/audit/export SQL files first: [audit_and_logging/etl_logging_framework.sql](audit_and_logging/etl_logging_framework.sql#L1), [audit_and_logging/healthcare_audit_framework.sql](audit_and_logging/healthcare_audit_framework.sql#L1), [audit_and_logging/file_output_logging.sql](audit_and_logging/file_output_logging.sql#L1).
- **Run ETL**: execute the star ETL script: [star_schema/star_schema_dml.sql](star_schema/star_schema_dml.sql#L1).
- **Run Validation**: execute and call the validator in [validation/schema_validation.sql](validation/schema_validation.sql#L1).

**Run ETL (one-off)**
- **Command**:
```bash
psql -d <your_db> -f "star_schema/star_schema_dml.sql"
```
- What it does: creates/updates star tables and runs the `run_healthcare_etl()` procedure which writes per-step runtime records to the `etl_logs.etl_execution_log` table.

**Run Schema Validation (one-off)**
- Load the validator then call it:
```bash
psql -d <your_db> -f "validation/schema_validation.sql"
psql -d <your_db> -c "CALL validate_healthcare_schemas();"
```
- Notes: `validate_healthcare_schemas()` creates its own `execution_id` and writes results to `etl_logs.schema_validation_log` and `etl_logs.data_quality_log`.

**Orchestration (recommended)**
- Use the orchestrator to run ETL, validation and exports in sequence: [main/run_etl_and_checks.sql](main/run_etl_and_checks.sql#L1).
- To run it:
```bash
psql -d <your_db> -f "main/run_etl_and_checks.sql"
```
- The orchestrator currently contains an ETL placeholder; replace the placeholder with `CALL run_healthcare_etl();` if you want the orchestrator to run the instrumented ETL.

**Linking ETL + Validation execution IDs**
- Current behavior: ETL (`run_healthcare_etl`) and validation (`validate_healthcare_schemas`) each create separate `execution_id` values.
- Options to link runs:
  - **Option A**: Modify `validate_healthcare_schemas()` to accept an optional `execution_id UUID` parameter so you can pass the ETL `execution_id` into the validator.
  - **Option B**: Update `main/run_etl_and_checks.sql` to `CALL run_healthcare_etl()` then capture its `execution_id` (or extract the most recent ETL `execution_id` from `etl_logs.etl_execution_log`) and pass it into validation/export functions.
- I can implement either option—tell me which.

**View logs and results**
- Recent ETL steps:
  - **Query**: `SELECT * FROM etl_logs.etl_execution_log ORDER BY start_time DESC LIMIT 50;`
- Data quality issues:
  - **Query**: `SELECT * FROM etl_logs.data_quality_log WHERE execution_id = '<uuid>' ORDER BY created_at DESC;`
- Schema validation results:
  - **Query**: `SELECT * FROM etl_logs.schema_validation_log WHERE execution_id = '<uuid>' ORDER BY created_at DESC;`
- ETL summary JSON:
  - **Query**: `SELECT etl_logs.generate_etl_summary('<uuid>');`

**Exporting reports**
- Use the export helper in [audit_and_logging/file_output_logging.sql](audit_and_logging/file_output_logging.sql#L1).
- Example (adjust output path on Windows):
```sql
CALL etl_exports.export_logs_to_files('<execution_uuid>'::uuid, ARRAY['LOGS','SUMMARY','QUALITY_REPORT'], 'C:/temp/etl_exports');
```

**Windows path note**
- Default example paths use `/tmp/etl_exports`. On Windows update to a valid path (for example `C:/temp/etl_exports`) when calling export functions or in `main/run_etl_and_checks.sql`.

**CI / gating checklist (minimal)**
- **Pre-deploy**: run `psql -f validation/schema_validation.sql -c "CALL validate_healthcare_schemas();"` and fail pipeline if `etl_logs.schema_validation_log` contains CRITICAL issues.
- **Smoke test**: run a lightweight ETL step against a small test schema and ensure no FAILED statuses in `etl_logs.etl_execution_log`.
- **Exports**: ensure `etl_exports.export_logs_to_files()` completes and artifacts are archived as pipeline outputs.

**Next steps I can do for you**
- **Implement Option A**: add optional `execution_id` parameter to `validate_healthcare_schemas()` and propagate into its `etl_logs` calls.
- **Implement Option B**: update `main/run_etl_and_checks.sql` to call `CALL run_healthcare_etl();` then run validation and exports with linked IDs.
- **Adjust exports**: change default export path for Windows and add a config variable.

**File references**
- ETL script: [star_schema/star_schema_dml.sql](star_schema/star_schema_dml.sql#L1)
- Validation: [validation/schema_validation.sql](validation/schema_validation.sql#L1)
- Logging framework: [audit_and_logging/etl_logging_framework.sql](audit_and_logging/etl_logging_framework.sql#L1)
- Orchestrator: [main/run_etl_and_checks.sql](main/run_etl_and_checks.sql#L1)

---
Generated on 2026-01-21.
