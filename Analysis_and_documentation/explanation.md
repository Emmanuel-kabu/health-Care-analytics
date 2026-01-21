# Explanation: Why runtime logging, separate validation, and dedicated audit

This document explains the design decisions behind the repository layout and
the emphasis on runtime logging (the `etl_logs` framework), separate schema
validation, and a dedicated HIPAA-aware audit framework instead of inlining
logging directly inside every DDL/DML/ETL script.

Goals
- Traceability: obtain an authoritative, timestamped record of ETL steps,
  validation results, exports, and data-quality issues tied to a single
  `execution_id`.
- Reusability: share logging/validation/export/audit services across multiple
  ETL jobs and environments.
- Security & compliance: ensure PHI access and data changes are auditable and
  retained according to policy.
- Observability & operations: enable monitoring, alerting, and post-mortem
  analysis without changing business logic scripts.

Why runtime logging (separate framework) instead of embedding logging in
main scripts

1) Separation of concerns
- ETL, schema migrations, validation, reporting, and logging are distinct
  responsibilities. Keeping logging in a dedicated, small API (`etl_logs`)
  reduces the cognitive load and prevents business logic from being cluttered
  with plumbing.

2) Reuse and consistency
- A single logging API ensures uniform log formats and semantics (fields,
  severity levels, `execution_id` usage), which simplifies downstream
  exports, dashboards, and automated checks.

3) Permission and security boundaries
- Audit and export systems often require stricter permissions and retention
  policies. Isolating them into specific schemas and functions makes it easier
  to grant minimal, auditable privileges to service accounts (e.g., `ETL_SERVICE`).

4) Easier testing and idempotency
- A small logging API can be unit tested and mocked. ETL scripts remain focused
  and can be validated independently. Idempotent logging functions help
  rerun ETL jobs safely.

5) Operational flexibility
- With a separate export system (`etl_exports`) you can change export formats
  (CSV/JSON/HTML), path locations, or retention rules without modifying core
  ETL jobs.

6) Performance and failure handling
- Logging functions can be optimized, batched, or written to append-only tables
  designed for high throughput. If exports fail, ETL can continue while the
  export subsystem retries or alerts.

Schema validation as a separate module

- Purpose: ensure the target star schema matches expected structure and
  semantic constraints before and after ETL. The `validate_healthcare_schemas()`
  procedure performs existence, type, nullability, duplicate, and business-rule
  checks and writes results to `etl_logs.schema_validation_log` and
  `etl_logs.data_quality_log`.

- Benefits:
  - Run validations automatically from orchestrator after ETL loads.  
  - Keep validation logic maintainable and versionable separately from ETL.
  - Provide actionable outputs that can be exported and reviewed by analysts.

HIPAA compliance and dedicated audit framework

- PHI requires robust auditing of who accessed or modified patient data.
  The `healthcare_audit` schema contains an `audit_log` and `phi_access_log`.

- Key compliance decisions implemented:
  - Triggers that capture DML events on OLTP tables and write rich JSONB
    context (session, client IP, old/new values).  
  - Separate PHI-specific tracking in `phi_access_log` to record minimum
    necessary justification and purpose.  
  - Long retention defaults (e.g., 7 years) in event-type metadata and an
    archival function to move old records to an archive table.

- Why separate from `etl_logs`?
  - Audit trails have different access patterns, retention, and security
    requirements; segregating audit data reduces risk and simplifies
    compliance reviews.

Practical integration patterns

- Instrument ETL steps (recommended pattern):

```sql
-- start
SELECT etl_logs.log_etl_step_start(v_exec_id, 'load_star', 'load_dim_patient', 10);
-- run ETL step (stored proc or SQL)
-- complete (with counts)
PERFORM etl_logs.log_etl_step_complete(v_log_id, 'COMPLETED', rows_processed, rows_inserted, rows_updated, rows_deleted);
```

- Run validation and export from the orchestrator (example: `main/run_etl_and_checks.sql`).
- For migrations that change schema, add a schema-audit call in migration scripts:

```sql
PERFORM log_audit_event('SCHEMA_CHANGE', 'DDL', 'OLTP_schema_ddl.sql', 'public', NULL, NULL, NULL, NULL, 'CREATE TABLE ...', TRUE, NULL, 'Applied migration 20260121');
```

Deployment and operational notes

- Order of installation: `etl_logging_framework` → OLTP DDL → `healthcare_audit_framework` → star schema → file-export → validation → orchestrator runs.
- Access control: grant execute on logging/audit functions only to service accounts.
- Retention and archival: use `archive_audit_records()` and periodic cleanup of `etl_exports.export_history` according to policy.

When to inline logging (rare)
- Very small one-off scripts where creating a dependency is not worth it. For
  all production or repeatable tasks, prefer calling shared logging/audit APIs.

Summary

Separating runtime logging, validation, and audit into dedicated modules
produces a maintainable, auditable, and secure architecture. It supports
consistent observability across ETL jobs and OLTP operations while keeping
business logic scripts simple and focused.

For more detail, see:
- `Analysis_and_documentation/etl_design.txt`  
- `audit_and_logging/etl_logging_framework.sql`  
- `validation/schema_validation.sql`
