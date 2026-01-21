**Audit Components & Justification**

This document lists the audit components used in the repository's audit framework, describes what each component does, and explains why it matters for compliance, forensic readiness, and operational trust.

**Audit Trail Tables**: central, append-only tables (e.g., `healthcare_audit.audit_log`) that record events, actor, timestamp, action, object, and metadata. Justification: provides a durable, queryable history for investigation, regulatory reporting, and reconstruction of events.

**DML Triggers (row-level)**: triggers on OLTP tables that capture INSERT/UPDATE/DELETE operations and write normalized entries to audit tables. Justification: ensures every change to source PHI and key records is recorded at the source (near-zero blind spots) and supports fine-grained forensics.

**DDL Audit (schema changes)**: record CREATE/ALTER/DROP operations, migration scripts executed, and the user who invoked them. Justification: schema changes can affect data meaning and compliance; tracking DDL enables rollback investigation and detects unauthorized structural changes.

**PHI Access Log**: specialized log capturing read/access events on PHI-sensitive tables/columns (who, when, which rows/filters). Justification: HIPAA requires monitoring and proving legitimate access to PHI; read-logs are critical for breach investigation and least-privilege enforcement.

**User / Role Change Log**: record GRANT/REVOKE and role membership changes, plus privileged account creation. Justification: access provisioning is a primary attack vector; tracking changes supports audits and fast remediation of mis-configured privileges.

**ETL Step Logging**: per-step ETL execution records (execution_id, step_name, start/end, rows processed, status) stored in `etl_logs.etl_execution_log`. Justification: ties data movement to audit records, helps triage broken loads, and links operational metrics to data provenance.

**Data Quality & Validation Logs**: structured records (e.g., `etl_logs.data_quality_log` and `etl_logs.schema_validation_log`) capturing null violations, duplicates, schema mismatches, and business-rule failures. Justification: provides evidence of data integrity and reasons for rejecting or quarantining records; essential for trustworthy analytics and regulatory reporting.

**File Output / Export Logging**: track exports, file names, destinations, checksums, and who requested exports (implemented in `etl_exports`). Justification: exported datasets are a major data egress risk; logging creates an audit trail for data exfiltration inquiries and compliance with disclosed uses.

**Tamper-Evidence (hashing / checksums)**: store cryptographic hashes or HMACs for important audit records or exported artifacts. Justification: enables detection of post-hoc tampering and supports chain-of-custody for legal or compliance investigations.

**Retention & Archival Controls**: policies and jobs that archive older audit records and enforce retention windows (with secure archives). Justification: balances forensic needs with storage/cost and legal retention requirements; ensures old logs are still accessible when required.

**Audit Integrity Verification**: periodic jobs that verify expected counts, hash chains, and cross-checks between OLTP and audit logs (e.g., reconcile DML events vs current row state). Justification: detects missed captures, silent failures, and ensures the audit system itself is functioning correctly.

**Anomaly Detection & Alerts**: lightweight rules (e.g., unusually high read volume on PHI, repeated failed DDL attempts) that generate alerts to security/ops teams. Justification: early detection shortens mean-time-to-detection for suspicious activity and triggers immediate review.

**Forensic Export & Reporting**: pre-built queries and export routines to produce report bundles for incidents (audit slices, related ETL logs, exports manifest). Justification: speeds incident response and delivers standardized evidence packages for legal/compliance teams.

**Secure Storage & Encryption**: ensure audit logs and exported artifacts are stored with appropriate encryption-at-rest and strict access controls. Justification: audit logs often contain sensitive metadata and potentially PHI — they must be protected at the same level as source data.

**Separation of Duties & Minimal Privilege**: enforce that audit-writing roles are constrained and that audit readers are governed by separate approvals. Justification: prevents insider tampering and ensures that audit data remains reliable and independently verifiable.

**Where implemented in this repo**:
- Audit triggers and audit tables: [audit_and_logging/healthcare_audit_framework.sql](audit_and_logging/healthcare_audit_framework.sql#L1)
- ETL runtime + validation logs: [audit_and_logging/etl_logging_framework.sql](audit_and_logging/etl_logging_framework.sql#L1)
- Export/file logging utilities: [audit_and_logging/file_output_logging.sql](audit_and_logging/file_output_logging.sql#L1)
- Orchestration wrapper: [main/run_etl_and_checks.sql](main/run_etl_and_checks.sql#L1)

**Guidance / Best Practices**:
- Deploy audit logging before the first production migration; apply audit triggers during DDL deployment windows to avoid gaps.
- Use immutable storage or restricted-role archives for older audit data.
- Keep read-logging selective (log only PHI-sensitive accesses) to balance privacy and volume.
- Regularly exercise audit verification and export processes as part of DR/incident playbooks.

