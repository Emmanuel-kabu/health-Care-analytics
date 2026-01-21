**Schema Validation Summary**

This document lists the schemas/tables/columns validated by `validation/schema_validation.sql` and explains why each validation is necessary.

How to run the validator
- Load and run: `psql -d <db> -f "validation/schema_validation.sql"` then `psql -d <db> -c "CALL validate_healthcare_schemas();"`
- Results are written to `etl_logs.schema_validation_log` and `etl_logs.data_quality_log`.

Validated mappings (source → target) and rationale

- `public.patients` → `public.dim_patient`
  - Columns validated: `patient_id` (PK, NOT NULL), `first_name`, `last_name`, `date_of_birth`, `gender`, `mrn`.
  - Why: ensure unique patient identifiers, correct data types and no critical NULLs for demographics; date and gender validations prevent downstream analytics errors and reporting miscounts.

- `public.providers` → `public.dim_provider`
  - Columns validated: `provider_id` (PK, NOT NULL), `first_name`, `last_name`, `credential`.
  - Why: provider dimension integrity is required for attribution of encounters, joins, and provider-level metrics; credential format checks preserve analytics consistency.

- `public.specialties` → `public.dim_specialty`
  - Columns validated: `specialty_id` (PK), `specialty_name`, `specialty_code`.
  - Why: specialty mapping drives segmentation and rollups; missing or malformed specialty keys cause incorrect grouping and financial reporting.

- `public.departments` → `public.dim_department`
  - Columns validated: `department_id` (PK), `department_name`, `floor`, `capacity`.
  - Why: department attributes are used in operational dashboards and cost-center mapping; capacity/floor range checks catch bad source values.

- `public.encounters` → `public.fact_encounters`
  - Columns validated: `encounter_id` (PK), `patient_id`→`patient_key`, `provider_id`→`provider_key`, `encounter_type`, `encounter_date`, `discharge_date`.
  - Why: fact grain and referential integrity are critical—invalid or orphaned fact rows break analytics, readmission calculations, and clinical metrics.

- `public.diagnoses` → `public.dim_diagnosis`
  - Columns validated: `diagnosis_id` (PK), `icd10_code`, `icd10_description`.
  - Why: ICD-10 format validation enforces clinical coding correctness and supports grouping by diagnosis categories.

- `public.procedures` → `public.dim_procedure`
  - Columns validated: `procedure_id` (PK), `cpt_code`, `cpt_description`.
  - Why: CPT format validation ensures procedure-level measures and cost buckets are accurate.

Additional validations performed and their purpose

- Table existence checks
  - Ensures expected star and bridge tables exist before running downstream queries; prevents silent failures when a deployment step was missed.

- Column schema checks
  - Verifies column presence and (basic) data type compatibility to catch migration drift or unexpected source changes.

- NULL constraint checks
  - Counts NULLs in critical columns (PKs and FK keys) and logs violations to `etl_logs.data_quality_log`—essential for determining whether to fail loads.

- Duplicate checks
  - Detects duplicate primary keys in dimensions and fact tables, preventing aggregation errors and inflated metrics.

- Business-rule checks
  - Examples: future birth dates, encounters before birth, discharge before encounter—these detect logical data problems that indicate ETL or source issues.

- Referential integrity checks
  - Finds orphaned fact rows (e.g., fact_encounters.patient_key without a matching dim_patient) which indicate mapping errors or missed loads.

Where to find results
- `etl_logs.data_quality_log` — row-level issues, severity, sample counts
- `etl_logs.schema_validation_log` — structural mismatches and schema-level findings
- Use `export_validation_results()` in `validation/schema_validation.sql` to get a JSON summary

Recommendations
- Run validation after each ETL run and fail the pipeline for CRITICAL issues.
- Consider running a lightweight quick-check before full validation to reduce runtime in CI.
- If you want a single `execution_id` to link ETL + validation, I can add an optional `execution_id` parameter to `validate_healthcare_schemas()` or update the orchestrator to capture and pass IDs.

Files consulted
- `validation/schema_validation.sql` ([file](validation/schema_validation.sql#L1))

---
Generated on 2026-01-21.
