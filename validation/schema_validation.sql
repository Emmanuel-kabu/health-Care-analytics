-- ===================================================================
-- COMPREHENSIVE SCHEMA VALIDATION AND MISMATCH DETECTION
-- ===================================================================
-- This script performs extensive schema validation between OLTP source
-- and Star Schema target, detecting mismatches and validation issues
-- ===================================================================

-- Load the logging framework first
\i ../audit_and_logging/etl_logging_framework.sql

-- ===================================================================
-- SCHEMA VALIDATION STORED PROCEDURE
-- ===================================================================

CREATE OR REPLACE PROCEDURE validate_healthcare_schemas(p_execution_id UUID DEFAULT NULL)
LANGUAGE plpgsql
AS $$
DECLARE
    v_execution_id UUID;
    v_log_id INTEGER;
    v_validation_errors INTEGER := 0;
    v_validation_warnings INTEGER := 0;
    v_critical_issues INTEGER := 0;
    v_start_time TIMESTAMP := CURRENT_TIMESTAMP;
    
    -- Variables for validation checks
    v_table_exists BOOLEAN;
    v_column_valid BOOLEAN;
    v_null_violations BIGINT;
    v_duplicate_count BIGINT;
    v_row_count BIGINT;
    
    -- Cursor for iterating through expected schema mappings
    validation_cursor CURSOR FOR
        SELECT 
            source_schema,
            source_table, 
            source_column,
            target_schema,
            target_table,
            target_column,
            expected_data_type,
            is_nullable,
            is_primary_key,
            validation_rule
        FROM (VALUES
            -- Patients table validation
            ('public', 'patients', 'patient_id', 'public', 'dim_patient', 'patient_id', 'integer', false, true, 'UNIQUE_NOT_NULL'),
            ('public', 'patients', 'first_name', 'public', 'dim_patient', 'first_name', 'varchar(100)', true, false, 'LENGTH_CHECK'),
            ('public', 'patients', 'last_name', 'public', 'dim_patient', 'last_name', 'varchar(100)', true, false, 'LENGTH_CHECK'),
            ('public', 'patients', 'date_of_birth', 'public', 'dim_patient', 'date_of_birth', 'date', true, false, 'DATE_RANGE'),
            ('public', 'patients', 'gender', 'public', 'dim_patient', 'gender', 'char(1)', true, false, 'GENDER_VALUES'),
            ('public', 'patients', 'mrn', 'public', 'dim_patient', 'mrn', 'varchar(20)', true, false, 'UNIQUE_FORMAT'),
            
            -- Providers table validation
            ('public', 'providers', 'provider_id', 'public', 'dim_provider', 'provider_id', 'integer', false, true, 'UNIQUE_NOT_NULL'),
            ('public', 'providers', 'first_name', 'public', 'dim_provider', 'first_name', 'varchar(100)', true, false, 'LENGTH_CHECK'),
            ('public', 'providers', 'last_name', 'public', 'dim_provider', 'last_name', 'varchar(100)', true, false, 'LENGTH_CHECK'),
            ('public', 'providers', 'credential', 'public', 'dim_provider', 'credential', 'varchar(20)', true, false, 'CREDENTIAL_FORMAT'),
            
            -- Specialties table validation
            ('public', 'specialties', 'specialty_id', 'public', 'dim_specialty', 'specialty_id', 'integer', false, true, 'UNIQUE_NOT_NULL'),
            ('public', 'specialties', 'specialty_name', 'public', 'dim_specialty', 'specialty_name', 'varchar(100)', false, false, 'NOT_NULL'),
            ('public', 'specialties', 'specialty_code', 'public', 'dim_specialty', 'specialty_code', 'varchar(10)', true, false, 'CODE_FORMAT'),
            
            -- Departments table validation
            ('public', 'departments', 'department_id', 'public', 'dim_department', 'department_id', 'integer', false, true, 'UNIQUE_NOT_NULL'),
            ('public', 'departments', 'department_name', 'public', 'dim_department', 'department_name', 'varchar(100)', false, false, 'NOT_NULL'),
            ('public', 'departments', 'floor', 'public', 'dim_department', 'floor', 'integer', true, false, 'FLOOR_RANGE'),
            ('public', 'departments', 'capacity', 'public', 'dim_department', 'capacity', 'integer', true, false, 'CAPACITY_RANGE'),
            
            -- Encounters table validation
            ('public', 'encounters', 'encounter_id', 'public', 'fact_encounters', 'encounter_id', 'integer', false, true, 'UNIQUE_NOT_NULL'),
            ('public', 'encounters', 'patient_id', 'public', 'fact_encounters', 'patient_key', 'integer', false, false, 'FK_PATIENT'),
            ('public', 'encounters', 'provider_id', 'public', 'fact_encounters', 'provider_key', 'integer', false, false, 'FK_PROVIDER'),
            ('public', 'encounters', 'encounter_type', 'public', 'fact_encounters', 'encounter_type_key', 'varchar(50)', false, false, 'ENCOUNTER_TYPE_VALUES'),
            ('public', 'encounters', 'encounter_date', 'public', 'fact_encounters', 'encounter_datetime', 'timestamp', false, false, 'DATE_RANGE'),
            ('public', 'encounters', 'discharge_date', 'public', 'fact_encounters', 'discharge_datetime', 'timestamp', true, false, 'DISCHARGE_LOGIC'),
            
            -- Diagnoses table validation
            ('public', 'diagnoses', 'diagnosis_id', 'public', 'dim_diagnosis', 'diagnosis_id', 'integer', false, true, 'UNIQUE_NOT_NULL'),
            ('public', 'diagnoses', 'icd10_code', 'public', 'dim_diagnosis', 'icd10_code', 'varchar(10)', false, false, 'ICD10_FORMAT'),
            ('public', 'diagnoses', 'icd10_description', 'public', 'dim_diagnosis', 'icd10_description', 'varchar(200)', true, false, 'DESCRIPTION_LENGTH'),
            
            -- Procedures table validation
            ('public', 'procedures', 'procedure_id', 'public', 'dim_procedure', 'procedure_id', 'integer', false, true, 'UNIQUE_NOT_NULL'),
            ('public', 'procedures', 'cpt_code', 'public', 'dim_procedure', 'cpt_code', 'varchar(10)', false, false, 'CPT_FORMAT'),
            ('public', 'procedures', 'cpt_description', 'public', 'dim_procedure', 'cpt_description', 'varchar(200)', true, false, 'DESCRIPTION_LENGTH')
        ) AS schema_mapping (source_schema, source_table, source_column, target_schema, target_table, target_column, expected_data_type, is_nullable, is_primary_key, validation_rule);
        
    rec RECORD;
BEGIN
    v_execution_id := COALESCE(p_execution_id, gen_random_uuid());
    -- ===============================
    -- 1. INITIALIZE VALIDATION SESSION
    -- ===============================
    v_log_id := etl_logs.log_etl_step_start(
        v_execution_id, 
        'validate_healthcare_schemas', 
        'Initialize Schema Validation', 
        1,
        jsonb_build_object(
            'validation_type', 'comprehensive_schema_validation',
            'source_database', 'hospital_db',
            'target_database', 'current_db'
        )
    );
    
    RAISE NOTICE '================================================================';
    RAISE NOTICE 'STARTING COMPREHENSIVE HEALTHCARE SCHEMA VALIDATION';
    RAISE NOTICE 'Execution ID: %', v_execution_id;
    RAISE NOTICE '================================================================';
    
    PERFORM etl_logs.log_etl_step_complete(v_log_id, 'COMPLETED', 1, 1, 0, 0);
    
    -- ===============================
    -- 2. VALIDATE TABLE EXISTENCE
    -- ===============================
    v_log_id := etl_logs.log_etl_step_start(
        v_execution_id, 
        'validate_healthcare_schemas', 
        'Validate Table Existence', 
        2
    );
    
    -- Check all required star schema tables exist
    DECLARE
        required_tables TEXT[] := ARRAY[
            'dim_date', 'dim_patient', 'dim_specialty', 'dim_department', 
            'dim_provider', 'dim_encounter_type', 'dim_diagnosis', 
            'dim_procedure', 'fact_encounters', 'bridge_encounter_diagnoses', 
            'bridge_encounter_procedures'
        ];
        table_name TEXT;
    BEGIN
        FOREACH table_name IN ARRAY required_tables
        LOOP
            v_table_exists := etl_logs.validate_table_exists(v_execution_id, 'public', table_name);
            IF NOT v_table_exists THEN
                v_critical_issues := v_critical_issues + 1;
                RAISE WARNING 'CRITICAL: Required table % does not exist!', table_name;
            END IF;
        END LOOP;
    END;
    
    PERFORM etl_logs.log_etl_step_complete(v_log_id, 'COMPLETED', 11, 0, 0, 0);
    
    -- ===============================
    -- 3. DETAILED COLUMN VALIDATION
    -- ===============================
    v_log_id := etl_logs.log_etl_step_start(
        v_execution_id, 
        'validate_healthcare_schemas', 
        'Validate Column Schemas', 
        3
    );
    
    FOR rec IN validation_cursor
    LOOP
        -- Validate source table exists
        v_table_exists := etl_logs.validate_table_exists(v_execution_id, rec.source_schema, rec.source_table);
        
        IF v_table_exists THEN
            -- Validate source column exists and has correct type
            v_column_valid := etl_logs.validate_column_schema(
                v_execution_id,
                rec.source_schema,
                rec.source_table, 
                rec.source_column,
                rec.expected_data_type
            );
            
            IF NOT v_column_valid THEN
                v_validation_errors := v_validation_errors + 1;
            END IF;
        END IF;
        
        -- Validate target table exists  
        v_table_exists := etl_logs.validate_table_exists(v_execution_id, rec.target_schema, rec.target_table);
        
        IF v_table_exists THEN
            -- Validate target column exists
            v_column_valid := etl_logs.validate_column_schema(
                v_execution_id,
                rec.target_schema,
                rec.target_table,
                rec.target_column,
                rec.expected_data_type
            );
            
            IF NOT v_column_valid THEN
                v_validation_errors := v_validation_errors + 1;
            END IF;
        END IF;
    END LOOP;
    
    PERFORM etl_logs.log_etl_step_complete(v_log_id, 'COMPLETED', 0, 0, 0, 0);
    
    -- ===============================
    -- 4. DATA INTEGRITY VALIDATION
    -- ===============================
    v_log_id := etl_logs.log_etl_step_start(
        v_execution_id, 
        'validate_healthcare_schemas', 
        'Validate Data Integrity', 
        4
    );
    
    -- Check for NULL values in NOT NULL columns (star schema)
    BEGIN
        -- Patient dimension critical fields
        v_null_violations := etl_logs.validate_null_constraints(v_execution_id, 'public', 'dim_patient', 'patient_id');
        v_critical_issues := v_critical_issues + CASE WHEN v_null_violations > 0 THEN 1 ELSE 0 END;
        
        -- Provider dimension critical fields
        v_null_violations := etl_logs.validate_null_constraints(v_execution_id, 'public', 'dim_provider', 'provider_id');
        v_critical_issues := v_critical_issues + CASE WHEN v_null_violations > 0 THEN 1 ELSE 0 END;
        
        -- Specialty dimension critical fields
        v_null_violations := etl_logs.validate_null_constraints(v_execution_id, 'public', 'dim_specialty', 'specialty_id');
        v_critical_issues := v_critical_issues + CASE WHEN v_null_violations > 0 THEN 1 ELSE 0 END;
        
        -- Department dimension critical fields  
        v_null_violations := etl_logs.validate_null_constraints(v_execution_id, 'public', 'dim_department', 'department_id');
        v_critical_issues := v_critical_issues + CASE WHEN v_null_violations > 0 THEN 1 ELSE 0 END;
        
        -- Fact table critical fields
        v_null_violations := etl_logs.validate_null_constraints(v_execution_id, 'public', 'fact_encounters', 'encounter_id');
        v_critical_issues := v_critical_issues + CASE WHEN v_null_violations > 0 THEN 1 ELSE 0 END;
        
        v_null_violations := etl_logs.validate_null_constraints(v_execution_id, 'public', 'fact_encounters', 'patient_key');
        v_critical_issues := v_critical_issues + CASE WHEN v_null_violations > 0 THEN 1 ELSE 0 END;
        
        v_null_violations := etl_logs.validate_null_constraints(v_execution_id, 'public', 'fact_encounters', 'provider_key');
        v_critical_issues := v_critical_issues + CASE WHEN v_null_violations > 0 THEN 1 ELSE 0 END;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error during NULL constraint validation: %', SQLERRM;
            v_validation_errors := v_validation_errors + 1;
    END;
    
    -- Check for duplicate primary keys
    BEGIN
        v_duplicate_count := etl_logs.validate_duplicates(v_execution_id, 'public', 'dim_patient', 'patient_id');
        v_critical_issues := v_critical_issues + CASE WHEN v_duplicate_count > 0 THEN 1 ELSE 0 END;
        
        v_duplicate_count := etl_logs.validate_duplicates(v_execution_id, 'public', 'dim_provider', 'provider_id');
        v_critical_issues := v_critical_issues + CASE WHEN v_duplicate_count > 0 THEN 1 ELSE 0 END;
        
        v_duplicate_count := etl_logs.validate_duplicates(v_execution_id, 'public', 'fact_encounters', 'encounter_id');
        v_critical_issues := v_critical_issues + CASE WHEN v_duplicate_count > 0 THEN 1 ELSE 0 END;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error during duplicate validation: %', SQLERRM;
            v_validation_errors := v_validation_errors + 1;
    END;
    
    PERFORM etl_logs.log_etl_step_complete(v_log_id, 'COMPLETED', 0, 0, 0, 0);
    
    -- ===============================
    -- 5. BUSINESS RULE VALIDATION
    -- ===============================
    v_log_id := etl_logs.log_etl_step_start(
        v_execution_id, 
        'validate_healthcare_schemas', 
        'Validate Business Rules', 
        5
    );
    
    -- Validate date ranges
    BEGIN
        -- Check for future birth dates
        SELECT COUNT(*) INTO v_row_count
        FROM dim_patient 
        WHERE date_of_birth > CURRENT_DATE;
        
        IF v_row_count > 0 THEN
            PERFORM etl_logs.log_data_quality_issue(
                v_execution_id,
                'dim_patient',
                'date_of_birth',
                'INVALID_DATE_RANGE',
                'Found patients with future birth dates',
                v_row_count,
                NULL,
                'HIGH'
            );
            v_validation_errors := v_validation_errors + 1;
        END IF;
        
        -- Check for encounter dates before birth dates
        SELECT COUNT(*) INTO v_row_count
        FROM fact_encounters fe
        JOIN dim_patient dp ON fe.patient_key = dp.patient_key
        WHERE fe.encounter_datetime::DATE < dp.date_of_birth;
        
        IF v_row_count > 0 THEN
            PERFORM etl_logs.log_data_quality_issue(
                v_execution_id,
                'fact_encounters',
                'encounter_datetime',
                'INVALID_DATE_LOGIC',
                'Found encounters before patient birth date',
                v_row_count,
                NULL,
                'CRITICAL'
            );
            v_critical_issues := v_critical_issues + 1;
        END IF;
        
        -- Check for discharge dates before encounter dates
        SELECT COUNT(*) INTO v_row_count
        FROM fact_encounters 
        WHERE discharge_datetime < encounter_datetime;
        
        IF v_row_count > 0 THEN
            PERFORM etl_logs.log_data_quality_issue(
                v_execution_id,
                'fact_encounters',
                'discharge_datetime',
                'INVALID_DATE_LOGIC',
                'Found discharge dates before encounter dates',
                v_row_count,
                NULL,
                'HIGH'
            );
            v_validation_errors := v_validation_errors + 1;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error during business rule validation: %', SQLERRM;
            v_validation_errors := v_validation_errors + 1;
    END;
    
    -- Validate gender values
    BEGIN
        SELECT COUNT(*) INTO v_row_count
        FROM dim_patient 
        WHERE gender NOT IN ('M', 'F', 'O', 'U') 
        AND gender IS NOT NULL;
        
        IF v_row_count > 0 THEN
            PERFORM etl_logs.log_data_quality_issue(
                v_execution_id,
                'dim_patient',
                'gender',
                'INVALID_VALUES',
                'Found invalid gender values (not M/F/O/U)',
                v_row_count,
                NULL,
                'MEDIUM'
            );
            v_validation_warnings := v_validation_warnings + 1;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error during gender validation: %', SQLERRM;
    END;
    
    -- Validate ICD-10 code format
    BEGIN
        SELECT COUNT(*) INTO v_row_count
        FROM dim_diagnosis 
        WHERE icd10_code !~ '^[A-Z][0-9]{2}(\.[0-9A-Z]*)?$' 
        AND icd10_code IS NOT NULL;
        
        IF v_row_count > 0 THEN
            PERFORM etl_logs.log_data_quality_issue(
                v_execution_id,
                'dim_diagnosis',
                'icd10_code',
                'INVALID_FORMAT',
                'Found invalid ICD-10 code format',
                v_row_count,
                NULL,
                'MEDIUM'
            );
            v_validation_warnings := v_validation_warnings + 1;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error during ICD-10 validation: %', SQLERRM;
    END;
    
    -- Validate CPT code format
    BEGIN
        SELECT COUNT(*) INTO v_row_count
        FROM dim_procedure 
        WHERE cpt_code !~ '^[0-9]{5}$' 
        AND cpt_code IS NOT NULL;
        
        IF v_row_count > 0 THEN
            PERFORM etl_logs.log_data_quality_issue(
                v_execution_id,
                'dim_procedure',
                'cpt_code',
                'INVALID_FORMAT',
                'Found invalid CPT code format (should be 5 digits)',
                v_row_count,
                NULL,
                'MEDIUM'
            );
            v_validation_warnings := v_validation_warnings + 1;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error during CPT validation: %', SQLERRM;
    END;
    
    PERFORM etl_logs.log_etl_step_complete(v_log_id, 'COMPLETED', 0, 0, 0, 0);
    
    -- ===============================
    -- 6. REFERENTIAL INTEGRITY VALIDATION
    -- ===============================
    v_log_id := etl_logs.log_etl_step_start(
        v_execution_id, 
        'validate_healthcare_schemas', 
        'Validate Referential Integrity', 
        6
    );
    
    -- Check for orphaned fact records
    BEGIN
        -- Orphaned patient references
        SELECT COUNT(*) INTO v_row_count
        FROM fact_encounters fe
        LEFT JOIN dim_patient dp ON fe.patient_key = dp.patient_key
        WHERE dp.patient_key IS NULL;
        
        IF v_row_count > 0 THEN
            PERFORM etl_logs.log_data_quality_issue(
                v_execution_id,
                'fact_encounters',
                'patient_key',
                'ORPHANED_REFERENCE',
                'Found encounters with invalid patient references',
                v_row_count,
                NULL,
                'CRITICAL'
            );
            v_critical_issues := v_critical_issues + 1;
        END IF;
        
        -- Orphaned provider references
        SELECT COUNT(*) INTO v_row_count
        FROM fact_encounters fe
        LEFT JOIN dim_provider dp ON fe.provider_key = dp.provider_key
        WHERE dp.provider_key IS NULL;
        
        IF v_row_count > 0 THEN
            PERFORM etl_logs.log_data_quality_issue(
                v_execution_id,
                'fact_encounters',
                'provider_key',
                'ORPHANED_REFERENCE',
                'Found encounters with invalid provider references',
                v_row_count,
                NULL,
                'CRITICAL'
            );
            v_critical_issues := v_critical_issues + 1;
        END IF;
        
        -- Orphaned specialty references
        SELECT COUNT(*) INTO v_row_count
        FROM dim_provider dp
        LEFT JOIN dim_specialty ds ON dp.specialty_key = ds.specialty_key
        WHERE ds.specialty_key IS NULL;
        
        IF v_row_count > 0 THEN
            PERFORM etl_logs.log_data_quality_issue(
                v_execution_id,
                'dim_provider',
                'specialty_key',
                'ORPHANED_REFERENCE',
                'Found providers with invalid specialty references',
                v_row_count,
                NULL,
                'HIGH'
            );
            v_validation_errors := v_validation_errors + 1;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error during referential integrity validation: %', SQLERRM;
            v_validation_errors := v_validation_errors + 1;
    END;
    
    PERFORM etl_logs.log_etl_step_complete(v_log_id, 'COMPLETED', 0, 0, 0, 0);
    
    -- ===============================
    -- 7. GENERATE VALIDATION SUMMARY
    -- ===============================
    v_log_id := etl_logs.log_etl_step_start(
        v_execution_id, 
        'validate_healthcare_schemas', 
        'Generate Validation Summary', 
        7
    );
    
    RAISE NOTICE '================================================================';
    RAISE NOTICE 'SCHEMA VALIDATION SUMMARY';
    RAISE NOTICE '================================================================';
    RAISE NOTICE 'Execution ID: %', v_execution_id;
    RAISE NOTICE 'Total Validation Duration: % seconds', 
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time));
    RAISE NOTICE 'Critical Issues: %', v_critical_issues;
    RAISE NOTICE 'Validation Errors: %', v_validation_errors;
    RAISE NOTICE 'Validation Warnings: %', v_validation_warnings;
    RAISE NOTICE '';
    
    IF v_critical_issues > 0 THEN
        RAISE WARNING 'VALIDATION FAILED: % critical issues found!', v_critical_issues;
    ELSIF v_validation_errors > 0 THEN
        RAISE WARNING 'VALIDATION COMPLETED WITH ERRORS: % errors found', v_validation_errors;
    ELSE
        RAISE NOTICE 'VALIDATION PASSED: Schema validation successful';
    END IF;
    
    RAISE NOTICE '================================================================';
    
    -- Log detailed summary in logs table
    PERFORM etl_logs.log_data_quality_issue(
        v_execution_id,
        'VALIDATION_SUMMARY',
        'ALL_TABLES',
        'SUMMARY_REPORT',
        format('Schema validation completed - Critical: %s, Errors: %s, Warnings: %s', 
               v_critical_issues, v_validation_errors, v_validation_warnings),
        v_critical_issues + v_validation_errors + v_validation_warnings,
        format('Duration: %s seconds', EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time))),
        CASE 
            WHEN v_critical_issues > 0 THEN 'CRITICAL'
            WHEN v_validation_errors > 0 THEN 'HIGH'
            WHEN v_validation_warnings > 0 THEN 'MEDIUM'
            ELSE 'LOW'
        END
    );
    
    PERFORM etl_logs.log_etl_step_complete(
        v_log_id, 
        CASE 
            WHEN v_critical_issues > 0 THEN 'FAILED'
            WHEN v_validation_errors > 0 THEN 'WARNING'
            ELSE 'COMPLETED'
        END, 
        1, 1, 0, 0
    );
    
    RAISE NOTICE 'Schema validation logs saved with execution ID: %', v_execution_id;
    RAISE NOTICE 'Query logs with: SELECT * FROM etl_logs.data_quality_log WHERE execution_id = ''%'';', v_execution_id;
    
END;
$$;

-- ===================================================================
-- HELPER FUNCTION TO RUN VALIDATION AND EXPORT RESULTS
-- ===================================================================

CREATE OR REPLACE FUNCTION export_validation_results(p_execution_id UUID DEFAULT NULL)
RETURNS TABLE (
    validation_report JSONB
) AS $$
DECLARE
    v_execution_id UUID;
BEGIN
    -- Use provided execution_id or get the most recent one
    IF p_execution_id IS NULL THEN
        SELECT execution_id INTO v_execution_id
        FROM etl_logs.etl_execution_log 
        WHERE procedure_name = 'validate_healthcare_schemas'
        ORDER BY start_time DESC 
        LIMIT 1;
    ELSE
        v_execution_id := p_execution_id;
    END IF;
    
    RETURN QUERY
    SELECT jsonb_build_object(
        'validation_summary', etl_logs.generate_etl_summary(v_execution_id),
        'data_quality_issues', (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'table_name', table_name,
                    'column_name', column_name,
                    'issue_type', issue_type,
                    'severity', severity_level,
                    'affected_rows', affected_rows,
                    'description', issue_description,
                    'timestamp', created_at
                )
            )
            FROM etl_logs.data_quality_log 
            WHERE execution_id = v_execution_id
        ),
        'schema_validation_results', (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'source_table', source_table,
                    'target_table', target_table,
                    'validation_type', validation_type,
                    'status', validation_status,
                    'expected', expected_value,
                    'actual', actual_value,
                    'issue', issue_description
                )
            )
            FROM etl_logs.schema_validation_log 
            WHERE execution_id = v_execution_id
        ),
        'performance_metrics', (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'table_name', table_name,
                    'operation', operation_type,
                    'duration_ms', duration_ms,
                    'rows_affected', rows_affected
                )
            )
            FROM etl_logs.performance_metrics 
            WHERE execution_id = v_execution_id
        )
    ) as validation_report;
END;
$$ LANGUAGE plpgsql;

-- ===================================================================
-- QUICK VALIDATION COMMANDS
-- ===================================================================

-- Run comprehensive schema validation
-- CALL validate_healthcare_schemas();

-- Export validation results to JSON
-- SELECT validation_report FROM export_validation_results();

-- View latest data quality issues
-- SELECT * FROM etl_logs.data_quality_log ORDER BY created_at DESC LIMIT 50;

-- View schema validation results
-- SELECT * FROM etl_logs.schema_validation_log ORDER BY created_at DESC;

RAISE NOTICE 'Schema Validation Framework installed successfully!';
RAISE NOTICE 'Usage: CALL validate_healthcare_schemas();';