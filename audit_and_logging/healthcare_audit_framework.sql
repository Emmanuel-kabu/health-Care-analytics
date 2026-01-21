-- ===================================================================
-- HEALTHCARE AUDIT FRAMEWORK - HIPAA COMPLIANT AUDITING SYSTEM
-- ===================================================================
-- Comprehensive audit logging system for healthcare data systems
-- Ensures HIPAA compliance for patient data access and modifications
-- ===================================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ===================================================================
-- AUDIT SCHEMA CREATION
-- ===================================================================
CREATE SCHEMA IF NOT EXISTS healthcare_audit;
SET search_path TO healthcare_audit, public;

-- ===================================================================
-- AUDIT CONFIGURATION TABLES
-- ===================================================================

-- Audit event types for healthcare operations
CREATE TABLE IF NOT EXISTS audit_event_types (
    event_type_id SERIAL PRIMARY KEY,
    event_type_code VARCHAR(50) NOT NULL UNIQUE,
    event_description TEXT NOT NULL,
    severity_level VARCHAR(10) NOT NULL CHECK (severity_level IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    hipaa_category VARCHAR(50),
    retention_days INTEGER NOT NULL DEFAULT 2555, -- 7 years HIPAA requirement
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert standard healthcare audit event types
INSERT INTO audit_event_types (event_type_code, event_description, severity_level, hipaa_category) VALUES
('PATIENT_ACCESS', 'Patient record accessed', 'MEDIUM', 'PHI_ACCESS'),
('PATIENT_CREATE', 'New patient record created', 'HIGH', 'PHI_CREATE'),
('PATIENT_UPDATE', 'Patient record modified', 'HIGH', 'PHI_MODIFY'),
('PATIENT_DELETE', 'Patient record deleted', 'CRITICAL', 'PHI_DELETE'),
('DIAGNOSIS_ACCESS', 'Diagnosis information accessed', 'MEDIUM', 'CLINICAL_ACCESS'),
('DIAGNOSIS_CREATE', 'New diagnosis entered', 'HIGH', 'CLINICAL_CREATE'),
('BILLING_ACCESS', 'Billing information accessed', 'MEDIUM', 'FINANCIAL_ACCESS'),
('BILLING_UPDATE', 'Billing information modified', 'HIGH', 'FINANCIAL_MODIFY'),
('USER_LOGIN', 'User authentication successful', 'LOW', 'SECURITY'),
('USER_LOGOUT', 'User session terminated', 'LOW', 'SECURITY'),
('FAILED_LOGIN', 'Authentication attempt failed', 'MEDIUM', 'SECURITY'),
('PRIVILEGE_ESCALATION', 'User privileges elevated', 'HIGH', 'SECURITY'),
('DATA_EXPORT', 'Healthcare data exported', 'HIGH', 'PHI_EXPORT'),
('SYSTEM_BACKUP', 'System backup performed', 'MEDIUM', 'SYSTEM'),
('ETL_EXECUTION', 'ETL process executed', 'LOW', 'SYSTEM'),
('SCHEMA_CHANGE', 'Database schema modified', 'HIGH', 'SYSTEM')
ON CONFLICT (event_type_code) DO NOTHING;

-- User roles for healthcare access control
CREATE TABLE IF NOT EXISTS user_roles (
    role_id SERIAL PRIMARY KEY,
    role_name VARCHAR(100) NOT NULL UNIQUE,
    role_description TEXT,
    access_level VARCHAR(20) NOT NULL CHECK (access_level IN ('READ', 'WRITE', 'ADMIN', 'SYSTEM')),
    department VARCHAR(100),
    can_access_phi BOOLEAN DEFAULT FALSE,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert standard healthcare roles
INSERT INTO user_roles (role_name, role_description, access_level, department, can_access_phi) VALUES
('PHYSICIAN', 'Licensed physician with full patient access', 'WRITE', 'CLINICAL', TRUE),
('NURSE', 'Registered nurse with patient care access', 'WRITE', 'CLINICAL', TRUE),
('BILLING_CLERK', 'Billing department with financial access', 'WRITE', 'BILLING', TRUE),
('IT_ADMIN', 'System administrator', 'ADMIN', 'IT', TRUE),
('ANALYST', 'Data analyst with aggregated data access', 'READ', 'ANALYTICS', FALSE),
('AUDITOR', 'Compliance auditor with audit trail access', 'READ', 'COMPLIANCE', TRUE),
('ETL_SERVICE', 'Automated ETL service account', 'SYSTEM', 'SYSTEM', TRUE)
ON CONFLICT (role_name) DO NOTHING;

-- ===================================================================
-- MAIN AUDIT LOG TABLE
-- ===================================================================
CREATE TABLE IF NOT EXISTS audit_log (
    audit_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    event_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event_type_code VARCHAR(50) NOT NULL REFERENCES audit_event_types(event_type_code),
    user_id VARCHAR(100),
    user_role VARCHAR(100),
    session_id VARCHAR(200),
    source_ip_address INET,
    database_name VARCHAR(100),
    schema_name VARCHAR(100),
    table_name VARCHAR(100),
    operation_type VARCHAR(20), -- INSERT, UPDATE, DELETE, SELECT
    record_id VARCHAR(100), -- Primary key of affected record
    patient_id INTEGER, -- For PHI access tracking
    affected_columns TEXT[], -- Columns accessed or modified
    old_values JSONB, -- Previous values for UPDATE operations
    new_values JSONB, -- New values for INSERT/UPDATE operations
    query_text TEXT, -- SQL query executed (for DDL/DML audit)
    application_name VARCHAR(200),
    success BOOLEAN NOT NULL DEFAULT TRUE,
    error_message TEXT,
    response_time_ms INTEGER,
    rows_affected INTEGER,
    compliance_notes TEXT,
    audit_metadata JSONB -- Additional audit context
);

-- Create indexes for performance and compliance queries
CREATE INDEX IF NOT EXISTS idx_audit_log_timestamp ON audit_log (event_timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_log_event_type ON audit_log (event_type_code);
CREATE INDEX IF NOT EXISTS idx_audit_log_user ON audit_log (user_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_patient ON audit_log (patient_id) WHERE patient_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_audit_log_table ON audit_log (schema_name, table_name);
CREATE INDEX IF NOT EXISTS idx_audit_log_session ON audit_log (session_id);

-- ===================================================================
-- PHI ACCESS TRACKING TABLE (HIPAA Specific)
-- ===================================================================
CREATE TABLE IF NOT EXISTS phi_access_log (
    phi_access_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    audit_id UUID REFERENCES audit_log(audit_id),
    patient_id INTEGER NOT NULL,
    patient_mrn VARCHAR(50),
    accessed_phi_elements TEXT[], -- What PHI was accessed
    access_purpose VARCHAR(200), -- Purpose of PHI access
    minimum_necessary_justification TEXT,
    authorized_by VARCHAR(100), -- Authorizing provider
    patient_consent_status VARCHAR(50),
    access_duration_seconds INTEGER,
    phi_disclosed_to TEXT[], -- If PHI was shared
    disclosure_purpose VARCHAR(200),
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_phi_access_patient ON phi_access_log (patient_id);
CREATE INDEX IF NOT EXISTS idx_phi_access_timestamp ON phi_access_log (created_timestamp);

-- ===================================================================
-- AUDIT TRIGGERS AND FUNCTIONS
-- ===================================================================

-- Function to capture current session information
CREATE OR REPLACE FUNCTION get_audit_session_info() 
RETURNS JSONB AS $$
DECLARE
    session_info JSONB;
BEGIN
    SELECT jsonb_build_object(
        'session_id', current_setting('application_name', true),
        'user_name', current_user,
        'database_name', current_database(),
        'client_addr', inet_client_addr(),
        'client_port', inet_client_port(),
        'backend_pid', pg_backend_pid(),
        'transaction_timestamp', transaction_timestamp()
    ) INTO session_info;
    
    RETURN session_info;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Function to log audit events
CREATE OR REPLACE FUNCTION log_audit_event(
    p_event_type_code VARCHAR(50),
    p_operation_type VARCHAR(20) DEFAULT NULL,
    p_table_name VARCHAR(100) DEFAULT NULL,
    p_schema_name VARCHAR(100) DEFAULT 'public',
    p_record_id VARCHAR(100) DEFAULT NULL,
    p_patient_id INTEGER DEFAULT NULL,
    p_affected_columns TEXT[] DEFAULT NULL,
    p_old_values JSONB DEFAULT NULL,
    p_new_values JSONB DEFAULT NULL,
    p_query_text TEXT DEFAULT NULL,
    p_success BOOLEAN DEFAULT TRUE,
    p_error_message TEXT DEFAULT NULL,
    p_compliance_notes TEXT DEFAULT NULL
) RETURNS UUID AS $$
DECLARE
    v_audit_id UUID;
    v_session_info JSONB;
BEGIN
    -- Generate audit ID
    v_audit_id := uuid_generate_v4();
    
    -- Get session information
    v_session_info := get_audit_session_info();
    
    -- Insert audit record
    INSERT INTO healthcare_audit.audit_log (
        audit_id,
        event_type_code,
        user_id,
        session_id,
        source_ip_address,
        database_name,
        schema_name,
        table_name,
        operation_type,
        record_id,
        patient_id,
        affected_columns,
        old_values,
        new_values,
        query_text,
        application_name,
        success,
        error_message,
        compliance_notes,
        audit_metadata
    ) VALUES (
        v_audit_id,
        p_event_type_code,
        current_user,
        v_session_info->>'session_id',
        (v_session_info->>'client_addr')::INET,
        current_database(),
        p_schema_name,
        p_table_name,
        p_operation_type,
        p_record_id,
        p_patient_id,
        p_affected_columns,
        p_old_values,
        p_new_values,
        p_query_text,
        current_setting('application_name', true),
        p_success,
        p_error_message,
        p_compliance_notes,
        v_session_info
    );
    
    RETURN v_audit_id;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Function to log PHI access
CREATE OR REPLACE FUNCTION log_phi_access(
    p_audit_id UUID,
    p_patient_id INTEGER,
    p_patient_mrn VARCHAR(50),
    p_accessed_phi_elements TEXT[],
    p_access_purpose VARCHAR(200),
    p_minimum_necessary_justification TEXT DEFAULT NULL,
    p_authorized_by VARCHAR(100) DEFAULT NULL
) RETURNS UUID AS $$
DECLARE
    v_phi_access_id UUID;
BEGIN
    v_phi_access_id := uuid_generate_v4();
    
    INSERT INTO healthcare_audit.phi_access_log (
        phi_access_id,
        audit_id,
        patient_id,
        patient_mrn,
        accessed_phi_elements,
        access_purpose,
        minimum_necessary_justification,
        authorized_by
    ) VALUES (
        v_phi_access_id,
        p_audit_id,
        p_patient_id,
        p_patient_mrn,
        p_accessed_phi_elements,
        p_access_purpose,
        p_minimum_necessary_justification,
        p_authorized_by
    );
    
    RETURN v_phi_access_id;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- ===================================================================
-- AUDIT TRIGGER FUNCTIONS
-- ===================================================================

-- Generic audit trigger function for DML operations
CREATE OR REPLACE FUNCTION audit_dml_trigger() 
RETURNS TRIGGER AS $$
DECLARE
    v_audit_id UUID;
    v_old_values JSONB;
    v_new_values JSONB;
    v_operation VARCHAR(20);
    v_patient_id INTEGER;
    v_affected_columns TEXT[];
BEGIN
    -- Determine operation type
    v_operation := TG_OP;
    
    -- Extract patient_id if available
    IF TG_OP = 'DELETE' THEN
        v_old_values := to_jsonb(OLD);
        IF OLD IS NOT NULL AND OLD::TEXT LIKE '%patient_id%' THEN
            v_patient_id := (OLD::JSONB->>'patient_id')::INTEGER;
        END IF;
    ELSIF TG_OP = 'UPDATE' THEN
        v_old_values := to_jsonb(OLD);
        v_new_values := to_jsonb(NEW);
        IF NEW IS NOT NULL AND NEW::TEXT LIKE '%patient_id%' THEN
            v_patient_id := (NEW::JSONB->>'patient_id')::INTEGER;
        END IF;
        
        -- Identify changed columns
        SELECT ARRAY_AGG(key) 
        INTO v_affected_columns
        FROM jsonb_each(v_old_values) o
        WHERE o.value IS DISTINCT FROM (v_new_values->o.key);
        
    ELSIF TG_OP = 'INSERT' THEN
        v_new_values := to_jsonb(NEW);
        IF NEW IS NOT NULL AND NEW::TEXT LIKE '%patient_id%' THEN
            v_patient_id := (NEW::JSONB->>'patient_id')::INTEGER;
        END IF;
    END IF;
    
    -- Log the audit event
    v_audit_id := log_audit_event(
        p_event_type_code := CASE 
            WHEN TG_TABLE_NAME LIKE '%patient%' THEN 'PATIENT_' || v_operation
            WHEN TG_TABLE_NAME LIKE '%diagnosis%' THEN 'DIAGNOSIS_' || v_operation
            WHEN TG_TABLE_NAME LIKE '%billing%' THEN 'BILLING_' || v_operation
            ELSE 'DATA_' || v_operation
        END,
        p_operation_type := v_operation,
        p_table_name := TG_TABLE_NAME,
        p_schema_name := TG_TABLE_SCHEMA,
        p_record_id := COALESCE(
            (v_new_values->>TG_ARGV[0]), 
            (v_old_values->>TG_ARGV[0])
        ),
        p_patient_id := v_patient_id,
        p_affected_columns := v_affected_columns,
        p_old_values := v_old_values,
        p_new_values := v_new_values
    );
    
    -- Log PHI access if patient data involved
    IF v_patient_id IS NOT NULL THEN
        PERFORM log_phi_access(
            p_audit_id := v_audit_id,
            p_patient_id := v_patient_id,
            p_patient_mrn := COALESCE(
                v_new_values->>'mrn',
                v_old_values->>'mrn'
            ),
            p_accessed_phi_elements := ARRAY[TG_TABLE_NAME],
            p_access_purpose := 'Clinical Care'
        );
    END IF;
    
    -- Return appropriate record
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    ELSE
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- ===================================================================
-- COMPLIANCE REPORTING FUNCTIONS
-- ===================================================================
-- Create audit triggers for OLTP tables (patients, providers, encounters,
-- diagnoses, procedures, billing). These attach the generic `audit_dml_trigger`
-- and pass the primary key column name as TG_ARGV[0].

DO $$
BEGIN
    -- Patients
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'audit_patients_dml'
    ) THEN
        EXECUTE 'CREATE TRIGGER audit_patients_dml
            AFTER INSERT OR UPDATE OR DELETE ON public.patients
            FOR EACH ROW EXECUTE FUNCTION healthcare_audit.audit_dml_trigger(''patient_id'');';
    END IF;

    -- Providers
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'audit_providers_dml'
    ) THEN
        EXECUTE 'CREATE TRIGGER audit_providers_dml
            AFTER INSERT OR UPDATE OR DELETE ON public.providers
            FOR EACH ROW EXECUTE FUNCTION healthcare_audit.audit_dml_trigger(''provider_id'');';
    END IF;

    -- Encounters
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'audit_encounters_dml'
    ) THEN
        EXECUTE 'CREATE TRIGGER audit_encounters_dml
            AFTER INSERT OR UPDATE OR DELETE ON public.encounters
            FOR EACH ROW EXECUTE FUNCTION healthcare_audit.audit_dml_trigger(''encounter_id'');';
    END IF;

    -- Diagnoses
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'audit_diagnoses_dml'
    ) THEN
        EXECUTE 'CREATE TRIGGER audit_diagnoses_dml
            AFTER INSERT OR UPDATE OR DELETE ON public.diagnoses
            FOR EACH ROW EXECUTE FUNCTION healthcare_audit.audit_dml_trigger(''diagnosis_id'');';
    END IF;

    -- Procedures
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'audit_procedures_dml'
    ) THEN
        EXECUTE 'CREATE TRIGGER audit_procedures_dml
            AFTER INSERT OR UPDATE OR DELETE ON public.procedures
            FOR EACH ROW EXECUTE FUNCTION healthcare_audit.audit_dml_trigger(''procedure_id'');';
    END IF;

    -- Billing
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'audit_billing_dml'
    ) THEN
        EXECUTE 'CREATE TRIGGER audit_billing_dml
            AFTER INSERT OR UPDATE OR DELETE ON public.billing
            FOR EACH ROW EXECUTE FUNCTION healthcare_audit.audit_dml_trigger(''billing_id'');';
    END IF;
END $$;


-- HIPAA Access Report
CREATE OR REPLACE FUNCTION generate_hipaa_access_report(
    p_start_date DATE DEFAULT CURRENT_DATE - INTERVAL '30 days',
    p_end_date DATE DEFAULT CURRENT_DATE,
    p_patient_id INTEGER DEFAULT NULL
)
RETURNS TABLE (
    patient_id INTEGER,
    patient_mrn VARCHAR(50),
    access_count BIGINT,
    unique_users BIGINT,
    phi_elements TEXT[],
    first_access TIMESTAMP,
    last_access TIMESTAMP,
    highest_risk_access TEXT
) AS $$
BEGIN
    RETURN QUERY
    WITH phi_summary AS (
        SELECT 
            p.patient_id,
            p.patient_mrn,
            COUNT(*) as access_count,
            COUNT(DISTINCT a.user_id) as unique_users,
            ARRAY_AGG(DISTINCT unnest(p.accessed_phi_elements)) as phi_elements,
            MIN(a.event_timestamp) as first_access,
            MAX(a.event_timestamp) as last_access,
            STRING_AGG(DISTINCT a.event_type_code, ', ') as access_types
        FROM healthcare_audit.phi_access_log p
        JOIN healthcare_audit.audit_log a ON p.audit_id = a.audit_id
        WHERE a.event_timestamp::DATE BETWEEN p_start_date AND p_end_date
        AND (p_patient_id IS NULL OR p.patient_id = p_patient_id)
        GROUP BY p.patient_id, p.patient_mrn
    )
    SELECT 
        ps.patient_id,
        ps.patient_mrn,
        ps.access_count,
        ps.unique_users,
        ps.phi_elements,
        ps.first_access,
        ps.last_access,
        ps.access_types
    FROM phi_summary ps
    ORDER BY ps.access_count DESC;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Audit Trail Integrity Check
CREATE OR REPLACE FUNCTION verify_audit_integrity()
RETURNS TABLE (
    check_name TEXT,
    status TEXT,
    message TEXT,
    record_count BIGINT
) AS $$
BEGIN
    -- Check for audit log gaps
    RETURN QUERY
    SELECT 
        'Audit Log Continuity'::TEXT,
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END::TEXT,
        CASE WHEN COUNT(*) = 0 
             THEN 'No gaps in audit log timestamps'
             ELSE 'Found ' || COUNT(*) || ' potential gaps in audit log'
        END::TEXT,
        COUNT(*)
    FROM (
        SELECT 
            event_timestamp,
            LAG(event_timestamp) OVER (ORDER BY event_timestamp) as prev_timestamp
        FROM healthcare_audit.audit_log
        WHERE event_timestamp >= CURRENT_DATE - INTERVAL '7 days'
    ) gaps
    WHERE event_timestamp - prev_timestamp > INTERVAL '1 hour';
    
    -- Check for orphaned PHI access records
    RETURN QUERY
    SELECT 
        'PHI Access Integrity'::TEXT,
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END::TEXT,
        CASE WHEN COUNT(*) = 0 
             THEN 'All PHI access records linked to audit log'
             ELSE 'Found ' || COUNT(*) || ' orphaned PHI access records'
        END::TEXT,
        COUNT(*)
    FROM healthcare_audit.phi_access_log p
    LEFT JOIN healthcare_audit.audit_log a ON p.audit_id = a.audit_id
    WHERE a.audit_id IS NULL;
    
    -- Check audit retention compliance
    RETURN QUERY
    SELECT 
        'Retention Compliance'::TEXT,
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END::TEXT,
        CASE WHEN COUNT(*) = 0 
             THEN 'No audit records exceed retention period'
             ELSE 'Found ' || COUNT(*) || ' audit records exceeding retention'
        END::TEXT,
        COUNT(*)
    FROM healthcare_audit.audit_log a
    JOIN healthcare_audit.audit_event_types t ON a.event_type_code = t.event_type_code
    WHERE a.event_timestamp < CURRENT_DATE - (t.retention_days || ' days')::INTERVAL;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- ===================================================================
-- AUDIT MAINTENANCE PROCEDURES
-- ===================================================================

-- Procedure to archive old audit records
CREATE OR REPLACE FUNCTION archive_audit_records()
RETURNS INTEGER AS $$
DECLARE
    archived_count INTEGER := 0;
    archive_date DATE;
BEGIN
    -- Archive records older than 7 years (HIPAA requirement)
    archive_date := CURRENT_DATE - INTERVAL '7 years';
    
    -- Create archive table if not exists
    CREATE TABLE IF NOT EXISTS healthcare_audit.audit_log_archive (
        LIKE healthcare_audit.audit_log INCLUDING ALL
    );
    
    -- Move old records to archive
    WITH moved_records AS (
        DELETE FROM healthcare_audit.audit_log
        WHERE event_timestamp < archive_date
        RETURNING *
    )
    INSERT INTO healthcare_audit.audit_log_archive
    SELECT * FROM moved_records;
    
    GET DIAGNOSTICS archived_count = ROW_COUNT;
    
    -- Log the archival process
    PERFORM log_audit_event(
        p_event_type_code := 'SYSTEM_ARCHIVE',
        p_operation_type := 'ARCHIVE',
        p_table_name := 'audit_log',
        p_schema_name := 'healthcare_audit',
        p_compliance_notes := 'Archived ' || archived_count || ' audit records older than ' || archive_date
    );
    
    RETURN archived_count;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Reset search path
SET search_path TO public;

-- ===================================================================
-- AUDIT FRAMEWORK SETUP VERIFICATION
-- ===================================================================
DO $$
BEGIN
    RAISE NOTICE 'Healthcare Audit Framework installed successfully';
    RAISE NOTICE 'Schemas created: healthcare_audit';
    RAISE NOTICE 'HIPAA compliance features enabled';
    RAISE NOTICE 'Audit retention set to 7 years (HIPAA requirement)';
    RAISE NOTICE 'Use log_audit_event() function for manual audit logging';
    RAISE NOTICE 'Use generate_hipaa_access_report() for compliance reports';
END $$;