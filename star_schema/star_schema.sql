
-- ============================================================================
-- HEALTHCARE ANALYTICS STAR SCHEMA DDL
-- ============================================================================
-- Fact table grain: One row per encounter
-- Optimized for 4 business questions with pre-aggregated metrics
-- Designed for fast analytical queries with minimal joins
-- ============================================================================

-- ==========================
-- DIMENSION TABLES
-- ==========================

-- Date Dimension - Pre-computed date attributes eliminate DATE_FORMAT() functions
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,             -- surrogate key YYYYMMDD format
    calendar_date DATE NOT NULL UNIQUE,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    day_of_month INT NOT NULL,
    week_of_year INT NOT NULL,
    day_of_week VARCHAR(10) NOT NULL,     -- 'Monday', 'Tuesday', etc.
    is_weekend BOOLEAN NOT NULL DEFAULT FALSE,
    fiscal_year INT,
    fiscal_quarter INT,
    holiday_flag BOOLEAN DEFAULT FALSE,
    INDEX idx_date_year_month (year, month),
    INDEX idx_date_quarter (year, quarter)
);

-- Patient Dimension - Age grouping enables demographic analysis
CREATE TABLE dim_patient (
    patient_key INT PRIMARY KEY AUTO_INCREMENT,
    patient_id INT UNIQUE NOT NULL,       -- original OLTP ID
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(201),               -- concatenated name for reporting
    gender CHAR(1),
    date_of_birth DATE,
    age_at_first_encounter INT,
    current_age INT,
    age_group VARCHAR(20),                -- '0-18', '19-35', '36-60', '60+'
    mrn VARCHAR(20),                      -- medical record number
    INDEX idx_patient_age_group (age_group),
    INDEX idx_patient_gender (gender)
);

-- Specialty Dimension - Separate dimension for specialty-focused analytics
CREATE TABLE dim_specialty (
    specialty_key INT PRIMARY KEY AUTO_INCREMENT,
    specialty_id INT UNIQUE NOT NULL,
    specialty_name VARCHAR(100) NOT NULL,
    specialty_code VARCHAR(10),
    specialty_category VARCHAR(50),       -- 'Medical', 'Surgical', 'Diagnostic'
    INDEX idx_specialty_name (specialty_name),
    INDEX idx_specialty_category (specialty_category)
);

-- Department Dimension - Operational analytics require department-level metrics  
CREATE TABLE dim_department (
    department_key INT PRIMARY KEY AUTO_INCREMENT,
    department_id INT UNIQUE NOT NULL,
    department_name VARCHAR(100) NOT NULL,
    floor INT,
    capacity INT,
    department_type VARCHAR(20),          -- 'Inpatient', 'Outpatient', 'Emergency'
    cost_center_code VARCHAR(20),
    INDEX idx_department_type (department_type),
    INDEX idx_department_name (department_name)
);

-- Provider Dimension - Denormalized specialty/department names eliminate joins
CREATE TABLE dim_provider (
    provider_key INT PRIMARY KEY AUTO_INCREMENT,
    provider_id INT UNIQUE NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(201),               -- concatenated name
    credential VARCHAR(20),
    provider_type VARCHAR(50),
    specialty_key INT NOT NULL,
    department_key INT NOT NULL,
    specialty_name VARCHAR(100),          -- denormalized for performance
    department_name VARCHAR(100),         -- denormalized for performance
    INDEX idx_provider_specialty (specialty_key),
    INDEX idx_provider_department (department_key),
    INDEX idx_provider_name (last_name, first_name),
    FOREIGN KEY (specialty_key) REFERENCES dim_specialty(specialty_key),
    FOREIGN KEY (department_key) REFERENCES dim_department(department_key)
);

-- Encounter Type Dimension - Small lookup dimension with business rules
CREATE TABLE dim_encounter_type (
    encounter_type_key INT PRIMARY KEY AUTO_INCREMENT,
    encounter_type VARCHAR(50) UNIQUE NOT NULL,
    type_description VARCHAR(200),
    typical_duration_hours INT,
    requires_admission BOOLEAN DEFAULT FALSE,
    INDEX idx_encounter_type (encounter_type)
);

-- Diagnosis Dimension - ICD-10 codes for clinical and billing analysis
CREATE TABLE dim_diagnosis (
    diagnosis_key INT PRIMARY KEY AUTO_INCREMENT,
    diagnosis_id INT UNIQUE NOT NULL,
    icd10_code VARCHAR(10) NOT NULL,
    icd10_description VARCHAR(200),
    diagnosis_category VARCHAR(100),       -- 'Cardiovascular', 'Endocrine', etc.
    body_system VARCHAR(100),
    severity_level VARCHAR(20),            -- 'Low', 'Medium', 'High'
    chronic_flag BOOLEAN DEFAULT FALSE,
    INDEX idx_diagnosis_icd10 (icd10_code),
    INDEX idx_diagnosis_category (diagnosis_category)
);

-- Procedure Dimension - CPT codes for revenue analysis and operational planning
CREATE TABLE dim_procedure (
    procedure_key INT PRIMARY KEY AUTO_INCREMENT,
    procedure_id INT UNIQUE NOT NULL,
    cpt_code VARCHAR(10) NOT NULL,
    cpt_description VARCHAR(200),
    procedure_category VARCHAR(100),       -- 'Diagnostic', 'Therapeutic', etc.
    procedure_type VARCHAR(100),
    typical_cost_range VARCHAR(50),        -- '$100-500', '$500-2000', etc.
    duration_minutes INT,
    INDEX idx_procedure_cpt (cpt_code),
    INDEX idx_procedure_category (procedure_category)
);

-- ============================================================================
-- FACT TABLE - One row per encounter with pre-aggregated metrics
-- ============================================================================

CREATE TABLE fact_encounters (
    encounter_key INT PRIMARY KEY AUTO_INCREMENT,
    encounter_id INT UNIQUE NOT NULL,     -- original OLTP ID
    
    -- Dimension Foreign Keys
    patient_key INT NOT NULL,
    provider_key INT NOT NULL,
    encounter_date_key INT NOT NULL,      -- FK to dim_date
    discharge_date_key INT,               -- FK to dim_date, NULL for ongoing
    encounter_type_key INT NOT NULL,
    specialty_key INT NOT NULL,           -- denormalized from provider
    department_key INT NOT NULL,          -- denormalized from provider
    primary_diagnosis_key INT,            -- FK to most significant diagnosis
    
    -- Pre-aggregated Metrics (eliminate expensive joins)
    diagnosis_count INT DEFAULT 0,        -- count of diagnoses per encounter
    procedure_count INT DEFAULT 0,        -- count of procedures per encounter
    total_claim_amount DECIMAL(12,2) DEFAULT 0,     -- sum of billing amounts
    total_allowed_amount DECIMAL(12,2) DEFAULT 0,   -- sum of allowed amounts
    length_of_stay_hours INT DEFAULT 0,   -- calculated duration
    
    -- Operational Fields
    encounter_datetime DATETIME,          -- original timestamp for reference
    discharge_datetime DATETIME,
    
    -- Performance Indexes
    INDEX idx_fact_patient (patient_key),
    INDEX idx_fact_provider (provider_key),
    INDEX idx_fact_encounter_date (encounter_date_key),
    INDEX idx_fact_specialty (specialty_key),
    INDEX idx_fact_department (department_key),
    INDEX idx_fact_encounter_type (encounter_type_key),
    INDEX idx_fact_discharge_date (discharge_date_key),
    INDEX idx_fact_primary_diagnosis (primary_diagnosis_key),
    
    -- Composite indexes for common query patterns
    INDEX idx_fact_date_specialty (encounter_date_key, specialty_key),
    INDEX idx_fact_patient_date (patient_key, encounter_date_key),
    INDEX idx_fact_specialty_type (specialty_key, encounter_type_key),
    
    -- Foreign Key Constraints
    CONSTRAINT fk_fact_patient FOREIGN KEY (patient_key) REFERENCES dim_patient(patient_key),
    CONSTRAINT fk_fact_provider FOREIGN KEY (provider_key) REFERENCES dim_provider(provider_key),
    CONSTRAINT fk_fact_encounter_date FOREIGN KEY (encounter_date_key) REFERENCES dim_date(date_key),
    CONSTRAINT fk_fact_discharge_date FOREIGN KEY (discharge_date_key) REFERENCES dim_date(date_key),
    CONSTRAINT fk_fact_encounter_type FOREIGN KEY (encounter_type_key) REFERENCES dim_encounter_type(encounter_type_key),
    CONSTRAINT fk_fact_specialty FOREIGN KEY (specialty_key) REFERENCES dim_specialty(specialty_key),
    CONSTRAINT fk_fact_department FOREIGN KEY (department_key) REFERENCES dim_department(department_key),
    CONSTRAINT fk_fact_primary_diagnosis FOREIGN KEY (primary_diagnosis_key) REFERENCES dim_diagnosis(diagnosis_key)
);


-- ============================================================================
-- BRIDGE TABLES - Handle Many-to-Many Relationships
-- ============================================================================

-- Bridge Table: Encounters ↔ Diagnoses (Many-to-Many)
-- Handles multiple diagnoses per encounter efficiently
CREATE TABLE bridge_encounter_diagnoses (
    bridge_id INT PRIMARY KEY AUTO_INCREMENT,
    encounter_key INT NOT NULL,
    diagnosis_key INT NOT NULL,
    diagnosis_sequence INT DEFAULT 1,     -- 1=primary, 2=secondary, etc.
    diagnosis_present_on_admission BOOLEAN DEFAULT TRUE,
    
    -- Performance indexes
    INDEX idx_bridge_diag_encounter (encounter_key),
    INDEX idx_bridge_diag_diagnosis (diagnosis_key),
    INDEX idx_bridge_diag_sequence (encounter_key, diagnosis_sequence),
    
    -- Composite index for diagnosis-procedure pairs (Query 2)
    INDEX idx_bridge_diag_enc_diag (encounter_key, diagnosis_key),
    
    -- Foreign Key Constraints
    CONSTRAINT fk_bridge_diag_encounter FOREIGN KEY (encounter_key) REFERENCES fact_encounters(encounter_key),
    CONSTRAINT fk_bridge_diag_diagnosis FOREIGN KEY (diagnosis_key) REFERENCES dim_diagnosis(diagnosis_key),
    
    -- Ensure unique diagnosis per encounter sequence
    UNIQUE KEY unique_encounter_sequence (encounter_key, diagnosis_sequence)
);

-- Bridge Table: Encounters ↔ Procedures (Many-to-Many)  
-- Handles multiple procedures per encounter efficiently
CREATE TABLE bridge_encounter_procedures (
    bridge_id INT PRIMARY KEY AUTO_INCREMENT,
    encounter_key INT NOT NULL,
    procedure_key INT NOT NULL,
    procedure_date_key INT,               -- FK to dim_date when procedure performed
    procedure_sequence INT DEFAULT 1,     -- order procedures were performed
    modifier_codes VARCHAR(50),           -- CPT modifier codes
    procedure_status VARCHAR(20) DEFAULT 'Completed', -- 'Scheduled', 'In Progress', 'Completed'
    
    -- Performance indexes  
    INDEX idx_bridge_proc_encounter (encounter_key),
    INDEX idx_bridge_proc_procedure (procedure_key),
    INDEX idx_bridge_proc_date (procedure_date_key),
    INDEX idx_bridge_proc_sequence (encounter_key, procedure_sequence),
    
    -- Composite index for diagnosis-procedure pairs (Query 2)
    INDEX idx_bridge_proc_enc_proc (encounter_key, procedure_key),
    
    -- Foreign Key Constraints
    CONSTRAINT fk_bridge_proc_encounter FOREIGN KEY (encounter_key) REFERENCES fact_encounters(encounter_key),
    CONSTRAINT fk_bridge_proc_procedure FOREIGN KEY (procedure_key) REFERENCES dim_procedure(procedure_key),
    CONSTRAINT fk_bridge_proc_date FOREIGN KEY (procedure_date_key) REFERENCES dim_date(date_key),
    
    -- Ensure unique procedure per encounter sequence
    UNIQUE KEY unique_encounter_proc_sequence (encounter_key, procedure_sequence)
);

-- ============================================================================
-- ADDITIONAL PERFORMANCE INDEXES
-- ============================================================================

-- Covering indexes for common analytical queries

-- Query 1 optimization: Monthly encounters by specialty  
CREATE INDEX idx_covering_monthly_encounters 
ON fact_encounters (encounter_date_key, specialty_key, encounter_type_key, patient_key);

-- Query 3 optimization: Readmission analysis
CREATE INDEX idx_covering_readmissions 
ON fact_encounters (patient_key, encounter_date_key, discharge_date_key, encounter_type_key, specialty_key);

-- Query 4 optimization: Revenue by specialty & month
CREATE INDEX idx_covering_revenue 
ON fact_encounters (encounter_date_key, specialty_key, total_allowed_amount, total_claim_amount);

-- Bridge table optimization for Query 2: diagnosis-procedure pairs
CREATE INDEX idx_diag_proc_pairs 
ON bridge_encounter_diagnoses (encounter_key, diagnosis_key);

CREATE INDEX idx_proc_diag_pairs 
ON bridge_encounter_procedures (encounter_key, procedure_key);

-- ============================================================================
-- VIEWS FOR SIMPLIFIED QUERYING
-- ============================================================================

-- View: Simplified fact table with dimension names (eliminates common joins)
CREATE VIEW vw_encounter_analytics AS
SELECT 
    f.encounter_key,
    f.encounter_id,
    
    -- Date information
    d.calendar_date AS encounter_date,
    d.year AS encounter_year,
    d.month AS encounter_month,
    d.quarter AS encounter_quarter,
    
    -- Patient information  
    p.patient_id,
    p.full_name AS patient_name,
    p.age_group,
    p.gender,
    
    -- Provider/Specialty information
    pr.provider_id,
    pr.full_name AS provider_name,
    pr.credential,
    s.specialty_name,
    dept.department_name,
    
    -- Encounter details
    et.encounter_type,
    f.diagnosis_count,
    f.procedure_count,
    f.total_claim_amount,
    f.total_allowed_amount,
    f.length_of_stay_hours,
    
    -- Calculated fields
    CASE 
        WHEN f.total_claim_amount > 0 THEN (f.total_allowed_amount / f.total_claim_amount) * 100 
        ELSE 0 
    END AS reimbursement_rate_percent
    
FROM fact_encounters f
JOIN dim_date d ON f.encounter_date_key = d.date_key
JOIN dim_patient p ON f.patient_key = p.patient_key  
JOIN dim_provider pr ON f.provider_key = pr.provider_key
JOIN dim_specialty s ON f.specialty_key = s.specialty_key
JOIN dim_department dept ON f.department_key = dept.department_key
JOIN dim_encounter_type et ON f.encounter_type_key = et.encounter_type_key;

-- ============================================================================
-- COMMENTS AND DOCUMENTATION
-- ============================================================================

-- Table Comments
ALTER TABLE dim_date COMMENT = 'Date dimension with pre-computed date attributes for performance';
ALTER TABLE dim_patient COMMENT = 'Patient dimension with demographic groupings for analytics';
ALTER TABLE dim_specialty COMMENT = 'Medical specialties dimension for provider categorization';
ALTER TABLE dim_department COMMENT = 'Hospital departments dimension for operational analytics';
ALTER TABLE dim_provider COMMENT = 'Healthcare providers dimension with denormalized specialty/department';
ALTER TABLE dim_encounter_type COMMENT = 'Encounter types dimension (Inpatient, Outpatient, ER)';
ALTER TABLE dim_diagnosis COMMENT = 'Diagnosis dimension with ICD-10 codes and classifications';
ALTER TABLE dim_procedure COMMENT = 'Procedure dimension with CPT codes and classifications';
ALTER TABLE fact_encounters COMMENT = 'Fact table: one row per encounter with pre-aggregated metrics';
ALTER TABLE bridge_encounter_diagnoses COMMENT = 'Bridge table for many-to-many encounter-diagnosis relationships';
ALTER TABLE bridge_encounter_procedures COMMENT = 'Bridge table for many-to-many encounter-procedure relationships';

-- ============================================================================
-- END OF STAR SCHEMA DDL
-- ============================================================================
