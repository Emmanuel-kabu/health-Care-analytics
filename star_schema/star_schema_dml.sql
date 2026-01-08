-- ============================================================================
-- HEALTHCARE ANALYTICS STAR SCHEMA - SAMPLE DATA POPULATION (DML)
-- ============================================================================
-- This file contains INSERT statements to populate the star schema with 
-- sample data transformed from the original OLTP system
-- ============================================================================

-- ==========================
-- POPULATE DIMENSION TABLES
-- ==========================

-- Populate dim_date (sample dates for 2024)
INSERT INTO dim_date VALUES
(20240101, '2024-01-01', 2024, 1, 1, 1, 1, 'Monday', FALSE, 2024, 1, FALSE),
(20240102, '2024-01-02', 2024, 1, 1, 2, 1, 'Tuesday', FALSE, 2024, 1, FALSE),
(20240510, '2024-05-10', 2024, 2, 5, 10, 19, 'Friday', FALSE, 2024, 2, FALSE),
(20240515, '2024-05-15', 2024, 2, 5, 15, 20, 'Wednesday', FALSE, 2024, 2, FALSE),
(20240602, '2024-06-02', 2024, 2, 6, 2, 22, 'Sunday', TRUE, 2024, 2, FALSE),
(20240606, '2024-06-06', 2024, 2, 6, 6, 23, 'Thursday', FALSE, 2024, 2, FALSE),
(20240608, '2024-06-08', 2024, 2, 6, 8, 23, 'Saturday', TRUE, 2024, 2, FALSE),
(20240612, '2024-06-12', 2024, 2, 6, 12, 24, 'Wednesday', FALSE, 2024, 2, FALSE),
(20240613, '2024-06-13', 2024, 2, 6, 13, 24, 'Thursday', FALSE, 2024, 2, FALSE);

-- Populate dim_specialty
INSERT INTO dim_specialty (specialty_id, specialty_name, specialty_code, specialty_category) VALUES
(1, 'Cardiology', 'CARD', 'Medical'),
(2, 'Internal Medicine', 'IM', 'Medical'), 
(3, 'Emergency Medicine', 'ER', 'Medical');

-- Populate dim_department  
INSERT INTO dim_department (department_id, department_name, floor, capacity, department_type, cost_center_code) VALUES
(1, 'Cardiology Unit', 3, 20, 'Inpatient', 'CC-CARD-001'),
(2, 'Internal Medicine Ward', 2, 30, 'Inpatient', 'CC-IM-002'),
(3, 'Emergency Department', 1, 45, 'Emergency', 'CC-ER-003');

-- Populate dim_encounter_type
INSERT INTO dim_encounter_type (encounter_type, type_description, typical_duration_hours, requires_admission) VALUES
('Outpatient', 'Outpatient clinic visit', 2, FALSE),
('Inpatient', 'Inpatient hospital admission', 96, TRUE),
('ER', 'Emergency department visit', 6, FALSE);

-- Populate dim_patient with age groups
INSERT INTO dim_patient (patient_id, first_name, last_name, full_name, gender, date_of_birth, current_age, age_group, mrn) VALUES
(1001, 'John', 'Doe', 'John Doe', 'M', '1955-03-15', 69, '60+', 'MRN001'),
(1002, 'Jane', 'Smith', 'Jane Smith', 'F', '1962-07-22', 62, '60+', 'MRN002'),
(1003, 'Robert', 'Johnson', 'Robert Johnson', 'M', '1948-11-08', 76, '60+', 'MRN003');

-- Populate dim_provider with denormalized specialty/department names
INSERT INTO dim_provider (provider_id, first_name, last_name, full_name, credential, provider_type, specialty_key, department_key, specialty_name, department_name) VALUES
(101, 'James', 'Chen', 'James Chen', 'MD', 'Attending Physician', 1, 1, 'Cardiology', 'Cardiology Unit'),
(102, 'Sarah', 'Williams', 'Sarah Williams', 'MD', 'Attending Physician', 2, 2, 'Internal Medicine', 'Internal Medicine Ward'),
(103, 'Michael', 'Rodriguez', 'Michael Rodriguez', 'MD', 'Emergency Physician', 3, 3, 'Emergency Medicine', 'Emergency Department');

-- Populate dim_diagnosis
INSERT INTO dim_diagnosis (diagnosis_id, icd10_code, icd10_description, diagnosis_category, body_system, severity_level, chronic_flag) VALUES
(3001, 'I10', 'Essential Hypertension', 'Cardiovascular', 'Circulatory', 'Medium', TRUE),
(3002, 'E11.9', 'Type 2 Diabetes Mellitus', 'Endocrine', 'Endocrine', 'Medium', TRUE),
(3003, 'I50.9', 'Heart Failure, Unspecified', 'Cardiovascular', 'Circulatory', 'High', TRUE);

-- Populate dim_procedure
INSERT INTO dim_procedure (procedure_id, cpt_code, cpt_description, procedure_category, procedure_type, typical_cost_range, duration_minutes) VALUES
(4001, '99213', 'Office Visit, Established Patient', 'Evaluation & Management', 'Clinical Visit', '$150-250', 30),
(4002, '93000', 'Electrocardiogram (EKG)', 'Diagnostic', 'Cardiac Testing', '$50-100', 15),
(4003, '71020', 'Chest X-ray, Two Views', 'Diagnostic', 'Imaging', '$100-200', 20);

-- ==========================
-- POPULATE FACT TABLE
-- ==========================

-- Populate fact_encounters with pre-aggregated metrics
INSERT INTO fact_encounters (
    encounter_id, 
    patient_key, 
    provider_key, 
    encounter_date_key, 
    discharge_date_key, 
    encounter_type_key, 
    specialty_key,
    department_key,
    primary_diagnosis_key,
    diagnosis_count, 
    procedure_count, 
    total_claim_amount, 
    total_allowed_amount,
    length_of_stay_hours,
    encounter_datetime,
    discharge_datetime
) VALUES
-- Encounter 7001: John Doe, Cardiology Outpatient, 2 diagnoses, 2 procedures
(7001, 1, 1, 20240510, 20240510, 1, 1, 1, 1, 2, 2, 350.00, 280.00, 2, '2024-05-10 10:00:00', '2024-05-10 11:30:00'),

-- Encounter 7002: John Doe, Cardiology Inpatient, 2 diagnoses, 1 procedure  
(7002, 1, 1, 20240602, 20240606, 2, 1, 1, 1, 2, 1, 12500.00, 10000.00, 92, '2024-06-02 14:00:00', '2024-06-06 09:00:00'),

-- Encounter 7003: Jane Smith, Internal Medicine Outpatient, 1 diagnosis, 1 procedure
(7003, 2, 2, 20240515, 20240515, 1, 2, 2, 2, 1, 1, 200.00, 150.00, 1, '2024-05-15 09:00:00', '2024-05-15 10:15:00'),

-- Encounter 7004: Robert Johnson, Emergency, 1 diagnosis, 0 procedures  
(7004, 3, 3, 20240612, 20240613, 3, 3, 3, 1, 1, 0, 800.00, 600.00, 7, '2024-06-12 23:45:00', '2024-06-13 06:30:00');

-- ==========================
-- POPULATE BRIDGE TABLES
-- ==========================

-- Populate bridge_encounter_diagnoses
INSERT INTO bridge_encounter_diagnoses (encounter_key, diagnosis_key, diagnosis_sequence, diagnosis_present_on_admission) VALUES
-- Encounter 7001: Hypertension (primary) + Diabetes (secondary)
(1, 1, 1, TRUE),  -- Primary: Hypertension
(1, 2, 2, TRUE),  -- Secondary: Diabetes

-- Encounter 7002: Hypertension (primary) + Heart Failure (secondary)  
(2, 1, 1, TRUE),  -- Primary: Hypertension
(2, 3, 2, FALSE), -- Secondary: Heart Failure (developed during stay)

-- Encounter 7003: Diabetes (primary only)
(3, 2, 1, TRUE),  -- Primary: Diabetes

-- Encounter 7004: Hypertension (primary only)
(4, 1, 1, TRUE);  -- Primary: Hypertension

-- Populate bridge_encounter_procedures  
INSERT INTO bridge_encounter_procedures (encounter_key, procedure_key, procedure_date_key, procedure_sequence, modifier_codes, procedure_status) VALUES
-- Encounter 7001: Office Visit + EKG
(1, 1, 20240510, 1, NULL, 'Completed'),  -- Office Visit
(1, 2, 20240510, 2, NULL, 'Completed'),  -- EKG

-- Encounter 7002: Office Visit only
(2, 1, 20240602, 1, NULL, 'Completed'),  -- Office Visit

-- Encounter 7003: Office Visit only  
(3, 1, 20240515, 1, NULL, 'Completed');  -- Office Visit

-- Note: Encounter 7004 (Emergency) had no procedures recorded

-- ==========================
-- DATA VALIDATION QUERIES
-- ==========================

-- Verify dimension counts
SELECT 'dim_date' AS table_name, COUNT(*) AS row_count FROM dim_date
UNION ALL
SELECT 'dim_patient', COUNT(*) FROM dim_patient  
UNION ALL
SELECT 'dim_provider', COUNT(*) FROM dim_provider
UNION ALL  
SELECT 'dim_specialty', COUNT(*) FROM dim_specialty
UNION ALL
SELECT 'dim_department', COUNT(*) FROM dim_department
UNION ALL
SELECT 'dim_encounter_type', COUNT(*) FROM dim_encounter_type
UNION ALL
SELECT 'dim_diagnosis', COUNT(*) FROM dim_diagnosis
UNION ALL
SELECT 'dim_procedure', COUNT(*) FROM dim_procedure
UNION ALL
SELECT 'fact_encounters', COUNT(*) FROM fact_encounters
UNION ALL  
SELECT 'bridge_encounter_diagnoses', COUNT(*) FROM bridge_encounter_diagnoses
UNION ALL
SELECT 'bridge_encounter_procedures', COUNT(*) FROM bridge_encounter_procedures;

-- Verify pre-aggregated metrics match bridge table counts
SELECT 
    f.encounter_id,
    f.diagnosis_count AS fact_diagnosis_count,
    COUNT(bd.diagnosis_key) AS bridge_diagnosis_count,
    f.procedure_count AS fact_procedure_count,
    COUNT(bp.procedure_key) AS bridge_procedure_count
FROM fact_encounters f
LEFT JOIN bridge_encounter_diagnoses bd ON f.encounter_key = bd.encounter_key
LEFT JOIN bridge_encounter_procedures bp ON f.encounter_key = bp.encounter_key  
GROUP BY f.encounter_key, f.encounter_id, f.diagnosis_count, f.procedure_count
ORDER BY f.encounter_id;

-- Verify revenue totals (should match original billing data)
SELECT 
    SUM(total_claim_amount) AS total_claims,
    SUM(total_allowed_amount) AS total_allowed,
    COUNT(*) AS total_encounters,
    AVG(total_allowed_amount) AS avg_allowed_per_encounter
FROM fact_encounters;

-- Test the optimized analytical view
SELECT * FROM vw_encounter_analytics 
ORDER BY encounter_date, patient_name;

-- ==========================
-- PERFORMANCE TEST QUERIES  
-- ==========================

-- Test Query 1 Performance (Monthly Encounters by Specialty)
SELECT 
    d.year,
    d.month,
    s.specialty_name,
    et.encounter_type,
    COUNT(*) AS total_encounters,
    COUNT(DISTINCT f.patient_key) AS unique_patients
FROM fact_encounters f
JOIN dim_date d ON f.encounter_date_key = d.date_key
JOIN dim_specialty s ON f.specialty_key = s.specialty_key  
JOIN dim_encounter_type et ON f.encounter_type_key = et.encounter_type_key
GROUP BY d.year, d.month, s.specialty_name, et.encounter_type
ORDER BY d.year, d.month, s.specialty_name;

-- Test Query 2 Performance (Top Diagnosis-Procedure Pairs)
SELECT 
    diag.icd10_code,
    diag.icd10_description,
    proc.cpt_code,
    proc.cpt_description,
    COUNT(*) AS encounter_count
FROM bridge_encounter_diagnoses bd
JOIN bridge_encounter_procedures bp ON bd.encounter_key = bp.encounter_key
JOIN dim_diagnosis diag ON bd.diagnosis_key = diag.diagnosis_key
JOIN dim_procedure proc ON bp.procedure_key = proc.procedure_key
GROUP BY diag.icd10_code, diag.icd10_description, proc.cpt_code, proc.cpt_description
ORDER BY encounter_count DESC;

-- Test Query 4 Performance (Revenue by Specialty & Month)  
SELECT 
    d.year,
    d.month,
    s.specialty_name,
    SUM(f.total_claim_amount) AS total_claim_amount,
    SUM(f.total_allowed_amount) AS total_allowed_amount,
    COUNT(*) AS total_encounters,
    ROUND(AVG(f.total_allowed_amount), 2) AS avg_allowed_per_encounter
FROM fact_encounters f
JOIN dim_date d ON f.encounter_date_key = d.date_key
JOIN dim_specialty s ON f.specialty_key = s.specialty_key
GROUP BY d.year, d.month, s.specialty_name
ORDER BY d.year, d.month, total_allowed_amount DESC;

-- ============================================================================
-- END OF STAR SCHEMA SAMPLE DATA
-- ============================================================================
