-- ======================================================
-- HEALTHCARE ANALYTICS LAB - PART 2: OLTP QUERY ANALYSIS
-- Performance Analysis of Normalized Schema Queries
-- ======================================================

-- ====================
-- QUESTION 1: MONTHLY ENCOUNTERS BY SPECIALTY
-- ====================

-- What we need: For each month and specialty, show total encounters 
-- and unique patients by encounter type.

SELECT 
    DATE_FORMAT(e.encounter_date, '%Y-%m') AS month,
    s.specialty_name,
    e.encounter_type,
    COUNT(*) AS total_encounters,
    COUNT(DISTINCT e.patient_id) AS unique_patients
FROM encounters e
JOIN providers p ON e.provider_id = p.provider_id
JOIN specialties s ON p.specialty_id = s.specialty_id
GROUP BY DATE_FORMAT(e.encounter_date, '%Y-%m'), s.specialty_name, e.encounter_type
ORDER BY month, s.specialty_name, e.encounter_type;

-- ====================
-- QUESTION 2: TOP DIAGNOSIS-PROCEDURE PAIRS  
-- ====================

-- What we need: Most common diagnosis-procedure combinations
-- Show ICD code, procedure code, and encounter count

SELECT 
    d.icd10_code,
    d.icd10_description,
    pr.cpt_code,
    pr.cpt_description,
    COUNT(*) AS encounter_count
FROM encounter_diagnoses ed
JOIN diagnoses d ON ed.diagnosis_id = d.diagnosis_id
JOIN encounters e ON ed.encounter_id = e.encounter_id
JOIN encounter_procedures ep ON e.encounter_id = ep.encounter_id
JOIN procedures pr ON ep.procedure_id = pr.procedure_id
GROUP BY d.icd10_code, d.icd10_description, pr.cpt_code, pr.cpt_description
ORDER BY encounter_count DESC
LIMIT 10;

-- ====================
-- QUESTION 3: 30-DAY READMISSION RATE
-- ====================

-- What we need: Specialty with highest readmission rate
-- Definition: inpatient discharge, then return within 30 days

WITH inpatient_discharges AS (
    SELECT 
        e.encounter_id,
        e.patient_id,
        e.discharge_date,
        p.specialty_id,
        s.specialty_name
    FROM encounters e
    JOIN providers p ON e.provider_id = p.provider_id  
    JOIN specialties s ON p.specialty_id = s.specialty_id
    WHERE e.encounter_type = 'Inpatient' 
    AND e.discharge_date IS NOT NULL
),
readmissions AS (
    SELECT 
        id.encounter_id AS initial_encounter,
        id.specialty_id,
        id.specialty_name,
        COUNT(e2.encounter_id) AS readmission_count
    FROM inpatient_discharges id
    LEFT JOIN encounters e2 ON id.patient_id = e2.patient_id
        AND e2.encounter_date > id.discharge_date
        AND e2.encounter_date <= DATE_ADD(id.discharge_date, INTERVAL 30 DAY)
        AND e2.encounter_type IN ('Inpatient', 'ER')
    GROUP BY id.encounter_id, id.specialty_id, id.specialty_name
),
specialty_readmission_rates AS (
    SELECT 
        r.specialty_name,
        COUNT(DISTINCT r.initial_encounter) AS total_discharges,
        SUM(r.readmission_count) AS total_readmissions,
        ROUND((SUM(r.readmission_count) / COUNT(DISTINCT r.initial_encounter)) * 100, 2) AS readmission_rate_percent
    FROM readmissions r
    GROUP BY r.specialty_id, r.specialty_name
)
SELECT 
    specialty_name,
    total_discharges,
    total_readmissions,
    readmission_rate_percent
FROM specialty_readmission_rates
ORDER BY readmission_rate_percent DESC;

-- ====================
-- QUESTION 4: REVENUE BY SPECIALTY & MONTH
-- ====================

-- What we need: Total allowed amounts by specialty and month
-- Which specialties generate most revenue?

SELECT 
    DATE_FORMAT(e.encounter_date, '%Y-%m') AS month,
    s.specialty_name,
    SUM(b.claim_amount) AS total_claim_amount,
    SUM(b.allowed_amount) AS total_allowed_amount,
    COUNT(DISTINCT e.encounter_id) AS total_encounters,
    ROUND(AVG(b.allowed_amount), 2) AS avg_allowed_per_encounter
FROM billing b
JOIN encounters e ON b.encounter_id = e.encounter_id
JOIN providers p ON e.provider_id = p.provider_id
JOIN specialties s ON p.specialty_id = s.specialty_id
GROUP BY DATE_FORMAT(e.encounter_date, '%Y-%m'), s.specialty_id, s.specialty_name
ORDER BY month, total_allowed_amount DESC;

-- ====================
-- PERFORMANCE ANALYSIS QUERIES
-- ====================

-- Query to analyze table sizes and join costs
SELECT 
    'patients' AS table_name,
    COUNT(*) AS row_count
FROM patients
UNION ALL
SELECT 'encounters', COUNT(*) FROM encounters  
UNION ALL
SELECT 'providers', COUNT(*) FROM providers
UNION ALL
SELECT 'specialties', COUNT(*) FROM specialties
UNION ALL
SELECT 'departments', COUNT(*) FROM departments
UNION ALL
SELECT 'diagnoses', COUNT(*) FROM diagnoses
UNION ALL
SELECT 'procedures', COUNT(*) FROM procedures
UNION ALL
SELECT 'encounter_diagnoses', COUNT(*) FROM encounter_diagnoses
UNION ALL  
SELECT 'encounter_procedures', COUNT(*) FROM encounter_procedures
UNION ALL
SELECT 'billing', COUNT(*) FROM billing;

-- Query to check index usage
SHOW INDEX FROM encounters;
SHOW INDEX FROM billing;
SHOW INDEX FROM encounter_diagnoses;
SHOW INDEX FROM encounter_procedures;