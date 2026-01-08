-- Query Performance Analysis

-- This query retrieves the total number of encounters and unique patients per specialty and encounter type on a monthly basis.
SELECT
    DATE_TRUNC(e.encounter_date, MONTH) AS month,
    s.specialty_name,
    e.encounter_type,
    COUNT(*) AS total_encounters,
    COUNT(DISTINCT e.patient_id) AS unique_patients
FROM encounters e
JOIN providers p ON e.provider_id = p.provider_id
JOIN specialties s ON p.specialty_id = s.specialty_id
GROUP BY month, s.specialty_name, e.encounter_type
ORDER BY month, s.specialty_name, e.encounter_type;

-- This query retrieves the topmost common diagnoses-procedures combinations. showing the ICD code, procedure code and encounter count.
SELECT
    d.icd10_code,
    pr.cpt_code,
    COUNT(*) AS encounter_count
FROM encounter_diagnoses ed
JOIN diagnoses d ON ed.diagnosis_id = d.diagnosis_id
JOIN encounter_procedures ep ON ed.encounter_id = ep.encounter_id
JOIN procedures pr ON ep.procedure_id = pr.procedure_id
GROUP BY d.icd10_code, pr.cpt_code
ORDER BY encounter_count DESC
LIMIT 10;

-- This query retrieves the specialty with the highest readmission rates within 30 days(definition: inpatient discharges, then return within 30 days).
WITH Readmissions AS (
    SELECT
        e1.encounter_id AS initial_encounter,
        e2.encounter_id AS readmission_encounter,
        p.specialty_id
    FROM encounters e1
    JOIN encounters e2 ON e1.patient_id = e2.patient_id
        AND e2.encounter_date > e1.discharge_date
        AND e2.encounter_date <= e1.discharge_date + INTERVAL '30 days'
    JOIN providers p ON e1.provider_id = p.provider_id
    WHERE e1.encounter_type = 'Inpatient'
)
SELECT
    s.specialty_name,
    COUNT(r.readmission_encounter) AS readmission_count,
    COUNT(DISTINCT e.encounter_id) AS total_discharges, 
    (COUNT(r.readmission_encounter)::DECIMAL / COUNT(DISTINCT e.encounter_id)) * 100 AS readmission_rate_percentage
FROM Readmissions r
JOIN specialties s ON r.specialty_id = s.specialty_id
JOIN encounters e ON r.initial_encounter = e.encounter_id
GROUP BY s.specialty_name
ORDER BY readmission_rate_percentage DESC
LIMIT 1;

-- This query retrieves the revenue by specialty and month
SELECT
    DATE_TRUNC(e.encounter_date, MONTH) AS month,
    s.specialty_name,
    SUM(b.claim_amount) AS total_revenue
FROM billing b
JOIN encounters e ON b.encounter_id = e.encounter_id
JOIN providers p ON e.provider_id = p.provider_id
JOIN specialties s ON p.specialty_id = s.specialty_id
GROUP BY month, s.specialty_name
ORDER BY month, s.specialty_name;