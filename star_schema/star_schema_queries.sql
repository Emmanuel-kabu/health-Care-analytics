-- ============================================================================
-- HEALTHCARE ANALYTICS STAR SCHEMA QUERIES
-- ============================================================================

-- Question 1: Monthly Encounters by Specialty
-- Shows the volume of encounters per specialty by month for trend analysis
-- ============================================================================
SELECT 
    d.year,
    d.month,
    TO_CHAR(TO_DATE(d.month::TEXT, 'MM'), 'Month') AS month_name,
    s.specialty_name,
    s.specialty_category,
    COUNT(*) AS total_encounters,
    COUNT(CASE WHEN et.encounter_type = 'Inpatient' THEN 1 END) AS inpatient_encounters,
    COUNT(CASE WHEN et.encounter_type = 'Outpatient' THEN 1 END) AS outpatient_encounters,
    COUNT(CASE WHEN et.encounter_type IN ('ER', 'Emergency') THEN 1 END) AS emergency_encounters,
    ROUND(AVG(f.total_allowed_amount), 2) AS avg_revenue_per_encounter
FROM fact_encounters f
JOIN dim_date d ON f.encounter_date_key = d.date_key
JOIN dim_specialty s ON f.specialty_key = s.specialty_key
JOIN dim_encounter_type et ON f.encounter_type_key = et.encounter_type_key
WHERE d.year >= EXTRACT(YEAR FROM CURRENT_DATE) - 2  -- Last 2 years
GROUP BY d.year, d.month, s.specialty_name, s.specialty_category
ORDER BY d.year DESC, d.month DESC, total_encounters DESC;

-- ============================================================================
-- Question 2: Top Diagnosis-Procedure Pairs
-- Identifies most common diagnosis-procedure combinations for clinical insights
-- ============================================================================
SELECT 
    diag.icd10_code,
    diag.icd10_description AS diagnosis_description,
    diag.diagnosis_category,
    proc.cpt_code,
    proc.cpt_description AS procedure_description,
    proc.procedure_category,
    COUNT(*) AS combination_count,
    COUNT(DISTINCT bd.encounter_key) AS encounters_with_pair,
    ROUND(AVG(f.total_allowed_amount), 2) AS avg_revenue_per_encounter,
    ROUND(AVG(f.length_of_stay_hours), 1) AS avg_length_of_stay_hours
FROM bridge_encounter_diagnoses bd
JOIN bridge_encounter_procedures bp ON bd.encounter_key = bp.encounter_key
JOIN dim_diagnosis diag ON bd.diagnosis_key = diag.diagnosis_key
JOIN dim_procedure proc ON bp.procedure_key = proc.procedure_key
JOIN fact_encounters f ON bd.encounter_key = f.encounter_key
WHERE bd.diagnosis_sequence = 1  -- Primary diagnosis only
GROUP BY 
    diag.icd10_code, diag.icd10_description, diag.diagnosis_category,
    proc.cpt_code, proc.cpt_description, proc.procedure_category
HAVING COUNT(*) >= 10  -- Filter for statistically significant pairs
ORDER BY combination_count DESC, avg_revenue_per_encounter DESC
LIMIT 20;

-- ============================================================================
-- Question 3: 30-Day Readmission Rate
-- Calculates readmission rates within 30 days by specialty and diagnosis
-- ============================================================================
WITH encounter_pairs AS (
    SELECT 
        f1.patient_key,
        f1.encounter_id AS initial_encounter_id,
        f1.encounter_datetime AS initial_encounter_date,
        f1.discharge_datetime AS initial_discharge_date,
        f1.specialty_key AS initial_specialty_key,
        s1.specialty_name AS initial_specialty,
        
        -- Find next encounter within 30 days
        f2.encounter_id AS readmit_encounter_id,
        f2.encounter_datetime AS readmit_encounter_date,
        f2.specialty_key AS readmit_specialty_key,
        s2.specialty_name AS readmit_specialty,
        
        -- Calculate days between discharge and readmission
        EXTRACT(DAY FROM (f2.encounter_datetime - f1.discharge_datetime)) AS days_to_readmission
        
    FROM fact_encounters f1
    JOIN dim_specialty s1 ON f1.specialty_key = s1.specialty_key
    LEFT JOIN fact_encounters f2 ON f1.patient_key = f2.patient_key
    JOIN dim_specialty s2 ON f2.specialty_key = s2.specialty_key
    WHERE 
        f1.discharge_datetime IS NOT NULL
        AND f2.encounter_datetime > f1.discharge_datetime
        AND f2.encounter_datetime <= f1.discharge_datetime + INTERVAL '30 days'
        AND f1.encounter_date_key >= generate_date_key((CURRENT_DATE - INTERVAL '1 year')::DATE)
),
readmission_summary AS (
    SELECT 
        initial_specialty_key,
        initial_specialty,
        COUNT(DISTINCT initial_encounter_id) AS total_discharges,
        COUNT(DISTINCT CASE WHEN readmit_encounter_id IS NOT NULL THEN initial_encounter_id END) AS readmissions_30day,
        ROUND(
            COUNT(DISTINCT CASE WHEN readmit_encounter_id IS NOT NULL THEN initial_encounter_id END) * 100.0 
            / COUNT(DISTINCT initial_encounter_id), 
            2
        ) AS readmission_rate_percent,
        ROUND(AVG(CASE WHEN readmit_encounter_id IS NOT NULL THEN days_to_readmission END), 1) AS avg_days_to_readmission
    FROM encounter_pairs
    GROUP BY initial_specialty_key, initial_specialty
)
SELECT 
    initial_specialty,
    total_discharges,
    readmissions_30day,
    readmission_rate_percent,
    avg_days_to_readmission,
    CASE 
        WHEN readmission_rate_percent > 15 THEN 'High Risk'
        WHEN readmission_rate_percent > 10 THEN 'Moderate Risk'
        ELSE 'Low Risk'
    END AS risk_category
FROM readmission_summary
WHERE total_discharges >= 50  -- Minimum volume for statistical significance
ORDER BY readmission_rate_percent DESC, total_discharges DESC;

-- ============================================================================
-- Question 4: Revenue by Specialty & Month
-- Analyzes financial performance trends by specialty over time
-- ============================================================================
SELECT 
    d.year,
    d.month,
    TO_CHAR(TO_DATE(d.month::TEXT, 'MM'), 'Month') AS month_name,
    s.specialty_name,
    s.specialty_category,
    
    -- Volume metrics
    COUNT(*) AS total_encounters,
    COUNT(CASE WHEN f.total_claim_amount > 0 THEN 1 END) AS billable_encounters,
    
    -- Revenue metrics
    SUM(f.total_claim_amount) AS total_charges,
    SUM(f.total_allowed_amount) AS total_collections,
    ROUND(AVG(f.total_claim_amount), 2) AS avg_charge_per_encounter,
    ROUND(AVG(f.total_allowed_amount), 2) AS avg_collection_per_encounter,
    
    -- Collection ratio
    ROUND(
        CASE 
            WHEN SUM(f.total_claim_amount) > 0 
            THEN SUM(f.total_allowed_amount) * 100.0 / SUM(f.total_claim_amount)
            ELSE 0 
        END, 
        1
    ) AS collection_rate_percent,
    
    -- Year-over-year growth (requires window function)
    ROUND(
        (SUM(f.total_allowed_amount) - 
         LAG(SUM(f.total_allowed_amount), 12) OVER (
             PARTITION BY s.specialty_key 
             ORDER BY d.year, d.month
         )) * 100.0 / 
         NULLIF(LAG(SUM(f.total_allowed_amount), 12) OVER (
             PARTITION BY s.specialty_key 
             ORDER BY d.year, d.month
         ), 0),
         1
    ) AS yoy_growth_percent
    
FROM fact_encounters f
JOIN dim_date d ON f.encounter_date_key = d.date_key
JOIN dim_specialty s ON f.specialty_key = s.specialty_key
WHERE 
    d.year >= EXTRACT(YEAR FROM CURRENT_DATE) - 2  -- Last 2 years
    AND f.total_claim_amount >= 0  -- Valid financial data only
GROUP BY d.year, d.month, s.specialty_key, s.specialty_name, s.specialty_category
HAVING COUNT(*) >= 10  -- Minimum volume for meaningful analysis
ORDER BY d.year DESC, d.month DESC, total_collections DESC;

-- ============================================================================
-- BONUS QUERIES: Additional Analytics
-- ============================================================================

-- Top Revenue Generating Diagnoses
SELECT 
    diag.icd10_code,
    diag.icd10_description,
    diag.diagnosis_category,
    COUNT(DISTINCT bd.encounter_key) AS total_encounters,
    SUM(f.total_allowed_amount) AS total_revenue,
    ROUND(AVG(f.total_allowed_amount), 2) AS avg_revenue_per_encounter,
    ROUND(AVG(f.length_of_stay_hours), 1) AS avg_length_of_stay
FROM bridge_encounter_diagnoses bd
JOIN dim_diagnosis diag ON bd.diagnosis_key = diag.diagnosis_key
JOIN fact_encounters f ON bd.encounter_key = f.encounter_key
WHERE bd.diagnosis_sequence = 1  -- Primary diagnosis
GROUP BY diag.icd10_code, diag.icd10_description, diag.diagnosis_category
HAVING COUNT(DISTINCT bd.encounter_key) >= 20
ORDER BY total_revenue DESC
LIMIT 15;

-- Patient Demographics Analysis
SELECT 
    p.age_group,
    p.gender,
    COUNT(*) AS total_encounters,
    COUNT(DISTINCT p.patient_key) AS unique_patients,
    ROUND(COUNT(*)::NUMERIC / COUNT(DISTINCT p.patient_key), 1) AS encounters_per_patient,
    SUM(f.total_allowed_amount) AS total_revenue,
    ROUND(AVG(f.total_allowed_amount), 2) AS avg_revenue_per_encounter
FROM fact_encounters f
JOIN dim_patient p ON f.patient_key = p.patient_key
JOIN dim_date d ON f.encounter_date_key = d.date_key
WHERE d.year >= EXTRACT(YEAR FROM CURRENT_DATE) - 1  -- Last year
GROUP BY p.age_group, p.gender
ORDER BY total_encounters DESC;