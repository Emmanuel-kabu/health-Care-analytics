-- ===================================================================
-- ENABLE REQUIRED EXTENSIONS
-- ===================================================================
CREATE EXTENSION IF NOT EXISTS dblink;

-- ===================================================================
-- CREATE ETL HELPER FUNCTIONS (if not exists)
-- ===================================================================
CREATE OR REPLACE FUNCTION generate_date_key(input_date DATE) 
RETURNS INT AS $$
BEGIN
    RETURN CASE 
        WHEN input_date IS NULL THEN NULL
        ELSE EXTRACT(YEAR FROM input_date) * 10000 + 
             EXTRACT(MONTH FROM input_date) * 100 + 
             EXTRACT(DAY FROM input_date)
    END;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION calculate_age(birth_date DATE, reference_date DATE DEFAULT CURRENT_DATE)
RETURNS INT AS $$
BEGIN
    RETURN EXTRACT(YEAR FROM AGE(reference_date, birth_date));
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_age_group(age INT)
RETURNS VARCHAR(20) AS $$
BEGIN
    RETURN CASE 
        WHEN age <= 18 THEN '0-18'
        WHEN age <= 35 THEN '19-35'
        WHEN age <= 60 THEN '36-60'
        ELSE '60+'
    END;
END;
$$ LANGUAGE plpgsql;

-- ===================================================================
-- CREATE ETL STORED PROCEDURE
-- ===================================================================
CREATE OR REPLACE PROCEDURE run_healthcare_etl()
LANGUAGE plpgsql
AS $$
BEGIN
    -- ===============================
    -- 1. LOGGING
    -- ===============================
    RAISE NOTICE '=======================================';
    RAISE NOTICE 'Starting Healthcare ETL Procedure...';
    RAISE NOTICE '=======================================';

    -- ===============================
    -- 2. POPULATE DATE DIMENSION
    -- ===============================
    RAISE NOTICE 'Populating date dimension...';
    INSERT INTO dim_date (
        date_key, calendar_date, year, quarter, month, day_of_month,
        week_of_year, day_of_week, is_weekend, fiscal_year, fiscal_quarter
    )
    SELECT 
        generate_date_key(date_series::DATE) as date_key,
        date_series::DATE as calendar_date,
        EXTRACT(YEAR FROM date_series) as year,
        EXTRACT(QUARTER FROM date_series) as quarter,
        EXTRACT(MONTH FROM date_series) as month,
        EXTRACT(DAY FROM date_series) as day_of_month,
        EXTRACT(WEEK FROM date_series) as week_of_year,
        TRIM(TO_CHAR(date_series, 'Day')) as day_of_week,
        CASE WHEN EXTRACT(DOW FROM date_series) IN (0,6) THEN TRUE ELSE FALSE END as is_weekend,
        CASE 
            WHEN EXTRACT(MONTH FROM date_series) >= 7 THEN EXTRACT(YEAR FROM date_series) + 1
            ELSE EXTRACT(YEAR FROM date_series)
        END as fiscal_year,
        CASE 
            WHEN EXTRACT(MONTH FROM date_series) IN (7,8,9) THEN 1
            WHEN EXTRACT(MONTH FROM date_series) IN (10,11,12) THEN 2
            WHEN EXTRACT(MONTH FROM date_series) IN (1,2,3) THEN 3
            ELSE 4
        END as fiscal_quarter
    FROM generate_series('2019-01-01'::DATE, '2028-12-31'::DATE, '1 day'::INTERVAL) AS date_series
    ON CONFLICT (date_key) DO NOTHING;

    -- ===============================
    -- 3. POPULATE DIMENSIONS (SPECIALTY, DEPARTMENT, ENCOUNTER TYPE)
    -- ===============================
    RAISE NOTICE 'Populating specialty dimension...';
    INSERT INTO dim_specialty (specialty_id, specialty_name, specialty_code, specialty_category)
    SELECT specialty_id,
           specialty_name,
           specialty_code,
           CASE 
               WHEN specialty_name ILIKE '%surgery%' OR specialty_name ILIKE '%surgical%' THEN 'Surgical'
               WHEN specialty_name ILIKE '%diagnostic%' OR specialty_name ILIKE '%radiology%' THEN 'Diagnostic'
               ELSE 'Medical'
           END
    FROM dblink('host=localhost dbname=hospital_db user=postgres password=password',
                'SELECT specialty_id, specialty_name, specialty_code FROM specialties')
         AS s(specialty_id INT, specialty_name VARCHAR(100), specialty_code VARCHAR(10))
    ON CONFLICT (specialty_id) DO NOTHING;

    RAISE NOTICE 'Populating department dimension...';
    INSERT INTO dim_department (department_id, department_name, floor, capacity, department_type, cost_center_code)
    SELECT department_id,
           department_name,
           floor,
           capacity,
           CASE 
               WHEN department_name ILIKE '%emergency%' OR department_name ILIKE '%er%' THEN 'Emergency'
               WHEN department_name ILIKE '%outpatient%' OR department_name ILIKE '%clinic%' THEN 'Outpatient'
               ELSE 'Inpatient'
           END as department_type,
           'CC' || LPAD(department_id::TEXT, 4, '0') as cost_center_code
    FROM dblink('host=localhost dbname=hospital_db user=postgres password=password',
                'SELECT department_id, department_name, floor, capacity FROM departments')
         AS d(department_id INT, department_name VARCHAR(100), floor INT, capacity INT)
    ON CONFLICT (department_id) DO NOTHING;

    RAISE NOTICE 'Populating encounter type dimension...';
    INSERT INTO dim_encounter_type (encounter_type, type_description, typical_duration_hours, requires_admission)
    SELECT DISTINCT encounter_type,
           CASE 
               WHEN encounter_type = 'Inpatient' THEN 'Hospital admission requiring overnight stay'
               WHEN encounter_type IN ('ER','Emergency') THEN 'Emergency department visit'
               WHEN encounter_type = 'Outpatient' THEN 'Clinic visit, same-day discharge'
               ELSE 'General healthcare encounter'
           END as type_description,
           CASE 
               WHEN encounter_type = 'Inpatient' THEN 48
               WHEN encounter_type IN ('ER','Emergency') THEN 4
               ELSE 2
           END as typical_duration_hours,
           CASE WHEN encounter_type = 'Inpatient' THEN TRUE ELSE FALSE END as requires_admission
    FROM dblink('host=localhost dbname=hospital_db user=postgres password=password',
                'SELECT DISTINCT encounter_type FROM encounters WHERE encounter_type IS NOT NULL')
         AS et(encounter_type VARCHAR(50))
    ON CONFLICT (encounter_type) DO NOTHING;

    -- ===============================
    -- 4. POPULATE DIAGNOSIS AND PROCEDURE DIMENSIONS
    -- ===============================
    RAISE NOTICE 'Populating diagnosis dimension...';
    INSERT INTO dim_diagnosis (diagnosis_id, icd10_code, icd10_description, diagnosis_category, body_system, chronic_flag)
    SELECT diagnosis_id,
           icd10_code,
           icd10_description,
           CASE 
               WHEN icd10_code LIKE 'I%' THEN 'Cardiovascular'
               WHEN icd10_code LIKE 'E%' THEN 'Endocrine'
               WHEN icd10_code LIKE 'M%' THEN 'Musculoskeletal'
               WHEN icd10_code LIKE 'G%' THEN 'Neurological'
               ELSE 'General'
           END,
           CASE 
               WHEN icd10_code LIKE 'I%' THEN 'Cardiovascular System'
               WHEN icd10_code LIKE 'E%' THEN 'Endocrine System'
               WHEN icd10_code LIKE 'M%' THEN 'Musculoskeletal System'
               WHEN icd10_code LIKE 'G%' THEN 'Nervous System'
               ELSE 'Multiple Systems'
           END,
           CASE 
               WHEN icd10_description ILIKE '%diabetes%' OR icd10_description ILIKE '%hypertension%' OR icd10_description ILIKE '%heart failure%' THEN TRUE
               ELSE FALSE
           END
    FROM dblink('host=localhost dbname=hospital_db user=postgres password=password',
                'SELECT diagnosis_id, icd10_code, icd10_description FROM diagnoses')
         AS d(diagnosis_id INT, icd10_code VARCHAR(10), icd10_description VARCHAR(200))
    ON CONFLICT (diagnosis_id) DO NOTHING;

    RAISE NOTICE 'Populating procedure dimension...';
    INSERT INTO dim_procedure (procedure_id, cpt_code, cpt_description, procedure_category, procedure_type, typical_cost_range, duration_minutes)
    SELECT procedure_id,
           cpt_code,
           cpt_description,
           CASE 
               WHEN cpt_description ILIKE '%x-ray%' OR cpt_description ILIKE '%ct%' OR cpt_description ILIKE '%ekg%' THEN 'Diagnostic'
               WHEN cpt_description ILIKE '%surgery%' OR cpt_description ILIKE '%procedure%' THEN 'Therapeutic'
               ELSE 'Clinical'
           END,
           CASE 
               WHEN cpt_description ILIKE '%imaging%' OR cpt_description ILIKE '%x-ray%' OR cpt_description ILIKE '%ct%' THEN 'Imaging'
               WHEN cpt_description ILIKE '%lab%' OR cpt_description ILIKE '%test%' THEN 'Laboratory'
               ELSE 'Clinical Procedure'
           END,
           CASE 
               WHEN cpt_description ILIKE '%visit%' THEN '$100-300'
               WHEN cpt_description ILIKE '%x-ray%' THEN '$200-500'
               WHEN cpt_description ILIKE '%ct%' THEN '$800-2000'
               ELSE '$300-800'
           END,
           CASE 
               WHEN cpt_description ILIKE '%visit%' THEN 30
               WHEN cpt_description ILIKE '%x-ray%' THEN 15
               WHEN cpt_description ILIKE '%ekg%' THEN 10
               WHEN cpt_description ILIKE '%ct%' THEN 45
               ELSE 20
           END
    FROM dblink('host=localhost dbname=hospital_db user=postgres password=password',
                'SELECT procedure_id, cpt_code, cpt_description FROM procedures')
         AS p(procedure_id INT, cpt_code VARCHAR(10), cpt_description VARCHAR(200))
    ON CONFLICT (procedure_id) DO NOTHING;

    -- ===============================
    -- 5. POPULATE PATIENT AND PROVIDER DIMENSIONS
    -- ===============================
    RAISE NOTICE 'Populating patient dimension...';
    INSERT INTO dim_patient (patient_id, first_name, last_name, full_name, gender, date_of_birth, current_age, age_group, mrn)
    SELECT patient_id,
           first_name,
           last_name,
           first_name || ' ' || last_name,
           gender,
           date_of_birth,
           calculate_age(date_of_birth) as current_age,
           get_age_group(calculate_age(date_of_birth)) as age_group,
           mrn
    FROM dblink('host=localhost dbname=hospital_db user=postgres password=Bukes',
                'SELECT patient_id, first_name, last_name, date_of_birth, gender, mrn FROM patients')
         AS p(patient_id INT, first_name VARCHAR(100), last_name VARCHAR(100), date_of_birth DATE, gender CHAR(1), mrn VARCHAR(20))
    ON CONFLICT (patient_id) DO NOTHING;

    RAISE NOTICE 'Populating provider dimension...';
    INSERT INTO dim_provider (provider_id, first_name, last_name, full_name, credential, specialty_key, department_key, specialty_name, department_name)
    SELECT p.provider_id,
           p.first_name,
           p.last_name,
           p.first_name || ' ' || p.last_name,
           p.credential,
           s.specialty_key,
           d.department_key,
           s.specialty_name,
           d.department_name
    FROM dblink('host=localhost dbname=hospital_db user=postgres password=password',
                'SELECT provider_id, first_name, last_name, credential, specialty_id, department_id FROM providers')
         AS p(provider_id INT, first_name VARCHAR(100), last_name VARCHAR(100), credential VARCHAR(20), specialty_id INT, department_id INT)
    JOIN dim_specialty s ON p.specialty_id = s.specialty_id
    JOIN dim_department d ON p.department_id = d.department_id
    ON CONFLICT (provider_id) DO NOTHING;

    -- ===============================
    -- 6. POPULATE FACT TABLE
    -- ===============================
    RAISE NOTICE 'Populating fact encounters table...';
    INSERT INTO fact_encounters (
        encounter_id, patient_key, provider_key, encounter_type_key,
        encounter_date_key, discharge_date_key, specialty_key, department_key,
        total_claim_amount, total_allowed_amount, diagnosis_count, procedure_count,
        length_of_stay_hours, encounter_datetime, discharge_datetime
    )
    SELECT
        e.encounter_id,
        p.patient_key,
        pr.provider_key,
        et.encounter_type_key,
        generate_date_key(e.encounter_date::DATE) as encounter_date_key,
        generate_date_key(e.discharge_date::DATE) as discharge_date_key,
        pr.specialty_key,
        pr.department_key,
        COALESCE(b.claim_amount, 0) as total_claim_amount,
        COALESCE(b.allowed_amount, 0) as total_allowed_amount,
        COALESCE(diag_counts.diagnosis_count, 0) as diagnosis_count,
        COALESCE(proc_counts.procedure_count, 0) as procedure_count,
        CASE WHEN e.discharge_date IS NOT NULL THEN EXTRACT(EPOCH FROM (e.discharge_date - e.encounter_date))/3600 ELSE 0 END as length_of_stay_hours,
        e.encounter_date as encounter_datetime,
        e.discharge_date as discharge_datetime
    FROM dblink('host=localhost dbname=hospital_db user=postgres password=password',
                'SELECT encounter_id, patient_id, provider_id, encounter_type, encounter_date, discharge_date, department_id FROM encounters')
         AS e(encounter_id INT, patient_id INT, provider_id INT, encounter_type VARCHAR(50), encounter_date TIMESTAMP, discharge_date TIMESTAMP, department_id INT)
    JOIN dim_patient p ON e.patient_id = p.patient_id
    JOIN dim_provider pr ON e.provider_id = pr.provider_id
    JOIN dim_encounter_type et ON e.encounter_type = et.encounter_type
    LEFT JOIN dblink('host=localhost dbname=hospital_db user=postgres password=password',
                     'SELECT encounter_id, claim_amount, allowed_amount FROM billing')
         AS b(encounter_id INT, claim_amount DECIMAL(12,2), allowed_amount DECIMAL(12,2))
         ON e.encounter_id = b.encounter_id
    LEFT JOIN (
        SELECT encounter_id, COUNT(*) AS diagnosis_count
        FROM dblink('host=localhost dbname=hospital_db user=postgres password=password',
                    'SELECT encounter_id, diagnosis_id FROM encounter_diagnoses')
             AS ed(encounter_id INT, diagnosis_id INT)
        GROUP BY encounter_id
    ) diag_counts ON e.encounter_id = diag_counts.encounter_id
    LEFT JOIN (
        SELECT encounter_id, COUNT(*) AS procedure_count
        FROM dblink('host=localhost dbname=hospital_db user=postgres password=password',
                    'SELECT encounter_id, procedure_id FROM encounter_procedures')
             AS ep(encounter_id INT, procedure_id INT)
        GROUP BY encounter_id
    ) proc_counts ON e.encounter_id = proc_counts.encounter_id
    ON CONFLICT (encounter_id) DO NOTHING;

    -- ===============================
    -- 7. POPULATE BRIDGE TABLES
    -- ===============================
    RAISE NOTICE 'Populating bridge tables...';

    -- Encounter-Diagnoses
    INSERT INTO bridge_encounter_diagnoses (encounter_key, diagnosis_key, diagnosis_sequence, diagnosis_present_on_admission)
    SELECT f.encounter_key, d.diagnosis_key, ed.diagnosis_sequence, TRUE
    FROM dblink('host=localhost dbname=hospital_db user=postgres password=password',
                'SELECT encounter_id, diagnosis_id, diagnosis_sequence FROM encounter_diagnoses')
         AS ed(encounter_id INT, diagnosis_id INT, diagnosis_sequence INT)
    JOIN fact_encounters f ON ed.encounter_id = f.encounter_id
    JOIN dim_diagnosis d ON ed.diagnosis_id = d.diagnosis_id;

    -- Encounter-Procedures
    INSERT INTO bridge_encounter_procedures (encounter_key, procedure_key, procedure_date_key, procedure_sequence, procedure_status)
    SELECT f.encounter_key,
           pr.procedure_key,
           generate_date_key(ep.procedure_date) as procedure_date_key,
           ROW_NUMBER() OVER (PARTITION BY ep.encounter_id ORDER BY ep.procedure_date) as procedure_sequence,
           'Completed' as procedure_status
    FROM dblink('host=localhost dbname=hospital_db user=postgres password=password',
                'SELECT encounter_id, procedure_id, procedure_date FROM encounter_procedures')
         AS ep(encounter_id INT, procedure_id INT, procedure_date DATE)
    JOIN fact_encounters f ON ep.encounter_id = f.encounter_id
    JOIN dim_procedure pr ON ep.procedure_id = pr.procedure_id;

    -- ===============================
    -- 8. CALCULATE READMISSION METRICS (STAR SCHEMA OPTIMIZATION)
    -- ===============================
    RAISE NOTICE 'Calculating readmission metrics for performance optimization...';
    
    -- First, add readmission columns to fact table if not exists
    BEGIN
        ALTER TABLE fact_encounters 
        ADD COLUMN IF NOT EXISTS has_30day_readmission BOOLEAN DEFAULT FALSE,
        ADD COLUMN IF NOT EXISTS days_to_readmission INTEGER,
        ADD COLUMN IF NOT EXISTS is_readmission BOOLEAN DEFAULT FALSE,
        ADD COLUMN IF NOT EXISTS readmission_count_30days INTEGER DEFAULT 0;
    EXCEPTION 
        WHEN OTHERS THEN
            RAISE NOTICE 'Readmission columns may already exist, continuing...';
    END;
    
    -- Calculate readmission flags for each discharge
    WITH readmission_analysis AS (
        SELECT 
            f1.encounter_key,
            f1.encounter_id,
            f1.patient_key,
            f1.discharge_datetime,
            -- Check if there's a readmission within 30 days
            EXISTS (
                SELECT 1 FROM fact_encounters f2 
                WHERE f2.patient_key = f1.patient_key 
                AND f2.encounter_datetime > f1.discharge_datetime
                AND f2.encounter_datetime <= f1.discharge_datetime + INTERVAL '30 days'
                AND f2.encounter_id != f1.encounter_id
            ) as has_readmission,
            -- Calculate days to first readmission
            (
                SELECT MIN(EXTRACT(DAY FROM (f2.encounter_datetime - f1.discharge_datetime)))
                FROM fact_encounters f2 
                WHERE f2.patient_key = f1.patient_key 
                AND f2.encounter_datetime > f1.discharge_datetime
                AND f2.encounter_datetime <= f1.discharge_datetime + INTERVAL '30 days'
                AND f2.encounter_id != f1.encounter_id
            ) as days_to_first_readmission,
            -- Count total readmissions within 30 days
            (
                SELECT COUNT(*)
                FROM fact_encounters f2 
                WHERE f2.patient_key = f1.patient_key 
                AND f2.encounter_datetime > f1.discharge_datetime
                AND f2.encounter_datetime <= f1.discharge_datetime + INTERVAL '30 days'
                AND f2.encounter_id != f1.encounter_id
            ) as readmission_count,
            -- Check if this encounter itself is a readmission
            EXISTS (
                SELECT 1 FROM fact_encounters f0
                WHERE f0.patient_key = f1.patient_key
                AND f0.discharge_datetime IS NOT NULL
                AND f1.encounter_datetime > f0.discharge_datetime  
                AND f1.encounter_datetime <= f0.discharge_datetime + INTERVAL '30 days'
                AND f0.encounter_id != f1.encounter_id
            ) as is_this_a_readmission
        FROM fact_encounters f1
        WHERE f1.discharge_datetime IS NOT NULL  -- Only for discharged encounters
    )
    UPDATE fact_encounters 
    SET 
        has_30day_readmission = ra.has_readmission,
        days_to_readmission = ra.days_to_first_readmission,
        is_readmission = ra.is_this_a_readmission,
        readmission_count_30days = ra.readmission_count
    FROM readmission_analysis ra
    WHERE fact_encounters.encounter_key = ra.encounter_key;
    
    -- ===============================
    -- 9. UPDATE PRIMARY DIAGNOSIS
    -- ===============================
    RAISE NOTICE 'Updating primary diagnosis keys...';
    UPDATE fact_encounters
    SET primary_diagnosis_key = (
        SELECT bd.diagnosis_key
        FROM bridge_encounter_diagnoses bd
        WHERE bd.encounter_key = fact_encounters.encounter_key
        AND bd.diagnosis_sequence = 1
        LIMIT 1
    );

    RAISE NOTICE 'ETL Procedure Completed Successfully!';
END;
$$;

-- ===================================================================
-- EXECUTE THE ETL PROCEDURE
-- ===================================================================
CALL run_healthcare_etl();
