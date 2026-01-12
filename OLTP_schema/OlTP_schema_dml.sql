-- Insert base reference data
INSERT INTO specialties VALUES (1, 'Cardiology', 'CARD'), (2, 'Internal Medicine', 'IM'), (3, 'Emergency', 'ER'), (4, 'Orthopedics', 'ORTHO'), (5, 'Neurology', 'NEURO') ON CONFLICT DO NOTHING;

INSERT INTO departments VALUES (1, 'Cardiology Unit', 3, 20), (2, 'Internal Medicine', 2, 30), (3, 'Emergency', 1, 45), (4, 'Orthopedics', 4, 25), (5, 'Neurology', 5, 15) ON CONFLICT DO NOTHING;

INSERT INTO providers VALUES 
(101, 'James', 'Chen', 'MD', 1, 1), 
(102, 'Sarah', 'Williams', 'MD', 2, 2), 
(103, 'Michael', 'Rodriguez', 'MD', 3, 3),
(104, 'Lisa', 'Anderson', 'MD', 4, 4),
(105, 'David', 'Brown', 'MD', 5, 5) ON CONFLICT DO NOTHING;

INSERT INTO diagnoses VALUES 
(3001, 'I10', 'Hypertension'), 
(3002, 'E11.9', 'Type 2 Diabetes'), 
(3003, 'I50.9', 'Heart Failure'),
(3004, 'M79.9', 'Soft tissue disorder'),
(3005, 'G93.9', 'Disorder of brain') ON CONFLICT DO NOTHING;

INSERT INTO procedures VALUES 
(4001, '99213', 'Office Visit'), 
(4002, '93000', 'EKG'), 
(4003, '71020', 'Chest X-ray'),
(4004, '73060', 'Knee X-ray'),
(4005, '70450', 'CT Head') ON CONFLICT DO NOTHING;

-- Function to insert 10,000 patients
CREATE OR REPLACE FUNCTION sp_InsertPatients() RETURNS void AS $$
DECLARE
    counter INTEGER := 1;
    first_names TEXT[] := ARRAY['John', 'Jane', 'Michael', 'Sarah', 'David', 'Lisa', 'James', 'Mary', 'Robert', 'Jennifer', 'William', 'Patricia', 'Charles', 'Linda', 'Joseph', 'Barbara', 'Thomas', 'Elizabeth', 'Christopher', 'Susan'];
    last_names TEXT[] := ARRAY['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin'];
    selected_first_name TEXT;
    selected_last_name TEXT;
    selected_gender CHAR(1);
    selected_dob DATE;
BEGIN
    WHILE counter <= 10000 LOOP
        -- Select random names
        selected_first_name := first_names[1 + (random() * array_length(first_names, 1))::INTEGER];
        selected_last_name := last_names[1 + (random() * array_length(last_names, 1))::INTEGER];
        
        -- Generate random gender
        selected_gender := CASE WHEN random() < 0.5 THEN 'M' ELSE 'F' END;
        
        -- Generate random date of birth (age 18-98)
        selected_dob := CURRENT_DATE - INTERVAL '18 years' - (random() * INTERVAL '80 years');
        
        INSERT INTO patients (patient_id, first_name, last_name, date_of_birth, gender, mrn)
        VALUES (
            counter,
            selected_first_name,
            selected_last_name,
            selected_dob,
            selected_gender,
            'MRN' || LPAD(counter::TEXT, 6, '0')
        );
        
        counter := counter + 1;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Function to insert 600,000 encounters
CREATE OR REPLACE FUNCTION sp_InsertEncounters() RETURNS void AS $$
DECLARE
    counter INTEGER := 1;
    encounter_types TEXT[] := ARRAY['Outpatient', 'Inpatient', 'ER', 'Emergency', 'Consultation'];
    selected_type TEXT;
    selected_start_time TIMESTAMP;
    selected_end_time TIMESTAMP;
BEGIN
    WHILE counter <= 600000 LOOP
        -- Select random encounter type
        selected_type := encounter_types[1 + (random() * array_length(encounter_types, 1))::INTEGER];
        
        -- Generate random start time (within last year)
        selected_start_time := CURRENT_TIMESTAMP - (random() * INTERVAL '365 days');
        
        -- Generate random end time (1-48 hours after start)
        selected_end_time := selected_start_time + (1 + random() * 47) * INTERVAL '1 hour';
        
        INSERT INTO encounters (encounter_id, patient_id, provider_id, encounter_type, encounter_date, discharge_date, department_id)
        VALUES (
            counter,
            1 + (random() * 9999)::INTEGER, -- Random patient_id 1-10000
            101 + (random() * 4)::INTEGER,  -- Random provider_id 101-105
            selected_type,
            selected_start_time,
            selected_end_time,
            1 + (random() * 4)::INTEGER     -- Random department_id 1-5
        );
        
        counter := counter + 1;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Function to insert encounter diagnoses
CREATE OR REPLACE FUNCTION sp_InsertEncounterDiagnoses() RETURNS void AS $$
DECLARE
    encounter_rec RECORD;
    diagnosis_counter INTEGER := 1;
    num_diagnoses INTEGER;
    diag_counter INTEGER;
BEGIN
    FOR encounter_rec IN SELECT encounter_id FROM encounters LOOP
        -- Insert 1-3 diagnoses per encounter
        num_diagnoses := 1 + (random() * 2)::INTEGER;
        diag_counter := 1;
        
        WHILE diag_counter <= num_diagnoses LOOP
            INSERT INTO encounter_diagnoses (encounter_diagnosis_id, encounter_id, diagnosis_id, diagnosis_sequence)
            VALUES (
                diagnosis_counter,
                encounter_rec.encounter_id,
                3001 + (random() * 4)::INTEGER, -- Random diagnosis_id 3001-3005
                diag_counter
            );
            
            diagnosis_counter := diagnosis_counter + 1;
            diag_counter := diag_counter + 1;
        END LOOP;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Function to insert encounter procedures
CREATE OR REPLACE FUNCTION sp_InsertEncounterProcedures() RETURNS void AS $$
DECLARE
    encounter_rec RECORD;
    procedure_counter INTEGER := 1;
    num_procedures INTEGER;
    proc_counter INTEGER;
BEGIN
    FOR encounter_rec IN SELECT encounter_id, encounter_date FROM encounters LOOP
        -- Insert 1-2 procedures per encounter (70% chance)
        IF random() < 0.7 THEN
            num_procedures := 1 + (random())::INTEGER;
            proc_counter := 1;
            
            WHILE proc_counter <= num_procedures LOOP
                INSERT INTO encounter_procedures (encounter_procedure_id, encounter_id, procedure_id, procedure_date)
                VALUES (
                    procedure_counter,
                    encounter_rec.encounter_id,
                    4001 + (random() * 4)::INTEGER, -- Random procedure_id 4001-4005
                    encounter_rec.encounter_date::DATE
                );
                
                procedure_counter := procedure_counter + 1;
                proc_counter := proc_counter + 1;
            END LOOP;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Function to insert billing records
CREATE OR REPLACE FUNCTION sp_InsertBilling() RETURNS void AS $$
DECLARE
    encounter_rec RECORD;
    billing_counter INTEGER := 1;
    total_amount DECIMAL(10,2);
    paid_amount DECIMAL(10,2);
    billing_status TEXT;
BEGIN
    FOR encounter_rec IN SELECT encounter_id, encounter_date FROM encounters LOOP
        -- Insert billing for 80% of encounters
        IF random() < 0.8 THEN
            total_amount := 100 + (random() * 4900)::DECIMAL(10,2);
            paid_amount := total_amount * (0.7 + random() * 0.3);
            
            billing_status := CASE 
                WHEN paid_amount >= total_amount THEN 'Paid' 
                WHEN paid_amount > 0 THEN 'Partial' 
                ELSE 'Pending' 
            END;
            
            INSERT INTO billing (billing_id, encounter_id, claim_amount, allowed_amount, claim_date, claim_status)
            VALUES (
                billing_counter,
                encounter_rec.encounter_id,
                total_amount,
                paid_amount,
                encounter_rec.encounter_date::DATE + (1 + random() * 29)::INTEGER,
                billing_status
            );
            
            billing_counter := billing_counter + 1;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Execute all functions to populate data (only if tables are empty)
DO $$ 
BEGIN
    -- Check if patients table is empty before populating
    IF (SELECT COUNT(*) FROM patients) = 0 THEN
        PERFORM sp_InsertPatients();
        RAISE NOTICE 'Inserted patients data';
    ELSE
        RAISE NOTICE 'Patients table already has data, skipping population';
    END IF;
    
    -- Check if encounters table is empty before populating  
    IF (SELECT COUNT(*) FROM encounters) = 0 THEN
        PERFORM sp_InsertEncounters();
        RAISE NOTICE 'Inserted encounters data';
    ELSE
        RAISE NOTICE 'Encounters table already has data, skipping population';
    END IF;
    
    -- Check if encounter_diagnoses table is empty before populating
    IF (SELECT COUNT(*) FROM encounter_diagnoses) = 0 THEN
        PERFORM sp_InsertEncounterDiagnoses();
        RAISE NOTICE 'Inserted encounter diagnoses data';
    ELSE
        RAISE NOTICE 'Encounter diagnoses table already has data, skipping population';
    END IF;
    
    -- Check if encounter_procedures table is empty before populating
    IF (SELECT COUNT(*) FROM encounter_procedures) = 0 THEN
        PERFORM sp_InsertEncounterProcedures();
        RAISE NOTICE 'Inserted encounter procedures data';
    ELSE
        RAISE NOTICE 'Encounter procedures table already has data, skipping population';
    END IF;
    
    -- Check if billing table is empty before populating
    IF (SELECT COUNT(*) FROM billing) = 0 THEN
        PERFORM sp_InsertBilling();
        RAISE NOTICE 'Inserted billing data';
    ELSE
        RAISE NOTICE 'Billing table already has data, skipping population';
    END IF;
END $$;

-- Clean up functions
DROP FUNCTION sp_InsertPatients();
DROP FUNCTION sp_InsertEncounters();
DROP FUNCTION sp_InsertEncounterDiagnoses();
DROP FUNCTION sp_InsertEncounterProcedures();
DROP FUNCTION sp_InsertBilling();