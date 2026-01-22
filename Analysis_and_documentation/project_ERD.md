# Healthcare Analytics Project - Entity Relationship Diagrams (ERD)

This document contains appropriate entity relationship diagrams for the three main schemas in the project.

## 1. Star Schema (Analytics Data Warehouse)

This schema follows a dimensional modeling approach with a central fact table and surrounding dimensions. Note the Bridge tables handling many-to-many relationships.

```mermaid
erDiagram
    %% Fact Table
    FACT_ENCOUNTERS {
        int encounter_key PK
        int encounter_id
        int patient_key FK
        int provider_key FK
        int encounter_date_key FK
        int discharge_date_key FK
        int specialty_key FK
        int department_key FK
        decimal total_claim_amount
        decimal total_allowed_amount
        int length_of_stay_hours
        boolean has_30day_readmission
    }

    %% Dimension Tables
    DIM_PATIENT {
        int patient_key PK
        int patient_id
        string full_name
        date date_of_birth
        string gender
        string age_group
        string mrn
    }

    DIM_PROVIDER {
        int provider_key PK
        int provider_id
        string full_name
        string credential
        string specialty_name
        string department_name
    }

    DIM_DATE {
        int date_key PK
        date calendar_date
        int year
        int quarter
        int month
        string day_of_week
    }

    DIM_SPECIALTY {
        int specialty_key PK
        string specialty_name
        string specialty_category
    }

    DIM_DEPARTMENT {
        int department_key PK
        string department_name
        string department_type
    }

    DIM_DIAGNOSIS {
        int diagnosis_key PK
        string icd10_code
        string icd10_description
        string diagnosis_category
        boolean chronic_flag
    }

    DIM_PROCEDURE {
        int procedure_key PK
        string cpt_code
        string cpt_description
        string procedure_category
        string typical_cost_range
    }

    %% Bridge Tables
    BRIDGE_ENCOUNTER_DIAGNOSES {
        int bridge_id PK
        int encounter_key FK
        int diagnosis_key FK
        int diagnosis_sequence
    }

    BRIDGE_ENCOUNTER_PROCEDURES {
        int bridge_id PK
        int encounter_key FK
        int procedure_key FK
        date procedure_date
    }

    %% Relationships
    FACT_ENCOUNTERS }|..|| DIM_PATIENT : "connects to"
    FACT_ENCOUNTERS }|..|| DIM_PROVIDER : "connects to"
    FACT_ENCOUNTERS }|..|| DIM_DATE : "happens on"
    FACT_ENCOUNTERS }|..|| DIM_SPECIALTY : "categorized by"
    FACT_ENCOUNTERS }|..|| DIM_DEPARTMENT : "occurs in"

    BRIDGE_ENCOUNTER_DIAGNOSES }|..|| FACT_ENCOUNTERS : "links"
    BRIDGE_ENCOUNTER_DIAGNOSES }|..|| DIM_DIAGNOSIS : "links"

    BRIDGE_ENCOUNTER_PROCEDURES }|..|| FACT_ENCOUNTERS : "links"
    BRIDGE_ENCOUNTER_PROCEDURES }|..|| DIM_PROCEDURE : "links"
```

<br>

## 2. OLTP Schema (Source System)

This schema represents the highly normalized transactional database (Hospital DB).

```mermaid
erDiagram
    PATIENTS {
        int patient_id PK
        string first_name
        string last_name
        date date_of_birth
        string mrn
    }
    
    PROVIDERS {
        int provider_id PK
        string first_name
        string last_name
        int specialty_id FK
        int department_id FK
    }

    ENCOUNTERS {
        int encounter_id PK
        int patient_id FK
        int provider_id FK
        timestamp encounter_date
        timestamp discharge_date
        int department_id FK
    }

    DEPARTMENTS {
        int department_id PK
        string department_name
        int capacity
    }

    SPECIALTIES {
        int specialty_id PK
        string specialty_name
    }

    DIAGNOSES {
        int diagnosis_id PK
        string icd10_code
    }

    ENCOUNTER_DIAGNOSES {
        int encounter_diagnosis_id PK
        int encounter_id FK
        int diagnosis_id FK
    }
    
    BILLING {
        int billing_id PK
        int encounter_id FK
        decimal claim_amount
    }

    %% Relationships
    ENCOUNTERS }|..|| PATIENTS : "has"
    ENCOUNTERS }|..|| PROVIDERS : "seen by"
    ENCOUNTERS }|..|| DEPARTMENTS : "located at"
    PROVIDERS }|..|| SPECIALTIES : "specializes in"
    PROVIDERS }|..|| DEPARTMENTS : "belongs to"
    ENCOUNTER_DIAGNOSES }|..|| ENCOUNTERS : "details"
    ENCOUNTER_DIAGNOSES }|..|| DIAGNOSES : "details"
    BILLING |o..|| ENCOUNTERS : "bills for"
```

<br>

## 3. Audit Framework (Compliance & Logging)

This schema manages HIPAA compliance logging and ETL operational monitoring.

```mermaid
erDiagram
    %% Audit Core
    AUDIT_LOG {
        uuid audit_id PK
        timestamp event_timestamp
        string event_type_code FK
        string user_id
        string table_name
        string operation_type
        jsonb old_values
        jsonb new_values
    }

    AUDIT_EVENT_TYPES {
        string event_type_code PK
        string severity_level
        string hipaa_category
        int retention_days
    }

    PHI_ACCESS_LOG {
        uuid phi_access_id PK
        uuid audit_id FK
        int patient_id
        string accessed_phi_elements
        string access_purpose
    }

    USER_ROLES {
        int role_id PK
        string role_name
        boolean can_access_phi
    }

    %% Relationships
    AUDIT_LOG }|..|| AUDIT_EVENT_TYPES : "classifies"
    PHI_ACCESS_LOG |o..|| AUDIT_LOG : "extends details for PHI"
```
