# Healthcare Analytics Lab

Concise, modular demo of: OLTP source schema → ETL → star schema (DW), with
runtime ETL logging, schema validation, file exports, and HIPAA-aware audit
trails.

Why this repo
- Compare normalized OLTP vs. star schema performance.  
- Provide production-ready helpers: ETL instrumentation, schema validation,
  export reports, and audit logging.

Recent additions (implemented)
- `audit_and_logging/etl_logging_framework.sql` — ETL runtime logging schema and helper functions (`etl_logs`).
- `audit_and_logging/file_output_logging.sql` — export procedures to produce CSV/JSON/HTML reports from `etl_logs`.
- `audit_and_logging/healthcare_audit_framework.sql` — HIPAA-aware audit schema; updated to create DML triggers for OLTP tables.
- `main/run_etl_and_checks.sql` — orchestration wrapper: runs ETL placeholder, validation, exports, and audit archival.

Quick start (recommended order)
1. `\i audit_and_logging/etl_logging_framework.sql` — create `etl_logs` objects
2. `\i OLTP_schema/OLTP_schema_ddl.sql` (then `OLTP_schema_dml.sql` to load samples)
3. `\i audit_and_logging/healthcare_audit_framework.sql` — install audit triggers
4. `\i star_schema/star_schema.sql` and `\i star_schema/star_schema_dml.sql`
5. `\i audit_and_logging/file_output_logging.sql` — exports and reports
6. `\i validation/schema_validation.sql` then `CALL validate_healthcare_schemas();`
7. `psql -f main/run_etl_and_checks.sql` — example orchestrator

Files & modules
- `main/` — orchestrator and helpers ([run_etl_and_checks.sql](main/run_etl_and_checks.sql))
- `audit_and_logging/` — `etl_logging_framework.sql`, `file_output_logging.sql`, `healthcare_audit_framework.sql`
- `OLTP_schema/` — source DDL/DML and queries
- `star_schema/` — star schema DDL/DML and performance queries
- `validation/` — schema validation routines
- `Analysis_and_documentation/` — design notes and analysis

Notes
- On Windows update export paths from `/tmp/etl_exports` to `C:\\temp\\etl_exports`.  
- Audit triggers are created for OLTP tables; to audit DDL, add `log_audit_event('SCHEMA_CHANGE',...)` in migrations.  
- Instrument your ETL procedures with `etl_logs.log_etl_step_start()` and `etl_logs.log_etl_step_complete()` for traceability and exports.

Useful commands
```powershell
psql -h <host> -U <user> -d <db> -f audit_and_logging/etl_logging_framework.sql
psql -h <host> -U <user> -d <db> -f OLTP_schema/OLTP_schema_ddl.sql
psql -h <host> -U <user> -d <db> -f audit_and_logging/healthcare_audit_framework.sql
psql -h <host> -U <user> -d <db> -f main/run_etl_and_checks.sql
```

Contact & docs
- See `Analysis_and_documentation/` for design rationale, ETL notes, and query analysis.

Project completion: January 2026

## Business Questions Analyzed

1. **Monthly Encounters by Specialty** - Track patient volume trends
2. **Top Diagnosis-Procedure Pairs** - Identify common treatment patterns  
3. **30-Day Readmission Rates** - Monitor healthcare quality metrics
4. **Revenue Analysis by Specialty** - Financial performance tracking

## Key Achievements

### Performance Improvements Achieved
- **Query 1**: 4.8x faster (1815ms → 379ms)
- **Query 2**: 6,509x faster (1627ms → 0.25ms) 
- **Query 3**: 1.9x faster (503ms → 267ms)
- **Query 4**: 5.9x faster (1343ms → 229ms)
- **Overall**: 6x total improvement (5.3 seconds → 0.88 seconds)

### Performance Rating Improvements
- **OLTP Schema**: 3 queries "Needs Optimization", 1 query "Fair"
- **Star Schema**: 1 query "Excellent", 3 queries "Good"

## Technical Architecture

### OLTP Schema (Normalized)
- 10 tables with normalized relationships
- Complex join chains for analytical queries
- Optimized for transactional operations
- Average query performance: 1.3 seconds

### Star Schema (Denormalized)
- 1 fact table with 8 dimension tables
- 2 bridge tables for many-to-many relationships
- Pre-computed measures and materialized views
- Average query performance: 0.22 seconds

## Database Systems Used

- **Primary Database**: PostgreSQL 18.1
- **Source Database**: hospital_db (OLTP schema)
- **Target Database**: hospital_star_db (Star schema)
- **ETL Method**: dblink cross-database queries

## System Requirements

### Software Requirements
- PostgreSQL 18.1 or later
- dblink extension enabled
- psql command line client
- Windows PowerShell or equivalent terminal

### Hardware Requirements
- Minimum 4GB RAM
- 2GB available disk space
- Multi-core processor recommended

## Database Setup

### 1. Create Source Database (OLTP)
```sql
psql -U postgres
CREATE DATABASE hospital_db;
\c hospital_db
\i OLTP_schema/OLTP_schema_ddl.sql
\i OLTP_schema/OLTP_schema_dml.sql
```

### 2. Create Target Database (Star Schema)
```sql
CREATE DATABASE hospital_star_db;
\c hospital_star_db
\i star_schema/star_schema.sql
\i star_schema/star_schema_dml.sql
```

### 3. Run Performance Analysis
```sql
\i OLTP_schema/OLTP_performance_queries.sql
\i star_schema/star_schema_performance_queries.sql
```

## Project File Structure

```
health_care_Analytics/
├── README.md
├── requirements.txt
├── main/
│   └── run_etl_and_checks.sql
├── audit_and_logging/
│   ├── etl_logging_framework.sql
│   ├── file_output_logging.sql
│   └── healthcare_audit_framework.sql
├── OLTP_schema/
│   ├── OLTP_schema_ddl.sql
+│   ├── OLTP_schema_dml.sql
│   ├── OLTP_schema_queries.sql
│   └── OLTP_performance_queries.sql
├── star_schema/
│   ├── star_schema.sql
│   ├── star_schema_dml.sql
│   ├── star_schema_queries.sql
│   └── star_schema_performance_queries.sql
├── validation/
│   └── schema_validation.sql
├── Analysis_and_documentation/
│   ├── design_decisions.txt
│   ├── etl_design.txt
│   ├── query_analysis.txt
│   ├── reflection.md
│   ├── star_schema.txt
│   └── star_schema_queries.txt
```

## Execution Instructions

### Complete Project Setup (45 minutes)

#### Step 1: Setup OLTP Database (15 minutes)
```sql
\c hospital_db
\i OLTP_schema/OLTP_schema_ddl.sql
\i OLTP_schema/OLTP_schema_dml.sql
```

#### Step 2: Setup Star Schema Database (20 minutes)
```sql
\c hospital_star_db
\i star_schema/star_schema.sql
\i star_schema/star_schema_dml.sql
```

#### Step 3: Run Performance Analysis (10 minutes)
```sql
\c hospital_db
\i OLTP_schema/OLTP_performance_queries.sql
\c hospital_star_db  
\i star_schema/star_schema_performance_queries.sql
```

#### Step 4: Review Results
Compare performance rankings from both analyses

## Performance Optimization Features

### Star Schema Optimizations Implemented

#### 1. Pre-computed Readmission Metrics
- has_30day_readmission Boolean flags
- days_to_readmission calculated values
- Eliminated expensive self-joins on fact table

#### 2. Materialized Views
- mv_diagnosis_procedure_pairs for Q2 optimization
- Reduced 4-table joins to simple lookup
- Achieved 6,509x performance improvement

#### 3. Specialized Indexes
- Covering indexes for common query patterns
- Bridge table optimizations
- Date-based access patterns

#### 4. ETL-time Calculations
- Complex temporal analysis moved to data load
- Pre-aggregated revenue measures
- Denormalized dimension attributes

## Business Intelligence Capabilities

### Analytical Query Types Supported
- Time series analysis (monthly trends)
- Healthcare quality metrics (readmission rates)
- Financial performance analysis (revenue by specialty)
- Clinical pattern analysis (diagnosis-procedure pairs)
- Patient demographic analysis (age groups, encounter types)

### Dashboard-Ready Performance
- All queries execute under 500ms
- Suitable for real-time business intelligence
- Interactive analysis capabilities enabled
- Concurrent user support improved

## Data Model Highlights

- **Fact Table Grain**: One row per patient encounter
- **Dimension Count**: 8 core dimensions
- **Bridge Tables**: 2 for many-to-many relationships
- **Pre-computed Measures**: 6 key business metrics
- **Historical Tracking**: Full patient journey analysis

### Key Design Decisions
- Encounter-level grain balances detail with performance
- Bridge tables handle complex medical relationships
- Pre-aggregation eliminates runtime calculations
- Dimensional hierarchy supports drill-down analysis

## Lessons Learned

### Star Schema Benefits Demonstrated
1. Materialized views provide extraordinary performance gains
2. Pre-computed measures eliminate expensive calculations
3. Proper denormalization reduces join complexity
4. ETL-time optimizations enable real-time analysis

### Performance Engineering Insights
1. Bridge table joins remain expensive without optimization
2. Self-joins on large tables require pre-computation strategies
3. Date dimension eliminates computed grouping overhead
4. Covering indexes critical for analytical query patterns

## Project Validation

### Success Criteria Met
- ✅ All analytical queries perform at Good or Excellent levels
- ✅ 6x overall performance improvement achieved
- ✅ Real-time business intelligence capabilities enabled
- ✅ Complex healthcare analytics patterns optimized
- ✅ Scalable dimensional model implemented

### Business Impact
- Interactive dashboards now feasible
- Real-time healthcare quality monitoring enabled
- Complex analytical queries accessible to business users
- Foundation for advanced analytics and machine learning

## Technical Contact

- **Database Platform**: PostgreSQL 18.1
- **ETL Framework**: dblink with stored procedures
- **Performance Tools**: psql execution timing
- **Analysis Tools**: SQL-based performance measurement

For technical questions or implementation support, refer to the comprehensive documentation in the Analysis_and_documentation folder.

---

**Project Completion**: January 2026