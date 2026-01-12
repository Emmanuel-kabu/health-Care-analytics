# Healthcare Analytics Lab Project

## Project Overview

This project demonstrates the performance improvements achieved by transforming a normalized OLTP healthcare database into an optimized star schema data warehouse. The project includes comprehensive performance analysis comparing OLTP and star schema approaches across four critical business intelligence queries.

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
├── README.md (this file)
├── requirements.txt (Python dependencies)
├── OLTP_schema/
│   ├── OLTP_schema_ddl.sql (Create normalized tables)
│   ├── OLTP_schema_dml.sql (Load sample data)
│   ├── OLTP_schema_queries.sql (Business intelligence queries)
│   └── OLTP_performance_queries.sql (Performance measurement)
├── star_schema/
│   ├── star_schema.sql (Create dimensional model)
│   ├── star_schema_dml.sql (ETL process with optimizations)
│   └── star_schema_performance_queries.sql (Performance analysis)
└── Analysis_and_documentation/
    ├── design_decisions.txt (Schema design rationale)
    ├── query_analysis.txt (Performance bottleneck analysis)
    ├── etl_design.txt (ETL process documentation)
    ├── reflection.md (Project analysis and learnings)
    ├── star_schema.txt (Dimensional model specification)
    └── star_schema_queries.txt (Optimized query examples)
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