# Healthcare CDC Data Quality & Snowflake Migration Framework

## Overview
This repository demonstrates a production-style approach to validating CDC-based ingestion pipelines into Snowflake, with a focus on healthcare data systems such as claims, eligibility, and HL7/FHIR datasets.

## What This Showcases
- CDC ingestion validation
- Source-to-target reconciliation
- Pipeline latency and watermark checks
- Python-based data quality controls
- SQL-based transformation validation
- CI/CD-integrated validation workflows

## Representative Business Context
This portfolio is intended to demonstrate my approach to healthcare cloud data engineering, data quality, and migration validation in regulated environments.

## Repository Structure
- `docs/` - architecture notes, assumptions, and reference material
- `sql/` - reconciliation, row-count, and transformation validation SQL
- `python/` - data quality checks and CDC validation scripts
- `tests/` - sample validation tests
- `.github/workflows/` - CI workflow examples

## Key Capabilities
### CDC Validation
- Log-based or CDC-style ingestion validation
- Watermark and latency checks
- Batch vs near-real-time comparison patterns

### Reconciliation
- Row-count comparisons
- Hash-based validation
- Aggregate and business-rule verification

### Data Quality
- Null checks
- Duplicate checks
- Referential integrity checks
- Threshold and anomaly checks

### Cloud Data Engineering Alignment
- Snowflake-oriented validation design
- Bronze / Silver / Gold style validation checkpoints
- Healthcare data controls for auditability and trust

## Technology
- Snowflake
- SQL
- Python
- Pandas
- GitHub Actions
- Healthcare data validation patterns

## Author
Ponnani Subramanian Ananthanarayanan
Philadelphia, PA
LinkedIn: linkedin.com/in/ponnani-a-1118082b
