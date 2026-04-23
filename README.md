# Healthcare CDC Data Quality & Snowflake Migration Framework

## Overview
This repository demonstrates a production-style approach to validating CDC-based ingestion pipelines into Snowflake, with a focus on healthcare data systems (claims, eligibility, HL7/FHIR datasets).

## Key Capabilities
- CDC ingestion validation (log-based replication patterns)
- Source-to-target reconciliation (row count, hash diff, aggregation checks)
- Pipeline latency and watermark validation
- Python-based data quality checks (Great Expectations style)
- SQL-based transformation validation
- CI/CD-integrated data validation workflows

## Architecture
Source Systems → CDC Capture → Staging → Snowflake (Bronze/Silver/Gold) → Reporting

## Tech Stack
- Snowflake
- Python (Pandas)
- SQL
- CI/CD (GitHub Actions)
- Data Quality Frameworks

## Use Case Alignment
- Healthcare payer/provider datasets
- Regulatory reporting (CMS, HEDIS, SDOH)
- Large-scale data migration validation

## Author
Ponnani Subramanian Ananthanarayanan
