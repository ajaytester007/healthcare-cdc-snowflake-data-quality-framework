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

## Repository Structure
- `python/` - validation framework and CDC scripts
- `sql/` - reconciliation and CDC monitoring queries
- `tests/` - unit tests
- `docs/` - architecture notes and interview talking points
- `.github/workflows/` - CI pipeline

## Proof of Execution
This framework was:
- Executed locally in VS Code
- Validated against sample CDC extract data
- Unit tested with pytest
- Prepared for CI/CD execution using GitHub Actions

## Sample Validation Output

```python
{'file': 'cdc_extract_claims.csv',
 'row_count': 4,
 'missing_columns': [],
 'duplicate_events': 1,
 'invalid_operations': 0,
 'late_arrivals_over_240_minutes': 2}

## Technology Stack
- Snowflake
- SQL
- Python
- Pandas
- GitHub Actions
- Healthcare data validation patterns

## Business Alignment
- Healthcare payer and provider datasets
- Regulatory reporting support
- Cloud migration validation
- CDC reliability and data trust controls

## Author
Ponnani Subramanian Ananthanarayanan
Philadelphia, PA
LinkedIn: linkedin.com/in/ponnani-a-1118082b
=======
# Advocate Health - CDC / Snowflake / Data Quality Work Samples

This repository is a GitHub-ready work sample package tailored for a Cloud Data Engineer / Snowflake migration role in a regulated healthcare environment.

## Purpose
Demonstrate practical patterns for:
- CDC-style ingestion validation and watermarking
- source-to-target reconciliation
- Python and SQL data-quality checks
- regulated healthcare controls such as PHI-safe validation, lineage-aware checks, and audit-ready evidence
- Snowflake-ready transformation and monitoring concepts
- CI execution of data quality rules

## Included
- `sql/source_to_target_reconciliation.sql` - row-count, checksum, duplicate, and null-check patterns
- `sql/cdc_watermark_latency_checks.sql` - CDC lag, late-arriving records, and replay detection logic
- `python/dq_rules_framework.py` - lightweight Python data quality framework
- `python/validate_hvr_extracts.py` - sample validator for CDC extract files
- `tests/test_dq_rules.py` - unit tests
- `.github/workflows/python-ci.yml` - GitHub Actions example
- `docs/reference_architecture.md` - reference architecture and controls
- `docs/interview_talking_points.md` - concise walk-through for recruiter / interview discussion

## Positioning
These are representative work samples built to demonstrate design thinking and delivery style. They should be presented as portfolio artifacts, not as production code from a client environment.

## Suggested GitHub repo name
`advocate-health-cdc-snowflake-dq-work-samples`

## Suggested description
Healthcare CDC / Snowflake migration work samples: reconciliation SQL, Python data-quality checks, latency monitoring, and audit-ready controls.