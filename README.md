![Python CI](https://github.com/ajaytester007/healthcare-cdc-snowflake-data-quality-framework/actions/workflows/python-ci.yml/badge.svg)

![Data Quality Test Reports](https://github.com/ajaytester007/healthcare-cdc-snowflake-data-quality-framework/actions/workflows/test-reports.yml/badge.svg)

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

## Quality Engineering & Reporting Showcase

This repository demonstrates:

- Healthcare CDC validation
- Source-to-target reconciliation
- SQL-based data validation
- Python automation
- Automated unit testing with pytest
- JUnit XML reporting
- HTML coverage reporting
- GitHub Actions CI/CD execution
- Downloadable test artifacts
- Stakeholder-ready quality dashboards

### Test Execution Evidence

| Metric | Result |
|----------|----------|
| Automated Tests | 4 Passed |
| Coverage | 60% |
| JUnit XML | Generated |
| HTML Coverage Report | Generated |
| GitHub Actions Workflow | Passing |
| Artifact Publication | Enabled |

### Generated Reports

- reports/junit-results.xml
- reports/coverage_html/index.html
- reports/stakeholder-test-summary.html

### GitHub Actions Validation

Latest successful workflow execution:

- Python CI Workflow
- Data Quality Test Reports Workflow
- Automated artifact publication

Artifacts can be downloaded directly from the GitHub Actions run page.

### Latest Validation Evidence

- Repository: https://github.com/ajaytester007/healthcare-cdc-snowflake-data-quality-framework
- Merged PR: https://github.com/ajaytester007/healthcare-cdc-snowflake-data-quality-framework/pull/1
- Latest Successful GitHub Actions Run: https://github.com/ajaytester007/healthcare-cdc-snowflake-data-quality-framework/actions/runs/27167725289
- Artifact: `healthcare-cdc-dq-test-reports`

### Reporting Outputs

The framework generates the following execution evidence:

| Report | Location | Purpose |
|---|---|---|
| Stakeholder Summary | `reports/stakeholder-test-summary.html` | QA leadership / business-facing execution summary |
| JUnit XML | `reports/junit-results.xml` | CI/CD test evidence and pipeline traceability |
| Coverage HTML | `reports/coverage_html/index.html` | Engineer-facing coverage drilldown |
| Coverage XML | `reports/coverage.xml` | Machine-readable coverage output |
| Coverage Summary | `reports/coverage-summary.txt` | Console-style coverage evidence |

### Local Execution

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1

pip install -r requirements-dev.txt
pip install pytest-cov

pytest tests -v --junitxml=reports/junit-results.xml --cov=python --cov-report=term --cov-report=html:reports/coverage_html

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
 'late_arrivals_over_240_minutes': 2
}

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


## Test Reporting / Evidence Pack

This repo includes generated QA evidence under `reports/`:

- `reports/stakeholder-test-summary.html` - stakeholder-ready pass/fail and coverage summary
- `reports/junit-results.xml` - CI/CD-compatible test execution result
- `reports/coverage_html/index.html` - detailed Python coverage report
- `reports/coverage-summary.txt` - terminal coverage summary

Run locally from VS Code:

```bash
python -m pip install -r requirements-dev.txt
coverage run -m pytest -q --junitxml=reports/junit-results.xml
coverage xml -o reports/coverage.xml
coverage html -d reports/coverage_html
coverage report
```

For Allure setup and CI/CD reporting, see `docs/allure_and_reporting_guide.md`.


## Interview Talking Points

This framework demonstrates practical experience with:

- SQL-based data validation
- Source-to-target reconciliation
- CDC validation
- Batch testing
- Python automation
- PyTest
- Coverage reporting
- GitHub Actions CI/CD
- Healthcare payer data quality controls
- Release readiness reporting