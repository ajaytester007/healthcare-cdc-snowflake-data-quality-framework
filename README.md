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
