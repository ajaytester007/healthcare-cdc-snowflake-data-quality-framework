# Test Results Reports

Generated for `healthcare-cdc-snowflake-data-quality-framework`.

## Current run summary

- Total tests: 4
- Passed: 4
- Failed/errors: 0
- Skipped: 0
- Execution time: 0.61s
- Coverage: 91%

## Open reports

- Stakeholder summary: `reports/stakeholder-test-summary.html`
- Coverage report: `reports/coverage_html/index.html`
- JUnit XML: `reports/junit-results.xml`
- Console evidence: `reports/pytest-console-output.txt`

## VS Code commands

```bash
python -m pip install -r requirements-dev.txt
coverage run -m pytest -q --junitxml=reports/junit-results.xml
coverage xml -o reports/coverage.xml
coverage html -d reports/coverage_html
coverage report
```
