# Allure and Stakeholder Reporting Guide

## Local VS Code execution

From the repo root:

```bash
python -m pip install -r requirements-dev.txt
pytest -q --alluredir=reports/allure-results --junitxml=reports/junit-results.xml
allure generate reports/allure-results --clean -o reports/allure-report
allure open reports/allure-report
```

If the Allure CLI is not installed globally, install it using your standard enterprise-approved method. In many environments this is handled through npm, Homebrew, Scoop, or a CI image.

## Coverage report

```bash
coverage run -m pytest -q --junitxml=reports/junit-results.xml
coverage xml -o reports/coverage.xml
coverage html -d reports/coverage_html
coverage report
```

Open:

```text
reports/coverage_html/index.html
```

## Stakeholder report

Open:

```text
reports/stakeholder-test-summary.html
```

This gives leadership a quick view of pass/fail results, coverage, validated rules, execution time, and generated artifacts.

## Interview explanation

I run tests locally in VS Code or through CI/CD, generate JUnit XML for pipeline integration, coverage reports for engineering visibility, and Allure HTML reports for test execution evidence. For stakeholders, I summarize the same execution data into a simplified HTML or dashboard view showing pass rate, coverage, failed scenarios, execution duration, and data quality checks covered.
