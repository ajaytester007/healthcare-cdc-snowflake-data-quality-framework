import pandas as pd
from python.dq_rules_framework import not_null_rule, unique_rule, accepted_values_rule, freshness_rule

def test_not_null_rule_fails_when_null_present():
    df = pd.DataFrame({"member_id": [1, None], "claim_id": ["A", "B"]})
    result = not_null_rule(df, ["member_id", "claim_id"])
    assert result.passed is False
    assert result.failure_count == 1

def test_unique_rule_detects_duplicates():
    df = pd.DataFrame({"claim_id": ["A", "A", "B"]})
    result = unique_rule(df, ["claim_id"])
    assert result.passed is False
    assert result.failure_count == 1

def test_accepted_values_rule():
    df = pd.DataFrame({"status_code": ["PAID", "DENIED", "BAD"]})
    result = accepted_values_rule(df, "status_code", ["PAID", "DENIED"])
    assert result.passed is False
    assert result.failure_count == 1

def test_freshness_rule_passes_with_recent_data():
    df = pd.DataFrame({"load_ts": pd.to_datetime(["2026-04-22 09:40:00"])})
    result = freshness_rule(df, "load_ts", 30, pd.Timestamp("2026-04-22 10:00:00"))
    assert result.passed is True
