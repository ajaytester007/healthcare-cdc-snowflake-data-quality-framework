"""
dq_rules_framework.py
Lightweight data-quality rule engine for portfolio demonstration.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, Iterable, List, Dict, Any
import pandas as pd


@dataclass
class RuleResult:
    rule_name: str
    passed: bool
    details: str
    failure_count: int


@dataclass
class DQRule:
    name: str
    func: Callable[[pd.DataFrame], RuleResult]


def not_null_rule(df: pd.DataFrame, columns: Iterable[str]) -> RuleResult:
    null_rows = df[list(columns)].isnull().any(axis=1).sum()
    return RuleResult(
        rule_name=f"not_null_{'_'.join(columns)}",
        passed=null_rows == 0,
        details=f"Rows with nulls in {list(columns)}: {null_rows}",
        failure_count=int(null_rows),
    )


def unique_rule(df: pd.DataFrame, columns: Iterable[str]) -> RuleResult:
    dupes = df.duplicated(subset=list(columns)).sum()
    return RuleResult(
        rule_name=f"unique_{'_'.join(columns)}",
        passed=dupes == 0,
        details=f"Duplicate rows on {list(columns)}: {dupes}",
        failure_count=int(dupes),
    )


def accepted_values_rule(df: pd.DataFrame, column: str, accepted_values: Iterable[Any]) -> RuleResult:
    accepted = set(accepted_values)
    invalid = (~df[column].isin(accepted)).sum()
    return RuleResult(
        rule_name=f"accepted_values_{column}",
        passed=invalid == 0,
        details=f"Invalid values in {column}: {invalid}",
        failure_count=int(invalid),
    )


def freshness_rule(df: pd.DataFrame, timestamp_col: str, max_age_minutes: int, now: pd.Timestamp) -> RuleResult:
    age_minutes = ((now - pd.to_datetime(df[timestamp_col]).max()).total_seconds()) / 60.0
    failed = age_minutes > max_age_minutes
    return RuleResult(
        rule_name=f"freshness_{timestamp_col}",
        passed=not failed,
        details=f"Latest {timestamp_col} age (minutes): {age_minutes:.2f}",
        failure_count=int(1 if failed else 0),
    )


def run_rules(df: pd.DataFrame, rules: List[DQRule]) -> List[RuleResult]:
    return [rule.func(df) for rule in rules]


if __name__ == "__main__":
    sample = pd.DataFrame(
        {
            "member_id": [101, 102, 103, 103],
            "claim_id": ["C1", "C2", "C3", "C3"],
            "status_code": ["PAID", "PENDING", "PAID", "PAID"],
            "load_ts": pd.to_datetime(
                ["2026-04-22 09:10:00", "2026-04-22 09:12:00", "2026-04-22 09:14:00", "2026-04-22 09:14:00"]
            ),
        }
    )

    rules = [
        DQRule("not_null_business_keys", lambda df: not_null_rule(df, ["member_id", "claim_id"])),
        DQRule("unique_claim_id", lambda df: unique_rule(df, ["claim_id"])),
        DQRule("accepted_status_code", lambda df: accepted_values_rule(df, "status_code", ["PAID", "PENDING", "DENIED"])),
        DQRule("freshness_load_ts", lambda df: freshness_rule(df, "load_ts", 120, pd.Timestamp("2026-04-22 10:00:00"))),
    ]

    for result in run_rules(sample, rules):
        print(result)
