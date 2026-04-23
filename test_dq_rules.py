"""
validate_hvr_extracts.py
Representative validator for CDC extract files landed from replication tooling.
This is intentionally generic and avoids vendor-locked implementation claims.
"""

from __future__ import annotations
import pandas as pd
from pathlib import Path


REQUIRED_COLUMNS = [
    "business_key",
    "operation_type",
    "change_seq",
    "extract_ts",
    "load_ts",
]


def validate_extract(file_path: str) -> dict:
    path = Path(file_path)
    df = pd.read_csv(path)

    missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    duplicate_events = df.duplicated(subset=["business_key", "change_seq"]).sum()
    invalid_ops = (~df["operation_type"].isin(["I", "U", "D"])).sum() if "operation_type" in df.columns else None
    late_arrivals = (
        (
            pd.to_datetime(df["load_ts"]) - pd.to_datetime(df["extract_ts"])
        ).dt.total_seconds().div(60).gt(240).sum()
        if {"load_ts", "extract_ts"}.issubset(df.columns)
        else None
    )

    return {
        "file": path.name,
        "row_count": int(len(df)),
        "missing_columns": missing_cols,
        "duplicate_events": int(duplicate_events),
        "invalid_operations": int(invalid_ops) if invalid_ops is not None else None,
        "late_arrivals_over_240_minutes": int(late_arrivals) if late_arrivals is not None else None,
    }


if __name__ == "__main__":
    result = validate_extract("sample_data/cdc_extract_claims.csv")
    print(result)
