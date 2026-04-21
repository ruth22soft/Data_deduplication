"""
pipeline/analyze_duplicates.py
──────────────────────────────
READ-ONLY duplicate analysis for enat_ledger.
This module never modifies source data.
"""

import json
from pathlib import Path
from loguru import logger
import duckdb

from pipeline.ingest import create_source_view

UNIQUE_KEY = "transaction_id"   # ← change this to your actual primary key column


def _resolve_unique_key(
    con: duckdb.DuckDBPyConnection,
    source_view: str,
    requested_key: str,
) -> str:
    cols = set(con.execute(f"SELECT * FROM {source_view} LIMIT 0").df().columns)
    if requested_key in cols:
        return requested_key
    for fallback in ("entry_id", "transaction_id"):
        if fallback in cols:
            logger.warning(
                f"Unique key '{requested_key}' not found. Falling back to '{fallback}'."
            )
            return fallback
    raise ValueError(f"No suitable unique key found. Available columns: {sorted(cols)}")

def run_duplicate_analysis(
    con: duckdb.DuckDBPyConnection,
    source_view: str,
    output_path: str,
    unique_key: str = UNIQUE_KEY,
):
    """Run read-only duplicate analysis and save a JSON report."""
    Path(output_path).mkdir(parents=True, exist_ok=True)
    unique_key = _resolve_unique_key(con, source_view, unique_key)

    total_rows = con.execute(f"SELECT COUNT(*) FROM {source_view}").fetchone()[0]
    unique_ids = con.execute(
        f"SELECT COUNT(DISTINCT {unique_key}) FROM {source_view}"
    ).fetchone()[0]
    duplicate_rows = total_rows - unique_ids
    dup_pct = (duplicate_rows / total_rows) * 100 if total_rows else 0

    freq_table_df = con.execute(f"""
        WITH appearances AS (
            SELECT {unique_key}, COUNT(*) AS appearances
            FROM {source_view}
            GROUP BY 1
        )
        SELECT appearances, COUNT(*) AS number_of_ids
        FROM appearances
        GROUP BY 1
        ORDER BY 1
    """).fetchdf()

    top_dupes_df = con.execute(f"""
        SELECT {unique_key}, COUNT(*) AS appearances
        FROM {source_view}
        GROUP BY 1
        ORDER BY appearances DESC
        LIMIT 10
    """).fetchdf()

    dupes_per_date_df = con.execute(f"""
        WITH dup_ids AS (
            SELECT {unique_key}
            FROM {source_view}
            GROUP BY 1
            HAVING COUNT(*) > 1
        )
        SELECT source_date, COUNT(*) AS duplicate_rows
        FROM {source_view}
        WHERE {unique_key} IN (SELECT {unique_key} FROM dup_ids)
        GROUP BY 1
        ORDER BY duplicate_rows DESC
    """).fetchdf()

    lag_df = con.execute(f"""
        WITH dup_ids AS (
            SELECT {unique_key}
            FROM {source_view}
            GROUP BY 1
            HAVING COUNT(*) > 1
        ), timeline AS (
            SELECT
                {unique_key},
                MIN(CAST(source_date AS DATE)) AS first_seen,
                MAX(CAST(source_date AS DATE)) AS last_seen,
                DATE_DIFF('day', MIN(CAST(source_date AS DATE)), MAX(CAST(source_date AS DATE))) AS lag_days
            FROM {source_view}
            WHERE {unique_key} IN (SELECT {unique_key} FROM dup_ids)
            GROUP BY 1
        )
        SELECT lag_days, COUNT(*) AS count_ids
        FROM timeline
        GROUP BY 1
        ORDER BY count_ids DESC
        LIMIT 5
    """).fetchdf()

    avg_lag_row = con.execute(f"""
        WITH dup_ids AS (
            SELECT {unique_key}
            FROM {source_view}
            GROUP BY 1
            HAVING COUNT(*) > 1
        )
        SELECT AVG(DATE_DIFF('day', first_seen, last_seen)) AS avg_lag
        FROM (
            SELECT
                {unique_key},
                MIN(CAST(source_date AS DATE)) AS first_seen,
                MAX(CAST(source_date AS DATE)) AS last_seen
            FROM {source_view}
            WHERE {unique_key} IN (SELECT {unique_key} FROM dup_ids)
            GROUP BY 1
        ) t
    """).fetchone()

    avg_lag = round(float(avg_lag_row[0]), 1) if avg_lag_row and avg_lag_row[0] is not None else 0
    lag_dist = {str(int(r[0])): int(r[1]) for r in lag_df.itertuples(index=False, name=None)}
    most_affected = dupes_per_date_df.iloc[0]["source_date"] if len(dupes_per_date_df) > 0 else "none"
    columns_changed = {}
    exact_copies = 0
    changed_copies = 0

    logger.info("Duplicate analysis summary")
    logger.info(f"Total rows: {total_rows:,}")
    logger.info(f"Duplicate rows: {duplicate_rows:,} ({dup_pct:.1f}%)")
    logger.info(f"Most affected date: {most_affected}")

    report = {
        "total_rows": total_rows,
        "unique_ids": unique_ids,
        "duplicate_rows": duplicate_rows,
        "duplicate_rate_pct": round(dup_pct, 2),
        "appearances_distribution": freq_table_df.to_dict("records"),
        "top_duplicate_ids": top_dupes_df.to_dict("records"),
        "duplicates_per_date": dupes_per_date_df.to_dict("records"),
        "average_lag_days": avg_lag,
        "lag_distribution": lag_dist,
        "exact_copies": exact_copies,
        "copies_with_changes": changed_copies,
        "columns_that_changed": columns_changed,
    }

    report_path = Path(output_path) / "duplicate_analysis.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    logger.success(f"Duplicate report saved -> {report_path}")


if __name__ == "__main__":
    import os

    ledger_path = os.getenv("LEDGER_PATH", "/data/enat_ledger")
    output_path = os.getenv("OUTPUT_PATH", "/app/output")
    logger.info("Reading all parquet files...")
    con = duckdb.connect()
    source_view = create_source_view(con, ledger_path)
    run_duplicate_analysis(con, source_view, output_path)