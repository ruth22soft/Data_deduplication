"""
pipeline/dedupe.py
──────────────────
Duplicate handling for the pipeline.

By default this file only DETECTS and REPORTS duplicates.
The removal block is commented out — uncomment it when you are
ready to actually remove duplicates from the output.
"""

import duckdb
from loguru import logger

UNIQUE_KEY = "transaction_id"   # ← change to your actual primary key column


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


def handle_duplicates(
    con: duckdb.DuckDBPyConnection,
    source_view: str,
    unique_key: str = UNIQUE_KEY,
):
    """
    Runs duplicate detection and logs a report.
    Returns a DuckDB view name for downstream steps.
    """

    unique_key = _resolve_unique_key(con, source_view, unique_key)
    total = con.execute(f"SELECT COUNT(*) FROM {source_view}").fetchone()[0]
    unique_ids = con.execute(
        f"SELECT COUNT(DISTINCT {unique_key}) FROM {source_view}"
    ).fetchone()[0]
    dup_count = total - unique_ids
    dup_pct = (dup_count / total) * 100 if total else 0

    logger.info(f"Total rows       : {total:,}")
    logger.info(f"Unique IDs       : {unique_ids:,}")
    logger.info(f"Duplicate rows   : {dup_count:,}  ({dup_pct:.1f}%)")

    dupes_per_date = con.execute(f"""
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

    if not dupes_per_date.empty:
        logger.warning("Duplicate rows by date folder:")
        logger.warning(f"\n{dupes_per_date.to_string(index=False)}")
    else:
        logger.success("No duplicates found.")

    con.execute(f"CREATE OR REPLACE VIEW ledger_working AS SELECT * FROM {source_view}")


    # ── Removal: COMMENTED OUT — uncomment when ready ─────────────
    #
    # Strategy: keep the most recent version of each ID.
    # If the same transaction_id appears in March 18 and March 25,
    # the March 25 version (the re-send) is kept.
    #
    # con.execute(f"""
    #     CREATE OR REPLACE VIEW ledger_working AS
    #     SELECT * EXCLUDE (rn)
    #     FROM (
    #         SELECT
    #             *,
    #             ROW_NUMBER() OVER (
    #                 PARTITION BY {unique_key}
    #                 ORDER BY CAST(source_date AS DATE) DESC
    #             ) AS rn
    #         FROM {source_view}
    #     ) t
    #     WHERE rn = 1
    # """)
    # kept = con.execute("SELECT COUNT(*) FROM ledger_working").fetchone()[0]
    # removed = total - kept
    # logger.success(f"Removed {removed:,} duplicate rows -> {kept:,} rows remain")


    return "ledger_working"