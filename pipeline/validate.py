"""
pipeline/validate.py
────────────────────
Checks data health before saving.
Raises warnings (does not crash) so the pipeline always completes.
"""

import duckdb
import pyarrow.parquet as pq
from pathlib import Path
from loguru import logger

MANDATORY_COLS = ["transaction_id", "entry_id"]   # ← at least one should exist


def check_schemas(ledger_path: str):
    """Read schema metadata from every parquet file (fast — no rows loaded)."""
    files = sorted(Path(ledger_path).glob("**/*.parquet"))
    if not files:
        logger.error("No parquet files found.")
        return

    reference     = pq.read_schema(files[0])
    reference_date = files[0].parent.name
    all_match     = True

    for f in files[1:]:
        schema = pq.read_schema(f)
        if schema.names != reference.names:
            logger.warning(
                f"Schema mismatch on {f.parent.name}  "
                f"(reference: {reference_date})\n"
                f"  Expected : {reference.names}\n"
                f"  Got      : {schema.names}"
            )
            all_match = False

    if all_match:
        logger.success(f"All {len(files)} files share the same schema: {reference.names}")


def check_nulls(con: duckdb.DuckDBPyConnection, table_or_view: str):
    """Check mandatory columns for null values using SQL aggregates."""
    available_cols = con.execute(f"SELECT * FROM {table_or_view} LIMIT 0").df().columns
    present_mandatory = [col for col in MANDATORY_COLS if col in available_cols]

    if not present_mandatory:
        logger.warning(
            f"None of the mandatory columns were found: {MANDATORY_COLS}"
        )
        return

    for col in present_mandatory:
        null_count = con.execute(
            f"SELECT COUNT(*) FROM {table_or_view} WHERE {col} IS NULL"
        ).fetchone()[0]
        if null_count > 0:
            logger.warning(f"'{col}' has {null_count:,} null values.")
        else:
            logger.success(f"'{col}' — no nulls.")


def check_row_counts(con: duckdb.DuckDBPyConnection, table_or_view: str):
    """Flag dates with unusually low row counts (possible missing data)."""
    counts_df = con.execute(f"""
        SELECT source_date, COUNT(*) AS row_count
        FROM {table_or_view}
        GROUP BY 1
        ORDER BY source_date
    """).fetchdf()

    if counts_df.empty:
        logger.warning("No rows available for row-count checks.")
        return

    median = counts_df["row_count"].median()
    low = counts_df[counts_df["row_count"] < median * 0.5]

    if not low.empty:
        logger.warning(f"Dates with low row counts (< 50% of median {median:.0f}):")
        logger.warning(f"\n{low.to_string(index=False)}")
    else:
        logger.success("All dates have healthy row counts.")