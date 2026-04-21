"""
pipeline/run_all.py
───────────────────
The main entrypoint. Docker runs this file.
Calls each step in order: validate schemas → ingest → detect dupes → validate data → save.

Run with:
    docker compose up
"""

import os
from pathlib import Path
import duckdb
from loguru import logger

from pipeline.ingest import create_source_view, export_table_to_parquet
from pipeline.analyze_duplicates import run_duplicate_analysis
from pipeline.dedupe import handle_duplicates
from pipeline.validate import check_schemas, check_nulls, check_row_counts

LEDGER = os.getenv("LEDGER_PATH", "/data/enat_ledger")
OUTPUT = os.getenv("OUTPUT_PATH", "/app/output")

Path(OUTPUT).mkdir(parents=True, exist_ok=True)


logger.info("═" * 50)
logger.info("  enat_ledger pipeline starting")
logger.info("═" * 50)

# Step 1 — check all file schemas match before loading anything
logger.info("Step 1: Checking schemas...")
check_schemas(LEDGER)

# Step 2 — create a lazy source view over parquet files
logger.info("Step 2: Ingesting data...")
con = duckdb.connect()
con.execute("PRAGMA temp_directory='/tmp'")
con.execute("PRAGMA threads=2")
source_view = create_source_view(con, LEDGER)

# Step 3 — read-only duplicate analysis report (saved to output/duplicate_analysis.json)
logger.info("Step 3: Duplicate analysis...")
run_duplicate_analysis(con, source_view, OUTPUT)

# Step 4 — detect duplicates in pipeline flow
logger.info("Step 4: Duplicate detection...")
working_view = handle_duplicates(con, source_view)

# Step 5 — validate the combined data
logger.info("Step 5: Validating...")
check_nulls(con, working_view)
check_row_counts(con, working_view)

# Step 6 — save the output
output_file = f"{OUTPUT}/enat_ledger_combined.parquet"
export_table_to_parquet(con, working_view, output_file)
final_rows = con.execute(f"SELECT COUNT(*) FROM {working_view}").fetchone()[0]

logger.info("═" * 50)
logger.success(f"Done. {final_rows:,} rows saved -> {output_file}")
logger.info("═" * 50)