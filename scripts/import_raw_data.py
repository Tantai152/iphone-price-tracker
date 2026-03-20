# scripts/import_raw_data.py
"""
Scan data/raw/<date>/*.json and upsert every record into Supabase.

Usage:
    # import everything not yet imported
    python scripts/import_raw_data.py

    # import a specific date only
    python scripts/import_raw_data.py --date 2026-03-20

    # import a specific file
    python scripts/import_raw_data.py --file data/raw/2026-03-20/cellphones_20260320_093221.json

    # dry-run (print what would be imported, no DB writes)
    python scripts/import_raw_data.py --dry-run
"""

import os
import json
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY in .env file")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME       = "staging_raw_prices"
RAW_DATA_ROOT    = Path("data", "raw")
BATCH_SIZE       = 500          # upsert in chunks to avoid payload limits
UPSERT_CONFLICT  = "model_name,source,scraped_date"

# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------

def discover_files(date_filter: str | None = None) -> list[Path]:
    """
    Walk data/raw/<date>/*.json and return all matching files, sorted oldest→newest.

    Args:
        date_filter: "YYYY-MM-DD" string to limit to a single day, or None for all.
    """
    if not RAW_DATA_ROOT.exists():
        logger.error("Raw data root not found: %s", RAW_DATA_ROOT.resolve())
        return []

    if date_filter:
        date_dirs = [RAW_DATA_ROOT / date_filter]
    else:
        # Every sub-directory that looks like a date (YYYY-MM-DD)
        date_dirs = sorted(
            d for d in RAW_DATA_ROOT.iterdir()
            if d.is_dir() and _is_date_folder(d.name)
        )

    files: list[Path] = []
    for d in date_dirs:
        if not d.exists():
            logger.warning("Date folder not found: %s", d)
            continue
        day_files = sorted(d.glob("*.json"))
        logger.info("📂 %s → %d file(s)", d.name, len(day_files))
        files.extend(day_files)

    return files


def _is_date_folder(name: str) -> bool:
    try:
        datetime.strptime(name, "%Y-%m-%d")
        return True
    except ValueError:
        return False

# ---------------------------------------------------------------------------
# JSON loading
# ---------------------------------------------------------------------------

def load_json_file(file_path: Path) -> list[dict] | None:
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, list):
            logger.warning("⚠️  %s: expected a JSON array, got %s — skipping", file_path.name, type(data).__name__)
            return None

        logger.info("Loaded %d records from %s", len(data), file_path.name)
        return data

    except FileNotFoundError:
        logger.error("File not found: %s", file_path)
    except json.JSONDecodeError as e:
        logger.error("Invalid JSON in %s: %s", file_path.name, e)

    return None

# ---------------------------------------------------------------------------
# Supabase upsert
# ---------------------------------------------------------------------------

def insert_data(data: list[dict], table: str, dry_run: bool = False) -> bool:
    """
    Upsert records in BATCH_SIZE chunks.
    Returns True if all batches succeeded.
    """
    total   = len(data)
    success = 0

    for start in range(0, total, BATCH_SIZE):
        batch = data[start : start + BATCH_SIZE]
        end   = start + len(batch)

        if dry_run:
            logger.info("🔍 [DRY-RUN] Would upsert records %d–%d into '%s'", start + 1, end, table)
            success += len(batch)
            continue

        try:
            supabase.table(table).upsert(batch, on_conflict=UPSERT_CONFLICT).execute()
            logger.info("Upserted records %d–%d / %d into '%s'", start + 1, end, total, table)
            success += len(batch)
        except Exception as exc:
            logger.error("Batch %d–%d failed: %s", start + 1, end, exc)

    if success == total:
        logger.info("%d / %d records upserted successfully", success, total)
        return True

    logger.warning("%d / %d records succeeded", success, total)
    return success > 0

# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_import(
    date_filter: str | None = None,
    single_file: str | None = None,
    dry_run: bool = False,
) -> None:
    """
    Main import logic.

    Priority: single_file > date_filter > all files.
    """
    if single_file:
        files = [Path(single_file)]
    else:
        files = discover_files(date_filter)

    if not files:
        logger.warning("No JSON files found to import.")
        return

    logger.info("=" * 60)
    logger.info("Starting import — %d file(s) | table: %s%s", len(files), TABLE_NAME, " [DRY-RUN]" if dry_run else "")
    logger.info("=" * 60)

    total_records  = 0
    failed_files   = []

    for file_path in files:
        data = load_json_file(file_path)
        if data is None:
            failed_files.append(file_path)
            continue

        ok = insert_data(data, TABLE_NAME, dry_run=dry_run)
        if ok:
            total_records += len(data)
        else:
            failed_files.append(file_path)

    # Summary
    logger.info("=" * 60)
    logger.info("Import complete — %d records across %d file(s)", total_records, len(files) - len(failed_files))
    if failed_files:
        logger.warning("⚠️  %d file(s) had errors:", len(failed_files))
        for f in failed_files:
            logger.warning("   • %s", f)
    logger.info("=" * 60)

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import raw scraped JSON files into Supabase."
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Import only files from a specific date folder.",
    )
    group.add_argument(
        "--file",
        metavar="PATH",
        help="Import a single JSON file.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be imported without writing to DB.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_import(
        date_filter=args.date,
        single_file=args.file,
        dry_run=args.dry_run,
    )