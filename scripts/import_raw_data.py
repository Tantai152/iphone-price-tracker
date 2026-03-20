# scripts/import_raw_data.py
"""
Scan data/raw/<date>/*.json and upsert every record into Supabase.

Usage:
    # import everything
    python scripts/import_raw_data.py

    # import + move successful files to data/archive/
    python scripts/import_raw_data.py --archive

    # import a specific date only
    python scripts/import_raw_data.py --date 2026-03-20

    # import a specific file
    python scripts/import_raw_data.py --file data/raw/2026-03-20/cellphones_20260320_093221.json

    # dry-run (show what would happen, no DB writes, no archiving)
    python scripts/import_raw_data.py --dry-run

    # combine: preview archive behaviour without touching anything
    python scripts/import_raw_data.py --archive --dry-run
"""

import os
import json
import shutil
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
    raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY in .env file")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME      = "staging_raw_prices"
RAW_DATA_ROOT   = Path("data", "raw")
ARCHIVE_ROOT    = Path("data", "archive")
BATCH_SIZE      = 500
UPSERT_CONFLICT = "model_name,source,scraped_date"

# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------

def _is_date_folder(name: str) -> bool:
    try:
        datetime.strptime(name, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def discover_files(date_filter: str | None = None) -> list[Path]:
    """
    Walk data/raw/<YYYY-MM-DD>/*.json, sorted oldest -> newest.
    Ignores non-date subfolders (e.g. 'mock').
    """
    if not RAW_DATA_ROOT.exists():
        logger.error("Raw data root not found: %s", RAW_DATA_ROOT.resolve())
        return []

    if date_filter:
        date_dirs = [RAW_DATA_ROOT / date_filter]
    else:
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
        logger.info("📂 %s -> %d file(s)", d.name, len(day_files))
        files.extend(day_files)

    return files

# ---------------------------------------------------------------------------
# JSON loading
# ---------------------------------------------------------------------------

def load_json_file(file_path: Path) -> list[dict] | None:
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, list):
            logger.warning(
                "⚠️  %s: expected JSON array, got %s — skipping",
                file_path.name, type(data).__name__,
            )
            return None

        if len(data) == 0:
            logger.warning("⚠️  %s: empty array — skipping", file_path.name)
            return None

        logger.info("✅ Loaded %d records from %s", len(data), file_path.name)
        return data

    except FileNotFoundError:
        logger.error("❌ File not found: %s", file_path)
    except json.JSONDecodeError as exc:
        logger.error("❌ Invalid JSON in %s: %s", file_path.name, exc)

    return None

# ---------------------------------------------------------------------------
# Supabase upsert
# ---------------------------------------------------------------------------

def insert_data(data: list[dict], dry_run: bool = False) -> bool:
    """
    Upsert records in BATCH_SIZE chunks.
    Returns True only if every batch succeeded.
    """
    total   = len(data)
    success = 0

    for start in range(0, total, BATCH_SIZE):
        batch = data[start : start + BATCH_SIZE]
        end   = start + len(batch)

        if dry_run:
            logger.info(
                "🔍 [DRY-RUN] Would upsert records %d-%d into '%s'",
                start + 1, end, TABLE_NAME,
            )
            success += len(batch)
            continue

        try:
            supabase.table(TABLE_NAME).upsert(batch, on_conflict=UPSERT_CONFLICT).execute()
            logger.info("⬆️  Upserted records %d-%d / %d", start + 1, end, total)
            success += len(batch)
        except Exception as exc:
            logger.error("❌ Batch %d-%d failed: %s", start + 1, end, exc)

    all_ok = success == total
    if all_ok:
        logger.info("🎉 %d / %d records upserted", success, total)
    else:
        logger.warning("⚠️  Only %d / %d records succeeded", success, total)

    return all_ok

# ---------------------------------------------------------------------------
# Archiving
# ---------------------------------------------------------------------------

def archive_file(file_path: Path, dry_run: bool = False) -> None:
    """
    Move  data/raw/<date>/file.json
      ->  data/archive/<date>/file.json

    Cleans up the source date-folder if it becomes empty.
    Only called on files that were imported successfully.
    """
    try:
        relative = file_path.relative_to(RAW_DATA_ROOT)
    except ValueError:
        relative = Path(file_path.parent.name) / file_path.name

    dest = ARCHIVE_ROOT / relative

    if dry_run:
        logger.info("🔍 [DRY-RUN] Would archive -> %s", dest)
        return

    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(file_path), str(dest))
    logger.info("📦 Archived -> %s", dest)

    # Remove empty date folder from raw/
    try:
        if not any(file_path.parent.iterdir()):
            file_path.parent.rmdir()
            logger.info("🗑️  Removed empty folder: %s", file_path.parent)
    except Exception:
        pass

# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_import(
    date_filter: str | None = None,
    single_file: str | None = None,
    archive: bool = False,
    dry_run: bool = False,
) -> None:
    if single_file:
        files = [Path(single_file)]
    else:
        files = discover_files(date_filter)

    if not files:
        logger.warning("No JSON files found to import.")
        return

    mode_tags = []
    if dry_run: mode_tags.append("DRY-RUN")
    if archive: mode_tags.append("ARCHIVE")
    mode_str = f" [{', '.join(mode_tags)}]" if mode_tags else ""

    logger.info("=" * 60)
    logger.info("Import start — %d file(s) | table: %s%s", len(files), TABLE_NAME, mode_str)
    logger.info("=" * 60)

    total_records = 0
    archived_files: list[Path] = []
    failed_files:   list[Path] = []

    for file_path in files:
        data = load_json_file(file_path)
        if data is None:
            failed_files.append(file_path)
            continue

        ok = insert_data(data, dry_run=dry_run)

        if ok:
            total_records += len(data)
            if archive:
                archive_file(file_path, dry_run=dry_run)
                archived_files.append(file_path)
        else:
            # Never archive files that failed — keep them in raw/ for retry
            failed_files.append(file_path)

    # Summary
    logger.info("=" * 60)
    logger.info(
        "Done — %d record(s) from %d/%d file(s)",
        total_records, len(files) - len(failed_files), len(files),
    )
    if archive:
        logger.info(
            "📦 %d file(s) archived to %s/",
            len(archived_files), ARCHIVE_ROOT,
        )
    if failed_files:
        logger.warning("⚠️  %d file(s) failed (kept in data/raw/ for retry):", len(failed_files))
        for f in failed_files:
            logger.warning("   • %s", f)
    logger.info("=" * 60)

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import raw scraped JSON files into Supabase.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    source = parser.add_mutually_exclusive_group()
    source.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Import only files from a specific date folder.",
    )
    source.add_argument(
        "--file",
        metavar="PATH",
        help="Import a single JSON file.",
    )
    parser.add_argument(
        "--archive",
        action="store_true",
        help="Move successfully imported files to data/archive/<date>/.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview all actions without writing to DB or moving files.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_import(
        date_filter=args.date,
        single_file=args.file,
        archive=args.archive,
        dry_run=args.dry_run,
    )