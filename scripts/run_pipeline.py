"""
scripts/run_pipeline.py
========================
Orchestrator — chạy toàn bộ ETP pipeline theo thứ tự:
  1. Scrape CellphoneS  → data/raw/<date>/*.json
  2. Scrape TGDD         → data/raw/<date>/*.json
  3. Import JSON → Supabase staging_raw_prices
  4. Transform staging → star schema (dim + fact)

Usage:
    python scripts/run_pipeline.py              # chạy full pipeline
    python scripts/run_pipeline.py --dry-run    # preview, no DB writes
"""

import sys
import logging
import argparse
from datetime import date
from pathlib import Path

# ---------------------------------------------------------------------------
# Logging → console + logs/pipeline_YYYY-MM-DD.log
# ---------------------------------------------------------------------------
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

log_file = LOG_DIR / f"pipeline_{date.today().isoformat()}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, encoding="utf-8"),
    ],
)
logger = logging.getLogger("pipeline")


def run_step(name: str, fn, *args, **kwargs) -> bool:
    """Run a pipeline step; return True on success, False on failure."""
    logger.info("=" * 60)
    logger.info("▶  Step: %s", name)
    logger.info("=" * 60)
    try:
        fn(*args, **kwargs)
        logger.info("✅  %s — OK", name)
        return True
    except Exception as exc:
        logger.error("❌  %s — FAILED: %s", name, exc, exc_info=True)
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the full ETP pipeline.")
    parser.add_argument("--dry-run", action="store_true", help="Preview only — no DB writes")
    args = parser.parse_args()

    today_str = date.today().isoformat()
    logger.info("Pipeline start — date=%s  dry_run=%s", today_str, args.dry_run)

    results: dict[str, bool] = {}

    # ------------------------------------------------------------------
    # 1. Extract — Scrape
    # ------------------------------------------------------------------
    from scrapers.cellphones import CellphonesRawScraper
    from scrapers.tgdd import TGDDRawScraper

    results["Scrape CellphoneS"] = run_step(
        "Scrape CellphoneS",
        lambda: CellphonesRawScraper(headless=True).run(),
    )

    results["Scrape TGDD"] = run_step(
        "Scrape TGDD",
        lambda: TGDDRawScraper(headless=True).run(),
    )

    # ------------------------------------------------------------------
    # 2. Load — Import raw JSON → Supabase staging
    # ------------------------------------------------------------------
    from scripts.import_raw_data import run_import

    results["Import to staging"] = run_step(
        "Import to staging",
        run_import,
        date_filter=today_str,
        archive=True,
        dry_run=args.dry_run,
    )

    # ------------------------------------------------------------------
    # 3. Transform — staging → star schema
    # ------------------------------------------------------------------
    from scripts.transform import run_transform

    results["Transform to star schema"] = run_step(
        "Transform to star schema",
        run_transform,
        target_date_str=today_str,
        dry_run=args.dry_run,
    )

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("Pipeline Summary")
    logger.info("=" * 60)
    all_ok = True
    for step, ok in results.items():
        status = "✅ OK" if ok else "❌ FAILED"
        logger.info("  %s — %s", step, status)
        if not ok:
            all_ok = False

    logger.info("=" * 60)
    if all_ok:
        logger.info("🎉 Pipeline completed successfully!")
    else:
        logger.error("⚠️  Pipeline completed with errors.")
        sys.exit(1)


if __name__ == "__main__":
    main()
