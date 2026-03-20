"""
scripts/transform.py
====================
ETL: staging_raw_prices → dim_date + dim_model + dim_source → fact_prices

Usage:
    python scripts/transform.py                   # process today
    python scripts/transform.py --date 2026-03-20
    python scripts/transform.py --all             # reprocess every date in staging
    python scripts/transform.py --dry-run         # preview, no DB writes
"""

import os
import re
import argparse
import logging
from datetime import date
from typing import Optional

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

supabase: Client = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_KEY"],
)

RAW_TABLE  = "staging_raw_prices"
FACT_TABLE = "fact_prices"
DIM_MODEL  = "dim_model"
DIM_SOURCE = "dim_source"
DIM_DATE   = "dim_date"

BATCH_SIZE = 500

# ---------------------------------------------------------------------------
# ── Parsing helpers ───────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

# Full series strings, longest-first to avoid greedy mismatch
_SERIES_LIST = [
    # iPhone 17 lineup (2025)
    "iPhone 17 Pro Max", "iPhone 17 Pro", "iPhone 17 Plus", "iPhone 17",
    # iPhone 17e
    "iPhone 17e",
    # iPhone Air
    "iPhone Air",
    # iPhone 16 lineup
    "iPhone 16 Pro Max", "iPhone 16 Pro", "iPhone 16 Plus", "iPhone 16",
    # iPhone 16e
    "iPhone 16e",
    # iPhone 15 lineup
    "iPhone 15 Pro Max", "iPhone 15 Pro", "iPhone 15 Plus", "iPhone 15",
    # iPhone 14 lineup
    "iPhone 14 Pro Max", "iPhone 14 Pro", "iPhone 14 Plus", "iPhone 14",
    # iPhone 13 lineup
    "iPhone 13 Pro Max", "iPhone 13 Pro", "iPhone 13 mini", "iPhone 13",
    # iPhone 12 lineup
    "iPhone 12 Pro Max", "iPhone 12 Pro", "iPhone 12 mini", "iPhone 12",
    # iPhone SE
    "iPhone SE",
    # iPhone 11 lineup
    "iPhone 11 Pro Max", "iPhone 11 Pro", "iPhone 11",
]

# full_series_string → (series col, model_name/variant col)
_VARIANT_MAP: dict[str, tuple[str, str]] = {
    # iPhone 17
    "iPhone 17 Pro Max": ("iPhone 17", "Pro Max"),
    "iPhone 17 Pro":     ("iPhone 17", "Pro"),
    "iPhone 17 Plus":    ("iPhone 17", "Plus"),
    "iPhone 17":         ("iPhone 17", ""),
    "iPhone 17e":        ("iPhone 17e", ""),
    # iPhone Air
    "iPhone Air":        ("iPhone Air", ""),
    # iPhone 16
    "iPhone 16 Pro Max": ("iPhone 16", "Pro Max"),
    "iPhone 16 Pro":     ("iPhone 16", "Pro"),
    "iPhone 16 Plus":    ("iPhone 16", "Plus"),
    "iPhone 16":         ("iPhone 16", ""),
    "iPhone 16e":        ("iPhone 16e", ""),
    # iPhone 15
    "iPhone 15 Pro Max": ("iPhone 15", "Pro Max"),
    "iPhone 15 Pro":     ("iPhone 15", "Pro"),
    "iPhone 15 Plus":    ("iPhone 15", "Plus"),
    "iPhone 15":         ("iPhone 15", ""),
    # iPhone 14
    "iPhone 14 Pro Max": ("iPhone 14", "Pro Max"),
    "iPhone 14 Pro":     ("iPhone 14", "Pro"),
    "iPhone 14 Plus":    ("iPhone 14", "Plus"),
    "iPhone 14":         ("iPhone 14", ""),
    # iPhone 13
    "iPhone 13 Pro Max": ("iPhone 13", "Pro Max"),
    "iPhone 13 Pro":     ("iPhone 13", "Pro"),
    "iPhone 13 mini":    ("iPhone 13", "mini"),
    "iPhone 13":         ("iPhone 13", ""),
    # iPhone 12
    "iPhone 12 Pro Max": ("iPhone 12", "Pro Max"),
    "iPhone 12 Pro":     ("iPhone 12", "Pro"),
    "iPhone 12 mini":    ("iPhone 12", "mini"),
    "iPhone 12":         ("iPhone 12", ""),
    # iPhone SE
    "iPhone SE":         ("iPhone SE", ""),
    # iPhone 11
    "iPhone 11 Pro Max": ("iPhone 11", "Pro Max"),
    "iPhone 11 Pro":     ("iPhone 11", "Pro"),
    "iPhone 11":         ("iPhone 11", ""),
}

_SERIES_RE  = re.compile(
    "(" + "|".join(re.escape(s) for s in _SERIES_LIST) + ")",
    re.IGNORECASE,
)
_STORAGE_RE = re.compile(r"(\d+)\s*(GB|TB)", re.IGNORECASE)

# Strip common Vietnamese noise prefixes
_NOISE_PREFIX_RE = re.compile(
    r"^(apple\s+|điện thoại\s+|dt\s+|smartphone\s+)",
    re.IGNORECASE,
)

# FIX B: Strip trailing metadata after "|" or "–"
# e.g. "| Chính hãng VN/A"  /  "| Chính hãng"  /  "– VN/A"
_SUFFIX_RE = re.compile(r"\s*[\|–-].*$")


def _clean_name(raw: str) -> str:
    """Strip noise prefix, trailing metadata suffix, fix whitespace & casing."""
    name = raw.strip()
    name = _SUFFIX_RE.sub("", name)          # remove "| Chính hãng VN/A" etc.
    name = _NOISE_PREFIX_RE.sub("", name)    # remove "Điện thoại " etc.
    name = re.sub(r"\s+", " ", name).title()
    name = re.sub(r"\b(\d+)\s*Gb\b", r"\1GB", name)
    name = re.sub(r"\b(\d+)\s*Tb\b", r"\1TB", name)
    return name.strip()


def parse_model_fields(raw_name: str) -> dict:
    """
    Parse raw product name → dim_model fields.

    Examples:
        'iPhone 17 Pro Max 256GB | Chính hãng'
            → { series: 'iPhone 17', model_name: 'Pro Max', storage_gb: 256 }
        'Điện thoại iPhone Air 256GB'
            → { series: 'iPhone Air', model_name: '',        storage_gb: 256 }
        'iPhone 16 Pro 1TB | Chính hãng VN/A'
            → { series: 'iPhone 16', model_name: 'Pro',      storage_gb: 1024 }
    """
    name = _clean_name(raw_name)

    # Series + variant
    m = _SERIES_RE.search(name)
    if m:
        raw_key = m.group(1)
        matched_key = next(
            (k for k in _VARIANT_MAP if k.lower() == raw_key.lower()), None
        )
        series, variant = _VARIANT_MAP[matched_key] if matched_key else (raw_key, "")
    else:
        series, variant = None, None

    # Storage
    sm = _STORAGE_RE.search(name)
    if sm:
        val, unit = int(sm.group(1)), sm.group(2).upper()
        storage_gb: Optional[int] = val * 1024 if unit == "TB" else val
    else:
        storage_gb = None

    return {"series": series, "model_name": variant, "storage_gb": storage_gb}


# ---------------------------------------------------------------------------
# ── dim_date ──────────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

def ensure_dim_date(d: date, dry_run: bool) -> int:
    row = {
        "date_id":    int(d.strftime("%Y%m%d")),
        "full_date":  d.isoformat(),
        "day":        d.day,
        "month":      d.month,
        "year":       d.year,
        "is_weekend": d.weekday() >= 5,
    }
    if not dry_run:
        supabase.table(DIM_DATE).upsert(row, on_conflict="date_id").execute()
    logger.info("dim_date  → date_id=%d  (%s)", row["date_id"], d.isoformat())
    return row["date_id"]


# ---------------------------------------------------------------------------
# ── dim_source ────────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

def ensure_dim_sources(source_names: list[str], dry_run: bool) -> dict[str, int]:
    """
    Upsert source rows and return {source_name: source_id}.

    FIX A: In dry-run mode we skip the DB upsert AND the DB fetch —
    instead we return a fake mapping so the rest of the pipeline can
    exercise its logic without touching the database.
    """
    if dry_run:
        fake_map = {name: idx + 1 for idx, name in enumerate(sorted(source_names))}
        logger.info("dim_source → [DRY-RUN] fake mapping: %s", fake_map)
        return fake_map

    rows = [{"source_name": s} for s in source_names]
    supabase.table(DIM_SOURCE).upsert(rows, on_conflict="source_name").execute()

    resp = (
        supabase.table(DIM_SOURCE)
        .select("source_id,source_name")
        .in_("source_name", source_names)
        .execute()
    )
    mapping = {r["source_name"]: r["source_id"] for r in (resp.data or [])}
    logger.info("dim_source → %s", mapping)
    return mapping


# ---------------------------------------------------------------------------
# ── dim_model ─────────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

def ensure_dim_models(parsed: list[dict], dry_run: bool) -> dict[tuple, int]:
    """
    FIX A: Same dry-run fix — return fake auto-increment IDs instead of
    querying an empty DB.
    """
    unique: dict[tuple, dict] = {}
    for r in parsed:
        if r["series"] is None:
            continue
        key = (r["series"], r["model_name"], r["storage_gb"])
        unique[key] = r

    if not unique:
        logger.warning("dim_model: nothing to upsert")
        return {}

    if dry_run:
        fake_map = {key: idx + 1 for idx, key in enumerate(sorted(unique.keys(), key=str))}
        logger.info("dim_model  → [DRY-RUN] %d unique models (fake IDs)", len(fake_map))
        return fake_map

    supabase.table(DIM_MODEL).upsert(
        list(unique.values()),
        on_conflict="series,model_name,storage_gb",
    ).execute()

    series_list = list({r["series"] for r in unique.values()})
    resp = (
        supabase.table(DIM_MODEL)
        .select("model_id,series,model_name,storage_gb")
        .in_("series", series_list)
        .execute()
    )
    mapping: dict[tuple, int] = {
        (r["series"], r["model_name"], r["storage_gb"]): r["model_id"]
        for r in (resp.data or [])
    }
    logger.info("dim_model  → %d unique models resolved", len(mapping))
    return mapping


# ---------------------------------------------------------------------------
# ── fact_prices ───────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

def build_fact_rows(
    raw_rows:   list[dict],
    date_id:    int,
    source_map: dict[str, int],
    model_map:  dict[tuple, int],
) -> list[dict]:
    facts   = []
    skipped = 0

    for r in raw_rows:
        fields    = parse_model_fields(r.get("model_name", ""))
        model_key = (fields["series"], fields["model_name"], fields["storage_gb"])
        model_id  = model_map.get(model_key)
        source_id = source_map.get(r.get("source", ""))

        if model_id is None or source_id is None:
            logger.warning(
                "Skip '%s' — model_id=%s  source_id=%s",
                r.get("model_name"), model_id, source_id,
            )
            skipped += 1
            continue

        facts.append({
            "model_id":    model_id,
            "source_id":   source_id,
            "date_id":     date_id,
            "price_vnd":   r["price_vnd"],
            "is_on_sale":  r.get("is_on_sale", False),
            "discount_pct": r.get("discount_pct"),
            "is_anomaly":  False,   # ML will update this later
            "scraped_at":  r.get("scraped_at"),
        })

    if skipped:
        logger.warning("Skipped %d row(s) (unresolvable model or source)", skipped)
    return facts


def _upsert_facts(rows: list[dict], dry_run: bool) -> None:
    if not rows:
        logger.info("No fact rows to insert.")
        return

    # Deduplicate: keep last row per (model_id, source_id, date_id).
    # Duplicates happen when a scraper lists the same product more than once.
    seen: dict[tuple, dict] = {}
    for row in rows:
        key = (row["model_id"], row["source_id"], row["date_id"])
        seen[key] = row  # last-write-wins
    deduped = list(seen.values())

    if len(deduped) < len(rows):
        logger.warning(
            "Deduped %d → %d fact rows (%d duplicates removed)",
            len(rows), len(deduped), len(rows) - len(deduped),
        )
    rows = deduped

    total = len(rows)
    for start in range(0, total, BATCH_SIZE):
        batch = rows[start : start + BATCH_SIZE]
        end   = start + len(batch)
        if dry_run:
            logger.info("[DRY-RUN] Would upsert fact_prices %d-%d / %d", start+1, end, total)
        else:
            supabase.table(FACT_TABLE).upsert(
                batch, on_conflict="model_id,source_id,date_id"
            ).execute()
            logger.info("fact_prices ↑ %d-%d / %d", start+1, end, total)


# ---------------------------------------------------------------------------
# ── Orchestrator ──────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

def run_transform(target_date_str: str, dry_run: bool = False) -> None:
    logger.info("=" * 60)
    logger.info("Transform  date=%s  dry_run=%s", target_date_str, dry_run)
    logger.info("=" * 60)

    target_date = date.fromisoformat(target_date_str)

    # 1. Raw data
    raw_rows = (
        supabase.table(RAW_TABLE)
        .select("*")
        .eq("scraped_date", target_date_str)
        .execute()
        .data or []
    )
    if not raw_rows:
        logger.warning("No staging rows for %s — skipping", target_date_str)
        return
    logger.info("Fetched %d raw rows from %s", len(raw_rows), RAW_TABLE)

    # 2. dim_date
    date_id = ensure_dim_date(target_date, dry_run)

    # 3. dim_source
    sources    = list({r["source"] for r in raw_rows if r.get("source")})
    source_map = ensure_dim_sources(sources, dry_run)

    # 4. dim_model
    parsed    = [parse_model_fields(r.get("model_name", "")) for r in raw_rows]
    model_map = ensure_dim_models(parsed, dry_run)

    # 5. fact_prices
    fact_rows = build_fact_rows(raw_rows, date_id, source_map, model_map)
    logger.info("Built %d fact rows", len(fact_rows))
    if dry_run:
        logger.info("-" * 40)
        for row in fact_rows[:5]:
            logger.info("  Sample fact row: %s", row)
        logger.info("-" * 40)
    _upsert_facts(fact_rows, dry_run)

    logger.info("✓  Done — %s", target_date_str)


# ---------------------------------------------------------------------------
# ── CLI ───────────────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

def _fetch_all_dates() -> list[str]:
    data = supabase.table(RAW_TABLE).select("scraped_date").execute().data or []
    return sorted({r["scraped_date"] for r in data})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transform staging_raw_prices → star schema (dim + fact).",
    )
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        default=date.today().isoformat(),
        help="Date to process (default: today)",
    )
    grp.add_argument(
        "--all",
        action="store_true",
        help="Reprocess all dates found in staging_raw_prices",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview only — no DB writes",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.all:
        dates = _fetch_all_dates()
        logger.info("Processing %d date(s): %s", len(dates), dates)
        for d in dates:
            run_transform(d, dry_run=args.dry_run)
    else:
        run_transform(args.date, dry_run=args.dry_run)