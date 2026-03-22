# Bước 1 — Scrape
python scrapers/tgdd.py
python scrapers/cellphones.py

# Bước 2 — Import JSON vào staging
python scripts/import_raw_data.py --archive

# Bước 3 — Transform vào star schema
python scripts/transform.py