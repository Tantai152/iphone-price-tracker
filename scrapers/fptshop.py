"""
TGDD Scraper for iPhone prices.
Extracts product information from The Gioi Di Dong iPhone listing page.
Saves raw data as JSON files in data/raw/YYYY-MM-DD/.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

from playwright.sync_api import sync_playwright, ElementHandle

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
LISTING_URL = "https://fptshop.com.vn/dien-thoai/apple-iphone"
SOURCE_NAME = "fptshop"
PRODUCT_SELECTOR = "ul.listproduct li.item"
PAGE_TIMEOUT_MS = 15_000


class TGDDRawScraper:
    """Scraper for The Gioi Di Dong (TGDD) iPhone products."""

    def __init__(self, headless: bool = True):
        self.headless = headless
        # Capture a single timestamp for the entire scrape run
        self._run_timestamp: Optional[datetime] = None

    # ------------------------------------------------------------------
    # Extraction helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_discount(item: ElementHandle) -> tuple[bool, Optional[float]]:
        """Return (is_on_sale, discount_pct) from discount / old-price elements."""
        discount_elem = item.query_selector("span.percent")
        if discount_elem:
            raw = discount_elem.inner_text().replace('%', '').replace('-', '').strip()
            try:
                return True, abs(float(raw))
            except (ValueError, TypeError):
                pass

        # Fallback: old-price element present means item is on sale
        if item.query_selector("span.price-old"):
            return True, None

        return False, None

    def _extract_product_info(self, item: ElementHandle) -> Optional[Dict]:
        """
        Extract product information from a single product element.

        Returns a dict with keys matching the staging_raw_prices table:
          model_name, price_vnd, is_on_sale, discount_pct,
          source, scraped_at, scraped_date
        """
        link = item.query_selector("a")
        if not link:
            return None

        model_name = (link.get_attribute("data-name") or "").strip()
        price_str = link.get_attribute("data-price")

        if not model_name or not price_str:
            return None

        try:
            price_vnd = int(float(price_str))
        except (ValueError, TypeError):
            return None

        if price_vnd <= 0:
            return None

        is_on_sale, discount_pct = self._parse_discount(item)

        return {
            "model_name": model_name,
            "price_vnd": price_vnd,
            "is_on_sale": is_on_sale,
            "discount_pct": discount_pct,
            "source": SOURCE_NAME,
            "scraped_at": self._run_timestamp.isoformat(),
            "scraped_date": self._run_timestamp.date().isoformat(),
        }

    # ------------------------------------------------------------------
    # Main scrape logic
    # ------------------------------------------------------------------

    def run(self) -> List[Dict]:
        """
        Launch browser, scrape listing page, save raw JSON, and return
        the list of extracted product dicts.
        """
        self._run_timestamp = datetime.now()
        products: List[Dict] = []

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=self.headless)
            context = browser.new_context(
                viewport={"width": 1280, "height": 800},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
            )
            page = context.new_page()

            try:
                logger.info("Navigating to %s URL: %s", SOURCE_NAME, LISTING_URL)
                page.goto(LISTING_URL, wait_until="domcontentloaded")

                # Wait for the product list container
                page.wait_for_selector("ul.listproduct", timeout=PAGE_TIMEOUT_MS)

                # Scroll to bottom to trigger any lazy-loaded items, then wait
                # for network activity to settle instead of a hard sleep.
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_load_state("networkidle")

                # Query all product cards
                items = page.query_selector_all(PRODUCT_SELECTOR)
                logger.info("Found %d raw items on %s", len(items), SOURCE_NAME)

                for idx, item in enumerate(items, 1):
                    product = self._extract_product_info(item)
                    if product:
                        products.append(product)
                        logger.debug("Extracted product %d: %s", idx, product["model_name"])
                    elif idx <= 5:
                        snippet = item.inner_html()[:120]
                        logger.debug("Item %d skipped — HTML snippet: %s", idx, snippet)

            finally:
                context.close()
                browser.close()

        logger.info("Successfully extracted %d products from %s", len(products), SOURCE_NAME)

        self._save_raw_data(products)
        return products

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _save_raw_data(self, data: List[Dict]) -> None:
        """Save products list to a timestamped JSON file in data/raw/."""
        if not data:
            logger.warning("No data to save for %s", SOURCE_NAME)
            return

        today = self._run_timestamp.strftime("%Y-%m-%d")
        folder = Path("data", "raw", today)
        folder.mkdir(parents=True, exist_ok=True)

        ts_label = self._run_timestamp.strftime("%Y%m%d_%H%M%S")
        file_path = folder / f"{SOURCE_NAME}_{ts_label}.json"

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        logger.info("Saved %d records to %s", len(data), file_path)


if __name__ == "__main__":
    scraper = TGDDRawScraper(headless=True)
    results = scraper.run()
    print(f"Extracted {len(results)} products")