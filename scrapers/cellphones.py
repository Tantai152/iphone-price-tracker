import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

from playwright.sync_api import sync_playwright, ElementHandle, Page, Browser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
LISTING_URL = "https://cellphones.com.vn/mobile/apple.html"
SOURCE_NAME = "cellphones"
PRODUCT_SELECTOR = ".product-info-container.product-item"
PAGE_TIMEOUT_MS = 15_000

LOAD_MORE_SELECTOR = ".btn-showmore, .view-more, button.btn-more"  # cellphones load-more button
SCROLL_PAUSE_MS = 1_500
MAX_LOAD_MORE_ATTEMPTS = 20  # safety cap


class CellphonesRawScraper:
    """
    Scrapes Apple iPhone product listings from cellphones.com.vn.

    Flow:
        1. Launch Playwright browser (headless configurable).
        2. Navigate to LISTING_URL.
        3. Repeatedly click "Xem thêm" / scroll until all products load.
        4. Extract structured data from every product card.
        5. Save results to a timestamped JSON under data/raw/<date>/.
    """

    def __init__(self, headless: bool = True) -> None:
        self._headless = headless
        self._run_timestamp: datetime = datetime.now()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> List[Dict]:
        """Main entry point. Returns list of extracted product dicts."""
        self._run_timestamp = datetime.now()
        logger.info("Starting scrape of %s", LISTING_URL)

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=self._headless)
            context = browser.new_context(
                viewport={"width": 1440, "height": 900},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
            )
            page = context.new_page()

            try:
                products = self._scrape(page)
            finally:
                browser.close()

        self._save_raw_data(products)
        return products

    # ------------------------------------------------------------------
    # Core scraping
    # ------------------------------------------------------------------

    def _scrape(self, page: Page) -> List[Dict]:
        """Navigate, expand listing, extract all products."""
        page.goto(LISTING_URL, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
        page.wait_for_selector(PRODUCT_SELECTOR, timeout=PAGE_TIMEOUT_MS)

        self._expand_all_products(page)

        cards = page.query_selector_all(PRODUCT_SELECTOR)
        logger.info("Found %d product cards", len(cards))

        results: List[Dict] = []
        for idx, card in enumerate(cards):
            try:
                product = self._extract_product(card, idx)
                if product:
                    results.append(product)
            except Exception as exc:
                logger.warning("Failed to parse card %d: %s", idx, exc)

        logger.info("Successfully extracted %d products", len(results))
        return results

    def _expand_all_products(self, page: Page) -> None:
        """
        Click 'Xem thêm sản phẩm' repeatedly until the button disappears
        or MAX_LOAD_MORE_ATTEMPTS is reached.
        Fallback: scroll-to-bottom for infinite-scroll variants.
        """
        for attempt in range(1, MAX_LOAD_MORE_ATTEMPTS + 1):
            try:
                btn = page.query_selector(LOAD_MORE_SELECTOR)
                if btn is None or not btn.is_visible():
                    logger.info("No more 'load more' button after %d click(s)", attempt - 1)
                    break

                before = len(page.query_selector_all(PRODUCT_SELECTOR))
                btn.scroll_into_view_if_needed()
                btn.click()
                page.wait_for_timeout(SCROLL_PAUSE_MS)

                after = len(page.query_selector_all(PRODUCT_SELECTOR))
                logger.info(
                    "Load-more attempt %d: %d → %d products", attempt, before, after
                )

                if after == before:
                    logger.info("Product count unchanged — stopping expansion")
                    break

            except Exception as exc:
                logger.warning("Load-more attempt %d failed: %s", attempt, exc)
                break

        # Final scroll to bottom to trigger any lazy images / remaining JS
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(SCROLL_PAUSE_MS)

    # ------------------------------------------------------------------
    # Product extraction
    # ------------------------------------------------------------------

    def _extract_product(self, card: ElementHandle, idx: int) -> Optional[Dict]:
        """
        Extract structured fields from a single .product-info-container card.

        Returns None if the card looks empty / invalid.
        """
        # --- URL & product ID ---
        link_el = card.query_selector("a.product__link")
        url: str = link_el.get_attribute("href") if link_el else ""
        if url and url.startswith("/"):
            url = "https://cellphones.com.vn" + url

        # --- Name & image (from <img alt="..."> inside product__image) ---
        img_el = card.query_selector("img.product__img")
        name: str = ""
        image_url: str = ""
        if img_el:
            name = img_el.get_attribute("alt") or ""
            image_url = img_el.get_attribute("src") or ""
            # strip CDN resize prefix to get clean image path
            image_url = re.sub(r"^.*/insecure/[^/]+/", "https://", image_url)

        # Fallback name from product__name div
        if not name:
            name_el = card.query_selector(".product__name")
            name = (name_el.inner_text() or "").strip() if name_el else ""

        if not name and not url:
            return None  # skip ghost cards

        # --- Price & discount ---
        price_vnd, original_vnd = self._parse_price(card)

        is_on_sale = (
            original_vnd is not None
            and price_vnd is not None
            and original_vnd > price_vnd
        )
        discount_pct: Optional[float] = None
        if is_on_sale:
            discount_pct = round((original_vnd - price_vnd) / original_vnd * 100, 1)

        return {
            "model_name": name.strip(),
            "price_vnd": price_vnd,
            "is_on_sale": is_on_sale,
            "discount_pct": discount_pct,
            "source": SOURCE_NAME,
            "scraped_at": self._run_timestamp.isoformat(),
            "scraped_date": self._run_timestamp.date().isoformat(),
        }

    def _parse_price(self, card: ElementHandle) -> tuple[Optional[int], Optional[int]]:
        """
        Returns (current_price_vnd, original_price_vnd).

        Looks for:
          - current/sale price  → .product__price--show  (or first .price)
          - original price      → .product__price--through / .price--through / strike

        Handles formats like:
            "29.990.000₫"  → 29_990_000
            "29,990,000đ"  → 29_990_000
            "Liên hệ"      → None
        """
        def _to_int(el: Optional[ElementHandle]) -> Optional[int]:
            if not el:
                return None
            digits = re.sub(r"[^\d]", "", el.inner_text() or "")
            return int(digits) if digits else None

        current = _to_int(
            card.query_selector(
                ".product__price--show, .block-box-price .price--main, "
                ".block-box-price .tpt---sale-price, .block-box-price .price"
            )
        )
        original = _to_int(
            card.query_selector(
                ".product__price--through, .price--through, "
                ".block-box-price strike, .block-box-price del"
            )
        )
        return (current, original)

    # ------------------------------------------------------------------
    # Persistence  (keep as-is per user instructions)
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
    scraper = CellphonesRawScraper(headless=True)
    results = scraper.run()
    print(f"Extracted {len(results)} products")