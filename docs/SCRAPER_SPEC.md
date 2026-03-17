# TGDD scraper spec
URL: https://www.thegioididong.com/dtdd-apple-iphone
CSS selector: .item .price   ← need real inspect
Fields to extract:
  - model_name  (str)
  - price_vnd   (int, strip dấu chấm)
  - is_on_sale  (bool)
  - discount_pct (float, nullable)
  - source      = "tgdd"
  - scraped_at  (ISO timestamp)

Error strategy:
  - if selector not found → log ERROR, skip source
  - if price parse error → log WARNING, set price = None
  - don't raise exception ra ngoài — continue pipeline