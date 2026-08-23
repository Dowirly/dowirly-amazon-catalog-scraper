# Dowirly Amazon.sa Catalog Scraper

Python pipeline for building a reusable Amazon.sa product catalog through Oxylabs Web Scraper API.

The scraper is intentionally **not coupled to a named Oxylabs plan**. It can use an optional user-defined result ceiling, but by default it keeps working until provider access/quota stops it or the configured search space is exhausted.

## Pipeline

1. Discover ASINs using `amazon_search` in balanced category order.
2. Save raw search responses and deduplicate candidates.
3. Enrich candidates using `amazon_product` in bounded durable waves.
4. After every product wave, download and fsync raw results before submitting the next wave.
5. Validate and normalize full products.
6. Write `data/final/products.jsonl` for the DB.
7. Write `data/final/embedding_input.jsonl` for the embedding worker.
8. Persist exact in-flight provider job IDs for reboot/crash recovery.

## Resilience and speed

- Production defaults to 100 full-product jobs per durable wave.
- Old large in-flight backlogs are recovered in those same small waves.
- Provider job submission is asynchronous and concurrent, not product-by-product.
- The configured submit rate is only a probe ceiling; HTTP 429 automatically tunes it downward.
- A completed wave is saved locally before the next wave is submitted.
- `.env` and `data/` are ignored by Git.
- systemd support keeps work running after SSH disconnects and automatically resumes after reboot.
- A process lock prevents a manual scraper and the systemd scraper from running at the same time.

See [RUN.md](RUN.md) for commands and [REPORT.md](REPORT.md) for schemas/output details.

## Official Oxylabs references

- Amazon search: https://developers.oxylabs.io/scraping-solutions/web-scraper-api/targets/amazon/search
- Amazon product: https://developers.oxylabs.io/scraping-solutions/web-scraper-api/targets/amazon/product
- Push-Pull / batch: https://developers.oxylabs.io/scraping-solutions/web-scraper-api/integration-methods/push-pull
- Usage statistics: https://developers.oxylabs.io/scraping-solutions/web-scraper-api/usage-and-billing/usage-statistics
- Rate limits: https://developers.oxylabs.io/scraping-solutions/web-scraper-api/usage-and-billing/rate-limits
- Response codes: https://developers.oxylabs.io/scraper-apis/web-scraper-api/response-codes
