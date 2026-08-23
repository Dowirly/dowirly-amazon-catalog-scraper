# Dowirly Amazon.sa Catalog Scraper

Python pipeline for building a test catalog from public Amazon.sa product pages through **Oxylabs Web Scraper API**.

It is deliberately budget-aware: only Oxylabs **Free Trial (2,000 results)** and **Micro (98,000 Amazon no-JS results, $49/month before applicable VAT)** are supported. The scraper never buys, upgrades, or tops up a plan.

## Pipeline

1. Discover ASINs with `amazon_search`.
2. Save every retrieved search response immediately to JSONL.
3. Extract ASINs (including variations), deduplicate, and checkpoint.
4. Enrich candidates with `amazon_product` via asynchronous Push-Pull batch jobs.
5. Save every raw product response before filtering.
6. Reject invalid/incomplete records and optionally duplicate parent-ASIN variants.
7. Normalize accepted products into a stable Dowirly product schema.
8. Create `embedding_input.jsonl` with semantic text that intentionally excludes volatile price/stock/review-count values.
9. Generate a measured run report with duration, counts, Oxylabs usage delta, and price notes.

See [RUN.md](RUN.md) for VPS commands and [REPORT.md](REPORT.md) for output/schema details.

## Safety / resilience

- Official Oxylabs usage statistics are read before each submission wave.
- Every job requests exactly one target page, making quota reservation deterministic.
- Batches finish before usage is refreshed; faulted/unbilled jobs return capacity on the next wave.
- Raw records are append-only JSONL and flushed/fsynced immediately.
- Atomic checkpoint permits restart with the same command.
- SIGINT/SIGTERM causes a graceful stop after in-flight work; collected data remains on disk.
- HTTP 429/5xx/network failures are retried with backoff.
- 2xx/4xx target results are treated as potentially billable, matching Oxylabs billing documentation.

## Official Oxylabs references

- Web Scraper API pricing: https://oxylabs.io/products/scraper-api/web/pricings
- Amazon search: https://developers.oxylabs.io/scraping-solutions/web-scraper-api/targets/amazon/search
- Amazon product: https://developers.oxylabs.io/scraping-solutions/web-scraper-api/targets/amazon/product
- Push-Pull / batch: https://developers.oxylabs.io/scraping-solutions/web-scraper-api/integration-methods/push-pull
- Usage statistics: https://developers.oxylabs.io/scraping-solutions/web-scraper-api/usage-and-billing/usage-statistics
- Billing: https://developers.oxylabs.io/scraping-solutions/web-scraper-api/usage-and-billing/billing-information
- Rate limits: https://developers.oxylabs.io/scraping-solutions/web-scraper-api/usage-and-billing/rate-limits
- Response codes: https://developers.oxylabs.io/scraper-apis/web-scraper-api/response-codes
