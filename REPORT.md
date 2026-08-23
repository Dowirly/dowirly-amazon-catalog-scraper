# Output, Timing, Pricing, and Error Report

## Output files

All runtime data lives under `data/` and is intentionally ignored by Git because it can become very large and contains marketplace-derived content.

### `data/raw/search_results.jsonl`
One line per completed Amazon search job. It stores the entire parsed Oxylabs result wrapper before any filtering.

### `data/intermediate/discovered_products.jsonl`
One line for every ASIN occurrence discovered from Amazon search output. Duplicates are expected here; this is an audit trail.

### `data/intermediate/unique_candidates.jsonl`
Deduplicated ASINs with all queries/categories/buckets that discovered them.

### `data/raw/product_results.jsonl`
One line per retrieved full Amazon product result. This is written **before** validation/normalization so no fetched data is lost.

### `data/intermediate/rejected_products.jsonl`
Rejected ASIN + reason(s), such as:

```json
{"asin":"B0EXAMPLE1","reasons":["missing_images","missing_category"],"job_id":"..."}
```

### `data/final/products.jsonl`
One normalized full product per line. Representative shape:

```json
{
  "schema_version": "1.0",
  "id": "amazon-sa:B0CHXS73N7",
  "source": "amazon",
  "marketplace": "amazon.sa",
  "external_id": "B0CHXS73N7",
  "parent_external_id": null,
  "url": "https://www.amazon.sa/dp/B0CHXS73N7",
  "title": "Apple iPhone 15 (128 GB) - Blue",
  "brand": "Apple",
  "manufacturer": "Apple",
  "description": "...",
  "bullet_points": ["..."],
  "category": {
    "primary": "Electronics",
    "leaf": "Mobile Phones",
    "path": ["Electronics", "Mobiles & Accessories", "Mobile Phones"],
    "breadcrumbs": [{"name":"Electronics","url":"..."}],
    "discovery_labels": ["Mobile Phones & Accessories"]
  },
  "images": ["https://..."],
  "pricing": {"currency":"SAR","current":2299.0,"initial":2459.0},
  "availability": {"stock":"In stock","prime_eligible":true},
  "rating": {"value":4.5,"reviews_count":1121,"distribution":[]},
  "seller": {"name":"...","seller_id":"...","is_amazon_fulfilled":true},
  "sales_rank": [],
  "variations": [],
  "delivery": [],
  "badges": {"amazon_choice":true,"has_videos":false},
  "attributes": {
    "product_overview": [{"title":"Brand","description":"Apple"}],
    "product_details": {},
    "developer_info": {},
    "product_dimensions": null,
    "item_form": null,
    "warranty_and_support": null
  },
  "embedding_text": "Title: ...\n\nBrand: ...\n\nCategory: ..."
}
```

### `data/final/embedding_input.jsonl`
Directly usable as the input layer for an embeddings worker:

```json
{"id":"amazon-sa:B0CHXS73N7","external_id":"B0CHXS73N7","text":"Title: ...","metadata":{"source":"amazon","marketplace":"amazon.sa","category":{},"brand":"Apple"}}
```

`embedding_text` deliberately excludes price, stock, and review counts because those values change frequently and work better as database filters/ranking signals.

## Runtime report

Every run generates `data/reports/run-<UTC timestamp>.md` containing measured wall-clock duration, guarded Oxylabs usage before/after, search/product jobs, faulted jobs, accepted/rejected counts, and the graceful-stop reason. The guarded usage is the maximum of provider-reported usage and the scraper's locally observed completed-job floor, because usage statistics can lag on fresh/free accounts.

There is no honest fixed time estimate before running because Oxylabs/Amazon latency varies. Push-Pull batch jobs run asynchronously at Oxylabs; the VPS is not scraping product pages sequentially. The measured run report is authoritative for the actual run.

## Pricing / quota facts used by the script

Current official Oxylabs Web Scraper API pricing used for guardrails:

| Plan | Max results | Job submission rate | Base price |
|---|---:|---:|---:|
| Free Trial | 2,000 | 10 jobs/s | $0 |
| Micro | 98,000 Amazon no-JS results | 50 jobs/s | $49/month before VAT |

Oxylabs defines a result as a distinct retrieved content entity/page. Target-site `2xx` and `4xx` results are billable; provider/system `5xx`/`6xx` failures are not. `429` rate-limit results are not billed. The usage-statistics endpoint itself is free.

The script uses `parse=true` without JS rendering for the Amazon dedicated parsers. It does not enable image/media download billing.

## Why Free Trial cannot produce 2,000 full products

A search page used to discover ASINs is itself a result. A full `amazon_product` page is another result. Therefore a 2,000-result trial must be split between discovery and enrichment. The pipeline uses discovery incrementally and stops as soon as it has enough unique candidates, maximizing the remaining result slots for full product pages.

## Why 90,000 clean products are tight on Micro

90,000 product pages consume about 90,000 of the 98,000-result maximum if each page is billable. Discovery pages and billable invalid `4xx` product pages also consume results. The program will safely maximize accepted products, but it will not exceed the 98,000 guard to chase a target.

## Error behavior

- `SIGINT` / `SIGTERM`: graceful stop; in-flight wave finishes where possible; files/checkpoint remain valid.
- VPS/process reboot during a submitted batch: exact in-flight Oxylabs job IDs are persisted before polling and are polled again after restart rather than submitted again.
- HTTP `429`: exponential retry; documented as unbilled.
- HTTP `5xx` / network timeout: retry with backoff.
- HTTP `401`: stop because credentials are invalid.
- HTTP `400` / `422`: stop because request shape is invalid.
- Quota-like HTTP `403`: graceful quota stop.
- Oxylabs job `faulted`: recorded; not treated as a product; official stats are refreshed so unbilled capacity can be reused.
- Amazon/target `4xx`: raw response is stored and the product is rejected; the request may still be billable according to Oxylabs.
- Parser `12000`: full success.
- Parser `12004` / `12005`: accepted only if required product fields are actually present.
- Other parser failure codes: rejected.

## Official docs

- https://oxylabs.io/products/scraper-api/web/pricings
- https://developers.oxylabs.io/scraping-solutions/web-scraper-api/targets/amazon/search
- https://developers.oxylabs.io/scraping-solutions/web-scraper-api/targets/amazon/product
- https://developers.oxylabs.io/scraping-solutions/web-scraper-api/integration-methods/push-pull
- https://developers.oxylabs.io/scraping-solutions/web-scraper-api/usage-and-billing/usage-statistics
- https://developers.oxylabs.io/scraping-solutions/web-scraper-api/usage-and-billing/billing-information
- https://developers.oxylabs.io/scraping-solutions/web-scraper-api/usage-and-billing/rate-limits
- https://developers.oxylabs.io/scraper-apis/web-scraper-api/response-codes
