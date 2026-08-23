# Output, Recovery, and Error Report

## Runtime outputs

All runtime data lives under `data/` and is ignored by Git.

- `data/raw/search_results.jsonl` — full retrieved Amazon search responses.
- `data/intermediate/discovered_products.jsonl` — every ASIN occurrence from search.
- `data/intermediate/unique_candidates.jsonl` — deduplicated partial products discovered from search.
- `data/raw/product_results.jsonl` — full retrieved Amazon product responses, written before normalization.
- `data/intermediate/rejected_products.jsonl` — rejected full-product results and reasons.
- `data/intermediate/checkpoint.json` — completed work plus exact in-flight provider job IDs.
- `data/final/products.jsonl` — normalized full products for the database.
- `data/final/embedding_input.jsonl` — embedding-ready semantic text and metadata.
- `data/reports/run-<UTC>.md` — measured run summary.

## Durable product waves

The scraper no longer submits all remaining product work and waits for one giant batch. Production defaults to 100 full products per durable wave:

```text
submit wave
→ poll/download wave
→ save raw product responses
→ validate/normalize
→ save final product + embedding input
→ checkpoint completion
→ submit next wave
```

This means accepted product counts increase continuously during a long run. If the VPS, network, or provider access fails later, earlier completed waves are already local and usable.

An old checkpoint containing a large in-flight product batch is migrated naturally: the saved provider job IDs are polled and processed only `wave_size` at a time without duplicate submission.

## No named-plan coupling

The scraper contains no `free`, `micro`, or other subscription-plan mapping.

Without `--max-results`, it does not guess an account quota. It keeps working until provider enforcement stops new/API work, `--max-products` is reached, or the configured search space is exhausted.

If an explicit result ceiling is desired, it can be set independently:

```bash
dowirly-scrape --mode production --max-results 50000
```

## Throughput

`--submit-rate` is a configurable probe ceiling, not a plan selection. The default is 50 jobs/s. HTTP 429 during submission causes the scraper to lower the active rate automatically. After stable windows it cautiously probes upward again.

Product wave size and submission rate are separate concepts: a 100-product wave can still be submitted rapidly and processed concurrently, but the next wave is not submitted until the current one is downloaded and saved.

## Final product model

`data/final/products.jsonl` contains one normalized product per line, including fields such as:

```json
{
  "id": "amazon-sa:B0...",
  "source": "amazon",
  "marketplace": "amazon.sa",
  "external_id": "B0...",
  "url": "https://www.amazon.sa/dp/B0...",
  "title": "...",
  "brand": "...",
  "manufacturer": "...",
  "description": "...",
  "bullet_points": [],
  "category": {
    "primary": "...",
    "leaf": "...",
    "path": [],
    "breadcrumbs": [],
    "discovery_labels": []
  },
  "images": [],
  "pricing": {},
  "availability": {},
  "rating": {},
  "seller": {},
  "sales_rank": [],
  "variations": [],
  "attributes": {},
  "embedding_text": "..."
}
```

## Embedding input

`data/final/embedding_input.jsonl` is keyed to the same product ID:

```json
{
  "id": "amazon-sa:B0...",
  "external_id": "B0...",
  "text": "Title: ...\nBrand: ...\nCategory: ...",
  "metadata": {
    "source": "amazon",
    "marketplace": "amazon.sa",
    "category": {},
    "brand": "..."
  }
}
```

Dynamic price/stock/review counts are intentionally excluded from embedding text and should remain structured DB/ranking fields.

## Partial product export

Even before full-product enrichment, `data/intermediate/unique_candidates.jsonl` is useful as a partial/staging catalog. It can contain ASIN, title, URL, image, price, currency, rating, review count, manufacturer, and discovery evidence when those values were present in Amazon search results.

## Error and recovery behavior

- VPS reboot/process crash: exact current-wave job IDs remain in the checkpoint and are resumed.
- SSH disconnect: systemd run is unaffected.
- HTTP 429 during submit: adaptive rate reduction; no plan-specific rate assumption.
- HTTP 429 during polling/results: paced retry/backoff.
- HTTP 5xx/network failure: retry/backoff; systemd can restart if the process eventually exits.
- HTTP 401: authentication/subscription access stop. Already-local waves remain valid; any current in-flight provider IDs remain checkpointed.
- Quota-like HTTP 403: graceful provider stop with checkpoint preserved.
- Provider-faulted jobs: optionally retried, then rejected if still faulted.
- Final product and embedding files are append-only/fsynced and deduplicated by ASIN on resume.

## Important boundary behavior

No client can guarantee that a provider will still allow result downloads after it hard-disables API access at an unknown quota/subscription boundary. The wave design minimizes this exposure: at most the current small wave remains provider-side, instead of thousands of products. If the provider allows the current wave to be downloaded, it is saved before any new wave is submitted.
