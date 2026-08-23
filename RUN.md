# Run Guide

## 1. VPS requirements

Recommended: Ubuntu 22.04/24.04, Python 3.11+, 2 vCPU, 2–4 GB RAM, and enough disk for JSONL outputs. Raw results can become large on a Micro-plan run; 10+ GB free disk is a comfortable starting point.

## 2. Clone and install

```bash
git clone https://github.com/Dowirly/dowirly-amazon-catalog-scraper.git
cd dowirly-amazon-catalog-scraper
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e .
cp .env.example .env
```

Edit `.env`:

```env
OXYLABS_USERNAME=your_api_username
OXYLABS_PASSWORD=your_api_password
OXYLABS_DOMAIN=sa
OXYLABS_LOCALE=en_AE
```

Do not commit `.env`.

## 3. Validate without spending quota

```bash
dowirly-scrape --dry-run --mode test --plan free
```

Run tests:

```bash
python -m pip install -e ".[dev]"
python -m pytest -q
```

## 4. Small test run

```bash
dowirly-scrape --mode test --plan free --max-products 25
```

`test` defaults to 25 normalized products when `--max-products` is omitted.

## 5. Use the rest of the Free Trial

```bash
dowirly-scrape --mode production --plan free
```

The script checks `GET https://data.oxylabs.io/v2/stats` first. If some of the 2,000 trial results are already used, it uses only the remaining guarded capacity. Your earlier manual 4xx product tests may count because Oxylabs documents 2xx and 4xx target results as billable.

The exact final product count will be **below the remaining Oxylabs-result count** because discovery search pages also consume results and some product pages may be rejected. The pipeline dynamically stops search discovery once it has enough ASIN candidates, preserving as much quota as possible for full `amazon_product` pages.

## 6. Specify an exact product goal

```bash
dowirly-scrape --mode production --plan free --max-products 500
```

Or place an even stricter Oxylabs hard cap:

```bash
dowirly-scrape --mode production --plan free --max-products 500 --max-results 700
```

`--max-results` is a cap on total official usage visible for the guarded period, not "extra requests from now".

## 7. Micro plan (never above the $49 tier)

After you manually subscribe to Oxylabs Micro:

```bash
dowirly-scrape --mode production --plan micro
```

Micro is guarded at 98,000 results. The program intentionally does not support Starter or higher plans. It also does not purchase top-ups. Oxylabs lists Micro at $49/month before applicable VAT, so VAT can make the invoice exceed $50 even though the base plan is $49.

For a target instead of consuming the whole remaining plan:

```bash
dowirly-scrape --mode production --plan micro --max-products 90000
```

90,000 **clean full products is not guaranteed** inside 98,000 total results because discovery and billable invalid 4xx pages also use the quota. The script maximizes what can safely fit.

## 8. Resume after disconnect/reboot

Simply run the same command again:

```bash
dowirly-scrape --mode production --plan free
```

`data/intermediate/checkpoint.json` remembers completed discovery waves and product ASINs. JSONL files are append-only. The official Oxylabs usage endpoint is re-checked before any new batch.

## 9. Keep it running over SSH

`tmux` is recommended:

```bash
sudo apt-get update && sudo apt-get install -y tmux
tmux new -s amazon-scrape
source .venv/bin/activate
dowirly-scrape --mode production --plan free
```

Detach with `Ctrl+B`, then `D`. Reattach:

```bash
tmux attach -t amazon-scrape
```

## Useful switches

```text
--max-products N              final normalized product target
--max-results N               hard Oxylabs result-usage cap
--batch-size N                product Push-Pull batch size (max 5000)
--poll-concurrency N          concurrent status/result fetches
--dedupe-parent-asin          keep one variation per parent ASIN
--include-paid                include sponsored search listings as candidates
--allow-missing-price         don't reject missing-price products
--allow-missing-image         don't reject missing-image products
--allow-missing-category      don't reject missing-category products
--verbose                     debug logging
```

## Categories

Edit `config/catalog_queries.yaml`. The labels there are discovery labels. The final category path is taken from the full Amazon product page's parsed `category[].ladder` field, so final records contain the real Amazon category breadcrumbs.
