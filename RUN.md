# Run Guide

## 1. VPS requirements

Recommended: Ubuntu 22.04/24.04, Python 3.11+, 2 vCPU, 2–4 GB RAM, and 10+ GB free disk for a comfortable Micro-plan run.

## 2. Clone and install

```bash
git clone https://github.com/Dowirly/dowirly-amazon-catalog-scraper.git
cd dowirly-amazon-catalog-scraper
sudo apt update
sudo apt install -y python3 python3-pip python3-venv
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

`.env` and `data/` are ignored by Git. Pulling new code does not publish the VPS credentials or collected catalog.

## 3. Validate without spending quota

```bash
dowirly-scrape --dry-run --mode test --plan free
python -m pip install -e ".[dev]"
python -m pytest -q
```

## 4. Small test

```bash
dowirly-scrape --mode test --plan free --max-products 25
```

## 5. Production / maximum Free Trial

```bash
dowirly-scrape --mode production --plan free
```

If `--max-products` is omitted in production, the scraper attempts to maximize useful full products within the guarded plan result limit.

The Free Trial guard is 2,000 results. The code also keeps a **local completed-job usage floor** because Oxylabs usage statistics may lag. This means jobs already completed by this scraper still reduce the remaining guarded capacity even when the provider stats endpoint temporarily says `0`.

The final useful product count will be lower than 2,000 because search discovery consumes results and incomplete/invalid product pages can be rejected.

## 6. Maximum safe speed

Oxylabs documents a job-submission rate of **10 jobs/s on Free Trial** and **50 jobs/s on Micro**. A batch request containing multiple query values still creates one provider job per value.

The scraper therefore automatically splits large logical batches into plan-sized chunks and submits them about once per second:

- Free Trial: up to 10 new jobs per submission window.
- Micro: up to 50 new jobs per submission window.

This keeps many Oxylabs Push-Pull jobs executing in parallel while staying just under the advertised submission limit. It is not a sequential product-by-product scraper.

Logs show submission and completion progress:

```text
SUBMIT | accepted_jobs=100/1800 | last_chunk=10 | safe_plan_rate=10 jobs/s
POLL | completed_jobs=350/1800
```

HTTP 429 is handled with adaptive backoff and retry instead of immediately killing the run. Oxylabs documents 429 rate-limit responses as unbilled.

## 7. Never run two scraper instances

Do not manually start `dowirly-scrape --mode production` while the systemd service is already running. Both processes would share the same Oxylabs account/rate limit.

The CLI now holds a Linux process lock. A second non-dry-run instance exits immediately with a clear message instead of competing for quota.

Check whether the service is already running:

```bash
sudo systemctl status dowirly-amazon-scraper --no-pager
```

## 8. Reboot-safe background execution (recommended)

For long runs, use the included systemd installer instead of tmux:

```bash
bash scripts/install_systemd.sh free
```

This creates and enables `dowirly-amazon-scraper.service`, starts it immediately, and starts it again automatically after a VPS reboot.

The scraper has two recovery layers:

1. Raw/final JSONL files are append-only and fsynced to disk.
2. `data/intermediate/checkpoint.json` stores completed search/product work **and the exact Oxylabs IDs of any submitted in-flight batch**.

During a large rate-paced batch, accepted provider job IDs are checkpointed after every submission chunk. If the VPS dies halfway through submitting or polling a batch, the restarted program polls those already-created Oxylabs job IDs instead of blindly re-submitting them.

Useful service commands:

```bash
sudo systemctl status dowirly-amazon-scraper --no-pager
sudo systemctl stop dowirly-amazon-scraper
sudo systemctl start dowirly-amazon-scraper
sudo systemctl restart dowirly-amazon-scraper
```

Disable automatic startup if you no longer want it:

```bash
sudo systemctl disable --now dowirly-amazon-scraper
```

## 9. Monitor progress

While Oxylabs jobs are being submitted and processed, logs emit `SUBMIT`, `POLL`, and `PROGRESS` records.

Follow logs live:

```bash
sudo journalctl -u dowirly-amazon-scraper -f -o cat
```

Only progress lines:

```bash
sudo journalctl -u dowirly-amazon-scraper -f -o cat | grep --line-buffered -E 'SUBMIT \||PROGRESS \||POLL \||RESUME \||RATE_LIMIT \|'
```

One-time file/service summary:

```bash
bash scripts/status.sh
```

Continuously refresh counts every five seconds:

```bash
bash scripts/status.sh --watch 5
```

The status script shows accepted products, rejected products, unique candidates, raw responses, completed checkpoint counts, in-flight jobs, systemd state, and the latest progress log.

## 10. Pull future code changes without touching `.env` or data

From a real Git clone:

```bash
cd ~/scripts/dowirly-amazon-catalog-scraper
git pull --ff-only origin main
source .venv/bin/activate
pip install -e .
sudo systemctl restart dowirly-amazon-scraper
```

Do not use `git clean -fdx`; that could remove ignored `.env`/runtime data.

## 11. Exact product/result targets

```bash
dowirly-scrape --mode production --plan free --max-products 500
dowirly-scrape --mode production --plan free --max-products 500 --max-results 700
```

`--max-results` is a hard guarded result count for the period, not an amount to add on top of existing usage.

## 12. Micro plan (never above the $49 base tier)

After manually subscribing to Micro:

```bash
dowirly-scrape --mode production --plan micro
```

or install the reboot-safe service for Micro:

```bash
bash scripts/install_systemd.sh micro
```

Micro is guarded at 98,000 Amazon no-JS results. The code intentionally supports no higher plan and buys no top-ups. Oxylabs lists Micro at $49/month before applicable VAT.

## 13. Balanced categories

`config/catalog_queries.yaml` remains grouped by category for humans, but the loader consumes it round-robin: first query from every category, then second query from every category, and so on. A limited Free Trial therefore does not get spent mostly on Mobile/Computers just because those groups appear first in YAML.

Final product categories still come from the real Amazon product breadcrumb, not merely the discovery label.

## Useful switches

```text
--max-products N
--max-results N
--batch-size N
--poll-concurrency N
--dedupe-parent-asin
--include-paid
--allow-missing-price
--allow-missing-image
--allow-missing-category
--verbose
```
