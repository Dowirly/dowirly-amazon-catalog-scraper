# Run Guide

## Install

```bash
git clone https://github.com/Dowirly/dowirly-amazon-catalog-scraper.git
cd dowirly-amazon-catalog-scraper
sudo apt update
sudo apt install -y python3 python3-pip python3-venv
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e ".[dev]"
cp .env.example .env
```

Edit `.env` with the Oxylabs Web Scraper API credentials.

## Validate without spending quota

```bash
source .venv/bin/activate
python -m pytest -q
dowirly-scrape --dry-run --mode test
```

## Production

```bash
dowirly-scrape --mode production
```

There is no named `free` / `micro` / other-plan setting in the scraper. If `--max-products` is omitted, production keeps going until one of these happens:

- the provider refuses more work / subscription access ends;
- an optional `--max-results` ceiling is reached;
- all configured search pages are exhausted;
- the process is stopped.

Example explicit product target:

```bash
dowirly-scrape --mode production --max-products 5000
```

Optional provider-independent result ceiling:

```bash
dowirly-scrape --mode production --max-results 90000
```

## Durable waves

Full-product work is not submitted as one huge provider-side backlog anymore. The production default is:

```text
submit up to 100 product jobs
→ poll/download them
→ save raw results
→ normalize products
→ save products.jsonl + embedding_input.jsonl
→ update checkpoint
→ only then submit the next wave
```

This makes progress visible and limits exposure if provider access disappears at a quota/subscription boundary. Existing old in-flight backlogs are also recovered 100 jobs at a time.

Change the wave size if needed:

```bash
dowirly-scrape --mode production --wave-size 200
```

`--batch-size` remains an alias for `--wave-size` for compatibility.

## Maximum speed without plan coupling

The scraper does not map a subscription name to a fixed rate. It starts from a configurable submission probe ceiling (default 50 jobs/s):

```bash
dowirly-scrape --mode production --submit-rate 100
```

or in `.env`:

```env
OXYLABS_SUBMIT_RATE=100
```

If Oxylabs returns HTTP 429, the submission rate is automatically tuned downward and then cautiously probed upward again. Polling/result downloads are separately paced to avoid burst throttling.

## Reboot-safe background service

Install once:

```bash
bash scripts/install_systemd.sh
```

The service starts immediately, is enabled at boot, and runs:

```text
dowirly-scrape --mode production
```

No named plan is stored in the systemd unit.

Useful commands:

```bash
sudo systemctl status dowirly-amazon-scraper --no-pager -l
sudo systemctl stop dowirly-amazon-scraper
sudo systemctl start dowirly-amazon-scraper
sudo systemctl restart dowirly-amazon-scraper
```

After a VPS reboot, systemd starts the service automatically. The checkpoint contains completed work plus exact provider job IDs for the current in-flight wave, so already-submitted work is resumed rather than blindly re-submitted.

## Monitoring

```bash
bash scripts/status.sh
bash scripts/status.sh --watch 5
```

Live logs:

```bash
sudo journalctl -u dowirly-amazon-scraper -f -o cat
```

Progress-only logs:

```bash
sudo journalctl -u dowirly-amazon-scraper -f -o cat \
  | grep --line-buffered -E 'WAVE \||SUBMIT \||SUBMIT_RATE_ADAPT \||RESUME \||POLL \||PROGRESS \||RATE_LIMIT \||PROVIDER_STOP'
```

## Pull updates without touching runtime data

`.env` and `data/` are ignored by Git.

```bash
cd ~/scripts/dowirly-amazon-catalog-scraper
sudo systemctl stop dowirly-amazon-scraper || true
git pull --ff-only origin main
source .venv/bin/activate
pip install -e ".[dev]"
python -m pytest -q
bash scripts/install_systemd.sh
```

Never use `git clean -fdx` on this checkout because it can remove ignored credentials/runtime data.

## Main outputs

```text
data/final/products.jsonl
```

Full normalized products for the database.

```text
data/final/embedding_input.jsonl
```

Embedding-ready text + metadata keyed by the same product ID.

Partial discovery-only products are kept at:

```text
data/intermediate/unique_candidates.jsonl
```
