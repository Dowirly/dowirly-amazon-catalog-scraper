#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="${DATA_DIR:-$ROOT/data}"
SERVICE="${SCRAPER_SERVICE:-dowirly-amazon-scraper.service}"
WATCH=false
INTERVAL=5

if [[ "${1:-}" == "--watch" || "${1:-}" == "-w" ]]; then
  WATCH=true
  INTERVAL="${2:-5}"
fi

count_lines() {
  local file="$1"
  if [[ -f "$file" ]]; then
    awk 'NF {n++} END {print n+0}' "$file"
  else
    echo 0
  fi
}

render() {
  local accepted rejected candidates discovered raw_products raw_search service_state
  accepted="$(count_lines "$DATA_DIR/final/products.jsonl")"
  rejected="$(count_lines "$DATA_DIR/intermediate/rejected_products.jsonl")"
  candidates="$(count_lines "$DATA_DIR/intermediate/unique_candidates.jsonl")"
  discovered="$(count_lines "$DATA_DIR/intermediate/discovered_products.jsonl")"
  raw_products="$(count_lines "$DATA_DIR/raw/product_results.jsonl")"
  raw_search="$(count_lines "$DATA_DIR/raw/search_results.jsonl")"

  echo "Dowirly Amazon scraper status"
  echo "============================"
  echo "Accepted full products : $accepted"
  echo "Rejected products      : $rejected"
  echo "Unique candidates      : $candidates"
  echo "Discovery occurrences  : $discovered"
  echo "Raw product responses  : $raw_products"
  echo "Raw search responses   : $raw_search"

  if [[ -f "$DATA_DIR/intermediate/checkpoint.json" ]]; then
    python3 - "$DATA_DIR/intermediate/checkpoint.json" <<'PY'
import json, sys
from pathlib import Path
p=Path(sys.argv[1])
try:
    data=json.loads(p.read_text(encoding='utf-8'))
except Exception as exc:
    print(f"Checkpoint             : unreadable ({exc})")
else:
    inflight=data.get('inflight_jobs') or {}
    jobs=sum(len((v or {}).get('jobs') or []) for v in inflight.values())
    phases=', '.join(sorted(inflight)) or 'none'
    print(f"Completed search keys  : {len(data.get('completed_search_keys') or [])}")
    print(f"Completed product ASINs: {len(data.get('completed_product_asins') or [])}")
    print(f"In-flight jobs         : {jobs} ({phases})")
PY
  fi

  service_state="unknown"
  if command -v systemctl >/dev/null 2>&1; then
    service_state="$(systemctl is-active "$SERVICE" 2>/dev/null || true)"
    echo "Service state          : $service_state"
  fi

  if command -v journalctl >/dev/null 2>&1; then
    local latest
    latest="$(journalctl -u "$SERVICE" -n 400 --no-pager -o cat 2>/dev/null | grep -E 'WAVE \||RESUME \||SUBMIT \||SUBMIT_RATE_ADAPT \||POLL \||PROGRESS \||PROVIDER_STOP' | tail -n 1 || true)"
    if [[ -n "$latest" ]]; then
      echo
      echo "Latest scraper activity:"
      echo "$latest"
    fi

    local stop_line
    stop_line="$(journalctl -u "$SERVICE" -n 400 --no-pager -o cat 2>/dev/null | grep -E 'PROVIDER_STOP|Graceful provider stop|AUTH_FAILURE|SUBMIT_STOP' | tail -n 1 || true)"
    if [[ -n "$stop_line" ]]; then
      echo
      echo "Latest provider stop/boundary:"
      echo "$stop_line"
    fi

    if [[ "$service_state" == "failed" ]]; then
      local last_error
      last_error="$(journalctl -u "$SERVICE" -n 120 --no-pager -o cat 2>/dev/null | grep -E 'ERROR|Traceback|RATE_LIMIT|failed|Failed' | tail -n 8 || true)"
      if [[ -n "$last_error" ]]; then
        echo
        echo "Recent failure details:"
        echo "$last_error"
      fi
    fi
  fi
}

if $WATCH; then
  while true; do
    clear || true
    date
    render
    sleep "$INTERVAL"
  done
else
  render
fi
