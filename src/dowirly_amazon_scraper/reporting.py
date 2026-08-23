from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import PLAN_AMAZON_PRICE_PER_1K_USD, PLAN_BASE_PRICE_USD, AppConfig
from .utils import utc_now_iso


@dataclass(slots=True)
class RunMetrics:
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: datetime | None = None
    usage_before: int = 0
    usage_after: int = 0
    search_jobs: int = 0
    product_jobs: int = 0
    faulted_jobs: int = 0
    discovered_records: int = 0
    unique_candidates: int = 0
    accepted_products: int = 0
    rejected_products: int = 0
    graceful_stop_reason: str | None = None

    @property
    def elapsed_seconds(self) -> float:
        end = self.finished_at or datetime.now(timezone.utc)
        return (end - self.started_at).total_seconds()


def write_run_report(path: Path, config: AppConfig, metrics: RunMetrics) -> None:
    delta = max(0, metrics.usage_after - metrics.usage_before)
    variable_equivalent = delta / 1000 * PLAN_AMAZON_PRICE_PER_1K_USD[config.plan]
    base = PLAN_BASE_PRICE_USD[config.plan]
    duration = _duration(metrics.elapsed_seconds)
    text = f"""# Scrape Run Report

Generated: {utc_now_iso()}

## Run

- Mode: `{config.mode}`
- Plan guard: `{config.plan}`
- Hard result limit: `{config.hard_result_limit:,}`
- Max requested final products: `{config.max_products if config.max_products is not None else 'maximize within budget'}`
- Wall-clock duration: **{duration}**
- Graceful stop reason: `{metrics.graceful_stop_reason or 'completed normally'}`

## Oxylabs usage

- Usage before run: **{metrics.usage_before:,} results**
- Usage after run: **{metrics.usage_after:,} results**
- Usage delta: **{delta:,} results**
- Search jobs submitted: **{metrics.search_jobs:,}**
- Product jobs submitted: **{metrics.product_jobs:,}**
- Faulted jobs observed: **{metrics.faulted_jobs:,}**

Oxylabs documents Amazon no-JS pricing at $0.50/1,000 results on Micro, with a $49 monthly minimum. Free Trial is $0 up to 2,000 results. The variable-equivalent value for this run is **${variable_equivalent:.2f}**; plan base price is **${base:.2f}** before any applicable VAT. The script does not purchase plans or top-ups.

## Catalog

- Discovery records: **{metrics.discovered_records:,}**
- Unique ASIN candidates: **{metrics.unique_candidates:,}**
- Accepted normalized products: **{metrics.accepted_products:,}**
- Rejected products: **{metrics.rejected_products:,}**

## Output files

- `raw/search_results.jsonl` — complete retrieved search-result responses.
- `raw/product_results.jsonl` — complete retrieved product-page responses.
- `raw/job_events.jsonl` — job metadata, faulted jobs, and API lifecycle events.
- `intermediate/discovered_products.jsonl` — every ASIN occurrence discovered in searches.
- `intermediate/unique_candidates.jsonl` — deduplicated ASIN candidates and discovery evidence.
- `intermediate/rejected_products.jsonl` — invalid/low-quality product responses and rejection reasons.
- `intermediate/checkpoint.json` — atomic resume checkpoint.
- `final/products.jsonl` — normalized full product models.
- `final/embedding_input.jsonl` — compact records ready to send to an embeddings provider.

See the repository `REPORT.md` for the field-level schema and billing notes.
"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _duration(seconds: float) -> str:
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"
