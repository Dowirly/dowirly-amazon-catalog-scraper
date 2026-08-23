from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from .config import AppConfig
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
    duration = _duration(metrics.elapsed_seconds)
    configured_limit = (
        f"{config.max_results:,}"
        if config.max_results is not None
        else "provider-managed / no local result cap"
    )
    max_products = (
        str(config.max_products)
        if config.max_products is not None
        else "unbounded until provider/search exhaustion"
    )

    text = f"""# Scrape Run Report

Generated: {utc_now_iso()}

## Run

- Mode: `{config.mode}`
- Local result ceiling: `{configured_limit}`
- Max requested final products: `{max_products}`
- Product wave size: **{config.wave_size:,}**
- Search wave size: **{config.search_wave_size:,}**
- Submission probe ceiling: **{config.submit_rate:,} jobs/s** (automatically reduced on HTTP 429)
- Wall-clock duration: **{duration}**
- Stop reason: `{metrics.graceful_stop_reason or 'completed normally'}`

## Oxylabs usage observed

- Usage before run: **{metrics.usage_before:,} results**
- Usage after run: **{metrics.usage_after:,} results**
- Usage delta: **{delta:,} results**
- Search jobs submitted this run: **{metrics.search_jobs:,}**
- Product jobs submitted this run: **{metrics.product_jobs:,}**
- Faulted jobs observed: **{metrics.faulted_jobs:,}**

The scraper does not encode or assume a named Oxylabs plan. If `--max-results` is omitted, provider-side quota/subscription enforcement is authoritative. Product work is submitted and saved in bounded waves so already downloaded results remain usable even if later work is refused.

## Catalog

- Discovery records added this run: **{metrics.discovered_records:,}**
- Unique ASIN candidates observed: **{metrics.unique_candidates:,}**
- Accepted normalized products: **{metrics.accepted_products:,}**
- Rejected products this run: **{metrics.rejected_products:,}**

## Output files

- `raw/search_results.jsonl` — complete retrieved search-result responses.
- `raw/product_results.jsonl` — complete retrieved product-page responses.
- `raw/job_events.jsonl` — job metadata, faulted jobs, and API lifecycle events.
- `intermediate/discovered_products.jsonl` — every ASIN occurrence discovered in searches.
- `intermediate/unique_candidates.jsonl` — deduplicated ASIN candidates and discovery evidence.
- `intermediate/rejected_products.jsonl` — invalid/low-quality product responses and rejection reasons.
- `intermediate/checkpoint.json` — atomic resume checkpoint, including provider-side in-flight jobs.
- `final/products.jsonl` — normalized full product models.
- `final/embedding_input.jsonl` — compact records ready for an embeddings provider.
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
