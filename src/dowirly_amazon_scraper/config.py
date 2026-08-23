from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

DEFAULT_SUBMIT_RATE = 50
DEFAULT_PRODUCT_WAVE_SIZE = 100
DEFAULT_SEARCH_WAVE_SIZE = 18


@dataclass(slots=True)
class AppConfig:
    username: str
    password: str
    domain: str
    locale: str
    geo_location: str | None
    mode: str
    max_products: int | None
    max_results: int | None
    query_config: Path
    data_dir: Path
    wave_size: int
    search_wave_size: int
    submit_rate: int
    poll_concurrency: int
    poll_interval_seconds: float
    max_job_retries: int
    require_price: bool
    require_image: bool
    require_category: bool
    dedupe_parent_asin: bool
    include_paid_search_results: bool
    dry_run: bool

    @property
    def hard_result_limit(self) -> int | None:
        return self.max_results


@dataclass(slots=True)
class SearchQuery:
    logical_category: str
    query: str


@dataclass(slots=True)
class SearchPlan:
    queries: list[SearchQuery]
    sorts: list[str]
    max_pages_per_query: int


def load_search_plan(path: Path) -> SearchPlan:
    with path.open("r", encoding="utf-8") as f:
        raw: dict[str, Any] = yaml.safe_load(f) or {}

    # Keep discovery balanced across categories: first query from every category,
    # then second query from every category, and so on.
    category_queries: list[list[SearchQuery]] = []
    for category in raw.get("categories", []):
        label = str(category["name"]).strip()
        items: list[SearchQuery] = []
        for query in category.get("queries", []):
            query = str(query).strip()
            if query:
                items.append(SearchQuery(label, query))
        if items:
            category_queries.append(items)

    queries: list[SearchQuery] = []
    max_depth = max((len(items) for items in category_queries), default=0)
    for index in range(max_depth):
        for items in category_queries:
            if index < len(items):
                queries.append(items[index])

    if not queries:
        raise ValueError(f"No search queries found in {path}")

    sorts = [str(v) for v in raw.get("sorts", ["featured", "bestsellers"])]
    pages = int(raw.get("max_pages_per_query", 5))
    return SearchPlan(queries=queries, sorts=sorts, max_pages_per_query=pages)


def _optional_positive_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    parsed = int(value)
    if parsed <= 0:
        return None
    return parsed


def build_config(args: Any) -> AppConfig:
    load_dotenv()

    username = os.getenv("OXYLABS_USERNAME", "").strip()
    password = os.getenv("OXYLABS_PASSWORD", "").strip()
    if not args.dry_run and (not username or not password):
        raise ValueError(
            "OXYLABS_USERNAME and OXYLABS_PASSWORD are required. Copy .env.example to .env."
        )

    mode = (args.mode or os.getenv("SCRAPER_MODE", "test")).lower()
    if mode not in {"test", "production"}:
        raise ValueError("mode must be test or production")

    if args.max_products is not None:
        max_products = args.max_products
    elif mode == "test":
        max_products = 25
    else:
        max_products = None

    max_results = _optional_positive_int(
        args.max_results
        if args.max_results is not None
        else os.getenv("SCRAPER_MAX_RESULTS")
    )

    submit_rate = max(
        1,
        int(
            args.submit_rate
            if args.submit_rate is not None
            else os.getenv("OXYLABS_SUBMIT_RATE", DEFAULT_SUBMIT_RATE)
        ),
    )

    default_wave = 25 if mode == "test" else DEFAULT_PRODUCT_WAVE_SIZE
    wave_size = min(
        5_000,
        max(
            1,
            int(
                args.wave_size
                if args.wave_size is not None
                else os.getenv("SCRAPER_WAVE_SIZE", default_wave)
            ),
        ),
    )
    search_wave_size = min(
        5_000,
        max(
            1,
            int(
                args.search_wave_size
                if args.search_wave_size is not None
                else os.getenv("SCRAPER_SEARCH_WAVE_SIZE", DEFAULT_SEARCH_WAVE_SIZE)
            ),
        ),
    )

    default_poll = max(20, submit_rate)
    poll_concurrency = max(
        1,
        int(
            args.poll_concurrency
            if args.poll_concurrency is not None
            else os.getenv("SCRAPER_POLL_CONCURRENCY", default_poll)
        ),
    )

    root = Path(args.project_root).resolve()
    return AppConfig(
        username=username,
        password=password,
        domain=os.getenv("OXYLABS_DOMAIN", "sa"),
        locale=os.getenv("OXYLABS_LOCALE", "en_AE"),
        geo_location=os.getenv("OXYLABS_GEO_LOCATION") or None,
        mode=mode,
        max_products=max_products,
        max_results=max_results,
        query_config=(root / args.query_config).resolve(),
        data_dir=(root / args.data_dir).resolve(),
        wave_size=wave_size,
        search_wave_size=search_wave_size,
        submit_rate=submit_rate,
        poll_concurrency=poll_concurrency,
        poll_interval_seconds=max(0.5, args.poll_interval),
        max_job_retries=max(0, args.job_retries),
        require_price=not args.allow_missing_price,
        require_image=not args.allow_missing_image,
        require_category=not args.allow_missing_category,
        dedupe_parent_asin=args.dedupe_parent_asin,
        include_paid_search_results=args.include_paid,
        dry_run=args.dry_run,
    )
