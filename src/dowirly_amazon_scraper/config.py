from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

PLAN_LIMITS = {"free": 2_000, "micro": 98_000}
PLAN_RATES = {"free": 10, "micro": 50}
PLAN_BASE_PRICE_USD = {"free": 0.0, "micro": 49.0}
PLAN_AMAZON_PRICE_PER_1K_USD = {"free": 0.0, "micro": 0.50}


@dataclass(slots=True)
class AppConfig:
    username: str
    password: str
    domain: str
    locale: str
    geo_location: str | None
    plan: str
    mode: str
    max_products: int | None
    max_results: int | None
    query_config: Path
    data_dir: Path
    batch_size: int
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
    def plan_limit(self) -> int:
        return PLAN_LIMITS[self.plan]

    @property
    def hard_result_limit(self) -> int:
        return min(self.plan_limit, self.max_results or self.plan_limit)

    @property
    def submit_rate(self) -> int:
        return PLAN_RATES[self.plan]


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
    queries: list[SearchQuery] = []
    for category in raw.get("categories", []):
        label = str(category["name"]).strip()
        for query in category.get("queries", []):
            query = str(query).strip()
            if query:
                queries.append(SearchQuery(label, query))
    if not queries:
        raise ValueError(f"No search queries found in {path}")
    sorts = [str(v) for v in raw.get("sorts", ["featured", "bestsellers"])]
    pages = int(raw.get("max_pages_per_query", 5))
    return SearchPlan(queries=queries, sorts=sorts, max_pages_per_query=pages)


def build_config(args: Any) -> AppConfig:
    load_dotenv()
    username = os.getenv("OXYLABS_USERNAME", "").strip()
    password = os.getenv("OXYLABS_PASSWORD", "").strip()
    if not args.dry_run and (not username or not password):
        raise ValueError("OXYLABS_USERNAME and OXYLABS_PASSWORD are required. Copy .env.example to .env.")

    plan = (args.plan or os.getenv("SCRAPER_PLAN", "free")).lower()
    mode = (args.mode or os.getenv("SCRAPER_MODE", "test")).lower()
    if plan not in PLAN_LIMITS:
        raise ValueError(f"Unsupported plan {plan!r}. Only free and micro are intentionally supported to stay under $50 base price.")
    if mode not in {"test", "production"}:
        raise ValueError("mode must be test or production")

    if args.max_products is not None:
        max_products = args.max_products
    elif mode == "test":
        max_products = 25
    else:
        max_products = None

    default_batch = 50 if mode == "test" else 5_000
    default_poll = 20 if plan == "free" else 100
    root = Path(args.project_root).resolve()

    return AppConfig(
        username=username,
        password=password,
        domain=os.getenv("OXYLABS_DOMAIN", "sa"),
        locale=os.getenv("OXYLABS_LOCALE", "en_AE"),
        geo_location=os.getenv("OXYLABS_GEO_LOCATION") or None,
        plan=plan,
        mode=mode,
        max_products=max_products,
        max_results=args.max_results,
        query_config=(root / args.query_config).resolve(),
        data_dir=(root / args.data_dir).resolve(),
        batch_size=min(max(1, args.batch_size or default_batch), 5_000),
        poll_concurrency=max(1, args.poll_concurrency or default_poll),
        poll_interval_seconds=max(0.5, args.poll_interval),
        max_job_retries=max(0, args.job_retries),
        require_price=not args.allow_missing_price,
        require_image=not args.allow_missing_image,
        require_category=not args.allow_missing_category,
        dedupe_parent_asin=args.dedupe_parent_asin,
        include_paid_search_results=args.include_paid,
        dry_run=args.dry_run,
    )
