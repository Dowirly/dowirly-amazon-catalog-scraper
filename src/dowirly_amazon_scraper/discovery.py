from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable

from .config import SearchQuery
from .utils import normalize_asin, valid_asin

SEARCH_BUCKETS = ("organic", "amazons_choices", "suggested", "instant_recommendations", "paid")


def extract_search_candidates(
    result_wrapper: dict[str, Any],
    *,
    query_to_category: dict[str, str],
    include_paid: bool,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for outer in result_wrapper.get("results") or []:
        content = outer.get("content")
        if not isinstance(content, dict):
            continue
        query = str(content.get("query") or "")
        logical_category = query_to_category.get(query)
        result_sets = content.get("results") or {}
        if not isinstance(result_sets, dict):
            continue
        for bucket in SEARCH_BUCKETS:
            if bucket == "paid" and not include_paid:
                continue
            items = result_sets.get(bucket) or []
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                _append_candidate(out, item, query, logical_category, bucket)
                for variation in item.get("variations") or []:
                    if isinstance(variation, dict):
                        _append_candidate(out, variation, query, logical_category, f"{bucket}.variation")
    return out


def _append_candidate(
    out: list[dict[str, Any]], item: dict[str, Any], query: str, logical_category: str | None, bucket: str
) -> None:
    asin = item.get("asin")
    if not valid_asin(asin):
        return
    out.append(
        {
            "asin": normalize_asin(str(asin)),
            "title": item.get("title"),
            "url": item.get("url"),
            "price": item.get("price"),
            "currency": item.get("currency"),
            "rating": item.get("rating"),
            "reviews_count": item.get("reviews_count"),
            "manufacturer": item.get("manufacturer"),
            "image": item.get("url_image") or item.get("image"),
            "search_query": query,
            "logical_category": logical_category,
            "search_bucket": bucket,
        }
    )


def merge_candidates(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for record in records:
        asin = record["asin"]
        target = merged.setdefault(
            asin,
            {
                "asin": asin,
                "title": record.get("title"),
                "url": record.get("url"),
                "image": record.get("image"),
                "price": record.get("price"),
                "currency": record.get("currency"),
                "rating": record.get("rating"),
                "reviews_count": record.get("reviews_count"),
                "manufacturer": record.get("manufacturer"),
                "search_queries": [],
                "logical_categories": [],
                "search_buckets": [],
            },
        )
        for field in ("title", "url", "image", "price", "currency", "rating", "reviews_count", "manufacturer"):
            if target.get(field) in (None, "", 0) and record.get(field) not in (None, ""):
                target[field] = record[field]
        for source_field, target_field in (
            ("search_query", "search_queries"),
            ("logical_category", "logical_categories"),
            ("search_bucket", "search_buckets"),
        ):
            value = record.get(source_field)
            if value and value not in target[target_field]:
                target[target_field].append(value)
    return list(merged.values())
