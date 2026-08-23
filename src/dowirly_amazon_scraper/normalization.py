from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from .utils import compact_text, listify_bullets, unique_strings, utc_now_iso, valid_asin

ACCEPTABLE_PARSE_CODES = {12000, 12004, 12005, None}


@dataclass(slots=True)
class NormalizationResult:
    product: dict[str, Any] | None
    rejection_reasons: list[str]


def extract_parsed_product(result_wrapper: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    for outer in result_wrapper.get("results") or []:
        content = outer.get("content")
        if isinstance(content, dict):
            return content, outer
    return None, None


def normalize_product(
    parsed: dict[str, Any],
    outer: dict[str, Any] | None,
    candidate: dict[str, Any] | None,
    *,
    require_price: bool,
    require_image: bool,
    require_category: bool,
) -> NormalizationResult:
    reasons: list[str] = []
    asin = str(parsed.get("asin") or parsed.get("asin_in_url") or "").upper()
    if not valid_asin(asin):
        reasons.append("invalid_or_missing_asin")

    title = compact_text(parsed.get("title") or parsed.get("product_name"))
    if not title:
        reasons.append("missing_title")

    parse_code = parsed.get("parse_status_code")
    if parse_code not in ACCEPTABLE_PARSE_CODES:
        reasons.append(f"unacceptable_parse_status:{parse_code}")

    status_code = (outer or {}).get("status_code") or parsed.get("status_code")
    if status_code and not (200 <= int(status_code) < 300):
        reasons.append(f"target_status:{status_code}")

    images = unique_strings(parsed.get("images") or [])
    if require_image and not images:
        reasons.append("missing_images")

    category = _category(parsed.get("category"), candidate)
    if require_category and not category["path"]:
        reasons.append("missing_category")

    price = _number(parsed.get("price") or parsed.get("price_buybox"))
    if require_price and (price is None or price <= 0):
        reasons.append("missing_or_invalid_price")

    if reasons:
        return NormalizationResult(None, reasons)

    bullets = listify_bullets(parsed.get("bullet_points"))
    product_overview = _key_value_list(parsed.get("product_overview"))
    product_details = _object_or_list(parsed.get("product_details"))
    variations = _variations(parsed.get("variation") or parsed.get("variations"))
    featured_merchant = parsed.get("featured_merchant") if isinstance(parsed.get("featured_merchant"), dict) else None

    product: dict[str, Any] = {
        "schema_version": "1.0",
        "id": f"amazon-sa:{asin}",
        "source": "amazon",
        "marketplace": "amazon.sa",
        "external_id": asin,
        "parent_external_id": parsed.get("parent_asin"),
        "url": parsed.get("url") or f"https://www.amazon.sa/dp/{asin}",
        "title": title,
        "product_name": compact_text(parsed.get("product_name")),
        "brand": compact_text(parsed.get("brand")),
        "manufacturer": compact_text(parsed.get("manufacturer")),
        "description": compact_text(parsed.get("description")),
        "bullet_points": bullets,
        "category": category,
        "images": images,
        "pricing": {
            "currency": parsed.get("currency"),
            "current": price,
            "initial": _number(parsed.get("price_initial")),
            "upper": _number(parsed.get("price_upper")),
            "buybox": _number(parsed.get("price_buybox")),
            "shipping": _number(parsed.get("price_shipping")),
            "discount_percentage": _number(((parsed.get("discount") or {}) if isinstance(parsed.get("discount"), dict) else {}).get("percentage")),
            "deal_type": parsed.get("deal_type"),
            "coupon": parsed.get("coupon"),
            "coupon_discount_percentage": _number(parsed.get("coupon_discount_percentage")),
        },
        "availability": {
            "stock": compact_text(parsed.get("stock")),
            "max_quantity": parsed.get("max_quantity"),
            "prime_eligible": parsed.get("is_prime_eligible"),
        },
        "rating": {
            "value": _number(parsed.get("rating")),
            "reviews_count": parsed.get("reviews_count"),
            "answered_questions_count": parsed.get("answered_questions_count"),
            "distribution": parsed.get("rating_stars_distribution") or [],
        },
        "seller": featured_merchant,
        "sales_rank": parsed.get("sales_rank") or [],
        "variations": variations,
        "delivery": parsed.get("delivery") or [],
        "badges": {
            "amazon_choice": parsed.get("amazon_choice"),
            "has_videos": parsed.get("has_videos"),
            "lightning_deal": parsed.get("lightning_deal"),
        },
        "attributes": {
            "product_overview": product_overview,
            "product_details": product_details,
            "developer_info": _object_or_list(parsed.get("developer_info")),
            "product_dimensions": compact_text(parsed.get("product_dimensions")),
            "item_form": compact_text(parsed.get("item_form")),
            "warranty_and_support": _object_or_list(parsed.get("warranty_and_support")),
        },
        "discovery": {
            "queries": (candidate or {}).get("search_queries", []),
            "logical_categories": (candidate or {}).get("logical_categories", []),
            "search_buckets": (candidate or {}).get("search_buckets", []),
        },
        "source_metadata": {
            "job_id": (outer or {}).get("job_id") or parsed.get("job_id"),
            "status_code": status_code,
            "parse_status_code": parse_code,
            "created_at": parsed.get("created_at") or (outer or {}).get("created_at"),
            "updated_at": parsed.get("updated_at") or (outer or {}).get("updated_at"),
            "normalized_at": utc_now_iso(),
        },
    }
    product["embedding_text"] = build_embedding_text(product)
    return NormalizationResult(product, [])


def build_embedding_text(product: dict[str, Any]) -> str:
    """Create stable semantic text. Dynamic price/stock/review counts are excluded.

    Those values should be used as structured filters/ranking signals rather than
    embedded into a vector that becomes stale quickly.
    """
    sections: list[str] = []
    for label, value in (
        ("Title", product.get("title")),
        ("Brand", product.get("brand")),
        ("Manufacturer", product.get("manufacturer")),
        ("Category", " > ".join((product.get("category") or {}).get("path") or [])),
        ("Description", product.get("description")),
    ):
        if value:
            sections.append(f"{label}: {value}")

    bullets = product.get("bullet_points") or []
    if bullets:
        sections.append("Features:\n" + "\n".join(f"- {b}" for b in bullets[:30]))

    attrs = product.get("attributes") or {}
    overview = attrs.get("product_overview") or []
    if overview:
        sections.append("Product overview:\n" + "\n".join(f"- {x['title']}: {x['description']}" for x in overview[:40]))

    details = attrs.get("product_details")
    if details:
        try:
            serialized = json.dumps(details, ensure_ascii=False, sort_keys=True)
        except TypeError:
            serialized = str(details)
        sections.append(f"Product details: {serialized[:8000]}")
    return "\n\n".join(sections).strip()


def _category(value: Any, candidate: dict[str, Any] | None) -> dict[str, Any]:
    ladders: list[dict[str, Any]] = []
    if isinstance(value, list):
        for category_obj in value:
            if isinstance(category_obj, dict) and isinstance(category_obj.get("ladder"), list):
                ladders.extend(x for x in category_obj["ladder"] if isinstance(x, dict))
    path = unique_strings(x.get("name") for x in ladders)
    return {
        "primary": path[0] if path else None,
        "leaf": path[-1] if path else None,
        "path": path,
        "breadcrumbs": [{"name": x.get("name"), "url": x.get("url")} for x in ladders if x.get("name")],
        "discovery_labels": (candidate or {}).get("logical_categories", []),
    }


def _variations(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    out: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        asin = item.get("asin")
        if isinstance(asin, list):
            asin = asin[0] if asin else None
        out.append(
            {
                "asin": asin,
                "selected": item.get("selected"),
                "dimensions": item.get("dimensions") or {},
                "tooltip_image": item.get("tooltip_image"),
            }
        )
    return out


def _key_value_list(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    out: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        title = compact_text(item.get("title"))
        description = compact_text(item.get("description"))
        if title and description:
            out.append({"title": title, "description": description})
    return out


def _object_or_list(value: Any) -> Any:
    return value if isinstance(value, (dict, list)) else None


def _number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
