from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .config import AppConfig


@dataclass(slots=True)
class UsageSnapshot:
    all_count: int
    provider_count: int
    raw: dict[str, Any]


class BudgetExhausted(RuntimeError):
    pass


class BudgetGuard:
    """Optional result-count guard, independent of any named subscription plan.

    If max_results is omitted, the scraper does not guess the account plan or its
    quota. It keeps working in durable waves until the provider stops accepting
    work, authentication/subscription access ends, the search plan is exhausted,
    or max_products is reached.

    If max_results is supplied, it is treated as a user-defined hard ceiling for
    new billable jobs. Provider usage can lag, so the locally observed completed-job
    count remains a conservative floor.
    """

    def __init__(self, config: AppConfig, client: Any, *, local_floor: int = 0) -> None:
        self.config = config
        self.client = client
        self.local_floor = max(0, int(local_floor))
        self.snapshot = UsageSnapshot(self.local_floor, 0, {})
        self.pending_reserved = 0

    async def refresh(self) -> UsageSnapshot:
        raw = await self.client.get_usage_stats()
        provider_count = self._extract_count(raw)
        all_count = max(provider_count, self.local_floor)
        self.snapshot = UsageSnapshot(
            all_count=all_count,
            provider_count=provider_count,
            raw=raw,
        )
        self.pending_reserved = 0
        return self.snapshot

    def set_local_floor(self, count: int) -> None:
        self.local_floor = max(self.local_floor, max(0, int(count)))
        if self.snapshot.all_count < self.local_floor:
            self.snapshot = UsageSnapshot(
                all_count=self.local_floor,
                provider_count=self.snapshot.provider_count,
                raw=self.snapshot.raw,
            )

    def capacity(self) -> int | None:
        if self.config.max_results is None:
            return None
        return max(
            0,
            self.config.max_results - self.snapshot.all_count - self.pending_reserved,
        )

    def reserve(self, requested: int) -> int:
        requested = max(0, int(requested))
        capacity = self.capacity()
        allowed = requested if capacity is None else min(requested, capacity)
        self.pending_reserved += allowed
        return allowed

    def ensure_capacity(self) -> None:
        capacity = self.capacity()
        if capacity is not None and capacity <= 0:
            raise BudgetExhausted(
                "Configured max-results reached: "
                f"used={self.snapshot.all_count}, max_results={self.config.max_results}"
            )

    @staticmethod
    def _extract_count(raw: dict[str, Any]) -> int:
        products = ((raw or {}).get("data") or {}).get("products") or []
        for product in products:
            if "web_scraper" in str(product.get("title", "")).lower():
                return int(product.get("all_count") or 0)
        if len(products) == 1:
            return int(products[0].get("all_count") or 0)
        return sum(int(p.get("all_count") or 0) for p in products)
