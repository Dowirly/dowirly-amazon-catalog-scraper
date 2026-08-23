from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from .config import AppConfig


@dataclass(slots=True)
class UsageSnapshot:
    # all_count is the guarded effective count: max(provider stats, local floor).
    all_count: int
    provider_count: int
    raw: dict[str, Any]


class BudgetExhausted(RuntimeError):
    pass


class BudgetGuard:
    """Conservative quota guard.

    Each submitted job in this project requests exactly one page (`pages=1`), so a
    submitted job can consume at most one billable result. We finish each wave and
    refresh official usage stats before submitting the next wave. That lets faulted
    (unbilled) jobs return capacity without risking an overrun.
    """

    def __init__(self, config: AppConfig, client: Any, *, local_floor: int = 0) -> None:
        self.config = config
        self.client = client
        self.local_floor = max(0, int(local_floor))
        self.snapshot = UsageSnapshot(self.local_floor, 0, {})
        self.pending_reserved = 0

    async def refresh(self) -> UsageSnapshot:
        raw = await self.client.get_usage_stats(self.config.plan)
        provider_count = self._extract_count(raw)
        all_count = max(provider_count, self.local_floor)
        self.snapshot = UsageSnapshot(all_count=all_count, provider_count=provider_count, raw=raw)
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

    def capacity(self) -> int:
        return max(0, self.config.hard_result_limit - self.snapshot.all_count - self.pending_reserved)

    def reserve(self, requested: int) -> int:
        allowed = min(max(0, requested), self.capacity())
        self.pending_reserved += allowed
        return allowed

    def ensure_capacity(self) -> None:
        if self.capacity() <= 0:
            raise BudgetExhausted(
                f"Oxylabs result budget reached: used={self.snapshot.all_count}, hard_limit={self.config.hard_result_limit}"
            )

    @staticmethod
    def _extract_count(raw: dict[str, Any]) -> int:
        products = ((raw or {}).get("data") or {}).get("products") or []
        # New accounts normally expose web_scraper_api. Prefer it if available.
        for product in products:
            if "web_scraper" in str(product.get("title", "")).lower():
                return int(product.get("all_count") or 0)
        # Fallback: if only one product is returned, its count is the relevant quota.
        if len(products) == 1:
            return int(products[0].get("all_count") or 0)
        # Conservative fallback for ambiguous accounts: sum all visible product usage.
        return sum(int(p.get("all_count") or 0) for p in products)
