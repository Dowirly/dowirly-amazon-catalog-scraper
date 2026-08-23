from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable

import httpx

from .config import AppConfig
from .utils import utc_now_iso

LOGGER = logging.getLogger(__name__)

DATA_BASE = "https://data.oxylabs.io"


class OxylabsError(RuntimeError):
    pass


class OxylabsQuotaStop(OxylabsError):
    """Raised for responses that look like an account/quota exhaustion condition."""


@dataclass(slots=True)
class JobResult:
    job_id: str
    query: str
    status: str
    metadata: dict[str, Any]
    result: dict[str, Any] | None
    attempts: int = 1


class OxylabsClient:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.auth = (config.username, config.password)
        self.http = httpx.AsyncClient(
            auth=self.auth,
            timeout=httpx.Timeout(45.0, connect=20.0),
            limits=httpx.Limits(max_connections=max(100, config.poll_concurrency + 20), max_keepalive_connections=50),
            headers={"User-Agent": "dowirly-amazon-catalog-scraper/0.1"},
        )

    async def close(self) -> None:
        await self.http.aclose()

    async def get_usage_stats(self, plan: str) -> dict[str, Any]:
        params: dict[str, str] = {}
        if plan == "micro":
            today = date.today()
            params = {"date_from": today.replace(day=1).isoformat(), "date_to": today.isoformat()}
        response = await self._request("GET", f"{DATA_BASE}/v2/stats", params=params)
        return response.json()

    async def submit_batch(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        response = await self._request("POST", f"{DATA_BASE}/v1/queries/batch", json=payload)
        body = response.json()
        queries = body.get("queries") if isinstance(body, dict) else None
        if not isinstance(queries, list):
            raise OxylabsError(f"Unexpected batch response shape: {str(body)[:1000]}")
        return queries

    async def poll_jobs(self, jobs: list[dict[str, Any]], *, max_retries: int) -> list[JobResult]:
        semaphore = asyncio.Semaphore(self.config.poll_concurrency)

        async def one(job: dict[str, Any]) -> JobResult:
            async with semaphore:
                return await self._poll_one(job, max_retries=max_retries)

        return await asyncio.gather(*(one(job) for job in jobs))

    async def _poll_one(self, job: dict[str, Any], *, max_retries: int) -> JobResult:
        current = job
        job_id = str(job.get("id"))
        query = str(job.get("query") or job.get("url") or "")
        attempts = 1
        while True:
            status = str(current.get("status") or "pending")
            if status == "done":
                result = await self.get_job_results(job_id)
                return JobResult(job_id, query, status, current, result, attempts)
            if status == "faulted":
                if attempts <= max_retries:
                    LOGGER.warning("Job %s faulted; re-submission is deferred to the pipeline retry pass.", job_id)
                return JobResult(job_id, query, status, current, None, attempts)
            await asyncio.sleep(self.config.poll_interval_seconds)
            response = await self._request("GET", f"{DATA_BASE}/v1/queries/{job_id}")
            current = response.json()

    async def get_job_results(self, job_id: str) -> dict[str, Any]:
        # Parsed is the default when parse=true, but making it explicit protects us
        # if a provider default changes.
        response = await self._request("GET", f"{DATA_BASE}/v1/queries/{job_id}/results", params={"type": "parsed"})
        return response.json()

    async def _request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        last_exc: Exception | None = None
        for attempt in range(1, 6):
            try:
                response = await self.http.request(method, url, **kwargs)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_exc = exc
                if attempt == 5:
                    break
                await asyncio.sleep(min(8, 0.5 * (2 ** (attempt - 1))))
                continue

            if response.status_code in {200, 202}:
                return response
            if response.status_code == 204:
                # Job is not completed yet; callers that poll metadata should retry.
                await asyncio.sleep(self.config.poll_interval_seconds)
                continue

            text = response.text[:2000]
            lower = text.lower()
            if response.status_code == 403 and any(k in lower for k in ("quota", "limit", "balance", "credit", "subscription", "usage")):
                raise OxylabsQuotaStop(f"Oxylabs stopped accepting jobs: HTTP 403 {text}")
            if response.status_code in {401, 403, 400, 422}:
                raise OxylabsError(f"Oxylabs API HTTP {response.status_code}: {text}")
            if response.status_code == 429 or response.status_code >= 500:
                if attempt == 5:
                    raise OxylabsError(f"Oxylabs API HTTP {response.status_code} after retries: {text}")
                retry_after = response.headers.get("retry-after")
                delay = float(retry_after) if retry_after and retry_after.isdigit() else min(10, 0.75 * (2 ** (attempt - 1)))
                await asyncio.sleep(delay)
                continue
            raise OxylabsError(f"Unexpected Oxylabs API HTTP {response.status_code}: {text}")

        raise OxylabsError(f"Oxylabs transport failed after retries: {last_exc}")


def base_payload(config: AppConfig, source: str) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "source": source,
        "domain": config.domain,
        "locale": config.locale,
        "parse": True,
    }
    if config.geo_location:
        payload["geo_location"] = config.geo_location
    return payload
