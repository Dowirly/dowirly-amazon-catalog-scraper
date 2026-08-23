from __future__ import annotations

import asyncio
import json
import logging
import random
from dataclasses import dataclass
from datetime import date
from typing import Any

import httpx

from .config import AppConfig
from .utils import atomic_write_json, utc_now_iso

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
            limits=httpx.Limits(
                max_connections=max(100, config.poll_concurrency + 20),
                max_keepalive_connections=50,
            ),
            headers={"User-Agent": "dowirly-amazon-catalog-scraper/0.2"},
        )
        # Oxylabs documents the plan limit as jobs/second. A /batch request still
        # creates one provider job per query value, so large batches are paced in
        # plan-sized chunks. 1.05s gives a tiny rolling-window safety margin while
        # remaining essentially at the advertised maximum throughput.
        self.submission_window_seconds = 1.05

    async def close(self) -> None:
        await self.http.aclose()

    async def get_usage_stats(self, plan: str) -> dict[str, Any]:
        params: dict[str, str] = {}
        if plan == "micro":
            today = date.today()
            params = {
                "date_from": today.replace(day=1).isoformat(),
                "date_to": today.isoformat(),
            }
        response = await self._request("GET", f"{DATA_BASE}/v2/stats", params=params)
        return response.json()

    async def submit_batch(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        """Submit a Push-Pull batch at the fastest safe plan rate.

        Oxylabs accepts up to 5,000 values in a batch payload, but the account also
        has a jobs/second rate limit. Every query inside the batch is a separate
        provider job, so we split a large payload into chunks no larger than the
        configured plan rate (10/s on Free, 50/s on Micro) and submit one chunk per
        ~1 second window.

        Accepted job IDs are checkpointed after *every* chunk. If the VPS reboots
        halfway through submitting a 2,000-item wave, the restarted scraper can
        poll the already-created Oxylabs jobs instead of paying for duplicates.
        """
        value_key: str | None = None
        for candidate in ("query", "url"):
            if isinstance(payload.get(candidate), list):
                value_key = candidate
                break

        if value_key is None:
            # Defensive fallback for a singular payload.
            response = await self._request(
                "POST", f"{DATA_BASE}/v1/queries/batch", json=payload
            )
            jobs = self._parse_batch_jobs(response)
            self._persist_partial_inflight(payload, jobs)
            return jobs

        values = list(payload.get(value_key) or [])
        if not values:
            return []

        safe_rate = max(1, int(self.config.submit_rate))
        total = len(values)
        all_jobs: list[dict[str, Any]] = []
        loop = asyncio.get_running_loop()
        previous_submission_started: float | None = None

        for offset in range(0, total, safe_rate):
            if previous_submission_started is not None:
                elapsed = loop.time() - previous_submission_started
                delay = max(0.0, self.submission_window_seconds - elapsed)
                if delay:
                    await asyncio.sleep(delay)

            chunk = values[offset : offset + safe_rate]
            chunk_payload = dict(payload)
            chunk_payload[value_key] = chunk
            previous_submission_started = loop.time()

            response = await self._request(
                "POST", f"{DATA_BASE}/v1/queries/batch", json=chunk_payload
            )
            jobs = self._parse_batch_jobs(response)
            all_jobs.extend(jobs)

            # Persist immediately, not after all chunks have been submitted.
            self._persist_partial_inflight(payload, all_jobs)

            LOGGER.info(
                "SUBMIT | accepted_jobs=%s/%s | last_chunk=%s | safe_plan_rate=%s jobs/s",
                len(all_jobs),
                total,
                len(jobs),
                safe_rate,
            )

        return all_jobs

    @staticmethod
    def _parse_batch_jobs(response: httpx.Response) -> list[dict[str, Any]]:
        body = response.json()
        queries = body.get("queries") if isinstance(body, dict) else None
        if not isinstance(queries, list):
            raise OxylabsError(
                f"Unexpected batch response shape: {str(body)[:1000]}"
            )
        return queries

    def _persist_partial_inflight(
        self, original_payload: dict[str, Any], new_jobs: list[dict[str, Any]]
    ) -> None:
        """Durably journal accepted provider jobs during chunked submission.

        Pipeline-level checkpointing remains authoritative. This lower-level journal
        closes the small crash window that otherwise exists while a large batch is
        still being rate-limited and submitted chunk by chunk.
        """
        source = str(original_payload.get("source") or "")
        phase = {
            "amazon_search": "search",
            "amazon_product": "product",
        }.get(source)
        if phase is None:
            return

        checkpoint_path = self.config.data_dir / "intermediate" / "checkpoint.json"
        checkpoint: dict[str, Any] = {}
        if checkpoint_path.exists():
            try:
                checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                checkpoint = {}

        checkpoint.setdefault("version", 2)
        checkpoint.setdefault("completed_search_keys", [])
        checkpoint.setdefault("completed_product_asins", [])
        inflight = checkpoint.setdefault("inflight_jobs", {})
        existing = inflight.get(phase) or {}

        merged: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        for job in list(existing.get("jobs") or []) + list(new_jobs):
            job_id = str(job.get("id") or "")
            if not job_id or job_id in seen_ids:
                continue
            seen_ids.add(job_id)
            merged.append(job)

        inflight[phase] = {
            "signature": existing.get("signature"),
            # Preserve the original full wave payload when a retry chunk is being
            # submitted; otherwise use the payload passed by the pipeline.
            "payload": existing.get("payload") or original_payload,
            "jobs": merged,
            "updated_at": utc_now_iso(),
        }
        atomic_write_json(checkpoint_path, checkpoint)

    async def poll_jobs(
        self, jobs: list[dict[str, Any]], *, max_retries: int
    ) -> list[JobResult]:
        semaphore = asyncio.Semaphore(self.config.poll_concurrency)

        async def one(job: dict[str, Any]) -> JobResult:
            async with semaphore:
                return await self._poll_one(job, max_retries=max_retries)

        total = len(jobs)
        if total == 0:
            return []
        tasks = [asyncio.create_task(one(job)) for job in jobs]
        results: list[JobResult] = []
        # Large Push-Pull batches can run for minutes. Emit lightweight polling
        # progress while they are in flight so journalctl remains useful even
        # before normalization of the whole batch begins.
        log_every = max(1, min(50, total // 10 or 1))
        for future in asyncio.as_completed(tasks):
            result = await future
            results.append(result)
            completed = len(results)
            if completed == total or completed % log_every == 0:
                LOGGER.info("POLL | completed_jobs=%s/%s", completed, total)
        return results

    async def _poll_one(
        self, job: dict[str, Any], *, max_retries: int
    ) -> JobResult:
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
                    LOGGER.warning(
                        "Job %s faulted; re-submission is deferred to the pipeline retry pass.",
                        job_id,
                    )
                return JobResult(job_id, query, status, current, None, attempts)
            await asyncio.sleep(self.config.poll_interval_seconds)
            response = await self._request(
                "GET", f"{DATA_BASE}/v1/queries/{job_id}"
            )
            current = response.json()

    async def get_job_results(self, job_id: str) -> dict[str, Any]:
        # Parsed is the default when parse=true, but making it explicit protects us
        # if a provider default changes.
        response = await self._request(
            "GET",
            f"{DATA_BASE}/v1/queries/{job_id}/results",
            params={"type": "parsed"},
        )
        return response.json()

    async def _request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        """HTTP wrapper with adaptive retry/backoff.

        429 responses are explicitly unbilled by Oxylabs. They can still occur when
        another process/API client shares the account or when a dynamic/domain limit
        is temporarily lower than the nominal plan rate, so we back off and retry
        rather than crashing a long catalog run.
        """
        last_exc: Exception | None = None
        max_attempts = 10

        for attempt in range(1, max_attempts + 1):
            try:
                response = await self.http.request(method, url, **kwargs)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_exc = exc
                if attempt == max_attempts:
                    break
                await asyncio.sleep(min(15.0, 0.5 * (2 ** (attempt - 1))))
                continue

            if response.status_code in {200, 202}:
                return response
            if response.status_code == 204:
                # Job is not completed yet; callers that poll metadata should retry.
                await asyncio.sleep(self.config.poll_interval_seconds)
                continue

            text = response.text[:2000]
            lower = text.lower()
            if response.status_code == 403 and any(
                key in lower
                for key in (
                    "quota",
                    "limit",
                    "balance",
                    "credit",
                    "subscription",
                    "usage",
                )
            ):
                raise OxylabsQuotaStop(
                    f"Oxylabs stopped accepting jobs: HTTP 403 {text}"
                )
            if response.status_code in {401, 403, 400, 422}:
                raise OxylabsError(
                    f"Oxylabs API HTTP {response.status_code}: {text}"
                )

            if response.status_code == 429:
                if attempt == max_attempts:
                    raise OxylabsError(
                        f"Oxylabs API HTTP 429 after adaptive retries: {text}"
                    )

                retry_after = response.headers.get("retry-after")
                try:
                    retry_after_seconds = float(retry_after) if retry_after else None
                except ValueError:
                    retry_after_seconds = None

                delay = (
                    retry_after_seconds
                    if retry_after_seconds is not None
                    else min(30.0, 1.1 * (2 ** (attempt - 1)))
                )
                delay += random.uniform(0.0, 0.25)
                rate_headers = {
                    key: value
                    for key, value in response.headers.items()
                    if key.lower().startswith("x-ratelimit")
                    or key.lower() == "retry-after"
                }
                LOGGER.warning(
                    "RATE_LIMIT | HTTP 429 | attempt=%s/%s | retry_in=%.2fs | headers=%s | body=%s",
                    attempt,
                    max_attempts,
                    delay,
                    rate_headers,
                    text[:500],
                )
                await asyncio.sleep(delay)
                continue

            if response.status_code >= 500:
                if attempt == max_attempts:
                    raise OxylabsError(
                        f"Oxylabs API HTTP {response.status_code} after retries: {text}"
                    )
                await asyncio.sleep(
                    min(15.0, 0.75 * (2 ** (attempt - 1)))
                    + random.uniform(0.0, 0.2)
                )
                continue

            raise OxylabsError(
                f"Unexpected Oxylabs API HTTP {response.status_code}: {text}"
            )

        raise OxylabsError(
            f"Oxylabs transport failed after retries: {last_exc}"
        )


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
