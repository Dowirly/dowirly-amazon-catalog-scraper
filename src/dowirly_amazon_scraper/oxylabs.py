from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass
from typing import Any, Callable

import httpx

from .config import AppConfig

LOGGER = logging.getLogger(__name__)
DATA_BASE = "https://data.oxylabs.io"


class OxylabsError(RuntimeError):
    pass


class OxylabsAuthError(OxylabsError):
    """Authentication/subscription access failure (HTTP 401)."""


class OxylabsQuotaStop(OxylabsError):
    """Provider refused more work because of quota/balance/subscription limits."""


class OxylabsJobMissing(OxylabsError):
    """A previously saved provider job/result is no longer available (HTTP 404)."""


class OxylabsRateLimitError(OxylabsError):
    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


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
            headers={"User-Agent": "dowirly-amazon-catalog-scraper/0.5"},
        )

        # This is an account-agnostic starting ceiling, not a plan mapping. If the
        # provider returns 429 while submitting, submit_batch automatically probes
        # downward until it finds a sustainable rate.
        self.submission_window_seconds = 1.05
        self.poll_request_rate = max(2, int(config.submit_rate * 0.8))
        self._poll_request_interval = 1.0 / self.poll_request_rate
        self._poll_pace_lock = asyncio.Lock()
        self._next_poll_request_at = 0.0

        # Set when a multi-chunk submission is partially accepted and then the
        # provider refuses further work. The caller can still collect the already
        # accepted jobs before stopping.
        self.submission_blocked_reason: str | None = None

    async def close(self) -> None:
        await self.http.aclose()

    async def get_usage_stats(self) -> dict[str, Any]:
        response = await self._request("GET", f"{DATA_BASE}/v2/stats")
        return response.json()

    async def submit_batch(
        self,
        payload: dict[str, Any],
        *,
        on_progress: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> list[dict[str, Any]]:
        """Submit a logical batch at the fastest rate the account currently accepts.

        No named plan is assumed. `config.submit_rate` is only the initial/max probe
        rate. On HTTP 429, the chunk rate is automatically reduced using a bounded
        search. After several successful windows it probes upward again, up to the
        configured ceiling.

        `on_progress` is called after every accepted chunk with all jobs accepted so
        far, allowing the pipeline to checkpoint provider job IDs immediately.
        """
        value_key: str | None = None
        for candidate in ("query", "url"):
            if isinstance(payload.get(candidate), list):
                value_key = candidate
                break

        if value_key is None:
            response = await self._request(
                "POST",
                f"{DATA_BASE}/v1/queries/batch",
                json=payload,
                raise_on_429=True,
            )
            jobs = self._parse_batch_jobs(response)
            if on_progress:
                on_progress(jobs)
            return jobs

        values = list(payload.get(value_key) or [])
        if not values:
            return []

        configured_ceiling = max(1, int(self.config.submit_rate))
        effective_rate = configured_ceiling
        lower_bound = 1
        upper_bound = configured_ceiling
        success_windows = 0

        total = len(values)
        offset = 0
        all_jobs: list[dict[str, Any]] = []
        loop = asyncio.get_running_loop()
        previous_submission_started: float | None = None

        while offset < total:
            if previous_submission_started is not None:
                elapsed = loop.time() - previous_submission_started
                delay = max(0.0, self.submission_window_seconds - elapsed)
                if delay:
                    await asyncio.sleep(delay)

            chunk = values[offset : offset + effective_rate]
            chunk_payload = dict(payload)
            chunk_payload[value_key] = chunk
            previous_submission_started = loop.time()

            try:
                response = await self._request(
                    "POST",
                    f"{DATA_BASE}/v1/queries/batch",
                    json=chunk_payload,
                    raise_on_429=True,
                )
            except OxylabsRateLimitError as exc:
                upper_bound = max(lower_bound, effective_rate - 1)
                effective_rate = max(1, (lower_bound + upper_bound) // 2)
                success_windows = 0
                wait = max(1.1, exc.retry_after or 0.0)
                LOGGER.warning(
                    "SUBMIT_RATE_ADAPT | 429 | new_rate=%s jobs/s | bounds=%s-%s | retry_in=%.2fs",
                    effective_rate,
                    lower_bound,
                    upper_bound,
                    wait,
                )
                await asyncio.sleep(wait)
                continue
            except (OxylabsQuotaStop, OxylabsAuthError) as exc:
                if all_jobs:
                    self.submission_blocked_reason = str(exc)
                    LOGGER.warning(
                        "SUBMIT_STOP | accepted_jobs=%s/%s | reason=%s | collecting accepted jobs before exit",
                        len(all_jobs),
                        total,
                        exc,
                    )
                    return all_jobs
                raise

            jobs = self._parse_batch_jobs(response)
            all_jobs.extend(jobs)
            offset += len(chunk)
            if on_progress:
                on_progress(all_jobs)

            LOGGER.info(
                "SUBMIT | accepted_jobs=%s/%s | last_chunk=%s | effective_rate=%s jobs/s",
                len(all_jobs),
                total,
                len(jobs),
                effective_rate,
            )

            lower_bound = max(lower_bound, effective_rate)
            success_windows += 1
            if success_windows >= 3 and effective_rate < upper_bound:
                effective_rate = min(
                    upper_bound,
                    max(effective_rate + 1, (effective_rate + upper_bound + 1) // 2),
                )
                success_windows = 0

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

        LOGGER.info(
            "POLL_CONFIG | jobs=%s | concurrency=%s | api_get_rate=%s req/s",
            total,
            self.config.poll_concurrency,
            self.poll_request_rate,
        )

        tasks = [asyncio.create_task(one(job)) for job in jobs]
        results: list[JobResult] = []
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
                try:
                    result = await self.get_job_results(job_id)
                except OxylabsJobMissing:
                    LOGGER.warning(
                        "JOB_MISSING | job_id=%s | query=%s | result is no longer available",
                        job_id,
                        query,
                    )
                    return JobResult(job_id, query, "faulted", current, None, attempts)
                return JobResult(job_id, query, status, current, result, attempts)
            if status == "faulted":
                if attempts <= max_retries:
                    LOGGER.warning(
                        "Job %s faulted; retry is handled by the pipeline after collection.",
                        job_id,
                    )
                return JobResult(job_id, query, status, current, None, attempts)

            await asyncio.sleep(self.config.poll_interval_seconds)
            try:
                response = await self._request(
                    "GET",
                    f"{DATA_BASE}/v1/queries/{job_id}",
                    paced=True,
                )
            except OxylabsJobMissing:
                LOGGER.warning(
                    "JOB_MISSING | job_id=%s | query=%s | status is no longer available",
                    job_id,
                    query,
                )
                return JobResult(job_id, query, "faulted", current, None, attempts)
            current = response.json()

    async def get_job_results(self, job_id: str) -> dict[str, Any]:
        response = await self._request(
            "GET",
            f"{DATA_BASE}/v1/queries/{job_id}/results",
            params={"type": "parsed"},
            paced=True,
        )
        return response.json()

    async def _pace_poll_request(self) -> None:
        loop = asyncio.get_running_loop()
        async with self._poll_pace_lock:
            now = loop.time()
            if self._next_poll_request_at > now:
                await asyncio.sleep(self._next_poll_request_at - now)
                now = loop.time()
            self._next_poll_request_at = (
                max(now, self._next_poll_request_at) + self._poll_request_interval
            )

    async def _request(
        self,
        method: str,
        url: str,
        *,
        paced: bool = False,
        raise_on_429: bool = False,
        **kwargs: Any,
    ) -> httpx.Response:
        last_exc: Exception | None = None
        transport_attempts = 10
        attempt = 0

        while True:
            attempt += 1
            if paced:
                await self._pace_poll_request()

            try:
                response = await self.http.request(method, url, **kwargs)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_exc = exc
                if attempt >= transport_attempts:
                    break
                await asyncio.sleep(min(15.0, 0.5 * (2 ** (attempt - 1))))
                continue

            if response.status_code in {200, 202}:
                return response
            if response.status_code == 204:
                await asyncio.sleep(self.config.poll_interval_seconds)
                continue

            text = response.text[:2000]
            lower = text.lower()

            if response.status_code == 404:
                raise OxylabsJobMissing(
                    f"Oxylabs job/result is no longer available: {url}"
                )

            if response.status_code == 401:
                raise OxylabsAuthError(
                    "Oxylabs API HTTP 401: authentication or subscription access is inactive."
                )

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

            if response.status_code in {403, 400, 422}:
                raise OxylabsError(
                    f"Oxylabs API HTTP {response.status_code}: {text}"
                )

            if response.status_code == 429:
                retry_after = response.headers.get("retry-after")
                try:
                    retry_after_seconds = float(retry_after) if retry_after else None
                except ValueError:
                    retry_after_seconds = None

                if raise_on_429:
                    raise OxylabsRateLimitError(
                        f"Oxylabs API HTTP 429: {text}",
                        retry_after=retry_after_seconds,
                    )

                delay = (
                    retry_after_seconds
                    if retry_after_seconds is not None
                    else min(30.0, 1.1 * (2 ** min(attempt - 1, 5)))
                )
                delay += random.uniform(0.0, 0.25)
                LOGGER.warning(
                    "RATE_LIMIT | HTTP 429 | attempt=%s | retry_in=%.2fs | body=%s",
                    attempt,
                    delay,
                    text[:500],
                )
                await asyncio.sleep(delay)
                continue

            if response.status_code >= 500:
                if attempt >= transport_attempts:
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
