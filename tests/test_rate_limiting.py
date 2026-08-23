import asyncio
from types import SimpleNamespace

import httpx

from dowirly_amazon_scraper.oxylabs import (
    OxylabsClient,
    OxylabsRateLimitError,
)


def _config(submit_rate: int = 10):
    return SimpleNamespace(
        username="u",
        password="p",
        poll_concurrency=20,
        poll_interval_seconds=0.01,
        submit_rate=submit_rate,
    )


def test_submit_batch_uses_configured_ceiling_and_progress_callback():
    client = OxylabsClient(_config(10))
    client.submission_window_seconds = 0.0
    chunks = []
    progress = []

    async def fake_request(method, url, **kwargs):
        values = list(kwargs["json"]["query"])
        chunks.append(values)
        return httpx.Response(
            200,
            json={
                "queries": [
                    {"id": f"job-{value}", "query": value, "status": "pending"}
                    for value in values
                ]
            },
        )

    client._request = fake_request  # type: ignore[method-assign]

    async def run():
        try:
            return await client.submit_batch(
                {
                    "source": "amazon_product",
                    "query": [f"B{i:09d}" for i in range(23)],
                },
                on_progress=lambda jobs: progress.append(len(jobs)),
            )
        finally:
            await client.close()

    jobs = asyncio.run(run())
    assert [len(chunk) for chunk in chunks] == [10, 10, 3]
    assert progress == [10, 20, 23]
    assert len(jobs) == 23


def test_submit_rate_auto_tunes_down_on_429(monkeypatch):
    client = OxylabsClient(_config(50))
    client.submission_window_seconds = 0.0
    successful_chunk_sizes = []
    attempts = []

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr("dowirly_amazon_scraper.oxylabs.asyncio.sleep", no_sleep)

    async def fake_request(method, url, **kwargs):
        values = list(kwargs["json"]["query"])
        attempts.append(len(values))
        if len(values) > 10:
            raise OxylabsRateLimitError("429", retry_after=0.0)
        successful_chunk_sizes.append(len(values))
        return httpx.Response(
            200,
            json={
                "queries": [
                    {"id": f"job-{value}", "query": value, "status": "pending"}
                    for value in values
                ]
            },
        )

    client._request = fake_request  # type: ignore[method-assign]

    async def run():
        try:
            return await client.submit_batch(
                {
                    "source": "amazon_product",
                    "query": [f"B{i:09d}" for i in range(30)],
                }
            )
        finally:
            await client.close()

    jobs = asyncio.run(run())
    assert len(jobs) == 30
    assert attempts[:3] == [30, 25, 12]
    assert successful_chunk_sizes
    assert max(successful_chunk_sizes) <= 10
    assert client._submit_rate_hint <= 10


def test_persistent_429_at_one_job_per_second_uses_exponential_cooldown(monkeypatch):
    client = OxylabsClient(_config(1))
    client.submission_window_seconds = 0.0
    waits = []
    calls = 0

    async def fake_sleep(seconds):
        waits.append(seconds)

    monkeypatch.setattr("dowirly_amazon_scraper.oxylabs.asyncio.sleep", fake_sleep)
    monkeypatch.setattr("dowirly_amazon_scraper.oxylabs.random.uniform", lambda *_: 0.0)

    async def fake_request(method, url, **kwargs):
        nonlocal calls
        calls += 1
        values = list(kwargs["json"]["query"])
        if calls <= 3:
            raise OxylabsRateLimitError(
                "429",
                retry_after=None,
                response_body='{"message":"Too many requests. (Total Dynamic)."}',
            )
        return httpx.Response(
            200,
            json={
                "queries": [
                    {"id": f"job-{value}", "query": value, "status": "pending"}
                    for value in values
                ]
            },
        )

    client._request = fake_request  # type: ignore[method-assign]

    async def run():
        try:
            return await client.submit_batch(
                {"source": "amazon_product", "query": ["B000000001"]}
            )
        finally:
            await client.close()

    jobs = asyncio.run(run())
    assert len(jobs) == 1
    # The first retry is 2s, then 4s, then 8s instead of an endless 1.1s hammer loop.
    assert waits[:3] == [2.0, 4.0, 8.0]


def test_learned_submit_rate_is_reused_by_next_wave(monkeypatch):
    client = OxylabsClient(_config(20))
    client.submission_window_seconds = 0.0
    first_wave_attempts = []
    second_wave_attempts = []
    phase = 1

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr("dowirly_amazon_scraper.oxylabs.asyncio.sleep", no_sleep)

    async def fake_request(method, url, **kwargs):
        values = list(kwargs["json"]["query"])
        target = first_wave_attempts if phase == 1 else second_wave_attempts
        target.append(len(values))
        if len(values) > 5:
            raise OxylabsRateLimitError("429", retry_after=0.0)
        return httpx.Response(
            200,
            json={
                "queries": [
                    {"id": f"job-{value}", "query": value, "status": "pending"}
                    for value in values
                ]
            },
        )

    client._request = fake_request  # type: ignore[method-assign]

    async def run():
        nonlocal phase
        try:
            await client.submit_batch(
                {"source": "amazon_product", "query": [f"B{i:09d}" for i in range(10)]}
            )
            phase = 2
            await client.submit_batch(
                {"source": "amazon_product", "query": [f"C{i:09d}" for i in range(5)]}
            )
        finally:
            await client.close()

    asyncio.run(run())
    assert first_wave_attempts[0] == 10
    assert second_wave_attempts[0] <= 5
