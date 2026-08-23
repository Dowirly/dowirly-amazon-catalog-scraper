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


def test_submit_rate_auto_tunes_down_on_429():
    client = OxylabsClient(_config(50))
    client.submission_window_seconds = 0.0
    successful_chunk_sizes = []
    attempts = []

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
    assert any(size > 10 for size in attempts)
    assert successful_chunk_sizes
    assert max(successful_chunk_sizes) <= 10
