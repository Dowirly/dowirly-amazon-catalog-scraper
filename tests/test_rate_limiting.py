import asyncio
from pathlib import Path
from types import SimpleNamespace

import httpx

from dowirly_amazon_scraper.oxylabs import OxylabsClient


def _config(tmp_path: Path):
    return SimpleNamespace(
        username="u",
        password="p",
        poll_concurrency=20,
        submit_rate=10,
        data_dir=tmp_path,
    )


def test_submit_batch_splits_at_plan_rate_and_checkpoints(tmp_path):
    client = OxylabsClient(_config(tmp_path))
    client.submission_window_seconds = 0.0
    chunks = []

    async def fake_request(method, url, **kwargs):
        payload = kwargs["json"]
        values = list(payload["query"])
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
                    "domain": "sa",
                    "parse": True,
                    "query": [f"B{i:09d}" for i in range(23)],
                }
            )
        finally:
            await client.close()

    jobs = asyncio.run(run())

    assert [len(chunk) for chunk in chunks] == [10, 10, 3]
    assert len(jobs) == 23

    checkpoint = tmp_path / "intermediate" / "checkpoint.json"
    assert checkpoint.exists()
    text = checkpoint.read_text(encoding="utf-8")
    assert "job-B000000000" in text
    assert "job-B000000022" in text
