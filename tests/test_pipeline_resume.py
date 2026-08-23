import asyncio
from pathlib import Path
from types import SimpleNamespace

from dowirly_amazon_scraper.oxylabs import JobResult
from dowirly_amazon_scraper.pipeline import Pipeline
from dowirly_amazon_scraper.reporting import RunMetrics
from dowirly_amazon_scraper.storage import Storage


class FakeBudget:
    def __init__(self) -> None:
        self.snapshot = SimpleNamespace(all_count=0, provider_count=0)
        self.floor = 0

    async def refresh(self):
        return self.snapshot

    def reserve(self, requested: int) -> int:
        return requested

    def set_local_floor(self, count: int) -> None:
        self.floor = max(self.floor, count)
        self.snapshot.all_count = max(self.snapshot.all_count, self.floor)


class ResumeOnlyClient:
    def __init__(self) -> None:
        self.poll_sizes = []
        self.submission_blocked_reason = None

    async def submit_batch(self, *args, **kwargs):
        raise AssertionError("saved in-flight jobs must not be submitted again")

    async def poll_jobs(self, jobs, *, max_retries):
        self.poll_sizes.append(len(jobs))
        return [
            JobResult(
                str(job["id"]),
                str(job["query"]),
                "done",
                {"status": "done"},
                {"results": []},
            )
            for job in jobs
        ]


def make_pipeline(tmp_path: Path, client) -> Pipeline:
    p = object.__new__(Pipeline)
    p.config = SimpleNamespace(
        wave_size=100,
        search_wave_size=18,
        max_job_retries=0,
        require_price=True,
        require_image=True,
        require_category=True,
        dedupe_parent_asin=False,
        max_products=None,
        max_results=None,
    )
    p.search_plan = SimpleNamespace(queries=[], sorts=[], max_pages_per_query=0)
    p.storage = Storage(tmp_path)
    p.client = client
    p.billable_job_ids = p.storage.completed_billable_job_ids()
    p.budget = FakeBudget()
    p.metrics = RunMetrics()
    p.stop_requested = asyncio.Event()
    p.checkpoint = p.storage.load_checkpoint()
    p.final_asins = set()
    p.embedding_asins = set()
    p.completed_asins = set()
    p.parent_seen = set()
    return p


def test_large_saved_product_backlog_is_recovered_in_durable_waves(tmp_path: Path) -> None:
    client = ResumeOnlyClient()
    pipeline = make_pipeline(tmp_path, client)

    jobs = [
        {
            "id": f"job-{i}",
            "query": f"B{i:09d}",
            "status": "pending",
        }
        for i in range(250)
    ]
    pipeline.checkpoint["inflight_jobs"]["product"] = {
        "signature": "old",
        "payload": {
            "source": "amazon_product",
            "query": [job["query"] for job in jobs],
        },
        "jobs": jobs,
    }
    pipeline.storage.save_checkpoint(pipeline.checkpoint)

    asyncio.run(pipeline._recover_inflight_products())

    assert client.poll_sizes == [100, 100, 50]
    checkpoint = pipeline.storage.load_checkpoint()
    assert "product" not in checkpoint["inflight_jobs"]
    assert len(checkpoint["completed_product_asins"]) == 250
