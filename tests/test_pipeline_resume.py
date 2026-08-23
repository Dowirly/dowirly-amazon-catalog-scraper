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


class FirstClient:
    def __init__(self) -> None:
        self.submit_calls = 0

    async def submit_batch(self, payload):
        self.submit_calls += 1
        return [{"id": "job-1", "query": payload["query"][0], "status": "pending"}]

    async def poll_jobs(self, jobs, *, max_retries):
        return [JobResult("job-1", "q1", "done", {"status": "done"}, {"results": []})]


class ResumeClient(FirstClient):
    async def submit_batch(self, payload):
        raise AssertionError("resume must not submit the same scraping job again")


def make_pipeline(tmp_path: Path, client) -> Pipeline:
    p = object.__new__(Pipeline)
    p.config = SimpleNamespace(max_job_retries=0, hard_result_limit=2000)
    p.storage = Storage(tmp_path)
    p.client = client
    p.billable_job_ids = p.storage.completed_billable_job_ids()
    p.budget = FakeBudget()
    p.metrics = RunMetrics()
    p.stop_requested = asyncio.Event()
    p.checkpoint = p.storage.load_checkpoint()
    return p


def test_inflight_jobs_are_resumed_without_resubmission(tmp_path: Path) -> None:
    payload = {"source": "amazon_search", "query": ["q1"], "pages": 1, "parse": True}

    first_client = FirstClient()
    first = make_pipeline(tmp_path, first_client)
    result = asyncio.run(first._submit_and_poll_with_fault_retries(payload, phase="search"))
    assert result[0].status == "done"
    assert first_client.submit_calls == 1
    assert first.storage.load_checkpoint()["inflight_jobs"]["search"]["jobs"][0]["id"] == "job-1"

    # Simulate a hard reboot after Oxylabs finished but before the caller atomically
    # cleared the in-flight checkpoint. The new process must poll job-1, not submit q1.
    resumed = make_pipeline(tmp_path, ResumeClient())
    result2 = asyncio.run(resumed._submit_and_poll_with_fault_retries(payload, phase="search"))
    assert result2[0].job_id == "job-1"
