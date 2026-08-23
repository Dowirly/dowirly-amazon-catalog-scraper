import json
from pathlib import Path

from dowirly_amazon_scraper.storage import Storage


def test_old_checkpoint_is_migrated_with_inflight_jobs(tmp_path: Path) -> None:
    storage = Storage(tmp_path)
    storage.paths.checkpoint.write_text(
        json.dumps({"version": 1, "completed_search_keys": ["x"]}),
        encoding="utf-8",
    )
    checkpoint = storage.load_checkpoint()
    assert checkpoint["version"] >= 2
    assert checkpoint["completed_search_keys"] == ["x"]
    assert checkpoint["completed_product_asins"] == []
    assert checkpoint["inflight_jobs"] == {}


def test_completed_done_jobs_form_local_usage_floor(tmp_path: Path) -> None:
    storage = Storage(tmp_path)
    storage.append(storage.paths.raw_jobs, {"event": "search_completed", "job_id": "1", "status": "done"})
    storage.append(storage.paths.raw_jobs, {"event": "search_completed", "job_id": "1", "status": "done"})
    storage.append(storage.paths.raw_jobs, {"event": "product_completed", "job_id": "2", "status": "done"})
    storage.append(storage.paths.raw_jobs, {"event": "product_completed", "job_id": "3", "status": "faulted"})
    assert storage.completed_billable_job_ids() == {"1", "2"}
