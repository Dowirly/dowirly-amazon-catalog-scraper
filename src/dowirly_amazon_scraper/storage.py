from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .utils import append_jsonl, atomic_write_json, read_jsonl


@dataclass(slots=True)
class Paths:
    root: Path

    @property
    def raw_search(self) -> Path:
        return self.root / "raw" / "search_results.jsonl"

    @property
    def raw_products(self) -> Path:
        return self.root / "raw" / "product_results.jsonl"

    @property
    def raw_jobs(self) -> Path:
        return self.root / "raw" / "job_events.jsonl"

    @property
    def discovered(self) -> Path:
        return self.root / "intermediate" / "discovered_products.jsonl"

    @property
    def unique_candidates(self) -> Path:
        return self.root / "intermediate" / "unique_candidates.jsonl"

    @property
    def rejected(self) -> Path:
        return self.root / "intermediate" / "rejected_products.jsonl"

    @property
    def final_products(self) -> Path:
        return self.root / "final" / "products.jsonl"

    @property
    def embedding_input(self) -> Path:
        return self.root / "final" / "embedding_input.jsonl"

    @property
    def checkpoint(self) -> Path:
        return self.root / "intermediate" / "checkpoint.json"

    @property
    def report_dir(self) -> Path:
        return self.root / "reports"


class Storage:
    def __init__(self, root: Path) -> None:
        self.paths = Paths(root)
        for child in ["raw", "intermediate", "final", "reports"]:
            (root / child).mkdir(parents=True, exist_ok=True)

    def append(self, path: Path, record: Any) -> None:
        append_jsonl(path, record)

    def load_checkpoint(self) -> dict[str, Any]:
        p = self.paths.checkpoint
        checkpoint: dict[str, Any]
        if not p.exists():
            checkpoint = {}
        else:
            import json
            with p.open("r", encoding="utf-8") as f:
                checkpoint = json.load(f)

        # Version 2 adds durable in-flight Oxylabs jobs. setdefault keeps old
        # checkpoints fully compatible when users pull this update mid-project.
        checkpoint.setdefault("version", 2)
        checkpoint["version"] = max(2, int(checkpoint.get("version") or 0))
        checkpoint.setdefault("completed_search_keys", [])
        checkpoint.setdefault("completed_product_asins", [])
        checkpoint.setdefault("inflight_jobs", {})
        return checkpoint

    def save_checkpoint(self, checkpoint: dict[str, Any]) -> None:
        atomic_write_json(self.paths.checkpoint, checkpoint)

    def discovered_asins(self) -> set[str]:
        return {str(r.get("asin")) for r in read_jsonl(self.paths.discovered) if r.get("asin")}

    def final_asins(self) -> set[str]:
        return {str(r.get("external_id")) for r in read_jsonl(self.paths.final_products) if r.get("external_id")}

    def completed_billable_job_ids(self) -> set[str]:
        """Return locally observed completed Oxylabs job IDs.

        The provider usage endpoint can lag on fresh/free accounts. Completed
        Push-Pull jobs are therefore also used as a conservative local usage floor.
        `faulted` jobs are intentionally excluded because Oxylabs documents them
        as unbilled.
        """
        ids: set[str] = set()
        for record in read_jsonl(self.paths.raw_jobs):
            event = str(record.get("event") or "")
            if event.endswith("_completed") and record.get("status") == "done" and record.get("job_id"):
                ids.add(str(record["job_id"]))
        return ids
