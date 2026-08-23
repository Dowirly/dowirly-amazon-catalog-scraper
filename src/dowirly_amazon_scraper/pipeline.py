from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from typing import Any

from .budget import BudgetExhausted, BudgetGuard
from .config import AppConfig, SearchPlan, SearchQuery
from .discovery import extract_search_candidates, merge_candidates
from .normalization import extract_parsed_product, normalize_product
from .oxylabs import (
    JobResult,
    OxylabsAuthError,
    OxylabsClient,
    OxylabsQuotaStop,
    base_payload,
)
from .reporting import RunMetrics, write_run_report
from .storage import Storage
from .utils import append_jsonl, count_jsonl, read_jsonl, utc_now_iso

LOGGER = logging.getLogger(__name__)


class Pipeline:
    """Durable discovery/enrichment pipeline.

    Product work is submitted, collected, normalized, fsynced, and checkpointed in
    bounded waves. Only after one wave is safely local do we submit the next one.
    """

    def __init__(self, config: AppConfig, search_plan: SearchPlan) -> None:
        self.config = config
        self.search_plan = search_plan
        self.storage = Storage(config.data_dir)
        self.client = OxylabsClient(config)
        self.billable_job_ids = self.storage.completed_billable_job_ids()
        self.budget = BudgetGuard(
            config,
            self.client,
            local_floor=len(self.billable_job_ids),
        )
        self.metrics = RunMetrics()
        self.stop_requested = asyncio.Event()
        self.checkpoint = self.storage.load_checkpoint()

        self.final_asins = self.storage.final_asins()
        self.embedding_asins = self.storage.embedding_asins()
        self.completed_asins = (
            set(self.checkpoint.get("completed_product_asins") or [])
            | self.final_asins
        )
        self.parent_seen: set[str] = set()
        if self.config.dedupe_parent_asin:
            for final in read_jsonl(self.storage.paths.final_products):
                parent = final.get("parent_external_id")
                if parent:
                    self.parent_seen.add(str(parent))

    async def run(self) -> RunMetrics:
        try:
            self._repair_missing_embeddings()

            if self.config.dry_run:
                LOGGER.info(
                    "Dry run: configuration validated; no Oxylabs requests will be made."
                )
                self.metrics.graceful_stop_reason = "dry_run"
                return self.metrics

            before = await self.budget.refresh()
            self.metrics.usage_before = before.all_count
            limit_text = (
                f"{self.config.max_results:,}"
                if self.config.max_results is not None
                else "provider-managed"
            )
            remaining = self.budget.capacity()
            LOGGER.info(
                "Oxylabs usage: %s results (provider=%s, local_floor=%s); "
                "configured_limit=%s; configured_remaining=%s",
                before.all_count,
                before.provider_count,
                len(self.billable_job_ids),
                limit_text,
                remaining if remaining is not None else "unbounded",
            )
            self._log_progress("startup")

            # Existing provider-side work is always recovered before any new work.
            await self._recover_inflight_products()
            await self._recover_inflight_search()

            while not self.stop_requested.is_set():
                if self.client.submission_blocked_reason:
                    self.metrics.graceful_stop_reason = self.client.submission_blocked_reason
                    break

                if self._max_products_reached():
                    self.metrics.graceful_stop_reason = "requested_product_limit_reached"
                    break

                candidates = self._load_candidates()
                if await self._enrich_one_wave(candidates):
                    continue

                if await self._discover_one_wave():
                    continue

                self.metrics.graceful_stop_reason = "search_plan_exhausted"
                break

            return self.metrics

        except (BudgetExhausted, OxylabsQuotaStop, OxylabsAuthError) as exc:
            LOGGER.warning("Graceful provider stop: %s", exc)
            self.metrics.graceful_stop_reason = str(exc)
            return self.metrics
        finally:
            try:
                if not self.config.dry_run:
                    after = await self.budget.refresh()
                    self.metrics.usage_after = after.all_count
            except Exception as exc:
                LOGGER.error("Could not refresh final usage stats: %s", exc)
                self.metrics.usage_after = max(
                    self.metrics.usage_before,
                    self.budget.snapshot.all_count,
                )

            self.metrics.finished_at = __import__("datetime").datetime.now(
                __import__("datetime").timezone.utc
            )
            report_name = self.metrics.started_at.strftime("run-%Y%m%dT%H%M%SZ.md")
            write_run_report(
                self.storage.paths.report_dir / report_name,
                self.config,
                self.metrics,
            )
            self._log_progress("finished")
            await self.client.close()

    def request_stop(self, reason: str) -> None:
        LOGGER.warning("Stop requested: %s. Current data will be kept.", reason)
        self.metrics.graceful_stop_reason = reason
        self.stop_requested.set()

    async def _recover_inflight_products(self) -> None:
        while not self.stop_requested.is_set():
            inflight = (self.checkpoint.get("inflight_jobs") or {}).get("product")
            if not inflight:
                return

            # If a previous process wrote the product before dying, remove its job
            # from the provider-side backlog without polling it again.
            self._remove_inflight_queries("product", self.completed_asins)
            inflight = (self.checkpoint.get("inflight_jobs") or {}).get("product")
            if not inflight:
                return

            jobs = list(inflight.get("jobs") or [])
            if not jobs:
                self.checkpoint.setdefault("inflight_jobs", {}).pop("product", None)
                self.storage.save_checkpoint(self.checkpoint)
                return

            wave_jobs = jobs[: self.config.wave_size]
            payload = dict(inflight.get("payload") or {})
            LOGGER.warning(
                "RESUME | phase=product | collecting %s/%s saved jobs in a durable wave; no duplicate submit",
                len(wave_jobs),
                len(jobs),
            )

            results = await self._poll_with_fault_retries(
                payload,
                wave_jobs,
                phase="product",
            )
            candidates = self._load_candidates()
            candidate_by_asin = {c["asin"]: c for c in candidates}
            self._process_product_results(results, candidate_by_asin)
            self._remove_inflight_queries(
                "product",
                {result.query.upper() for result in results},
            )
            self._save_completed_products_checkpoint()
            await self._refresh_after_wave()
            self._log_progress("recover_product_wave")

            if self.client.submission_blocked_reason:
                return

    async def _recover_inflight_search(self) -> None:
        while not self.stop_requested.is_set():
            inflight = (self.checkpoint.get("inflight_jobs") or {}).get("search")
            if not inflight:
                return

            jobs = list(inflight.get("jobs") or [])
            if not jobs:
                self.checkpoint.setdefault("inflight_jobs", {}).pop("search", None)
                self.storage.save_checkpoint(self.checkpoint)
                return

            wave_jobs = jobs[: self.config.search_wave_size]
            payload = dict(inflight.get("payload") or {})
            LOGGER.warning(
                "RESUME | phase=search | collecting %s/%s saved jobs in a durable wave; no duplicate submit",
                len(wave_jobs),
                len(jobs),
            )
            results = await self._poll_with_fault_retries(
                payload,
                wave_jobs,
                phase="search",
            )
            self._process_search_results(results, payload)
            self._remove_inflight_queries(
                "search",
                {result.query for result in results},
            )
            await self._refresh_after_wave()
            self._log_progress("recover_search_wave")

            if self.client.submission_blocked_reason:
                return

    async def _enrich_one_wave(self, candidates: list[dict[str, Any]]) -> bool:
        if (self.checkpoint.get("inflight_jobs") or {}).get("product"):
            await self._recover_inflight_products()
            return True

        queue = [
            candidate
            for candidate in candidates
            if candidate.get("asin") not in self.completed_asins
        ]
        if not queue:
            return False

        queue.sort(key=self._candidate_score, reverse=True)

        wave_target = self.config.wave_size
        if self.config.max_products is not None:
            remaining_target = max(
                0,
                self.config.max_products - len(self.final_asins),
            )
            if remaining_target <= 0:
                return False
            wave_target = min(wave_target, remaining_target)

        selected = queue[:wave_target]
        allowed = self.budget.reserve(len(selected))
        if allowed <= 0:
            raise BudgetExhausted("Configured max-results reached before next product wave")
        selected = selected[:allowed]
        asins = [str(item["asin"]).upper() for item in selected]

        payload = base_payload(self.config, "amazon_product")
        payload.update(
            {
                "query": asins,
                "context": [
                    {"key": "currency", "value": "SAR"},
                    {"key": "autoselect_variant", "value": True},
                ],
            }
        )

        LOGGER.info(
            "WAVE | phase=product | submitting=%s | wave_size=%s | accepted_total=%s",
            len(asins),
            self.config.wave_size,
            len(self.final_asins),
        )
        jobs = await self._submit_new_wave(payload, phase="product")
        if not jobs:
            return False

        results = await self._poll_with_fault_retries(payload, jobs, phase="product")
        candidate_by_asin = {c["asin"]: c for c in candidates}
        self._process_product_results(results, candidate_by_asin)
        self._remove_inflight_queries(
            "product",
            {result.query.upper() for result in results},
        )
        self._save_completed_products_checkpoint()
        await self._refresh_after_wave()
        self._log_progress("enrichment_wave")
        return True

    def _process_product_results(
        self,
        results: list[JobResult],
        candidate_by_asin: dict[str, dict[str, Any]],
    ) -> None:
        for result in results:
            asin = result.query.upper()
            if asin in self.completed_asins:
                continue

            if result.status == "faulted" or result.result is None:
                self._reject(asin, ["provider_faulted"], result)
                self.completed_asins.add(asin)
                continue

            self.storage.append(
                self.storage.paths.raw_products,
                {
                    "asin": asin,
                    "job_id": result.job_id,
                    "retrieved_at": utc_now_iso(),
                    "response": result.result,
                },
            )

            parsed, outer = extract_parsed_product(result.result)
            if parsed is None:
                self._reject(asin, ["missing_parsed_content"], result)
                self.completed_asins.add(asin)
                continue

            norm = normalize_product(
                parsed,
                outer,
                candidate_by_asin.get(asin),
                require_price=self.config.require_price,
                require_image=self.config.require_image,
                require_category=self.config.require_category,
            )
            if norm.product is None:
                self._reject(asin, norm.rejection_reasons, result)
                self.completed_asins.add(asin)
                continue

            parent = norm.product.get("parent_external_id")
            if (
                self.config.dedupe_parent_asin
                and parent
                and str(parent) in self.parent_seen
            ):
                self._reject(asin, [f"duplicate_parent_asin:{parent}"], result)
                self.completed_asins.add(asin)
                continue
            if parent:
                self.parent_seen.add(str(parent))

            if asin not in self.final_asins:
                self.storage.append(self.storage.paths.final_products, norm.product)
                self.final_asins.add(asin)
                self.metrics.accepted_products = len(self.final_asins)

            if asin not in self.embedding_asins:
                self.storage.append(
                    self.storage.paths.embedding_input,
                    self._embedding_record(norm.product),
                )
                self.embedding_asins.add(asin)

            self.completed_asins.add(asin)

    async def _discover_one_wave(self) -> bool:
        if (self.checkpoint.get("inflight_jobs") or {}).get("search"):
            await self._recover_inflight_search()
            return True

        selection = self._next_search_wave()
        if selection is None:
            return False

        sort, page, queries = selection
        allowed = self.budget.reserve(len(queries))
        if allowed <= 0:
            raise BudgetExhausted("Configured max-results reached before next search wave")
        queries = queries[:allowed]

        payload = base_payload(self.config, "amazon_search")
        payload.update(
            {
                "query": [q.query for q in queries],
                "start_page": page,
                "pages": 1,
                "context": [
                    {"key": "currency", "value": "SAR"},
                    {"key": "sort_by", "value": sort},
                ],
            }
        )

        LOGGER.info(
            "WAVE | phase=search | submitting=%s | sort=%s | page=%s",
            len(queries),
            sort,
            page,
        )
        jobs = await self._submit_new_wave(payload, phase="search")
        if not jobs:
            return False

        results = await self._poll_with_fault_retries(payload, jobs, phase="search")
        self._process_search_results(results, payload)
        self._remove_inflight_queries(
            "search",
            {result.query for result in results},
        )
        await self._refresh_after_wave()
        self._log_progress("discovery_wave")
        return True

    def _next_search_wave(self) -> tuple[str, int, list[SearchQuery]] | None:
        completed = set(self.checkpoint.get("completed_search_keys") or [])
        for sort in self.search_plan.sorts:
            for page in range(1, self.search_plan.max_pages_per_query + 1):
                pending = [
                    q
                    for q in self.search_plan.queries
                    if f"sort={sort}|page={page}|query={q.query}" not in completed
                ]
                if pending:
                    return sort, page, pending[: self.config.search_wave_size]
        return None

    def _process_search_results(
        self,
        results: list[JobResult],
        payload: dict[str, Any],
    ) -> None:
        query_to_category = {
            q.query: q.logical_category for q in self.search_plan.queries
        }
        completed = set(self.checkpoint.get("completed_search_keys") or [])
        page = int(payload.get("start_page") or 1)
        sort = self._payload_context_value(payload, "sort_by") or "featured"

        for result in results:
            completed.add(f"sort={sort}|page={page}|query={result.query}")

            if result.status == "faulted" or result.result is None:
                continue

            self.storage.append(
                self.storage.paths.raw_search,
                {
                    "query": result.query,
                    "job_id": result.job_id,
                    "retrieved_at": utc_now_iso(),
                    "response": result.result,
                },
            )
            discovered = extract_search_candidates(
                result.result,
                query_to_category=query_to_category,
                include_paid=self.config.include_paid_search_results,
            )
            for record in discovered:
                self.storage.append(self.storage.paths.discovered, record)
            self.metrics.discovered_records += len(discovered)

        self.checkpoint["completed_search_keys"] = sorted(completed)
        self.storage.save_checkpoint(self.checkpoint)

        merged = merge_candidates(read_jsonl(self.storage.paths.discovered))
        self.metrics.unique_candidates = len(merged)
        self._rewrite_unique_candidates(merged)
        LOGGER.info("Discovery catalog now has %s unique ASIN candidates.", len(merged))

    async def _submit_new_wave(
        self,
        payload: dict[str, Any],
        *,
        phase: str,
    ) -> list[dict[str, Any]]:
        signature = self._payload_signature(payload)

        def checkpoint_progress(jobs: list[dict[str, Any]]) -> None:
            self._set_inflight(phase, signature, payload, jobs)

        jobs = await self.client.submit_batch(
            payload,
            on_progress=checkpoint_progress,
        )
        if jobs:
            self._count_new_jobs(phase, jobs)
            self._record_jobs(f"{phase}_submitted", jobs)
        return jobs

    async def _poll_with_fault_retries(
        self,
        payload: dict[str, Any],
        jobs: list[dict[str, Any]],
        *,
        phase: str,
    ) -> list[JobResult]:
        original_queries = [
            str(job.get("query") or job.get("url") or "") for job in jobs
        ]
        all_results = await self.client.poll_jobs(jobs, max_retries=0)
        latest: dict[str, JobResult] = {}
        for result in all_results:
            latest[result.query] = result
            self._record_job_result(result, phase)

        for _retry_round in range(self.config.max_job_retries):
            retry_queries = [
                query
                for query in original_queries
                if query in latest
                and (
                    latest[query].status == "faulted"
                    or latest[query].result is None
                )
            ]
            if not retry_queries or self.client.submission_blocked_reason:
                break

            allowed = self.budget.reserve(len(retry_queries))
            if allowed <= 0:
                break
            retry_queries = retry_queries[:allowed]
            retry_payload = dict(payload)
            retry_payload["query"] = retry_queries
            signature = self._payload_signature(retry_payload)

            def checkpoint_retry_progress(new_jobs: list[dict[str, Any]]) -> None:
                self._append_inflight_jobs(
                    phase,
                    signature,
                    payload,
                    new_jobs,
                )

            try:
                retry_jobs = await self.client.submit_batch(
                    retry_payload,
                    on_progress=checkpoint_retry_progress,
                )
            except (OxylabsQuotaStop, OxylabsAuthError) as exc:
                self.client.submission_blocked_reason = str(exc)
                LOGGER.warning(
                    "Retry submission stopped by provider; preserving already collected results: %s",
                    exc,
                )
                break

            if not retry_jobs:
                break
            self._count_new_jobs(phase, retry_jobs)
            self._record_jobs(f"{phase}_retry_submitted", retry_jobs)
            retry_results = await self.client.poll_jobs(retry_jobs, max_retries=0)
            for result in retry_results:
                latest[result.query] = result
                self._record_job_result(result, phase)

        return [latest[q] for q in original_queries if q in latest]

    async def _refresh_after_wave(self) -> None:
        try:
            await self.budget.refresh()
        except (OxylabsQuotaStop, OxylabsAuthError) as exc:
            self.client.submission_blocked_reason = str(exc)
            LOGGER.warning(
                "PROVIDER_STOP_AFTER_SAVE | current wave is safe locally | %s",
                exc,
            )

    def _set_inflight(
        self,
        phase: str,
        signature: str,
        payload: dict[str, Any],
        jobs: list[dict[str, Any]],
    ) -> None:
        self.checkpoint.setdefault("inflight_jobs", {})[phase] = {
            "signature": signature,
            "payload": payload,
            "jobs": list(jobs),
            "updated_at": utc_now_iso(),
        }
        self.storage.save_checkpoint(self.checkpoint)

    def _append_inflight_jobs(
        self,
        phase: str,
        signature: str,
        payload: dict[str, Any],
        jobs: list[dict[str, Any]],
    ) -> None:
        inflight = self.checkpoint.setdefault("inflight_jobs", {})
        existing = inflight.get(phase) or {}
        merged: list[dict[str, Any]] = []
        seen: set[str] = set()
        for job in list(existing.get("jobs") or []) + list(jobs):
            job_id = str(job.get("id") or "")
            if not job_id or job_id in seen:
                continue
            seen.add(job_id)
            merged.append(job)
        inflight[phase] = {
            "signature": existing.get("signature") or signature,
            "payload": existing.get("payload") or payload,
            "jobs": merged,
            "updated_at": utc_now_iso(),
        }
        self.storage.save_checkpoint(self.checkpoint)

    def _remove_inflight_queries(self, phase: str, queries: set[str]) -> None:
        if not queries:
            return
        inflight_map = self.checkpoint.setdefault("inflight_jobs", {})
        record = inflight_map.get(phase)
        if not record:
            return

        normalized = {q.upper() for q in queries} if phase == "product" else queries
        remaining: list[dict[str, Any]] = []
        for job in record.get("jobs") or []:
            query = str(job.get("query") or job.get("url") or "")
            key = query.upper() if phase == "product" else query
            if key not in normalized:
                remaining.append(job)

        if remaining:
            record["jobs"] = remaining
            record["updated_at"] = utc_now_iso()
            inflight_map[phase] = record
        else:
            inflight_map.pop(phase, None)
        self.storage.save_checkpoint(self.checkpoint)

    def _save_completed_products_checkpoint(self) -> None:
        self.checkpoint["completed_product_asins"] = sorted(self.completed_asins)
        self.storage.save_checkpoint(self.checkpoint)

    def _load_candidates(self) -> list[dict[str, Any]]:
        candidates = read_jsonl(self.storage.paths.unique_candidates)
        if candidates:
            return candidates
        discovered = read_jsonl(self.storage.paths.discovered)
        if not discovered:
            return []
        candidates = merge_candidates(discovered)
        self._rewrite_unique_candidates(candidates)
        return candidates

    def _rewrite_unique_candidates(self, candidates: list[dict[str, Any]]) -> None:
        path = self.storage.paths.unique_candidates
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        if tmp.exists():
            tmp.unlink()
        for item in candidates:
            append_jsonl(tmp, item)
        tmp.replace(path)

    def _repair_missing_embeddings(self) -> None:
        for product in read_jsonl(self.storage.paths.final_products):
            asin = str(product.get("external_id") or "").upper()
            if not asin or asin in self.embedding_asins:
                continue
            self.storage.append(
                self.storage.paths.embedding_input,
                self._embedding_record(product),
            )
            self.embedding_asins.add(asin)

    @staticmethod
    def _embedding_record(product: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": product["id"],
            "external_id": product["external_id"],
            "text": product["embedding_text"],
            "metadata": {
                "source": "amazon",
                "marketplace": "amazon.sa",
                "category": product["category"],
                "brand": product.get("brand"),
            },
        }

    def _max_products_reached(self) -> bool:
        return (
            self.config.max_products is not None
            and len(self.final_asins) >= self.config.max_products
        )

    @staticmethod
    def _payload_signature(payload: dict[str, Any]) -> str:
        canonical = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @staticmethod
    def _payload_context_value(payload: dict[str, Any], key: str) -> Any:
        for item in payload.get("context") or []:
            if isinstance(item, dict) and item.get("key") == key:
                return item.get("value")
        return None

    def _count_new_jobs(self, phase: str, jobs: list[dict[str, Any]]) -> None:
        if phase == "search":
            self.metrics.search_jobs += len(jobs)
        else:
            self.metrics.product_jobs += len(jobs)

    def _log_progress(self, stage: str) -> None:
        accepted = count_jsonl(self.storage.paths.final_products)
        rejected = count_jsonl(self.storage.paths.rejected)
        discovered = count_jsonl(self.storage.paths.discovered)
        candidates = count_jsonl(self.storage.paths.unique_candidates)
        inflight = sum(
            len((record or {}).get("jobs") or [])
            for record in (self.checkpoint.get("inflight_jobs") or {}).values()
        )
        limit = (
            str(self.config.max_results)
            if self.config.max_results is not None
            else "provider-managed"
        )
        LOGGER.info(
            "PROGRESS | stage=%s | accepted=%s | rejected=%s | candidates=%s | discovered=%s "
            "| search_jobs_this_run=%s | product_jobs_this_run=%s | usage=%s/%s "
            "| provider_usage=%s | inflight_jobs=%s | product_wave=%s",
            stage,
            accepted,
            rejected,
            candidates,
            discovered,
            self.metrics.search_jobs,
            self.metrics.product_jobs,
            self.budget.snapshot.all_count,
            limit,
            self.budget.snapshot.provider_count,
            inflight,
            self.config.wave_size,
        )

    def _reject(self, asin: str, reasons: list[str], result: JobResult) -> None:
        self.metrics.rejected_products += 1
        self.storage.append(
            self.storage.paths.rejected,
            {
                "asin": asin,
                "reasons": reasons,
                "job_id": result.job_id,
                "rejected_at": utc_now_iso(),
            },
        )

    def _record_jobs(self, event: str, jobs: list[dict[str, Any]]) -> None:
        for job in jobs:
            self.storage.append(
                self.storage.paths.raw_jobs,
                {
                    "event": event,
                    "at": utc_now_iso(),
                    "job_id": str(job.get("id")),
                    "query": job.get("query"),
                    "status": job.get("status"),
                },
            )

    def _record_job_result(self, result: JobResult, phase: str) -> None:
        self.storage.append(
            self.storage.paths.raw_jobs,
            {
                "event": f"{phase}_completed",
                "at": utc_now_iso(),
                "job_id": result.job_id,
                "query": result.query,
                "status": result.status,
                "metadata": result.metadata,
            },
        )
        if result.status == "done":
            self.billable_job_ids.add(result.job_id)
            self.budget.set_local_floor(len(self.billable_job_ids))

    @staticmethod
    def _candidate_score(candidate: dict[str, Any]) -> int:
        score = len(candidate.get("search_queries") or []) * 3
        score += 2 if candidate.get("title") else 0
        score += 2 if candidate.get("image") else 0
        score += 2 if candidate.get("price") else 0
        score += 1 if candidate.get("rating") else 0
        score += 1 if candidate.get("reviews_count") else 0
        return score
