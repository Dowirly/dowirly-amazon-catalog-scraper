from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any

from .budget import BudgetExhausted, BudgetGuard
from .config import AppConfig, SearchPlan
from .discovery import extract_search_candidates, merge_candidates
from .normalization import extract_parsed_product, normalize_product
from .oxylabs import JobResult, OxylabsClient, OxylabsError, OxylabsQuotaStop, base_payload
from .reporting import RunMetrics, write_run_report
from .storage import Storage
from .utils import append_jsonl, read_jsonl, utc_now_iso

LOGGER = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, config: AppConfig, search_plan: SearchPlan) -> None:
        self.config = config
        self.search_plan = search_plan
        self.storage = Storage(config.data_dir)
        self.client = OxylabsClient(config)
        self.budget = BudgetGuard(config, self.client)
        self.metrics = RunMetrics()
        self.stop_requested = asyncio.Event()
        self.checkpoint = self.storage.load_checkpoint()

    async def run(self) -> RunMetrics:
        try:
            if self.config.dry_run:
                LOGGER.info("Dry run: configuration validated; no Oxylabs requests will be made.")
                self.metrics.graceful_stop_reason = "dry_run"
                return self.metrics

            before = await self.budget.refresh()
            self.metrics.usage_before = before.all_count
            LOGGER.info(
                "Oxylabs usage: %s / %s results; remaining guarded capacity=%s",
                before.all_count,
                self.config.hard_result_limit,
                self.budget.capacity(),
            )
            if self.budget.capacity() <= 0:
                self.metrics.graceful_stop_reason = "result_budget_already_exhausted"
                return self.metrics

            candidates = await self._discover()
            if self.stop_requested.is_set():
                self.metrics.graceful_stop_reason = self.metrics.graceful_stop_reason or "stop_requested_after_discovery"
                return self.metrics

            await self._enrich(candidates)
            return self.metrics
        except (BudgetExhausted, OxylabsQuotaStop) as exc:
            LOGGER.warning("Graceful stop: %s", exc)
            self.metrics.graceful_stop_reason = str(exc)
            return self.metrics
        finally:
            try:
                if not self.config.dry_run:
                    after = await self.budget.refresh()
                    self.metrics.usage_after = after.all_count
            except Exception as exc:  # reporting must not destroy already-collected data
                LOGGER.error("Could not refresh final usage stats: %s", exc)
                self.metrics.usage_after = self.metrics.usage_before
            self.metrics.finished_at = __import__("datetime").datetime.now(__import__("datetime").timezone.utc)
            report_name = self.metrics.started_at.strftime("run-%Y%m%dT%H%M%SZ.md")
            write_run_report(self.storage.paths.report_dir / report_name, self.config, self.metrics)
            await self.client.close()

    def request_stop(self, reason: str) -> None:
        LOGGER.warning("Stop requested: %s. Current data will be kept.", reason)
        self.metrics.graceful_stop_reason = reason
        self.stop_requested.set()

    async def _discover(self) -> list[dict[str, Any]]:
        existing_records = read_jsonl(self.storage.paths.discovered)
        merged = merge_candidates(existing_records)
        query_to_category = {q.query: q.logical_category for q in self.search_plan.queries}
        completed_keys = set(self.checkpoint.get("completed_search_keys") or [])

        desired = self._desired_candidate_count()
        if len(merged) >= desired:
            LOGGER.info("Discovery already has %s candidates; desired=%s. Resuming enrichment.", len(merged), desired)
            self.metrics.unique_candidates = len(merged)
            return merged

        # Search each page as one Oxylabs job. Batch only varies `query`, which follows
        # the provider's /batch contract; page/sort remain singular per batch.
        search_chunk_size = 10 if self.config.mode == "test" else 100
        query_chunks = [self.search_plan.queries[i : i + search_chunk_size] for i in range(0, len(self.search_plan.queries), search_chunk_size)]
        for sort in self.search_plan.sorts:
            for page in range(1, self.search_plan.max_pages_per_query + 1):
                for chunk_index, chunk in enumerate(query_chunks):
                    if self.stop_requested.is_set() or len(merged) >= desired:
                        break
                    pending_chunk = [
                        q for q in chunk
                        if f"sort={sort}|page={page}|query={q.query}" not in completed_keys
                    ]
                    if not pending_chunk:
                        continue

                    await self.budget.refresh()
                    capacity = self.budget.capacity()
                    if capacity <= 0:
                        raise BudgetExhausted("No result budget left during discovery")

                    # Never spend so much on discovery that there is nothing left to
                    # enrich. For small target runs reserve at least target count.
                    reserve_for_products = self._product_reserve(len(merged))
                    search_capacity = max(0, capacity - reserve_for_products)
                    if search_capacity <= 0:
                        LOGGER.info("Stopping discovery to preserve %s result slots for product pages.", reserve_for_products)
                        self.metrics.graceful_stop_reason = self.metrics.graceful_stop_reason or "discovery_budget_reserve_reached"
                        self.metrics.unique_candidates = len(merged)
                        return merged

                    selected = pending_chunk[: min(len(pending_chunk), search_capacity)]
                    if not selected:
                        return merged
                    payload = base_payload(self.config, "amazon_search")
                    payload.update(
                        {
                            "query": [q.query for q in selected],
                            "start_page": page,
                            "pages": 1,
                            "context": [
                                {"key": "currency", "value": "SAR"},
                                {"key": "sort_by", "value": sort},
                            ],
                        }
                    )
                    allowed = self.budget.reserve(len(selected))
                    payload["query"] = payload["query"][:allowed]
                    if not payload["query"]:
                        raise BudgetExhausted("No safe capacity for another search batch")

                    LOGGER.info("Submitting search batch: %s jobs (sort=%s page=%s)", len(payload["query"]), sort, page)
                    results = await self._submit_and_poll_with_fault_retries(payload, phase="search")
                    for result in results:
                        if result.status == "faulted" or result.result is None:
                            continue
                        self.storage.append(
                            self.storage.paths.raw_search,
                            {"query": result.query, "job_id": result.job_id, "retrieved_at": utc_now_iso(), "response": result.result},
                        )
                        discovered = extract_search_candidates(
                            result.result,
                            query_to_category=query_to_category,
                            include_paid=self.config.include_paid_search_results,
                        )
                        for record in discovered:
                            self.storage.append(self.storage.paths.discovered, record)
                        self.metrics.discovered_records += len(discovered)

                    for result in results:
                        if result.status == "done" and result.result is not None:
                            completed_keys.add(f"sort={sort}|page={page}|query={result.query}")
                    self.checkpoint["completed_search_keys"] = sorted(completed_keys)
                    self.storage.save_checkpoint(self.checkpoint)
                    merged = merge_candidates(read_jsonl(self.storage.paths.discovered))
                    self.metrics.unique_candidates = len(merged)
                    self._rewrite_unique_candidates(merged)
                    LOGGER.info("Discovery now has %s unique ASIN candidates.", len(merged))
                    await self.budget.refresh()  # releases capacity from faulted, unbilled jobs

                if self.stop_requested.is_set() or len(merged) >= desired:
                    break
            if self.stop_requested.is_set() or len(merged) >= desired:
                break

        self._rewrite_unique_candidates(merged)
        return merged

    def _desired_candidate_count(self) -> int:
        if self.config.max_products is not None:
            # Oversample to absorb missing/invalid product pages without another search pass.
            return max(self.config.max_products + 20, int(self.config.max_products * 1.5))
        # In maximize mode, aim for enough candidates to consume almost all quota on
        # product pages, while leaving a small fraction for discovery.
        return max(100, int(self.budget.capacity() * 0.98))

    def _product_reserve(self, current_candidates: int) -> int:
        if self.config.max_products is not None:
            remaining_target = max(0, self.config.max_products - len(self.storage.final_asins()))
            return min(self.budget.capacity(), remaining_target)
        # For maximize mode, reserve the candidates we already have for enrichment.
        return min(self.budget.capacity(), max(25, current_candidates))

    def _rewrite_unique_candidates(self, candidates: list[dict[str, Any]]) -> None:
        p = self.storage.paths.unique_candidates
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        if tmp.exists():
            tmp.unlink()
        for item in candidates:
            append_jsonl(tmp, item)
        tmp.replace(p)

    async def _enrich(self, candidates: list[dict[str, Any]]) -> None:
        candidate_by_asin = {c["asin"]: c for c in candidates}
        existing_final_asins = self.storage.final_asins()
        self.metrics.accepted_products = len(existing_final_asins)
        completed_asins = set(self.checkpoint.get("completed_product_asins") or []) | existing_final_asins
        queue = [asin for asin in candidate_by_asin if asin not in completed_asins]
        if not queue:
            LOGGER.info("No product ASINs left to enrich.")
            return

        # Best discovery candidates first: products seen more often, with image/title/price.
        queue.sort(key=lambda a: self._candidate_score(candidate_by_asin[a]), reverse=True)
        parent_seen: set[str] = set()
        if self.config.dedupe_parent_asin:
            for final in read_jsonl(self.storage.paths.final_products):
                parent = final.get("parent_external_id")
                if parent:
                    parent_seen.add(str(parent))

        index = 0
        while index < len(queue) and not self.stop_requested.is_set():
            if self.config.max_products is not None and self.metrics.accepted_products >= self.config.max_products:
                self.metrics.graceful_stop_reason = self.metrics.graceful_stop_reason or "requested_product_limit_reached"
                break

            await self.budget.refresh()
            capacity = self.budget.capacity()
            if capacity <= 0:
                raise BudgetExhausted("Oxylabs result budget reached during product enrichment")

            remaining_target = (
                self.config.max_products - self.metrics.accepted_products
                if self.config.max_products is not None
                else capacity
            )
            take = min(self.config.batch_size, capacity, max(0, remaining_target), len(queue) - index)
            if take <= 0:
                break
            asins = queue[index : index + take]
            index += take

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
            allowed = self.budget.reserve(len(asins))
            payload["query"] = payload["query"][:allowed]
            LOGGER.info("Submitting product batch: %s ASINs", len(payload["query"]))
            results = await self._submit_and_poll_with_fault_retries(payload, phase="product")

            for result in results:
                asin = result.query.upper()
                if result.status == "faulted" or result.result is None:
                    continue

                self.storage.append(
                    self.storage.paths.raw_products,
                    {"asin": asin, "job_id": result.job_id, "retrieved_at": utc_now_iso(), "response": result.result},
                )
                parsed, outer = extract_parsed_product(result.result)
                if parsed is None:
                    self._reject(asin, ["missing_parsed_content"], result)
                    completed_asins.add(asin)
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
                    completed_asins.add(asin)
                    continue

                parent = norm.product.get("parent_external_id")
                if self.config.dedupe_parent_asin and parent and parent in parent_seen:
                    self._reject(asin, [f"duplicate_parent_asin:{parent}"], result)
                    completed_asins.add(asin)
                    continue
                if parent:
                    parent_seen.add(str(parent))

                self.storage.append(self.storage.paths.final_products, norm.product)
                self.storage.append(
                    self.storage.paths.embedding_input,
                    {
                        "id": norm.product["id"],
                        "external_id": norm.product["external_id"],
                        "text": norm.product["embedding_text"],
                        "metadata": {
                            "source": "amazon",
                            "marketplace": "amazon.sa",
                            "category": norm.product["category"],
                            "brand": norm.product.get("brand"),
                        },
                    },
                )
                self.metrics.accepted_products += 1
                completed_asins.add(asin)

            self.checkpoint["completed_product_asins"] = sorted(completed_asins)
            self.storage.save_checkpoint(self.checkpoint)
            await self.budget.refresh()
            LOGGER.info(
                "Enrichment progress: accepted=%s rejected=%s usage=%s/%s",
                self.metrics.accepted_products,
                self.metrics.rejected_products,
                self.budget.snapshot.all_count,
                self.config.hard_result_limit,
            )


    async def _submit_and_poll_with_fault_retries(
        self, payload: dict[str, Any], *, phase: str
    ) -> list[JobResult]:
        """Submit a Push-Pull batch and retry only provider-faulted jobs.

        Oxylabs documents `faulted` jobs as unbilled. We therefore refresh the
        official usage counter before each retry wave and only resubmit faulted
        query values that still fit inside the configured hard result guard.
        Successful jobs are never submitted twice.
        """
        pending = [str(v) for v in payload.get("query") or []]
        finished: list[JobResult] = []
        attempt = 0

        while pending and not self.stop_requested.is_set():
            if attempt > 0:
                await self.budget.refresh()
                allowed = self.budget.reserve(len(pending))
                if allowed <= 0:
                    LOGGER.warning("No quota headroom remains to retry %s faulted %s jobs.", len(pending), phase)
                    break
                pending = pending[:allowed]
                LOGGER.info("Retrying %s faulted %s jobs (retry %s/%s).", len(pending), phase, attempt, self.config.max_job_retries)

            wave_payload = dict(payload)
            wave_payload["query"] = pending
            jobs = await self.client.submit_batch(wave_payload)
            if phase == "search":
                self.metrics.search_jobs += len(jobs)
            else:
                self.metrics.product_jobs += len(jobs)
            self._record_jobs(f"{phase}_submitted", jobs)

            results = await self.client.poll_jobs(jobs, max_retries=0)
            retry_queries: list[str] = []
            final_faults: list[JobResult] = []
            for result in results:
                self._record_job_result(result, phase)
                if result.status == "faulted" or result.result is None:
                    self.metrics.faulted_jobs += 1
                    if attempt < self.config.max_job_retries:
                        retry_queries.append(result.query)
                    else:
                        final_faults.append(result)
                else:
                    finished.append(result)

            if not retry_queries:
                finished.extend(final_faults)
                break

            pending = retry_queries
            attempt += 1

        return finished

    def _reject(self, asin: str, reasons: list[str], result: JobResult) -> None:
        self.metrics.rejected_products += 1
        self.storage.append(
            self.storage.paths.rejected,
            {"asin": asin, "reasons": reasons, "job_id": result.job_id, "rejected_at": utc_now_iso()},
        )

    def _record_jobs(self, event: str, jobs: list[dict[str, Any]]) -> None:
        for job in jobs:
            self.storage.append(
                self.storage.paths.raw_jobs,
                {"event": event, "at": utc_now_iso(), "job_id": str(job.get("id")), "query": job.get("query"), "status": job.get("status")},
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

    @staticmethod
    def _candidate_score(candidate: dict[str, Any]) -> int:
        score = len(candidate.get("search_queries") or []) * 3
        score += 2 if candidate.get("title") else 0
        score += 2 if candidate.get("image") else 0
        score += 2 if candidate.get("price") else 0
        score += 1 if candidate.get("rating") else 0
        score += 1 if candidate.get("reviews_count") else 0
        return score
