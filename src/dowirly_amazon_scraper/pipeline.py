from __future__ import annotations

import asyncio
import hashlib
import json
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
from .utils import append_jsonl, count_jsonl, read_jsonl, utc_now_iso

LOGGER = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, config: AppConfig, search_plan: SearchPlan) -> None:
        self.config = config
        self.search_plan = search_plan
        self.storage = Storage(config.data_dir)
        self.client = OxylabsClient(config)
        # Provider usage statistics can lag on new/free accounts. The locally
        # persisted completed-job audit trail is a conservative floor, so a reboot
        # or delayed provider counter cannot make us overspend our configured plan.
        self.billable_job_ids = self.storage.completed_billable_job_ids()
        self.budget = BudgetGuard(config, self.client, local_floor=len(self.billable_job_ids))
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
                "Oxylabs guarded usage: %s / %s results (provider=%s, local_floor=%s); remaining=%s",
                before.all_count,
                self.config.hard_result_limit,
                before.provider_count,
                len(self.billable_job_ids),
                self.budget.capacity(),
            )
            self._log_progress("startup")
            # A submitted batch must still be collected even if the provider has
            # already advanced the visible usage counter to the plan limit.
            has_inflight = bool(self.checkpoint.get("inflight_jobs"))
            if self.budget.capacity() <= 0 and not has_inflight:
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
            self._log_progress("finished")
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
        if len(merged) >= desired and not (self.checkpoint.get("inflight_jobs") or {}).get("search"):
            LOGGER.info("Discovery already has %s candidates; desired=%s. Resuming enrichment.", len(merged), desired)
            self.metrics.unique_candidates = len(merged)
            return merged

        # Free Trial discovery deliberately uses roughly one query per configured
        # category per wave. This preserves diversity and avoids wasting dozens of
        # scarce results after we already have enough ASIN candidates. Micro can use
        # larger waves for throughput.
        category_count = len({q.logical_category for q in self.search_plan.queries})
        if self.config.mode == "test":
            search_chunk_size = 10
        elif self.config.plan == "free":
            search_chunk_size = max(10, category_count)
        else:
            search_chunk_size = 100
        query_chunks = [self.search_plan.queries[i : i + search_chunk_size] for i in range(0, len(self.search_plan.queries), search_chunk_size)]

        for sort in self.search_plan.sorts:
            for page in range(1, self.search_plan.max_pages_per_query + 1):
                for chunk_index, chunk in enumerate(query_chunks):
                    inflight_search = (self.checkpoint.get("inflight_jobs") or {}).get("search")
                    if (self.stop_requested.is_set() or len(merged) >= desired) and not inflight_search:
                        break
                    pending_chunk = [
                        q for q in chunk
                        if f"sort={sort}|page={page}|query={q.query}" not in completed_keys
                    ]
                    if not pending_chunk and not inflight_search:
                        continue

                    if inflight_search:
                        # Resume the exact provider-side batch, regardless of whether
                        # the provider usage counter changed while this VPS was down.
                        payload = dict(inflight_search.get("payload") or {})
                        saved_page = int(payload.get("start_page") or 1)
                        saved_sort = self._payload_context_value(payload, "sort_by") or "featured"
                        if saved_page != page or saved_sort != sort:
                            continue
                        LOGGER.warning("Found durable in-flight search batch; resuming it before any new search submission.")
                    else:
                        await self.budget.refresh()
                        capacity = self.budget.capacity()
                        if capacity <= 0:
                            raise BudgetExhausted("No result budget left during discovery")

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

                    LOGGER.info("Submitting/resuming search batch: %s jobs (sort=%s page=%s)", len(payload.get("query") or []), sort, page)
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
                    # Commit completion and clear the durable in-flight marker in
                    # one atomic checkpoint write. If the VPS dies before this
                    # write, the same Oxylabs job IDs are polled again after reboot.
                    self.checkpoint["completed_search_keys"] = sorted(completed_keys)
                    self.checkpoint.setdefault("inflight_jobs", {}).pop("search", None)
                    self.storage.save_checkpoint(self.checkpoint)
                    merged = merge_candidates(read_jsonl(self.storage.paths.discovered))
                    self.metrics.unique_candidates = len(merged)
                    self._rewrite_unique_candidates(merged)
                    LOGGER.info("Discovery now has %s unique ASIN candidates.", len(merged))
                    await self.budget.refresh()
                    self._log_progress("discovery")

                if (self.stop_requested.is_set() or len(merged) >= desired) and not (self.checkpoint.get("inflight_jobs") or {}).get("search"):
                    break
            if (self.stop_requested.is_set() or len(merged) >= desired) and not (self.checkpoint.get("inflight_jobs") or {}).get("search"):
                break

        self._rewrite_unique_candidates(merged)
        return merged

    def _desired_candidate_count(self) -> int:
        if self.config.max_products is not None:
            return max(self.config.max_products + 20, int(self.config.max_products * 1.5))
        return max(100, int(self.budget.capacity() * 0.98))

    def _product_reserve(self, current_candidates: int) -> int:
        if self.config.max_products is not None:
            remaining_target = max(0, self.config.max_products - len(self.storage.final_asins()))
            return min(self.budget.capacity(), remaining_target)
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
        existing_embedding_asins = self.storage.embedding_asins()
        self.metrics.accepted_products = len(existing_final_asins)
        completed_asins = set(self.checkpoint.get("completed_product_asins") or []) | existing_final_asins
        queue = [asin for asin in candidate_by_asin if asin not in completed_asins]
        inflight_product = (self.checkpoint.get("inflight_jobs") or {}).get("product")
        if not queue and not inflight_product:
            LOGGER.info("No product ASINs left to enrich.")
            return

        queue.sort(key=lambda a: self._candidate_score(candidate_by_asin[a]), reverse=True)
        parent_seen: set[str] = set()
        if self.config.dedupe_parent_asin:
            for final in read_jsonl(self.storage.paths.final_products):
                parent = final.get("parent_external_id")
                if parent:
                    parent_seen.add(str(parent))

        index = 0
        while (index < len(queue) or (self.checkpoint.get("inflight_jobs") or {}).get("product")) and not self.stop_requested.is_set():
            while index < len(queue) and queue[index] in completed_asins:
                index += 1

            inflight_product = (self.checkpoint.get("inflight_jobs") or {}).get("product")
            if self.config.max_products is not None and self.metrics.accepted_products >= self.config.max_products and not inflight_product:
                self.metrics.graceful_stop_reason = self.metrics.graceful_stop_reason or "requested_product_limit_reached"
                break

            if inflight_product:
                payload = dict(inflight_product.get("payload") or {})
                asins = [str(v).upper() for v in payload.get("query") or []]
                LOGGER.warning("Found durable in-flight product batch; resuming %s ASIN jobs before any new submission.", len(asins))
            else:
                if index >= len(queue):
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

            LOGGER.info("Submitting/resuming product batch: %s ASINs", len(payload.get("query") or []))
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

                # Final product and embedding are individually fsynced. On replay,
                # fill whichever side is missing without duplicating the other.
                if asin not in existing_final_asins:
                    self.storage.append(self.storage.paths.final_products, norm.product)
                    existing_final_asins.add(asin)
                    self.metrics.accepted_products += 1

                if asin not in existing_embedding_asins:
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
                    existing_embedding_asins.add(asin)
                completed_asins.add(asin)

            self.checkpoint["completed_product_asins"] = sorted(completed_asins)
            self.checkpoint.setdefault("inflight_jobs", {}).pop("product", None)
            self.storage.save_checkpoint(self.checkpoint)
            await self.budget.refresh()
            self._log_progress("enrichment")

    async def _submit_and_poll_with_fault_retries(
        self, payload: dict[str, Any], *, phase: str
    ) -> list[JobResult]:
        """Submit/poll a durable Push-Pull wave and retry only provider faults.

        Submitted Oxylabs job IDs are atomically persisted immediately after the
        provider accepts the batch and before we spend time writing audit events or
        polling. A restarted process therefore polls those exact IDs rather than
        paying for duplicate scraping requests.
        """
        inflight_map = self.checkpoint.setdefault("inflight_jobs", {})
        saved = inflight_map.get(phase)
        all_jobs: list[dict[str, Any]] = []

        if saved:
            saved_payload = dict(saved.get("payload") or {})
            if saved_payload:
                payload = saved_payload
            all_jobs = list(saved.get("jobs") or [])
            LOGGER.warning(
                "RESUME | phase=%s | polling %s previously submitted Oxylabs jobs; no duplicate submit",
                phase,
                len(all_jobs),
            )

        original_queries = [str(v) for v in payload.get("query") or []]
        if not original_queries:
            return []
        signature = str((saved or {}).get("signature") or self._payload_signature(payload))

        if not all_jobs:
            jobs = await self.client.submit_batch(payload)
            all_jobs.extend(jobs)
            # Persist provider job IDs before slower per-job audit fsyncs.
            self._persist_inflight(phase, signature, payload, all_jobs)
            self._count_new_jobs(phase, jobs)
            self._record_jobs(f"{phase}_submitted", jobs)

        while True:
            results = await self.client.poll_jobs(all_jobs, max_retries=0)
            by_query: dict[str, list[JobResult]] = defaultdict(list)
            for result in results:
                by_query[result.query].append(result)
                self._record_job_result(result, phase)

            selected_results: list[JobResult] = []
            retry_queries: list[str] = []
            jobs_per_query: dict[str, int] = defaultdict(int)
            for job in all_jobs:
                jobs_per_query[str(job.get("query") or job.get("url") or "")] += 1

            for query in original_queries:
                attempts = by_query.get(query, [])
                successes = [r for r in attempts if r.status == "done" and r.result is not None]
                if successes:
                    selected_results.append(successes[-1])
                    continue

                faults = [r for r in attempts if r.status == "faulted" or r.result is None]
                if faults:
                    retries_already_used = max(0, jobs_per_query.get(query, 1) - 1)
                    if retries_already_used < self.config.max_job_retries:
                        retry_queries.append(query)
                    else:
                        selected_results.append(faults[-1])
                        self.metrics.faulted_jobs += 1

            if not retry_queries:
                return selected_results

            await self.budget.refresh()
            allowed = self.budget.reserve(len(retry_queries))
            if allowed <= 0:
                LOGGER.warning("No quota headroom remains to retry %s faulted %s jobs.", len(retry_queries), phase)
                for query in retry_queries:
                    attempts = by_query.get(query, [])
                    if attempts:
                        selected_results.append(attempts[-1])
                        self.metrics.faulted_jobs += 1
                return selected_results

            retry_queries = retry_queries[:allowed]
            retry_payload = dict(payload)
            retry_payload["query"] = retry_queries
            LOGGER.info("Retrying %s faulted %s jobs; prior attempts remain durably tracked.", len(retry_queries), phase)
            retry_jobs = await self.client.submit_batch(retry_payload)
            all_jobs.extend(retry_jobs)
            # Same ordering as initial submit: checkpoint first, audit second.
            self._persist_inflight(phase, signature, payload, all_jobs)
            self._count_new_jobs(phase, retry_jobs)
            self._record_jobs(f"{phase}_retry_submitted", retry_jobs)

    def _persist_inflight(
        self,
        phase: str,
        signature: str,
        payload: dict[str, Any],
        jobs: list[dict[str, Any]],
    ) -> None:
        self.checkpoint.setdefault("inflight_jobs", {})[phase] = {
            "signature": signature,
            "payload": payload,
            "jobs": jobs,
            "updated_at": utc_now_iso(),
        }
        self.storage.save_checkpoint(self.checkpoint)

    @staticmethod
    def _payload_signature(payload: dict[str, Any]) -> str:
        canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
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
        LOGGER.info(
            "PROGRESS | stage=%s | accepted=%s | rejected=%s | candidates=%s | discovered=%s "
            "| search_jobs_this_run=%s | product_jobs_this_run=%s | usage=%s/%s "
            "| provider_usage=%s | inflight_jobs=%s",
            stage,
            accepted,
            rejected,
            candidates,
            discovered,
            self.metrics.search_jobs,
            self.metrics.product_jobs,
            self.budget.snapshot.all_count,
            self.config.hard_result_limit,
            self.budget.snapshot.provider_count,
            inflight,
        )

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
