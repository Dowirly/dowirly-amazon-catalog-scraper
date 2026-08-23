from __future__ import annotations

import argparse
import asyncio
import fcntl
import logging
import os
import signal
from pathlib import Path
from typing import IO

from .config import build_config, load_search_plan
from .oxylabs import OxylabsAuthError
from .pipeline import Pipeline


class AlreadyRunningError(RuntimeError):
    pass


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Collect and normalize Amazon.sa products via Oxylabs in durable waves."
    )
    p.add_argument(
        "--mode",
        choices=["test", "production"],
        default=None,
        help="test defaults to 25 products; production keeps going unless a limit/provider stop is reached",
    )
    p.add_argument(
        "--max-products",
        type=int,
        default=None,
        help="stop after this many normalized products; omit in production to keep going",
    )
    p.add_argument(
        "--max-results",
        type=int,
        default=None,
        help="optional user-defined result ceiling; independent of any provider plan",
    )
    p.add_argument("--query-config", default="config/catalog_queries.yaml")
    p.add_argument("--data-dir", default="data")
    p.add_argument("--project-root", default=".")
    p.add_argument(
        "--wave-size",
        "--batch-size",
        dest="wave_size",
        type=int,
        default=None,
        help="full-product jobs per durable collect/save wave; default 100 in production",
    )
    p.add_argument(
        "--search-wave-size",
        type=int,
        default=None,
        help="search jobs per discovery wave; default 18",
    )
    p.add_argument(
        "--submit-rate",
        type=int,
        default=None,
        help="initial/max job submission probe rate; 429 responses auto-tune it downward",
    )
    p.add_argument("--poll-concurrency", type=int, default=None)
    p.add_argument("--poll-interval", type=float, default=2.0)
    p.add_argument("--job-retries", type=int, default=1)
    p.add_argument("--allow-missing-price", action="store_true")
    p.add_argument("--allow-missing-image", action="store_true")
    p.add_argument("--allow-missing-category", action="store_true")
    p.add_argument(
        "--dedupe-parent-asin",
        action="store_true",
        help="keep only one child ASIN per Amazon parent ASIN",
    )
    p.add_argument(
        "--include-paid",
        action="store_true",
        help="include sponsored products found on Amazon search pages",
    )
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--verbose", action="store_true")
    return p


def setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)


def acquire_instance_lock(data_dir: Path) -> IO[str]:
    data_dir.mkdir(parents=True, exist_ok=True)
    lock_path = data_dir / ".scraper.lock"
    handle = lock_path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        handle.close()
        raise AlreadyRunningError(
            "Another Dowirly scraper instance is already running. "
            "If systemd is enabled, monitor it with: "
            "sudo systemctl status dowirly-amazon-scraper --no-pager"
        ) from exc

    handle.seek(0)
    handle.truncate()
    handle.write(f"pid={os.getpid()}\n")
    handle.flush()
    return handle


async def _main_async(args: argparse.Namespace) -> int:
    config = build_config(args)
    lock_handle: IO[str] | None = None
    if not config.dry_run:
        lock_handle = acquire_instance_lock(config.data_dir)

    try:
        search_plan = load_search_plan(config.query_config)
        pipeline = Pipeline(config, search_plan)

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(
                    sig,
                    pipeline.request_stop,
                    f"received_{sig.name}",
                )
            except NotImplementedError:
                pass

        metrics = await pipeline.run()
        print(
            f"Done. Accepted={metrics.accepted_products}, rejected={metrics.rejected_products}, "
            f"Oxylabs usage={metrics.usage_before}->{metrics.usage_after}, "
            f"duration={metrics.elapsed_seconds:.1f}s"
        )
        if metrics.graceful_stop_reason:
            print(f"Stop reason: {metrics.graceful_stop_reason}")
        return 0
    finally:
        if lock_handle is not None:
            try:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
            finally:
                lock_handle.close()


def main() -> None:
    args = parser().parse_args()
    setup_logging(args.verbose)
    try:
        code = asyncio.run(_main_async(args))
    except AlreadyRunningError as exc:
        logging.getLogger(__name__).error("%s", exc)
        code = 2
    except OxylabsAuthError as exc:
        logging.getLogger(__name__).error("AUTH_FAILURE | %s", exc)
        code = 3
    except Exception as exc:
        logging.getLogger(__name__).exception(
            "Fatal configuration/runtime error: %s", exc
        )
        code = 1
    raise SystemExit(code)


if __name__ == "__main__":
    main()
