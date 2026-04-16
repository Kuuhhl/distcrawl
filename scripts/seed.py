"""seed crawl experiment into nats using faststream."""

import asyncio
import csv
import logging
import sys
import uuid
from datetime import datetime
from itertools import islice
from pathlib import Path
from typing import List, Literal, Optional

import defopt
import obstore as obs
from faststream.nats import NatsBroker, JStream, RetentionPolicy
from dist_common import CrawlTask, ExperimentMetadata, ExperimentParams
from config import ScriptSettings
from url_normalize import url_normalize

settings = ScriptSettings()
logging.basicConfig(
    level=settings.logging_level,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

broker = NatsBroker(settings.nats_url, token=settings.nats_token, logger=logger)


async def seed_experiment_async(
    urls: List[str],
    params: ExperimentParams,
    exp_id: str,
    config: ScriptSettings,
) -> None:
    """upload metadata to object store and publish tasks.

    :param urls: list of URLs to seed
    :param params: experiment parameters
    :param exp_id: experiment ID
    :param config: script settings
    """
    logger.info("Starting seed for experiment: %s", exp_id)

    metadata = ExperimentMetadata(
        id=exp_id, timestamp=datetime.now(), total_urls=len(urls), params=params
    )

    broker = NatsBroker(config.nats_url, token=config.nats_token, logger=logger)

    stream = JStream(
        name=config.stream_name,
        subjects=[f"{config.subject_prefix}.>"],
        storage="file",
        retention=RetentionPolicy.WORK_QUEUE,
        declare=True,
    )

    task_publisher = broker.publisher(f"{config.subject_prefix}.>", stream=stream)

    await broker.start()

    try:
        storage = config.get_storage()
        metadata_key = f"experiment={exp_id}/metadata.json"
        await obs.put_async(
            storage, metadata_key, metadata.model_dump_json(indent=2).encode()
        )
        logger.info("Metadata uploaded to object store: %s", metadata_key)

        mode = "headless" if getattr(params, "headless", True) else "headed"
        browser = getattr(params, "browser_type", "chromium")
        subject = f"{config.subject_prefix}.{browser}.{mode}.{exp_id}"

        batch_size = config.seed_publish_batch_size

        for i in range(0, len(urls), batch_size):
            batch = urls[i : i + batch_size]
            tasks = []

            for url in batch:
                task_data = params.model_dump()
                task_data.update({"url": url, "experiment_id": exp_id})
                task = CrawlTask(**task_data)

                tasks.append(
                    task_publisher.publish(message=task, subject=subject, timeout=15)
                )

            await asyncio.gather(*tasks)
            logger.info(
                "Seeded %s/%s URLs...", min(i + batch_size, len(urls)), len(urls)
            )

        logger.info("Successfully seeded %s tasks to subject: %s", len(urls), subject)

    finally:
        await broker.stop()


def seed(
    name: str,
    *,
    accept_cookies: bool,
    navigate: bool,
    depth: int,
    dwell_seconds: int,
    scroll_amounts: List[int],
    num_tranco: int,
    browser: Literal["chromium", "firefox", "webkit"] = "chromium",
    headless: bool = True,
    tranco_path: Optional[str] = None,
    nats_url: Optional[str] = None,
    results_bucket: Optional[str] = None,
) -> None:
    """
    seed a new crawl experiment.

    :param name: alphanumeric experiment name prefix.
    :param accept_cookies: auto-accept cookie banners.
    :param navigate: follow links to subpages.
    :param depth: max navigation depth.
    :param dwell_seconds: seconds to wait on each page.
    :param scroll_amounts: vertical scroll steps in pixels.
    :param num_tranco: number of tranco sites to seed.
    :param browser: browser type to use.
    :param headless: whether to run in headless mode.
    :param tranco_path: path to tranco cache (create using fetch_tranco.py).
    :param nats_url: nats server url.
    :param results_bucket: nats object store bucket.
    """
    if not name.isalnum():
        logger.error("Invalid name: %s. Must be alphanumeric.", name)
        sys.exit(1)

    if not tranco_path:
        tranco_path = Path("data") / "tranco_cache"
        logger.info(
            "No tranco cache path provided. Checking default path: %s", tranco_path
        )
    else:
        tranco_path = Path(tranco_path)

    if not tranco_path.exists():
        logger.error("Tranco cache path does not exist: %s", tranco_path)
        sys.exit(1)

    try:
        csv_files = list(tranco_path.glob("*.csv"))
        if csv_files:
            with open(csv_files[0], mode="r", encoding="utf-8") as csvfile:
                csv_reader = csv.reader(csvfile)
                urls = [url_normalize(row[1]) for row in islice(csv_reader, num_tranco)]
        else:
            raise ValueError("No CSV files found")
    except Exception as e:
        logger.error("Failed to load URLs from CSV: %s", e)
        sys.exit(1)

    if not urls:
        logger.warning("No URLs found. List seems to be empty!")
        sys.exit(1)

    params = ExperimentParams(
        auto_accept_cookies=accept_cookies,
        navigate_subpages=navigate,
        max_depth=depth,
        dwell_time=dwell_seconds,
        scroll_amounts=scroll_amounts,
        browser_type=browser,
        headless=headless,
    )

    overrides = {
        k: v
        for k, v in {
            "nats_url": nats_url,
            "results_bucket": results_bucket,
        }.items()
        if v is not None
    }
    config = ScriptSettings(**overrides)

    exp_id = f"{name}_{str(uuid.uuid4())[:8]}"

    asyncio.run(seed_experiment_async(urls, params, exp_id, config))


def main() -> None:
    defopt.run(seed)


if __name__ == "__main__":
    main()
