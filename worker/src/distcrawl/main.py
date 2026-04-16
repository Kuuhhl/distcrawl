"""worker entry-point using faststream and nats broker."""

import asyncio
import logging
import sys
import uuid
import time
from faststream import ExceptionMiddleware, FastStream, AckPolicy, Context, ContextRepo
from faststream.nats import (
    NatsBroker,
    NatsMessage,
    ConsumerConfig,
    PullSub,
    RetentionPolicy,
    StorageType,
)
import aiohttp

from faststream.nats.schemas.js_stream import JStream
from dist_common import CrawlTask, NodeInfo
from distcrawl import (
    Crawler,
    CrawlNavigator,
    PlaywrightEngine,
    BrowserEngine,
    ParquetBatcher,
    TelemetrySink,
    WorkerSettings,
)
from distcrawl.crawl.errors import BrowserCrashError

settings = WorkerSettings()
logging.basicConfig(
    level=settings.logging_level,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("distcrawl")

exception_middleware = ExceptionMiddleware()


@exception_middleware.add_handler(BrowserCrashError)
async def on_browser_crash(exc: BrowserCrashError):
    logger.critical("Browser crash detected, terminating worker: %s", exc)
    sys.exit(1)


broker = NatsBroker(
    settings.nats_url,
    token=settings.nats_token,
    logger=logger,
    middlewares=[exception_middleware],
)
app = FastStream(broker, logger=logger)


@app.on_startup
async def initialize_worker_components(context: ContextRepo):
    """bootstrap all necessary worker components."""
    logger.info("Checking worker information...")
    try:
        # this api gives us some general information about the worker node
        # (country, whether the internet connection is residential)
        async with aiohttp.ClientSession("https://api.ipquery.io") as session:
            async with session.get("/") as response:
                ip_addr = await response.text()
            async with session.get(f"/{ip_addr}") as response:
                ipInfo = await response.json()
        node_info = NodeInfo(
            country_code=ipInfo["location"]["country_code"],
            is_residential=True
            if not (
                ipInfo["risk"]["is_vpn"]
                or ipInfo["risk"]["is_proxy"]
                or ipInfo["risk"]["is_datacenter"]
            )
            else False,
            browser_type=settings.browser_type,
            is_headless=settings.headless,
        )
    except Exception as e:
        logger.error("Failed to fetch / parse worker information: %s", e)
        sys.exit(1)
    if settings.only_allow_residential_connections and not node_info.is_residential:
        logger.error(
            "Worker is not on residential connection (known vpn / tor / datacenter). Will abort."
        )
        sys.exit(1)

    worker_id = uuid.uuid4().hex[:8]
    logger.info(
        "Worker information found:\nCountry Code: %s\nIs Residential: %s\nBrowser type: %s\nIs Headless: %s\nAssigned Worker ID: %s",
        node_info.country_code,
        node_info.is_residential,
        settings.browser_type,
        settings.headless,
        worker_id,
    )

    logger.info("Starting worker component initialization...")

    # wait for message broker
    if not broker._connection:
        logger.info("Waiting for broker to connect...")
        await broker.connect()

    # init storage
    storage_backend = settings.get_storage()
    logger.info("Selected storage backend: %s", storage_backend.__class__.__name__)
    telemetry_batcher = ParquetBatcher(
        storage=storage_backend,
        batch_size=settings.persistence_batch_size,
    )
    telemetry_sink = TelemetrySink(
        batcher=telemetry_batcher, worker_id=worker_id, node_info=node_info
    )

    # flush worker metadata once at startup
    # we use "system" as experiment_id for the worker metadata
    logger.info("Saving worker metadata to storage...")
    await telemetry_batcher.append(
        "system",
        "worker_metadata",
        {
            "worker_id": worker_id,
            "country_code": node_info.country_code,
            "is_residential": node_info.is_residential,
            "timestamp": time.time(),
        },
    )
    await telemetry_batcher.flush_buffer("system", "worker_metadata")

    # start browser and link to sink
    logger.info("Starting browser engine %s...", settings.browser_type)
    browser_engine = PlaywrightEngine(config=settings)
    await browser_engine.start_browser_engine(headless=settings.headless)
    browser_engine.set_sink(telemetry_sink)

    # setup crawler
    crawl_navigator = CrawlNavigator(engine=browser_engine)
    task_crawler = Crawler(
        navigator=crawl_navigator, sink=telemetry_sink, config=settings
    )

    # make available to context
    # (which is automatically passed by faststream framework using Context())
    context.set_global("browser_engine", browser_engine)
    context.set_global("telemetry_sink", telemetry_sink)
    context.set_global("task_crawler", task_crawler)

    logger.info("Worker components successfully initialized. Asking for tasks...")


@app.on_shutdown
async def shutdown_worker_components(
    task_crawler: Crawler = Context(), browser_engine: BrowserEngine = Context()
):
    """perform cleanup of all components."""
    logger.info("Shutting down worker components...")
    task_crawler.prepare_shutdown()

    await task_crawler.persist_telemetry_and_commit_batch()
    await browser_engine.stop_browser_engine()

    logger.info("Graceful worker shutdown complete.")


@broker.subscriber(
    settings.subject_pattern,
    stream=JStream(
        name=settings.stream_name,
        subjects=[f"{settings.subject_prefix}.>"],
        retention=RetentionPolicy.WORK_QUEUE,
        storage=StorageType.FILE,
    ),
    durable=settings.consumer_name,
    pull_sub=PullSub(batch_size=settings.num_crawlers),
    ack_policy=AckPolicy.MANUAL,
    max_workers=settings.num_crawlers,  # how many max. concurrent crawlers
    config=ConsumerConfig(
        ack_wait=settings.nats_ack_wait_seconds,
        max_deliver=settings.nats_max_retries,
    ),
)
async def handle_crawl_task_received(
    task: CrawlTask,
    msg: NatsMessage,
    task_crawler: Crawler = Context(),
):
    """dispatch received NATS messages to the crawler."""
    await task_crawler.process_incoming_task(task, msg)


@app.after_startup
async def start_lease_extension_heartbeat(
    task_crawler: Crawler = Context(),
):
    """periodic background task to prevent task timeouts during long crawls."""
    while True:
        try:
            await asyncio.sleep(settings.nats_heartbeat_seconds)
            await task_crawler.extend_active_message_leases()
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error("Heartbeat pulse error: %s", exc)


@app.after_startup
async def start_idle_watchdog(task_crawler: Crawler = Context()):
    """exit if no messages received for too long. Autoscaling Group or Docker will restart / reassign."""
    while True:
        try:
            await asyncio.sleep(60)
            idle_seconds = time.time() - task_crawler.last_activity_time
            if idle_seconds > settings.watchdog_timeout_seconds:
                logger.error(
                    "Watchdog: no messages received for %.0f seconds, exiting",
                    idle_seconds,
                )
                sys.exit(1)
        except asyncio.CancelledError:
            break


def main() -> int:
    """main application entry point."""
    try:
        asyncio.run(app.run())
    except KeyboardInterrupt:
        return 130
    except Exception:
        logger.exception("Worker execution failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
