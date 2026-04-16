from tests.nats_utils import wait_for_nats, get_nats_stats
import asyncio
import logging
import os
import sys
from pathlib import Path

import pytest

# allow importing seeder helpers
root_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(root_dir / "scripts"))
sys.path.insert(0, str(root_dir / "common" / "src"))

from dist_common import ExperimentParams
from seed import seed_experiment_async
from config import ScriptSettings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_RETRIES = 2


@pytest.mark.asyncio
async def test_failing_task_exhausts_retries(dc):
    """A task that always crashes the browser must be redelivered up to
    max_deliver times and then dropped by JetStream (no infinite retry loop).
    """
    dc.up(
        build=True,
        env={
            "EXPERIMENT_ID": "test_dlq",
            "FLUSH_THRESHOLD": "1",
            "BROWSER_TYPE": "chromium",
            "MAX_RETRIES": str(MAX_RETRIES),
            "NATS_ACK_WAIT_SECONDS": "10",
            "HEADLESS": "true",
            "GOTO_TIMEOUT_MS": "10000",
        },
    )
    assert wait_for_nats(), "NATS did not become healthy"

    # Seed a single task pointing at a page that will crash the browser
    config = ScriptSettings()
    config.nats_url = "nats://localhost:14222"
    config.nats_token = "e2e_test_token"
    config.stream_name = "CRAWL_E2E"
    config.results_bucket = "e2e-results"
    config.subject_prefix = "crawl.urls"
    config.storage_type = "memory"

    params = ExperimentParams(
        auto_accept_cookies=False,
        navigate_subpages=False,
        max_depth=0,
        dwell_time=2,
        scroll_amounts=[],
        browser_type="chromium",
        headless=True,
    )

    # /crash endpoint triggers an infinite JS memory allocation loop
    urls = ["http://mock_server:8080/crash"]
    await seed_experiment_async(urls, params, "test_dlq", config)
    logger.info("Seeded crash URL, waiting for retries to exhaust...")

    # Wait long enough for max_deliver attempts + ack_wait timeouts
    # Each attempt: ~10s ack_wait + processing time
    max_wait = MAX_RETRIES * 30 + 60
    elapsed = 0
    message_exhausted = False

    while elapsed < max_wait:
        await asyncio.sleep(10)
        elapsed += 10

        stats = get_nats_stats()
        consumers = stats.get("consumers", [])
        if not consumers:
            continue

        for c in consumers:
            num_pending = c.get("num_pending", -1)
            num_ack_pending = c.get("num_ack_pending", -1)
            num_redelivered = c.get("num_redelivered", 0)
            delivered_seq = c.get("delivered", {}).get("consumer_seq", 0)

            logger.info(
                "Consumer stats: pending=%s ack_pending=%s redelivered=%s delivered_seq=%s",
                num_pending, num_ack_pending, num_redelivered, delivered_seq,
            )

            # Message is exhausted when: nothing pending, nothing awaiting ack,
            # and it was delivered at least once
            if num_pending == 0 and num_ack_pending == 0 and delivered_seq > 0:
                message_exhausted = True
                break

        if message_exhausted:
            break

    assert message_exhausted, (
        f"Message was not exhausted after {max_wait}s. "
        f"Expected JetStream to drop message after {MAX_RETRIES} delivery attempts."
    )
    logger.info("Message correctly exhausted after max retries.")
