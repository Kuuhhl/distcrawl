from tests.nats_utils import wait_for_nats, wait_for_completion
import asyncio
import logging
import os

import pytest


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_nats_delayed_startup(dc):
    """Test worker startup before nats is ready."""
    env = {
        "MAX_NUM_URLS": "5",
        "EXPERIMENT_ID": "test_nats_startup",
        "FLUSH_THRESHOLD": "1",
        "BROWSER_TYPE": "chromium",
        "NATS_CONNECT_TIMEOUT": "60.0",
        "NATS_URL": "nats://nats:4222",
    }
    # Start worker first
    dc.up(services=["crawler_worker"], build=True, no_deps=True, env=env)
    print("[step 1] worker starting without nats")
    await asyncio.sleep(10)

    # Start nats and seeder later
    dc.up(services=["nats", "seeder"], env=env)
    print("[step 2] nats and seeder started")
    assert wait_for_nats()

    print("[step 3] waiting for processing")
    all_processed = await wait_for_completion(timeout=120, interval=10)

    assert all_processed, "Worker failed to connect after NATS startup"
