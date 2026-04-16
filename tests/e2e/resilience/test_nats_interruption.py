from tests.nats_utils import wait_for_nats, wait_for_completion
import asyncio
import logging
import os

import pytest


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_nats_goes_down_during_work(dc):
    """Test worker recovery during nats interruption."""
    # Start all services with overrides
    dc.up(
        build=True,
        env={
            "MAX_NUM_URLS": "5",
            "EXPERIMENT_ID": "test_nats_interruption",
            "FLUSH_THRESHOLD": "1",
            "BROWSER_TYPE": "chromium",
            "NATS_CONNECT_TIMEOUT": "60.0",
        },
    )
    assert wait_for_nats()

    await asyncio.sleep(5)  # Wait for startup

    # Stop NATS
    dc.stop(["nats"])
    print("[step 1] nats stopped")
    await asyncio.sleep(10)

    # Restart NATS
    dc.up(services=["nats"])
    print("[step 2] nats restarted")
    assert wait_for_nats()

    print("[step 3] waiting for recovery")
    all_processed = await wait_for_completion(timeout=120, interval=10)

    assert all_processed, "Worker failed to recover after NATS restart"
