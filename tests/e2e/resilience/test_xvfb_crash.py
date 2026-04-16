import asyncio
import logging
import subprocess

import pytest

from config import ScriptSettings
from dist_common.models import ExperimentParams
from seed import seed_experiment_async
from tests.nats_utils import get_nats_stats, wait_for_nats

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _kill_xvfb_in_worker(dc):
    """Kill Xvfb inside the crawler_worker container."""
    result = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            dc.file,
            "exec",
            "crawler_worker",
            "pkill",
            "-9",
            "-f",
            "Xvfb",
        ],
        capture_output=True,
    )
    return result.returncode == 0


async def _wait_for_worker_ready(dc, timeout=60):
    """Poll worker logs until initialization is complete."""
    for _ in range(timeout // 2):
        if "Worker components successfully initialized" in dc.logs("crawler_worker"):
            return True
        await asyncio.sleep(2)
    return False


async def _assert_worker_exited_and_messages_nacked(dc):
    """Assert worker exited and the message was nacked."""
    await asyncio.sleep(10)

    assert not dc.is_running("crawler_worker"), (
        "Worker should have exited after Xvfb crash"
    )

    stats = get_nats_stats()
    assert stats.get("consumers"), "No consumers found in NATS stats"

    message_not_acked = False
    for c in stats["consumers"]:
        pending = c.get("num_pending", 0)
        ack_pending = c.get("num_ack_pending", 0)
        if pending > 0 or ack_pending > 0:
            message_not_acked = True
            break

    if not message_not_acked:
        print("\nDiagnostic Dump:")
        print("NATS stats:", stats)
        print(dc.logs("crawler_worker"))

    assert message_not_acked, (
        "Message was acked despite Xvfb crash. it should have been nacked!"
    )


@pytest.mark.parametrize("browser_type", ["chromium"])
@pytest.mark.asyncio
async def test_xvfb_crash_nacks_message(dc, browser_type):
    """Killing Xvfb mid-crawl in headed mode should crash the worker and nack the message."""
    dc.up(
        build=True,
        env={
            "FLUSH_THRESHOLD": "1",
            "MAX_NUM_URLS": "1",
            "EXPERIMENT_ID": f"test_xvfb_crash_{browser_type}",
            "BROWSER_TYPE": browser_type,
            "HEADLESS": "false",
        },
    )
    assert wait_for_nats()

    urls = ["http://mock_server:8080/basic"]
    params = ExperimentParams(
        auto_accept_cookies=True,
        navigate_subpages=False,
        max_depth=0,
        dwell_time=30,
        scroll_amounts=[],
        browser_type=browser_type,
        headless=False,
    )
    exp_id = f"test_xvfb_crash_{browser_type}_run"
    config = ScriptSettings(
        nats_url="nats://localhost:14222",
        nats_token="e2e_test_token",
        stream_name="CRAWL_E2E",
        results_bucket="e2e-results",
        subject_prefix="crawl.urls",
        storage_type="memory",
    )

    await seed_experiment_async(urls, params, exp_id, config)

    ready = await _wait_for_worker_ready(dc, timeout=60)
    assert ready, "Worker failed to initialize in time"

    xvfb_killed = False
    for _ in range(20):
        await asyncio.sleep(2)
        if _kill_xvfb_in_worker(dc):
            xvfb_killed = True
            logger.info("Xvfb process killed inside worker container")
            break

    assert xvfb_killed, "Could not find/kill the Xvfb process in the worker"

    await _assert_worker_exited_and_messages_nacked(dc)
