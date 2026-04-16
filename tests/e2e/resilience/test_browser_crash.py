from tests.nats_utils import get_nats_stats, wait_for_nats
import asyncio
import logging
import os
import subprocess

import pytest


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _kill_browser_in_worker(dc, browser_type):
    """Kill the browser process inside the crawler_worker container.

    Returns True if a browser process was found and killed.
    """
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
            browser_type,
        ],
        capture_output=True,
    )
    return result.returncode == 0


async def _wait_for_worker_ready(dc, timeout=60):
    """Poll worker logs until 'Worker components successfully initialized' appears."""
    for _ in range(timeout // 2):
        if "Worker components successfully initialized" in dc.logs("crawler_worker"):
            return True
        await asyncio.sleep(2)
    return False


async def _assert_worker_exited_and_messages_nacked(dc):
    """Wait for worker to exit, then assert NATS shows messages not fully acked."""
    # Wait for the worker to detect the crash and exit
    await asyncio.sleep(10)

    assert not dc.is_running("crawler_worker"), (
        "Worker should have exited after browser crash"
    )

    # Check NATS: the message should NOT be acked.
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
        "Message was acked despite browser crash. it should have been nacked!"
    )


@pytest.mark.parametrize(
    "browser_type", ["chromium", "firefox", "webkit"]
)  # we test all browsers for this since crash error might be specific to browser
@pytest.mark.asyncio
async def test_browser_crash_nacks_message(dc, browser_type):
    """When the browser process is killed mid-crawl, the task must be nacked so 
it can be redelivered to another worker.

    The worker should exit after detecting the crash via BrowserCrashError.
    """
    # Start all services with parameterized environment
    dc.up(
        build=True,
        env={
            "FLUSH_THRESHOLD": "1",
            "MAX_NUM_URLS": "1",
            "EXPERIMENT_ID": f"test_browser_crash_{browser_type}",
            "BROWSER_TYPE": browser_type,
        },
    )
    assert wait_for_nats()

    from seed import seed_experiment_async
    from config import ScriptSettings

    # Use local mock server
    urls = ["http://mock_server:8080/basic"]
    params_mod = __import__("dist_common.models", fromlist=["ExperimentParams"])
    ExperimentParams = params_mod.ExperimentParams
    params = ExperimentParams(
        auto_accept_cookies=True,
        navigate_subpages=False,
        max_depth=0,
        dwell_time=30,  # long dwell to give us time to kill the browser
        scroll_amounts=[],
        browser_type=browser_type,
        headless=True,
    )
    exp_id = f"test_browser_crash_{browser_type}_run"
    config = ScriptSettings(
        nats_url="nats://localhost:14222",
        nats_token="e2e_test_token",
        stream_name="CRAWL_E2E",
        results_bucket="e2e-results",
        subject_prefix="crawl.urls",
        storage_type="memory",
    )

    await seed_experiment_async(urls, params, exp_id, config)

    # Wait for the worker to be ready
    ready = await _wait_for_worker_ready(dc, timeout=60)
    assert ready, "Worker failed to initialize in time"

    # Wait for the worker to pick up the task and start the browser
    browser_killed = False
    for _ in range(20):
        await asyncio.sleep(2)
        if _kill_browser_in_worker(dc, browser_type):
            browser_killed = True
            logger.info(
                f"Browser process ({browser_type}) killed inside worker container"
            )
            break

    assert browser_killed, "Could not find/kill the browser process in the worker"

    await _assert_worker_exited_and_messages_nacked(dc)


@pytest.mark.parametrize(
    "browser_type", ["chromium"]
)  # single browser is enough; parametrized for extensibility
@pytest.mark.asyncio
async def test_browser_crash_during_navigation_nacks_message(dc, browser_type):
    """Browser dies while navigating (during page.goto), not during dwell.

    This tests the navigate_to_url -> _browser_operation -> BrowserCrashError path.
    """
    dc.up(
        build=True,
        env={
            "FLUSH_THRESHOLD": "1",
            "MAX_NUM_URLS": "1",
            "EXPERIMENT_ID": f"test_browser_crash_nav_{browser_type}",
            "BROWSER_TYPE": browser_type,
        },
    )
    assert wait_for_nats()

    from seed import seed_experiment_async
    from config import ScriptSettings

    # Use /slow endpoint (2s delay) with dwell_time=0 so we kill during navigation
    urls = ["http://mock_server:8080/slow"]
    params_mod = __import__("dist_common.models", fromlist=["ExperimentParams"])
    ExperimentParams = params_mod.ExperimentParams
    params = ExperimentParams(
        auto_accept_cookies=True,
        navigate_subpages=False,
        max_depth=0,
        dwell_time=0,
        scroll_amounts=[],
        browser_type=browser_type,
        headless=True,
    )
    exp_id = f"test_browser_crash_nav_{browser_type}_run"
    config = ScriptSettings(
        nats_url="nats://localhost:14222",
        nats_token="e2e_test_token",
        stream_name="CRAWL_E2E",
        results_bucket="e2e-results",
        subject_prefix="crawl.urls",
        storage_type="memory",
    )

    await seed_experiment_async(urls, params, exp_id, config)

    # Wait for the worker to be ready
    ready = await _wait_for_worker_ready(dc, timeout=60)
    assert ready, "Worker failed to initialize in time"

    # Wait for navigation to start (poll for "Navigating to" in logs), then kill
    navigation_started = False
    for _ in range(30):
        await asyncio.sleep(1)
        logs = dc.logs("crawler_worker")
        if "Navigating to" in logs:
            navigation_started = True
            # Kill the browser immediately
            _kill_browser_in_worker(dc, browser_type)
            logger.info("Browser killed during navigation")
            break

    assert navigation_started, "Navigation never started - could not trigger mid-nav crash"

    await _assert_worker_exited_and_messages_nacked(dc)


@pytest.mark.parametrize(
    "browser_type", ["chromium"]
)
@pytest.mark.asyncio
async def test_browser_crash_before_navigation_nacks_message(dc, browser_type):
    """Browser is killed before the worker picks up a task.

    When the worker tries open_new_crawl_context on a dead browser, it should
    detect the crash and nack the message.
    """
    dc.up(
        build=True,
        env={
            "FLUSH_THRESHOLD": "1",
            "MAX_NUM_URLS": "1",
            "EXPERIMENT_ID": f"test_browser_crash_pre_nav_{browser_type}",
            "BROWSER_TYPE": browser_type,
        },
    )
    assert wait_for_nats()

    # Wait for the worker to be ready
    ready = await _wait_for_worker_ready(dc, timeout=60)
    assert ready, "Worker failed to initialize in time"

    # Kill browser *before* seeding the task
    _kill_browser_in_worker(dc, browser_type)
    logger.info("Browser killed before task seeding")

    # Seed a task - the worker will try to open a context on a dead browser
    from seed import seed_experiment_async
    from config import ScriptSettings

    urls = ["http://mock_server:8080/basic"]
    params_mod = __import__("dist_common.models", fromlist=["ExperimentParams"])
    ExperimentParams = params_mod.ExperimentParams
    params = ExperimentParams(
        auto_accept_cookies=True,
        navigate_subpages=False,
        max_depth=0,
        dwell_time=0,
        scroll_amounts=[],
        browser_type=browser_type,
        headless=True,
    )
    exp_id = f"test_browser_crash_pre_nav_{browser_type}_run"
    config = ScriptSettings(
        nats_url="nats://localhost:14222",
        nats_token="e2e_test_token",
        stream_name="CRAWL_E2E",
        results_bucket="e2e-results",
        subject_prefix="crawl.urls",
        storage_type="memory",
    )

    await seed_experiment_async(urls, params, exp_id, config)

    await _assert_worker_exited_and_messages_nacked(dc)


@pytest.mark.parametrize(
    "browser_type", ["chromium"]
)
@pytest.mark.asyncio
async def test_browser_crash_with_multiple_pending_tasks_nacks_all(dc, browser_type):
    """Multiple tasks queued, browser crashes during first task.

    Remaining tasks should still be pending/unacked in NATS, not lost.
    """
    dc.up(
        build=True,
        env={
            "FLUSH_THRESHOLD": "1",
            "MAX_NUM_URLS": "3",
            "EXPERIMENT_ID": f"test_browser_crash_multi_{browser_type}",
            "BROWSER_TYPE": browser_type,
            "NUM_CRAWLERS": "1",
        },
    )
    assert wait_for_nats()

    from seed import seed_experiment_async
    from config import ScriptSettings

    # Use /slow endpoint with long dwell_time so we have time to kill mid-crawl
    urls = [
        "http://mock_server:8080/slow",
        "http://mock_server:8080/basic",
        "http://mock_server:8080/links",
    ]
    params_mod = __import__("dist_common.models", fromlist=["ExperimentParams"])
    ExperimentParams = params_mod.ExperimentParams
    params = ExperimentParams(
        auto_accept_cookies=True,
        navigate_subpages=False,
        max_depth=0,
        dwell_time=30,
        scroll_amounts=[],
        browser_type=browser_type,
        headless=True,
    )
    exp_id = f"test_browser_crash_multi_{browser_type}_run"
    config = ScriptSettings(
        nats_url="nats://localhost:14222",
        nats_token="e2e_test_token",
        stream_name="CRAWL_E2E",
        results_bucket="e2e-results",
        subject_prefix="crawl.urls",
        storage_type="memory",
    )

    await seed_experiment_async(urls, params, exp_id, config)

    # Wait for the worker to be ready
    ready = await _wait_for_worker_ready(dc, timeout=60)
    assert ready, "Worker failed to initialize in time"

    # Wait for the worker to pick up the first task, then kill browser
    browser_killed = False
    for _ in range(30):
        await asyncio.sleep(2)
        if _kill_browser_in_worker(dc, browser_type):
            browser_killed = True
            logger.info("Browser killed during multi-task crawl")
            break

    assert browser_killed, "Could not kill browser process"

    # Wait for worker to exit
    await asyncio.sleep(10)
    assert not dc.is_running("crawler_worker"), (
        "Worker should have exited after browser crash"
    )

    # Check NATS: there should still be pending messages (the remaining 2 tasks)
    stats = get_nats_stats()
    assert stats.get("consumers"), "No consumers found in NATS stats"

    remaining_pending = 0
    for c in stats["consumers"]:
        pending = c.get("num_pending", 0)
        ack_pending = c.get("num_ack_pending", 0)
        remaining_pending += pending + ack_pending

    # At least some messages should still be pending (the 2 unprocessed tasks)
    assert remaining_pending > 0, (
        f"Expected remaining pending messages in NATS, but got {remaining_pending}. "
        "Messages may have been lost instead of nacked."
    )
