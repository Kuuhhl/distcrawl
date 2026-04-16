import logging

import pytest

from config import ScriptSettings
from dist_common.models import ExperimentParams
from seed import seed_experiment_async
from tests.nats_utils import wait_for_completion, wait_for_nats

logger = logging.getLogger("distcrawl.test.timeout")


@pytest.mark.asyncio
async def test_navigation_timeout_completes_task(dc):
    """Navigation timeout should not stall the worker; the task must still be acked."""
    dc.up(
        build=True,
        env={
            "FLUSH_THRESHOLD": "1",
            "MAX_NUM_URLS": "1",
            "EXPERIMENT_ID": "test_timeout",
            "GOTO_TIMEOUT_MS": "10000",
            "BROWSER_TYPE": "chromium",
        },
    )
    assert wait_for_nats()

    urls = ["http://mock_server:8080/hang"]
    params = ExperimentParams(
        auto_accept_cookies=False,
        navigate_subpages=False,
        max_depth=0,
        dwell_time=1,
        scroll_amounts=[],
        browser_type="chromium",
        headless=True,
    )
    exp_id = "test_timeout_run"
    config = ScriptSettings(
        nats_url="nats://localhost:14222",
        nats_token="e2e_test_token",
        stream_name="CRAWL_E2E",
        results_bucket="e2e-results",
        subject_prefix="crawl.urls",
        storage_type="memory",
        goto_timeout_ms=10000,
    )

    await seed_experiment_async(urls, params, exp_id, config)

    found_completion = await wait_for_completion(timeout=120, interval=10)

    assert found_completion, (
        "Crawl failed to acknowledge message after navigation timeout"
    )
    assert dc.is_running("crawler_worker")
