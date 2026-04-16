import logging
import os

import pytest
from dist_common.models import ExperimentParams
from seed import seed_experiment_async
from config import ScriptSettings
from tests.nats_utils import wait_for_nats, wait_for_completion

logger = logging.getLogger("distcrawl.test.basic")


@pytest.mark.asyncio
async def test_basic_crawl_flow(dc):
    """end-to-end happy path crawl using local mock server."""
    # start all services with specific e2e environment overrides
    dc.up(
        build=True,
        env={
            "FLUSH_THRESHOLD": "1",
            "MAX_NUM_URLS": "1",
            "EXPERIMENT_ID": "test_basic",
            "BROWSER_TYPE": "chromium",
        },
    )
    assert wait_for_nats()

    # local mock server
    urls = ["http://mock_server:8080/basic"]
    params = ExperimentParams(
        auto_accept_cookies=False,
        navigate_subpages=True,
        max_depth=1,
        dwell_time=1,
        scroll_amounts=[],
        browser_type="chromium",
        headless=True,
    )
    exp_id = "test_basic_run"
    config = ScriptSettings(
        nats_url="nats://localhost:14222",
        nats_token="e2e_test_token",
        stream_name="CRAWL_E2E",
        results_bucket="e2e-results",
        subject_prefix="crawl.urls",
    )

    await seed_experiment_async(urls, params, exp_id, config)

    found = await wait_for_completion()

    if not found:
        print("\nTIMEOUT. Diagnostic Dump:")
        print(dc.logs("crawler_worker"))
        print(dc.logs("seeder"))
        pytest.fail("Crawl timeout")

    assert dc.is_running("crawler_worker")


@pytest.mark.asyncio
async def test_on3_crawl(dc):
    """test specifically on3.com as it previously broke the crawler."""
    dc.up(
        build=True,
        env={
            "FLUSH_THRESHOLD": "1",
            "MAX_NUM_URLS": "1",
            "EXPERIMENT_ID": "test_on3",
            "BROWSER_TYPE": "chromium",
        },
    )
    assert wait_for_nats()

    urls = ["https://www.on3.com/"]
    params = ExperimentParams(
        auto_accept_cookies=True,
        navigate_subpages=False,
        max_depth=0,
        dwell_time=10,
        scroll_amounts=[500, 500],
        browser_type="chromium",
        headless=True,
    )
    exp_id = "test_on3_run"
    config = ScriptSettings(
        nats_url="nats://localhost:14222",
        nats_token="e2e_test_token",
        stream_name="CRAWL_E2E",
        results_bucket="e2e-results",
        subject_prefix="crawl.urls",
    )

    await seed_experiment_async(urls, params, exp_id, config)

    found = await wait_for_completion(timeout=180, interval=10)

    if not found:
        print("\nTIMEOUT. Diagnostic Dump:")
        print(dc.logs("crawler_worker"))
        print(dc.logs("seeder"))

    assert found, "on3.com crawl failed or timed out"
    assert dc.is_running("crawler_worker")
