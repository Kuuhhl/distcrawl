from config import ScriptSettings
from seed import seed_experiment_async
from tests.nats_utils import wait_for_nats, wait_for_completion
import logging
import os
from pathlib import Path

import obstore as obs
import pytest
from obstore.store import LocalStore

from dist_common.models import ExperimentParams

logger = logging.getLogger("distcrawl.test.cookies")


@pytest.mark.asyncio
async def test_cookie_consent_automatic_acceptance(dc):
    """ensure that cookie banners are found and auto-accepted."""
    # start services with environment overrides
    dc.up(
        build=True,
        env={
            "FLUSH_THRESHOLD": "1",
            "MAX_NUM_URLS": "1",
            "EXPERIMENT_ID": "test_cookies",
            "BROWSER_TYPE": "chromium",
        },
    )
    assert wait_for_nats()

    # /cookies route has a cookie banner
    urls = ["http://mock_server:8080/cookies"]
    params = ExperimentParams(
        auto_accept_cookies=True,
        navigate_subpages=False,
        max_depth=0,
        dwell_time=5,
        scroll_amounts=[],
        browser_type="chromium",
        headless=True,
    )
    exp_id = "test_cookies_run"
    config = ScriptSettings(
        nats_url="nats://localhost:14222",
        nats_token="e2e_test_token",
        stream_name="CRAWL_E2E",
        results_bucket="e2e-results",
        subject_prefix="crawl.urls",
    )


    await seed_experiment_async(urls, params, exp_id, config)

    # wait for completion
    found_completion = await wait_for_completion(timeout=120, interval=10)

    assert found_completion, "Crawl timed out or failed to process task"
    assert dc.is_running("crawler_worker")

    # verify storage contains the cookie consent event
    # the localstore path on the host matches the mounted volume
    host_storage_path = Path.cwd() / "tests" / "e2e" / "data_e2e" / "results"
    store = LocalStore(str(host_storage_path))

    # list all objects in storage
    found_cookie_file = False
    async for chunk in obs.list(store):
        for obj in chunk:
            if f"experiment={exp_id}/data_type=cookie_warning_consents/" in obj["path"]:
                found_cookie_file = True
                # verify it's a valid parquet file with content
                resp = await obs.get_async(store, obj["path"])
                data = await resp.bytes_async()
                assert len(data) > 0, "Cookie consent parquet file is empty"
                break
        if found_cookie_file:
            break

    assert found_cookie_file, (
        f"Cookie consent parquet file not found in storage for experiment {exp_id}"
    )
