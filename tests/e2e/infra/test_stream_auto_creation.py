from seed import seed_experiment_async
from config import ScriptSettings

from tests.nats_utils import wait_for_nats

import os

import nats
import nats.errors
import pytest
from dist_common.models import ExperimentParams


@pytest.mark.asyncio
async def test_seed_script_creates_stream_if_missing(dc):
    """Verify automatic stream creation."""
    dc.up(services=["nats"])
    assert wait_for_nats()

    config = ScriptSettings(
        nats_url="nats://localhost:14222",
        nats_token="e2e_test_token",
        stream_name="SEED_AUTO_STREAM",
        subject_prefix="seed.test",
    )

    nc = await nats.connect(config.nats_url, token=config.nats_token)
    js = nc.jetstream()

    # Verify stream does not exist
    with pytest.raises(Exception):
        await js.stream_info(config.stream_name)

    urls = ["http://mock_server:8080/basic"]
    params = ExperimentParams(
        auto_accept_cookies=True,
        navigate_subpages=False,
        max_depth=0,
        dwell_time=1,
        scroll_amounts=[],
        browser_type="chromium",
        headless=True,
    )
    exp_id = "seed_test"

    # Trigger auto-creation
    await seed_experiment_async(urls, params, exp_id, config)

    # Verify stream exists
    stream_info = await js.stream_info(config.stream_name)
    assert stream_info.config.name == config.stream_name
    subjects = stream_info.config.subjects or []
    assert f"{config.subject_prefix}.>" in subjects

    await nc.close()
