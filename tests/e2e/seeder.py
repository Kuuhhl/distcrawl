"""Seed crawl tasks for e2e tests using the faststream-based seed API."""

import asyncio
import logging
import os

from dist_common import ExperimentParams
from seed import seed_experiment_async
from config import ScriptSettings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("e2e_seeder")


async def main():
    """Seed crawl tasks for e2e tests using in-memory storage."""
    # Settings will automatically pick up from environment variables
    # default to localhost for development, but allow override
    config = ScriptSettings()
    config.stream_name = "CRAWL_E2E"
    config.results_bucket = "e2e-results"
    config.subject_prefix = "crawl.urls"
    config.storage_type = "memory"
    
    if os.environ.get("NATS_URL"):
        config.nats_url = os.environ.get("NATS_URL")
    if os.environ.get("NATS_TOKEN"):
        config.nats_token = os.environ.get("NATS_TOKEN")

    # Allow dynamic configuration via parameters if needed, otherwise use sensible e2e defaults
    base_url = "https://www.wikipedia.org"
    max_num_urls = int(os.environ.get("MAX_NUM_URLS", "1"))
    urls = [f"{base_url}/wiki/{i}" for i in range(max_num_urls)]

    params = ExperimentParams(
        auto_accept_cookies=True,
        navigate_subpages=False,
        max_depth=0,
        dwell_time=5,
        scroll_amounts=[],
        browser_type="chromium",
        headless=True,
    )

    exp_id = os.environ.get("EXPERIMENT_ID", "e2e_test")

    logger.info(
        f"Seeding {len(urls)} URLs for experiment {exp_id} to NATS at {config.nats_url}"
    )

    await seed_experiment_async(urls, params, exp_id, config)

    logger.info("Seeding complete.")


if __name__ == "__main__":
    asyncio.run(main())
