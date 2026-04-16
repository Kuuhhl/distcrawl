import os
import sys
from typing import Any
from pathlib import Path

import pytest
from dist_common.models import CrawlTask


@pytest.fixture
def crawl_task_factory():
    """Fixture to create CrawlTask instances with defaults."""

    def _make(**overrides):
        defaults = {
            "url": "https://www.wikipedia.org",
            "experiment_id": "test_exp",
            "auto_accept_cookies": True,
            "navigate_subpages": False,
            "max_depth": 0,
            "dwell_time": 0,
            "scroll_amounts": [],
            "browser_type": "chromium",
            "headless": True,
        }
        defaults.update(overrides)
        return CrawlTask(**defaults)

    return _make


# Add modules to sys.path so they can be imported in all tests
root_dir = Path(__file__).parent.parent
scripts_dir = root_dir / "scripts"
common_src_dir = root_dir / "common" / "src"
worker_src_dir = root_dir / "worker" / "src"

sys.path.insert(0, str(scripts_dir))
sys.path.insert(0, str(common_src_dir))
sys.path.insert(0, str(worker_src_dir))

# Environment variables to isolate across ALL tests to prevent leakage from .env
_ENV_VARS_TO_ISOLATE = [
    "NATS_URL",
    "NATS_STREAM",
    "NATS_SUBJECT_PREFIX",
    "NATS_TOKEN",
    "RESULTS_BUCKET_NAME",
    "MAX_NUM_URLS",
    "EXPERIMENT_ID",
    "FLUSH_THRESHOLD",
    "BROWSER_TYPE",
    "STORAGE_TYPE",
    "LOCAL_STORAGE_PATH",
    "NATS_DURABLE",
    "ACK_WAIT_SECONDS",
    "HEARTBEAT_SECONDS",
    "MAX_RETRIES",
    "S3_ENDPOINT_URL",
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
]


@pytest.fixture(autouse=True)
def isolate_env():
    """Clear env vars between tests to ensure test isolation."""
    saved_env: dict[str, Any] = {k: os.environ.get(k) for k in _ENV_VARS_TO_ISOLATE}
    for k in _ENV_VARS_TO_ISOLATE:
        os.environ.pop(k, None)

    yield

    # Restore env vars
    for k, v in saved_env.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
