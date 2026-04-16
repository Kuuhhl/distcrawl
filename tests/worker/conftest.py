from unittest.mock import AsyncMock, MagicMock

import pytest
from distcrawl.config import WorkerSettings


@pytest.fixture
def config():
    """fixture for worker settings."""
    return WorkerSettings(
        flush_threshold=10,
        s3_access_key="mock",
        s3_secret_key="mock",
        browser_type="chromium",
        num_crawlers=10,
        goto_timeout_ms=30000,
        scroll_delay_seconds=0.1,
    )


@pytest.fixture
def mock_navigator():
    nav = MagicMock()
    nav.execute = AsyncMock(return_value=True)
    return nav


@pytest.fixture
def mock_sink():
    sink = MagicMock()
    sink.flush = AsyncMock()
    return sink


@pytest.fixture
def mock_msg():
    msg = MagicMock()
    msg.raw_message.metadata.sequence.stream = 1
    msg.ack = AsyncMock()
    msg.in_progress = AsyncMock()
    return msg


@pytest.fixture
def mock_engine():
    """create mock browser engine."""
    engine = MagicMock()
    engine.config = MagicMock()
    engine.config.goto_timeout_ms = 30000
    engine.open_new_crawl_context = AsyncMock(return_value=MagicMock(name="page"))
    engine.close_crawl_context = AsyncMock()
    engine.execute_scrolling_sequence = AsyncMock()
    engine.wait_on_page = AsyncMock()
    engine.navigate_to_url = AsyncMock(return_value=[])
    return engine
