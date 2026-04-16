import pytest
import obstore as obs
from unittest.mock import AsyncMock, MagicMock, patch
import seed
from dist_common.models import ExperimentParams
from obstore.store import MemoryStore
from config import ScriptSettings

# placeholder
DEFAULT_ARGS = {
    "accept_cookies": True,
    "navigate": False,
    "depth": 0,
    "dwell_seconds": 30,
    "scroll_amounts": [],
    "browser": "chromium",
    "headless": True,
}


@pytest.fixture
def mock_broker():
    """Provides a mocked FastStream NatsBroker."""
    with patch("seed.NatsBroker") as mock_broker_class:
        mock_instance = MagicMock()
        mock_instance.start = AsyncMock()
        mock_instance.stop = AsyncMock()
        mock_instance._connection = MagicMock()

        # Mock the publisher that gets returned by broker.publisher()
        mock_publisher = MagicMock()
        mock_publisher.publish = AsyncMock()
        mock_instance.publisher.return_value = mock_publisher

        mock_broker_class.return_value = mock_instance
        yield mock_instance, mock_publisher


@pytest.fixture
def mock_storage():
    """Provides a MemoryStore for testing."""
    store = MemoryStore()

    with patch.object(seed.ScriptSettings, "get_storage", return_value=store):
        yield store


class TestTrancoSeeding:
    def test_invalid_name_exits(self):
        """Verify that a non-alphanumeric name causes the script to exit."""
        with pytest.raises(SystemExit):
            seed.seed("invalid-name-123", num_tranco=5, **DEFAULT_ARGS)

    def test_tranco_cache_missing_exits(self):
        with patch("seed.Path") as MockPath:
            mock_path = MagicMock()
            mock_path.exists.return_value = False
            # Make Path("data") / "tranco_cache" return our mock
            MockPath.return_value = mock_path
            # Also mock the division operator for Path("data") / "tranco_cache"
            mock_path.__truediv__.return_value = mock_path

            with pytest.raises(SystemExit):
                # "test" is alphanumeric, passing the first validation check
                seed.seed("test", num_tranco=5, **DEFAULT_ARGS)

    def test_tranco_empty_exits(self):
        with (
            patch("seed.Path") as MockPath,
            patch("seed.open", create=True) as mock_open,
            patch("seed.seed_experiment_async", new_callable=AsyncMock),
        ):
            mock_path = MagicMock()
            mock_path.exists.return_value = True
            mock_path.glob.return_value = [MagicMock()]
            MockPath.return_value = mock_path

            # Mock empty CSV file
            mock_file = MagicMock()
            mock_file.__enter__ = MagicMock(return_value=mock_file)
            mock_file.__exit__ = MagicMock(return_value=False)
            mock_open.return_value = mock_file

            with patch("seed.csv.reader", return_value=iter([])):
                with pytest.raises(SystemExit):
                    seed.seed("test", num_tranco=5, **DEFAULT_ARGS)

    def test_successful_tranco_seed(self, tmp_path):
        # Create a temporary CSV file
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("1,google.com\n2,facebook.com\n")

        with patch(
            "seed.seed_experiment_async", new_callable=AsyncMock
        ) as mock_seed_async:
            seed.seed("test", num_tranco=2, tranco_path=str(tmp_path), **DEFAULT_ARGS)

            mock_seed_async.assert_called_once()
            # check if urls were passed correctly (as a list)
            args, _ = mock_seed_async.call_args
            urls = args[0]
            assert isinstance(urls, list)
            assert len(urls) == 2
            assert "https://google.com/" in urls
            assert "https://facebook.com/" in urls


@pytest.mark.asyncio
async def test_seed_experiment_success(mock_storage):
    from seed import seed_experiment_async, broker

    config = ScriptSettings(
        nats_url="nats://localhost:4222",
        results_bucket="test_bucket",
        stream_name="test_stream",
        subject_prefix="crawl",
    )

    urls = ["https://example.com"]
    params = ExperimentParams(browser_type="chromium", headless=True)

    from faststream.nats import TestNatsBroker

    with patch("seed.NatsBroker", return_value=broker):
        async with TestNatsBroker(broker):
            await seed_experiment_async(urls, params, "test_id", config)

            # Verify Object Store upload
            objs = [o async for chunk in obs.list(mock_storage) for o in chunk]
            assert len(objs) == 1
            assert "experiment=test_id/metadata.json" in objs[0]["path"]

        # In TestBroker, we can't easily check if a message was published
        # to a specific subject without a subscriber, but we've verified
        # the function runs without errors and uploads metadata.


def test_experiment_params_creation():
    """Verify that params are correctly mapped (bools, lists, and new browser args)."""
    params = ExperimentParams(
        auto_accept_cookies=True,
        navigate_subpages=False,
        max_depth=2,
        dwell_time=10,
        scroll_amounts=[100, 200],
        browser_type="firefox",
        headless=False,
    )
    assert params.auto_accept_cookies is True
    assert params.navigate_subpages is False
    assert params.max_depth == 2
    assert params.dwell_time == 10
    assert params.scroll_amounts == [100, 200]
    assert params.browser_type == "firefox"
    assert params.headless is False
