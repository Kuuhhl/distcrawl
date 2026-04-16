import json
import os
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import obstore as obs
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from obstore.store import MemoryStore
from download import (
    build_adblocker,
    download_metadata,
    process_and_enrich_data,
    sync_results_from_s3,
    download,
)
from dist_common.models import ExperimentMetadata, ExperimentParams


class TestDownloadMetadata:
    def test_success(self, tmp_path):
        meta = ExperimentMetadata(
            id="exp1",
            timestamp=datetime.fromisoformat("2024-01-01T00:00:00"),
            total_urls=10,
            params=ExperimentParams(),
        )
        metadata_path = os.path.join(tmp_path, "metadata.json")
        with open(metadata_path, "w") as f:
            f.write(meta.model_dump_json())

        result = download_metadata(str(tmp_path))

        assert result is not None
        assert result.id == "exp1"

    def test_no_metadata_file_returns_none(self, tmp_path):
        result = download_metadata(str(tmp_path))
        assert result is None

    def test_bad_json_returns_none(self, tmp_path):
        metadata_path = os.path.join(tmp_path, "metadata.json")
        with open(metadata_path, "w") as f:
            f.write("not json")

        result = download_metadata(str(tmp_path))
        assert result is None


class TestSyncResultsFromS3:
    @pytest.mark.asyncio
    async def test_sync_success(self, tmp_path):
        from config import ScriptSettings

        store = MemoryStore()
        obs.put(store, "exp1/data.parquet", b"parquet-data")
        obs.put(store, "exp1/metadata.json", b'{"id": "exp1"}')

        config = ScriptSettings()

        with patch.object(ScriptSettings, "get_storage", return_value=store):
            result = await sync_results_from_s3(str(tmp_path), config)

        assert result is True
        assert os.path.exists(os.path.join(tmp_path, "exp1", "data.parquet"))
        assert os.path.exists(os.path.join(tmp_path, "exp1", "metadata.json"))

        with open(os.path.join(tmp_path, "exp1", "data.parquet"), "rb") as f:
            assert f.read() == b"parquet-data"

    @pytest.mark.asyncio
    async def test_sync_empty_store(self, tmp_path):
        from config import ScriptSettings

        store = MemoryStore()
        config = ScriptSettings()

        with patch.object(ScriptSettings, "get_storage", return_value=store):
            result = await sync_results_from_s3(str(tmp_path), config)

        assert result is True

    @pytest.mark.asyncio
    async def test_sync_error_returns_false(self, tmp_path):
        from config import ScriptSettings

        config = ScriptSettings()

        with patch.object(
            ScriptSettings, "get_storage", side_effect=Exception("connection failed")
        ):
            result = await sync_results_from_s3(str(tmp_path), config)

        assert result is False


def _mock_aiohttp_session(text="", raise_error=None):
    """Create a mock aiohttp.ClientSession context manager."""
    mock_response = AsyncMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.text = AsyncMock(return_value=text)

    mock_context = AsyncMock()
    mock_context.__aenter__ = AsyncMock(return_value=mock_response)
    mock_context.__aexit__ = AsyncMock(return_value=False)

    mock_session = AsyncMock()
    if raise_error:
        mock_session.get = MagicMock(side_effect=raise_error)
    else:
        mock_session.get = MagicMock(return_value=mock_context)

    mock_session_ctx = AsyncMock()
    mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_ctx.__aexit__ = AsyncMock(return_value=False)
    return mock_session_ctx


class TestBuildAdblocker:
    @pytest.mark.asyncio
    @patch("download.aiohttp.ClientSession")
    async def test_fetches_and_compiles(self, mock_cls):
        mock_cls.return_value = _mock_aiohttp_session(
            text="||example.com^\n@@allowed.com"
        )
        engines = await build_adblocker(["http://list.txt"])

        assert isinstance(engines, dict)
        assert "list" in engines
        res = engines["list"].check_network_urls(
            "http://example.com/ads", "http://page.com", "script"
        )
        assert res.matched is True

    @pytest.mark.asyncio
    @patch("download.aiohttp.ClientSession")
    async def test_handles_request_error(self, mock_cls):
        mock_cls.return_value = _mock_aiohttp_session(raise_error=Exception("timeout"))
        engines = await build_adblocker(["http://bad.txt"])
        assert isinstance(engines, dict)
        assert len(engines) == 0

    @pytest.mark.asyncio
    @patch("download.aiohttp.ClientSession")
    async def test_multiple_lists_keyed_by_name(self, mock_cls):
        mock_cls.return_value = _mock_aiohttp_session(text="||tracker.com^")
        engines = await build_adblocker(["http://list1.txt", "http://list2.txt"])

        assert "list1" in engines
        assert "list2" in engines

    @pytest.mark.asyncio
    @patch("download.aiohttp.ClientSession")
    async def test_empty_list_returns_empty_dict(self, mock_cls):
        mock_cls.return_value = _mock_aiohttp_session()
        engines = await build_adblocker([])
        assert engines == {}


# Minimal schemas required by process_and_enrich_data's DuckDB queries.
_REQUESTS_SCHEMA = pa.schema(
    [
        ("crawled_url", pa.string()),
        ("crawl_session_id", pa.string()),
        ("current_page_url", pa.string()),
        ("url", pa.string()),
        ("frame_url", pa.string()),
        ("resource_type", pa.string()),
    ]
)
_RESPONSES_SCHEMA = pa.schema(
    [
        ("crawled_url", pa.string()),
        ("crawl_session_id", pa.string()),
        ("url", pa.string()),
    ]
)
_CONSENTS_SCHEMA = pa.schema(
    [
        ("crawled_url", pa.string()),
        ("crawl_session_id", pa.string()),
    ]
)
_SITE_METADATA_SCHEMA = pa.schema(
    [
        ("crawl_session_id", pa.string()),
        ("description", pa.string()),
        ("timestamp", pa.string()),
    ]
)
_WORKER_METADATA_SCHEMA = pa.schema(
    [
        ("worker_id", pa.string()),
        ("country_code", pa.string()),
        ("is_residential", pa.bool_()),
        ("timestamp", pa.string()),
    ]
)


def _write_parquet(path: str, rows: list[dict], schema: pa.Schema) -> None:
    if rows:
        df = pd.DataFrame(rows)
        pq.write_table(pa.Table.from_pandas(df, schema=schema), path)
    else:
        pq.write_table(
            pa.table(
                {f.name: pa.array([], type=f.type) for f in schema}, schema=schema
            ),
            path,
        )


def _seed_tmp_dir(
    tmp_dir: str,
    request_rows: list[dict] | None = None,
    response_rows: list[dict] | None = None,
    consent_rows: list[dict] | None = None,
    site_metadata_rows: list[dict] | None = None,
    worker_metadata_rows: list[dict] | None = None,
) -> None:
    """Write one parquet file per dtype into tmp_dir with proper schemas."""
    _write_parquet(
        os.path.join(tmp_dir, "requests_exp_data_type_requests_x.parquet"),
        request_rows or [],
        _REQUESTS_SCHEMA,
    )
    _write_parquet(
        os.path.join(tmp_dir, "responses_exp_data_type_responses_x.parquet"),
        response_rows or [],
        _RESPONSES_SCHEMA,
    )
    _write_parquet(
        os.path.join(
            tmp_dir,
            "cookie_warning_consents_exp_data_type_cookie_warning_consents_x.parquet",
        ),
        consent_rows or [],
        _CONSENTS_SCHEMA,
    )
    _write_parquet(
        os.path.join(
            tmp_dir,
            "site_metadata_exp_data_type_site_metadata_x.parquet",
        ),
        site_metadata_rows or [],
        _SITE_METADATA_SCHEMA,
    )
    _write_parquet(
        os.path.join(
            tmp_dir, "worker_metadata_exp_data_type_worker_metadata_x.parquet"
        ),
        worker_metadata_rows or [],
        _WORKER_METADATA_SCHEMA,
    )


class TestProcessAndEnrichData:
    def _make_adblocker(self):
        engine = MagicMock()
        engine.check_network_urls.return_value = MagicMock(matched=False)
        return {"easylist": engine}

    def test_creates_enriched_directory(self, tmp_path):
        _seed_tmp_dir(str(tmp_path))
        adblocker = self._make_adblocker()
        process_and_enrich_data(str(tmp_path), str(tmp_path), adblocker)
        assert os.path.isdir(os.path.join(tmp_path, "enriched"))

    def test_writes_enriched_parquet_files(self, tmp_path):
        _seed_tmp_dir(str(tmp_path))
        adblocker = self._make_adblocker()
        process_and_enrich_data(str(tmp_path), str(tmp_path), adblocker)

        enriched_dir = os.path.join(tmp_path, "enriched")
        assert os.path.exists(os.path.join(enriched_dir, "labeled_requests.parquet"))
        assert os.path.exists(os.path.join(enriched_dir, "responses.parquet"))
        assert os.path.exists(
            os.path.join(enriched_dir, "cookie_warning_consents.parquet")
        )
        assert os.path.exists(os.path.join(enriched_dir, "site_metadata.parquet"))
        assert os.path.exists(os.path.join(enriched_dir, "worker_metadata.parquet"))

    def test_adblocker_labeling_applied(self, tmp_path):
        _seed_tmp_dir(
            str(tmp_path),
            request_rows=[
                {
                    "crawled_url": "https://example.com",
                    "crawl_session_id": "s1",
                    "current_page_url": "https://example.com",
                    "url": "https://tracker.com/pixel.gif",
                    "frame_url": "https://example.com",
                    "resource_type": "image",
                }
            ],
        )

        engine = MagicMock()
        engine.check_network_urls.return_value = MagicMock(matched=True)
        adblocker = {"easylist": engine}

        process_and_enrich_data(str(tmp_path), str(tmp_path), adblocker)

        result = pd.read_parquet(
            os.path.join(tmp_path, "enriched", "labeled_requests.parquet")
        )
        assert "blocked_by" in result.columns
        assert json.loads(result["blocked_by"].iloc[0]) == ["easylist"]

    def test_deduplicates_sessions_keeps_most_requests(self, tmp_path):
        # s1 has 2 requests, s2 has 1 - s1 should be kept
        _seed_tmp_dir(
            str(tmp_path),
            request_rows=[
                {
                    "crawled_url": "https://example.com",
                    "crawl_session_id": "s1",
                    "current_page_url": "https://example.com",
                    "url": "https://example.com/a.js",
                    "frame_url": "https://example.com",
                    "resource_type": "script",
                },
                {
                    "crawled_url": "https://example.com",
                    "crawl_session_id": "s1",
                    "current_page_url": "https://example.com",
                    "url": "https://example.com/b.js",
                    "frame_url": "https://example.com",
                    "resource_type": "script",
                },
                {
                    "crawled_url": "https://example.com",
                    "crawl_session_id": "s2",
                    "current_page_url": "https://example.com",
                    "url": "https://example.com/c.js",
                    "frame_url": "https://example.com",
                    "resource_type": "script",
                },
            ],
        )

        adblocker = self._make_adblocker()
        process_and_enrich_data(str(tmp_path), str(tmp_path), adblocker)

        result = pd.read_parquet(
            os.path.join(tmp_path, "enriched", "labeled_requests.parquet")
        )
        assert set(result["crawl_session_id"].unique()) == {"s1"}
        assert len(result) == 2

    def test_site_metadata_linked_to_correct_session(self, tmp_path):
        # s1 has 2 requests, s2 has 1 - s1 should be kept
        _seed_tmp_dir(
            str(tmp_path),
            request_rows=[
                {
                    "crawled_url": "url1",
                    "crawl_session_id": "s1",
                    "url": "a",
                    "current_page_url": "url1",
                    "frame_url": "url1",
                    "resource_type": "script",
                },
                {
                    "crawled_url": "url1",
                    "crawl_session_id": "s1",
                    "url": "b",
                    "current_page_url": "url1",
                    "frame_url": "url1",
                    "resource_type": "script",
                },
                {
                    "crawled_url": "url1",
                    "crawl_session_id": "s2",
                    "url": "c",
                    "current_page_url": "url1",
                    "frame_url": "url1",
                    "resource_type": "script",
                },
            ],
            site_metadata_rows=[
                {
                    "crawl_session_id": "s1",
                    "description": "A news website",
                    "timestamp": "1",
                },
                {
                    "crawl_session_id": "s2",
                    "description": "A sports website",
                    "timestamp": "2",
                },
            ],
        )

        adblocker = self._make_adblocker()
        process_and_enrich_data(str(tmp_path), str(tmp_path), adblocker)

        result = pd.read_parquet(
            os.path.join(tmp_path, "enriched", "site_metadata.parquet")
        )
        assert len(result) == 1
        assert result["crawl_session_id"].iloc[0] == "s1"
        assert result["description"].iloc[0] == "A news website"


class TestDownload:
    """Tests for the main download function."""

    @patch("download.shutil.copy")
    @patch("download.sync_results_from_s3")
    @patch("download.build_adblocker", new_callable=AsyncMock)
    @patch("download.process_and_enrich_data")
    def test_download_full_experiment(
        self, mock_process, mock_build_adblocker, mock_sync, mock_copy, tmp_path
    ):
        """Test full download flow with mocked sync."""
        mock_sync.return_value = True
        mock_build_adblocker.return_value = MagicMock()

        # Create a mock experiment directory structure in a temp dir
        exp_dir = tmp_path / "exp1"
        exp_dir.mkdir()

        # Create metadata file
        meta = ExperimentMetadata(
            id="exp1",
            timestamp=datetime.fromisoformat("2024-01-01T00:00:00"),
            total_urls=5,
            params=ExperimentParams(),
        )
        with open(exp_dir / "metadata.json", "w") as f:
            f.write(meta.model_dump_json())

        # Create some parquet files
        _seed_tmp_dir(str(exp_dir))

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        # Mock TemporaryDirectory to use our tmp_path
        mock_temp_dir = MagicMock()
        mock_temp_dir.__enter__ = MagicMock(return_value=str(tmp_path))
        mock_temp_dir.__exit__ = MagicMock(return_value=False)

        # Patch only the output directory creation, use real filesystem for tmp
        with (
            patch("download.tempfile.TemporaryDirectory", return_value=mock_temp_dir),
            patch("download.os.makedirs", side_effect=lambda *args, **kwargs: None),
        ):
            download()

            mock_sync.assert_called_once()
            mock_build_adblocker.assert_called()
            mock_process.assert_called()

    @patch("download.sync_results_from_s3")
    @patch("download.build_adblocker", new_callable=AsyncMock)
    def test_download_sync_failure_returns_early(
        self, mock_build, mock_sync, tmp_path, caplog
    ):
        """Test that download exits early if sync fails."""
        mock_sync.return_value = False
        mock_build.return_value = {}

        mock_temp_dir = MagicMock()
        mock_temp_dir.__enter__ = MagicMock(return_value=str(tmp_path))
        mock_temp_dir.__exit__ = MagicMock(return_value=False)

        with (
            patch("download.tempfile.TemporaryDirectory", return_value=mock_temp_dir),
            patch("download.os.makedirs", side_effect=lambda *args, **kwargs: None),
        ):
            download()

            mock_sync.assert_called_once()
            assert "Failed to download data from object store" in caplog.text

    @patch("download.sync_results_from_s3")
    def test_download_custom_filter_lists(self, mock_sync, tmp_path):
        """Test that custom filter lists are passed through."""
        mock_sync.return_value = True

        # Create a mock experiment directory
        exp_dir = tmp_path / "exp1"
        exp_dir.mkdir()
        meta = ExperimentMetadata(
            id="exp1",
            timestamp=datetime.fromisoformat("2024-01-01T00:00:00"),
            total_urls=5,
            params=ExperimentParams(),
        )
        with open(exp_dir / "metadata.json", "w") as f:
            f.write(meta.model_dump_json())

        # Create parquet files
        _seed_tmp_dir(str(exp_dir))

        mock_temp_dir = MagicMock()
        mock_temp_dir.__enter__ = MagicMock(return_value=str(tmp_path))
        mock_temp_dir.__exit__ = MagicMock(return_value=False)

        with (
            patch("download.tempfile.TemporaryDirectory", return_value=mock_temp_dir),
            patch("download.os.makedirs", side_effect=lambda *args, **kwargs: None),
            patch("download.shutil.copy"),
            patch("download.build_adblocker", new_callable=AsyncMock) as mock_build,
            patch("download.process_and_enrich_data"),
        ):
            mock_build.return_value = MagicMock()

            download(
                filter_lists="https://list1.txt,https://list2.txt",
            )

            mock_build.assert_called_once_with(
                ["https://list1.txt", "https://list2.txt"]
            )
