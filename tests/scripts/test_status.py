"""tests for status script."""

import os
from unittest.mock import AsyncMock, MagicMock

import obstore as obs
import pytest
from obstore.store import MemoryStore
from dist_common.models import ExperimentMetadata, ExperimentParams
from status import (
    _get_nats_remaining,
    _list_experiment_ids,
    _load_metadata,
    generate_table,
    get_experiment_status,
)

_ENV_KEYS = frozenset(
    {
        "NATS_URL",
        "NATS_STREAM",
        "NATS_SUBJECT_PREFIX",
        "NATS_DURABLE",
    }
)  # env keys for scriptsettings


def _clean_env(**extras):
    """return cleaned environment dict."""
    env = {k: v for k, v in os.environ.items() if k not in _ENV_KEYS}
    env.update(extras)
    return env


def _make_meta(
    exp_id="exp1", total=100, cookies=True, subpages=False
) -> ExperimentMetadata:
    """build mock metadata."""
    import datetime

    return ExperimentMetadata(
        id=exp_id,
        timestamp=datetime.datetime(2024, 6, 15, 12, 0, 0),
        total_urls=total,
        params=ExperimentParams(
            auto_accept_cookies=cookies,
            navigate_subpages=subpages,
        ),
    )


def _make_exp(exp_id="exp1", total=100, remaining=50, meta=None) -> dict:
    """build mock experiment dict."""
    if meta is None:
        meta = _make_meta(exp_id=exp_id, total=total)
    completed = max(0, total - remaining) if remaining >= 0 else 0
    return {
        "id": exp_id,
        "metadata": meta,
        "total": total,
        "remaining": remaining,
        "completed": completed,
    }


class TestListExperimentIds:
    @pytest.mark.asyncio
    async def test_returns_ids_from_experiment_keys(self):
        storage = MemoryStore()
        await obs.put_async(storage, "experiment=exp1/metadata.json", b"{}")
        await obs.put_async(storage, "experiment=exp2/metadata.json", b"{}")

        result = await _list_experiment_ids(storage)
        assert result == ["exp2", "exp1"]

    @pytest.mark.asyncio
    async def test_ignores_non_experiment_keys(self):
        storage = MemoryStore()
        await obs.put_async(storage, "experiment=real/metadata.json", b"{}")
        await obs.put_async(storage, "some_other_file.txt", b"{}")

        result = await _list_experiment_ids(storage)
        assert result == ["real"]

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_no_objects(self):
        storage = MemoryStore()

        result = await _list_experiment_ids(storage)
        assert result == []


class TestLoadMetadata:
    @pytest.mark.asyncio
    async def test_returns_metadata_on_success(self):
        meta = _make_meta("test1")
        storage = MemoryStore()
        await obs.put_async(
            storage, "experiment=test1/metadata.json", meta.model_dump_json().encode()
        )

        result = await _load_metadata(storage, "test1")

        assert result is not None
        assert result.id == "test1"
        assert result.total_urls == 100

    @pytest.mark.asyncio
    async def test_returns_none_on_error(self):
        storage = MemoryStore()

        result = await _load_metadata(storage, "missing")
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_on_bad_json(self):
        storage = MemoryStore()
        await obs.put_async(storage, "experiment=exp1/metadata.json", b"not json")

        result = await _load_metadata(storage, "exp1")
        assert result is None


class TestGetNatsRemaining:
    @pytest.mark.asyncio
    async def test_returns_subject_counts(self):
        config = MagicMock()
        config.stream_name = "CRAWL"
        config.subject_prefix = "crawl.urls"

        mock_info = MagicMock()
        mock_info.state.subjects = {"crawl.urls.exp1": 42}

        mock_js = MagicMock()
        mock_js.stream_info = AsyncMock(return_value=mock_info)

        mock_nc = MagicMock()
        mock_nc.jetstream.return_value = mock_js

        result = await _get_nats_remaining(mock_nc, config, ["exp1"])

        assert result["exp1"] == 42

    @pytest.mark.asyncio
    async def test_returns_zero_when_subject_not_in_state(self):
        config = MagicMock()
        config.stream_name = "CRAWL"
        config.subject_prefix = "crawl.urls"

        mock_info = MagicMock()
        mock_info.state.subjects = {}

        mock_js = MagicMock()
        mock_js.stream_info = AsyncMock(return_value=mock_info)

        mock_nc = MagicMock()
        mock_nc.jetstream.return_value = mock_js

        result = await _get_nats_remaining(mock_nc, config, ["exp1"])

        assert result["exp1"] == 0


class TestGenerateTable:
    def test_empty_experiments_renders(self):
        table = generate_table({})
        assert table.title == "Crawl Experiments Status"
        assert table.row_count == 0

    def test_single_running_experiment(self):
        experiments = {"exp1": _make_exp("exp1", total=100, remaining=50)}
        table = generate_table(experiments)
        assert table.row_count == 1

    def test_done_experiment_zero_remaining(self):
        experiments = {"exp2": _make_exp("exp2", total=10, remaining=0)}
        table = generate_table(experiments)
        assert table.row_count == 1


@pytest.mark.asyncio
async def test_get_experiment_status_full():
    meta = _make_meta("test1", total=5)
    config = MagicMock()
    config.results_bucket = "bucket"
    config.subject_prefix = "crawl.urls"
    config.stream_name = "CRAWL"

    nc = MagicMock()
    js = MagicMock()
    nc.jetstream.return_value = js

    storage = MemoryStore()
    await obs.put_async(
        storage, "experiment=test1/metadata.json", meta.model_dump_json().encode()
    )

    mock_info = MagicMock()
    mock_info.state.subjects = {"crawl.urls.test1": 3}
    js.stream_info = AsyncMock(return_value=mock_info)

    result = await get_experiment_status(storage, nc, config)

    assert "test1" in result
    assert result["test1"]["total"] == 5
    assert result["test1"]["remaining"] == 3
    assert result["test1"]["completed"] == 2
