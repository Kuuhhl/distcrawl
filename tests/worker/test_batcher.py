"""tests for parquet batcher."""

import obstore as obs
import pytest
from obstore.store import MemoryStore
from distcrawl.telemetry.batcher import ParquetBatcher


@pytest.fixture
def mock_storage():
    return MemoryStore()


@pytest.mark.asyncio
async def test_batcher_append_and_flush(mock_storage):
    batcher = ParquetBatcher(storage=mock_storage, batch_size=2)

    item1 = {"url": "http://example.com/1", "status": 200}
    item2 = {"url": "http://example.com/2", "status": 200}

    await batcher.append("exp1", "responses", item1)
    # nothing flushed yet
    objs = [o async for chunk in obs.list(mock_storage) for o in chunk]
    assert len(objs) == 0

    await batcher.append("exp1", "responses", item2)

    # should have flushed after 2 items
    objs = [o async for chunk in obs.list(mock_storage) for o in chunk]
    assert len(objs) == 1
    assert "experiment=exp1/data_type=responses/" in objs[0]["path"]

    result = await obs.get_async(mock_storage, objs[0]["path"])
    data = bytes(await result.bytes_async())
    assert isinstance(data, bytes)
    assert len(data) > 0


@pytest.mark.asyncio
async def test_batcher_flush_all(mock_storage):
    batcher = ParquetBatcher(storage=mock_storage, batch_size=10)

    await batcher.append("exp1", "requests", {"id": 1})
    await batcher.append("exp2", "responses", {"id": 2})

    await batcher.flush_all()

    objs = [o async for chunk in obs.list(mock_storage) for o in chunk]
    assert len(objs) == 2
