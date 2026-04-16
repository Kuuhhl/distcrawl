import pytest
from unittest.mock import AsyncMock, MagicMock
from distcrawl.telemetry.sink import TelemetrySink
from dist_common.types import RequestEvent, NodeInfo


@pytest.mark.asyncio
async def test_telemetry_integration_attaches_worker_id():
    """Verify that TelemetrySink correctly enriches RequestEvent with worker_id."""
    mock_batcher = MagicMock()
    mock_batcher.append = AsyncMock()

    worker_id = "test-worker-123"
    node_info: NodeInfo = {
        "country_code": "DE",
        "is_residential": True,
        "browser_type": "chromium",
        "is_headless": True,
    }

    sink = TelemetrySink(batcher=mock_batcher, worker_id=worker_id, node_info=node_info)

    # Simulate a raw request event coming from the browser engine
    raw_event: RequestEvent = {
        "experiment_id": "exp_test",
        "request_id": "req_abc",
        "worker_id": "",
        "crawl_session_id": "sess_xyz",
        "timestamp": "123456789",
        "crawled_url": "http://start.com",
        "current_page_url": "http://start.com/page1",
        "url": "http://example.com/pixel.gif",
        "frame_url": "http://start.com",
        "resource_type": "image",
        "method": "GET",
        "headers": "{}",
        "crawl_depth": 0,
    }

    await sink.on_request(raw_event)

    # Check if the event was enriched before being passed to the batcher
    mock_batcher.append.assert_called_once()
    exp_id, data_type, enriched_event = mock_batcher.append.call_args[0]

    assert exp_id == "exp_test"
    assert data_type == "requests"
    assert enriched_event["worker_id"] == worker_id
    assert "node_info" not in enriched_event
    assert enriched_event["url"] == "http://example.com/pixel.gif"
