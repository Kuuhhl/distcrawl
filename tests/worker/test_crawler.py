import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from distcrawl.config import WorkerSettings
from distcrawl.crawl.crawler import Crawler
from distcrawl.crawl.errors import BrowserCrashError


@pytest.mark.asyncio
async def test_crawler_process_incoming_task_success(
    mock_navigator, mock_sink, config, mock_msg
):
    crawler = Crawler(mock_navigator, mock_sink, config)
    task = MagicMock()
    task.browser_type = "chromium"
    task.headless = False
    task.url = "http://example.com"
    task.experiment_id = "test_exp"

    # In actual usage, this is called and because it's the only task, it will flush at the end (idle flush)
    await crawler.process_incoming_task(task, mock_msg)

    assert mock_navigator.execute.called
    # After idle flush, pending ACKs should be 0 and sink.flush should have been called
    assert len(crawler._pending_acknowledgments) == 0
    assert mock_sink.flush.called


@pytest.mark.asyncio
async def test_crawler_process_incoming_task_failure(
    mock_navigator, mock_sink, config, mock_msg
):
    mock_navigator.execute.return_value = False
    crawler = Crawler(mock_navigator, mock_sink, config)
    task = MagicMock()
    task.browser_type = "chromium"
    task.headless = False

    await crawler.process_incoming_task(task, mock_msg)

    assert mock_navigator.execute.called
    assert len(crawler._pending_acknowledgments) == 0
    assert mock_msg.ack.called
    assert 1 not in crawler._active_message_lease_map


@pytest.mark.asyncio
async def test_crawler_flushes_at_threshold(mock_navigator, mock_sink):
    config = WorkerSettings(
        flush_threshold=2, s3_access_key="mock", s3_secret_key="mock"
    )
    crawler = Crawler(mock_navigator, mock_sink, config)

    msg1 = MagicMock()
    msg1.raw_message.metadata.sequence.stream = 1
    msg1.ack = AsyncMock()

    msg2 = MagicMock()
    msg2.raw_message.metadata.sequence.stream = 2
    msg2.ack = AsyncMock()

    task = MagicMock()
    task.browser_type = "chromium"
    task.headless = False

    async def slow_navigate(*args):
        await asyncio.sleep(0.1)
        return True

    mock_navigator.execute.side_effect = slow_navigate

    t1 = asyncio.create_task(crawler.process_incoming_task(task, msg1))
    t2 = asyncio.create_task(crawler.process_incoming_task(task, msg2))

    await asyncio.gather(t1, t2)

    # Should have flushed at least once
    assert mock_sink.flush.called
    assert msg1.ack.called
    assert msg2.ack.called
    assert len(crawler._pending_acknowledgments) == 0


@pytest.mark.asyncio
async def test_crawler_extend_active_message_leases(
    mock_navigator, mock_sink, config, mock_msg
):
    crawler = Crawler(mock_navigator, mock_sink, config)

    # manually add to lease map
    crawler._active_message_lease_map[1] = mock_msg

    await crawler.extend_active_message_leases()

    mock_msg.in_progress.assert_called_once()


@pytest.mark.asyncio
async def test_crawler_persist_telemetry_and_commit_batch_success(
    mock_navigator, mock_sink, config, mock_msg
):
    crawler = Crawler(mock_navigator, mock_sink, config)
    crawler._pending_acknowledgments.append(mock_msg)
    crawler._active_message_lease_map[1] = mock_msg

    await crawler.persist_telemetry_and_commit_batch()

    mock_sink.flush.assert_called_once()
    mock_msg.ack.assert_called_once()
    assert len(crawler._pending_acknowledgments) == 0
    assert 1 not in crawler._active_message_lease_map


@pytest.mark.asyncio
async def test_crawler_persist_telemetry_and_commit_batch_partial_failure(
    mock_navigator, mock_sink, config
):
    crawler = Crawler(mock_navigator, mock_sink, config)

    msg1 = MagicMock()
    msg1.raw_message.metadata.sequence.stream = 1
    msg1.ack = AsyncMock()

    msg2 = MagicMock()
    msg2.raw_message.metadata.sequence.stream = 2
    msg2.ack = AsyncMock(side_effect=Exception("Ack failed"))

    crawler._pending_acknowledgments.extend([msg1, msg2])
    crawler._active_message_lease_map[1] = msg1
    crawler._active_message_lease_map[2] = msg2

    await crawler.persist_telemetry_and_commit_batch()

    assert msg1.ack.called
    assert msg2.ack.called
    assert len(crawler._pending_acknowledgments) == 1
    assert crawler._pending_acknowledgments[0] is msg2
    assert 1 not in crawler._active_message_lease_map
    assert 2 in crawler._active_message_lease_map


@pytest.mark.asyncio
async def test_browser_crash_nacks_and_raises(
    mock_navigator, mock_sink, config, mock_msg
):
    """BrowserCrashError must nack the message and propagate for the middleware to handle."""
    mock_navigator.execute = AsyncMock(
        side_effect=BrowserCrashError("browser has been closed")
    )
    crawler = Crawler(mock_navigator, mock_sink, config)
    task = MagicMock()
    task.url = "http://example.com"
    task.experiment_id = "test_exp"

    with pytest.raises(BrowserCrashError):
        await crawler.process_incoming_task(task, mock_msg)

    mock_msg.nack.assert_called_once()
    mock_msg.ack.assert_not_called()
    assert 1 not in crawler._active_message_lease_map
