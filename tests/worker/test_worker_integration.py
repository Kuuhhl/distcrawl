from distcrawl.main import settings
import pytest
from faststream.nats import TestNatsBroker
from unittest.mock import AsyncMock, MagicMock
from distcrawl.main import broker, handle_crawl_task_received
from distcrawl import Crawler

@pytest.mark.asyncio
async def test_worker_subscriber_integration(crawl_task_factory):
    """Verify that the NATS subscriber correctly routes messages to the crawler using TestBroker."""

    # Mock the crawler that is normally provided via FastStream context
    mock_crawler = MagicMock(spec=Crawler)
    mock_crawler.process_incoming_task = AsyncMock()
    mock_crawler.last_activity_time = 0

    # Create a dummy task
    task = crawl_task_factory(url="http://example.com")

    async with TestNatsBroker(broker) as tester:
        # Publish a message to the subject the worker is listening to
        subject = f"{settings.subject_prefix}.chromium.headless.test"

        await tester.publish(task, subject=subject)

        mock_msg = MagicMock()
        await handle_crawl_task_received(task, mock_msg, task_crawler=mock_crawler)

        mock_crawler.process_incoming_task.assert_called_once_with(task, mock_msg)
