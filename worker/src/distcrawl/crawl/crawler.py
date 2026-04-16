"""crawler logic: process tasks and manage message acknowledgments."""

import logging
import time
from typing import Dict, List

from faststream.nats import NatsMessage
from dist_common import CrawlTask
from distcrawl.config import WorkerSettings
from distcrawl.crawl.errors import BrowserCrashError
from distcrawl.crawl.navigator import CrawlNavigator
from distcrawl.telemetry.protocol import CallbackSink

logger = logging.getLogger(__name__)


class Crawler:
    """processes incoming crawl tasks and manages message acknowledgments for lease extension."""

    def __init__(
        self,
        navigator: CrawlNavigator,
        sink: CallbackSink,
        config: WorkerSettings,
    ) -> None:
        self.navigator = navigator
        self.sink = sink
        self.config = config
        self._shutting_down = False

        # messages that need to be acked in the next batch
        # (that means that all telemetry has been pushed to the object store already)
        self._pending_acknowledgments: List[NatsMessage] = []

        # messages currently being processed (used for the heartbeat)
        # the key is the sequence number of the message
        self._active_message_lease_map: Dict[int, NatsMessage] = {}

        # how many messages are currently being processed
        self._processing_count = 0

        # watchdog: tracks when we last received a message
        self._last_activity_time: float = time.time()

    def prepare_shutdown(self) -> None:
        self._shutting_down = True

    @property
    def last_activity_time(self) -> float:
        return self._last_activity_time

    async def process_incoming_task(self, task: CrawlTask, msg: NatsMessage) -> None:
        """entry point for a new crawl task from the nats queue."""
        self._last_activity_time = time.time()
        seq = msg.raw_message.metadata.sequence.stream

        # update what we are working on
        self._active_message_lease_map[seq] = msg
        self._processing_count += 1

        try:
            logger.info(
                "Processing %s (Experiment ID: %s)", task.url, task.experiment_id
            )

            # execute the navigation sequence and gather telemetry (requests, responses, ...)
            success = await self._navigate_and_collect_telemetry(task)

            if success:
                self._pending_acknowledgments.append(msg)
                logger.info(
                    "Task success: %s - pending ACKs count: %d",
                    task.url,
                    len(self._pending_acknowledgments),
                )
            else:
                logger.warning(
                    "Task failed, acknowledging message immediately: %s", task.url
                )
                await self._acknowledge_single_message_immediately(msg)

            # evaluate if we should commit the current batch of work
            if len(self._pending_acknowledgments) >= self.config.flush_threshold:
                logger.info(
                    "Flush threshold reached (%d), persisting telemetry and committing batch",
                    len(self._pending_acknowledgments),
                )
                try:
                    await self.persist_telemetry_and_commit_batch()
                except Exception as exc:
                    logger.error("Failed to commit batch: %s", exc)
        except Exception as exc:
            # nack so NATS can redeliver to another worker
            self._active_message_lease_map.pop(seq, None)
            try:
                await msg.nack()
            except Exception:
                pass

            # browser crash will be handled by FastStream exception middleware
            if isinstance(exc, BrowserCrashError):
                raise

            if not self._shutting_down:
                logger.warning(
                    "Task interrupted by unexpected error: %s", exc, exc_info=True
                )
            else:
                logger.warning(
                    "Task interrupted by shutdown, will be redelivered: %s", task.url
                )
        finally:
            self._processing_count -= 1
            if self._processing_count == 0 and self._pending_acknowledgments:
                logger.info(
                    "Worker idle, flushing %d pending tasks",
                    len(self._pending_acknowledgments),
                )
                try:
                    await self.persist_telemetry_and_commit_batch()
                except Exception as exc:
                    logger.error("Failed to commit batch during idle flush: %s", exc)

    async def _navigate_and_collect_telemetry(self, task: CrawlTask) -> bool:
        """delegate to the navigator to perform browser actions."""
        return await self.navigator.execute(task)

    async def extend_active_message_leases(self) -> None:
        """Reset the ack_wait timer for active tasks so they are not reassigned to another worker."""
        current_active_tasks = list(self._active_message_lease_map.values())
        if not current_active_tasks:
            return

        logger.debug(
            "sending heartbeat pulse for %d active messages.",
            len(current_active_tasks),
        )
        for msg in current_active_tasks:
            try:
                await msg.in_progress()
            except Exception as exc:
                logger.warning("Failed to send heartbeat for message: %s", exc)

    async def persist_telemetry_and_commit_batch(self) -> None:
        """flush the telemetry sink to storage and acknowledge the batch of messages."""
        if not self._pending_acknowledgments:
            return

        try:
            # we have to make sure that the telemetry data is flushed before we acknowledge the messages!
            await self.sink.flush()
        except Exception as exc:
            # if it didn't work, we don't ack! another worker can pick it up
            logger.error(
                "Telemetry sink flush failed - skipping ACKs to allow task redelivery: %s",
                exc,
            )
            raise

        logger.info(
            "Committing batch: acknowledging %d messages",
            len(self._pending_acknowledgments),
        )
        remaining_failed_acks: List[NatsMessage] = []
        for msg in self._pending_acknowledgments:
            try:
                await msg.ack()
                seq = msg.raw_message.metadata.sequence.stream
                self._active_message_lease_map.pop(seq, None)
            except Exception as exc:
                logger.warning("Failed to acknowledge individual message: %s", exc)
                remaining_failed_acks.append(msg)

        # store any messages that failed to ack for the next attempt
        self._pending_acknowledgments = remaining_failed_acks

    async def _acknowledge_single_message_immediately(self, msg: NatsMessage) -> None:
        """acknowledge a single failed task to remove it from the queue quickly."""
        # we use this if the website doesn't load.
        # we can ack directly so that no other worker wastes their time on it.
        try:
            await msg.ack()
        except Exception:
            pass
        finally:
            seq = msg.raw_message.metadata.sequence.stream
            self._active_message_lease_map.pop(seq, None)
