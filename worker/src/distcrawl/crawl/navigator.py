"""runs the crawl sequence for a single url task."""

import asyncio
import hashlib
import logging
import random
import uuid

from dist_common import CrawlTask
from opentelemetry import trace
from opentelemetry.trace.status import StatusCode

from distcrawl.crawl.errors import BrowserCrashError, TelemetryIncompleteError
from distcrawl.engine.protocol import BrowserEngine, PageHandle

logger = logging.getLogger(__name__)


class CrawlNavigator:
    """visits target url and optionally follows subpage links."""

    def __init__(self, engine: BrowserEngine) -> None:
        self.engine = engine

    async def execute(self, task: CrawlTask) -> bool:
        """run crawl sequence for task."""
        tracer = trace.get_tracer(__name__)
        page: PageHandle | None = None
        crawl_session_id = uuid.uuid4().hex
        try:
            with tracer.start_as_current_span("open_context") as open_span:
                open_span.set_attribute("crawled_url", task.url)
                open_span.set_attribute("experiment_id", task.experiment_id)
                open_span.set_attribute(
                    "auto_accept_cookies",
                    task.auto_accept_cookies,
                )
                page = await self.engine.open_new_crawl_context(
                    experiment_id=task.experiment_id,
                    crawl_session_id=crawl_session_id,
                    crawled_url=task.url,
                    auto_accept_cookies=task.auto_accept_cookies,
                )

            urls_to_visit = [task.url]
            visited_urls: set[str] = set()
            depth = 0

            # depth 0 = seed, depth n = n hops out
            while urls_to_visit and depth <= task.max_depth:
                current_url = urls_to_visit.pop(0)
                if current_url in visited_urls:
                    continue

                visited_urls.add(current_url)
                logger.info(
                    "Navigating to %s (depth %d/%d)",
                    current_url,
                    depth,
                    task.max_depth,
                )

                # i wrapped this inside a separate timeout block, because on
                # some websites (that are programmed improperly), they overload
                # the browser context so the playwright-level timeouts do not
                # trigger. this way we can kill it if it takes too long. we
                # calculate the budget based on the browser timeout and the
                # dwell time.
                page_budget = (
                    (self.engine.config.goto_timeout_ms / 1000.0)
                    + task.dwell_time
                    + 30.0
                )

                try:
                    async with asyncio.timeout(page_budget):
                        with tracer.start_as_current_span("navigate") as nav_span:
                            nav_span.set_attribute("navigate.url", current_url)
                            nav_span.set_attribute("navigate.depth", depth)

                            found_links = await self.engine.navigate_to_url(
                                current_url, page=page, crawl_depth=depth
                            )

                            if found_links is None:
                                nav_span.set_attribute("navigate.success", False)
                                nav_span.set_status(
                                    StatusCode.ERROR, "navigation failed"
                                )
                                if depth == 0:  # seed url failed
                                    logger.warning(
                                        "Seed URL navigation failed: %s", current_url
                                    )
                                    return False
                                depth += 1  # subpage failed, skip it
                                continue

                            nav_span.set_attribute("navigate.success", True)
                            nav_span.set_attribute(
                                "navigate.links_found", len(found_links)
                            )
                            logger.info(
                                "Navigation successful, found %d links",
                                len(found_links),
                            )

                        concurrent_actions = []

                        async def _dwell() -> None:
                            with tracer.start_as_current_span("dwell"):
                                await self.engine.wait_on_page(
                                    task.dwell_time, page=page
                                )

                        async def _scroll() -> None:
                            with tracer.start_as_current_span("scroll"):
                                await self.engine.execute_scrolling_sequence(
                                    task.scroll_amounts, page=page
                                )

                        if task.dwell_time > 0:
                            logger.debug("Scheduling dwell time: %ds", task.dwell_time)
                            concurrent_actions.append(_dwell())

                        if task.scroll_amounts:
                            logger.debug(
                                "Scheduling scroll sequence: %s", task.scroll_amounts
                            )
                            concurrent_actions.append(_scroll())

                        if concurrent_actions:
                            await asyncio.gather(*concurrent_actions)
                            logger.debug("Dwell and scroll actions completed")

                except asyncio.TimeoutError:
                    logger.error(
                        "Hard timeout reached for %s (depth %d)", current_url, depth
                    )
                    trace.get_current_span().add_event(
                        "depth_timeout",
                        {"depth": depth, "url": current_url},
                    )
                    if await self.engine.browser_engine_dead():
                        raise BrowserCrashError(
                            f"Browser died during crawl of {current_url}"
                        )
                    if depth == 0:
                        return False
                    # if subpage hangs, stop crawling this task but return
                    # success for pages already done
                    break

                if task.navigate_subpages and depth < task.max_depth and found_links:
                    # deterministic seed from task url + depth so both browsers
                    # follow the exact same path through each website.
                    seed = hashlib.sha256(f"{task.url}:{depth}".encode()).digest()
                    rng = random.Random(seed)
                    subpage_url = rng.choice(sorted(set(found_links)))
                    logger.info("Selected subpage for next hop: %s", subpage_url)
                    trace.get_current_span().add_event(
                        "subpage_selected",
                        {"subpage.url": subpage_url, "subpage.depth": depth},
                    )
                    urls_to_visit.append(subpage_url)

                depth += 1

            logger.info("Crawl sequence completed successfully")
            return True
        except (BrowserCrashError, TelemetryIncompleteError):
            raise
        except Exception as exc:
            logger.error(
                "Failed to crawl %s: %s (type: %s)",
                task.url,
                exc,
                type(exc).__name__,
            )
            return False
        finally:
            if page:
                try:
                    with tracer.start_as_current_span("close_context"):
                        logger.info("Closing page and context...")
                        await self.engine.close_crawl_context(page)
                        logger.info("Page and context closed")
                except TelemetryIncompleteError:
                    raise
                except Exception as exc:
                    logger.debug("Error closing page for %s: %s", task.url, exc)
