"""playwright browser engine for web crawling."""

import asyncio
import time
import json
import logging
from functools import wraps
from typing import Callable, List, Optional

from dist_common.types import (
    CookieAcceptEvent,
    RequestEvent,
    ResponseEvent,
    SiteMetadataEvent,
)
from distcrawl.config import WorkerSettings
from distcrawl.engine.consent_acceptor import COOKIE_ACCEPTOR_JS
from distcrawl.telemetry.protocol import CallbackSink
from playwright.async_api import (
    Browser,
    Page,
    Playwright,
    async_playwright,
)
from playwright.async_api import (
    TimeoutError as PWTimeoutError,
)

from distcrawl.crawl.errors import BrowserCrashError

logger = logging.getLogger(__name__)

SUPPORTED_BROWSERS = ("chromium", "firefox", "webkit")


def _browser_operation(func):
    """Catches browser disconnections and converts them to BrowserCrashError."""

    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        try:
            return await func(self, *args, **kwargs)
        except Exception as exc:
            if await self.browser_engine_dead():
                logger.critical("Browser died during %s: %s", func.__name__, exc)
                raise BrowserCrashError(str(exc)) from exc
            # browser alive
            raise

    return wrapper


class PlaywrightEngine:
    """encapsulates playwright browser interactions and event handling."""

    def __init__(self, config: WorkerSettings) -> None:
        if config.browser_type not in SUPPORTED_BROWSERS:
            raise ValueError(
                "Unsupported browser type '%s'. Must be one of: %s"
                % (config.browser_type, ", ".join(SUPPORTED_BROWSERS))
            )
        self.config = config
        self.browser: Browser | None = None
        self.pw: Optional[Playwright] = None

        self._on_response_callback: Callable | None = None
        self._on_request_callback: Callable | None = None
        self._on_cookie_accept_callback: Callable | None = None
        self._on_site_metadata_callback: Callable | None = None

    def set_sink(self, sink: CallbackSink) -> None:
        """link telemetry callbacks to the browser engine."""
        self._on_cookie_accept_callback = sink.on_cookie_accept
        self._on_request_callback = sink.on_request
        self._on_response_callback = sink.on_response
        self._on_site_metadata_callback = sink.on_site_metadata

    async def _handle_cookie_accept_event(
        self,
        url: str,
        timestamp: str,
        experiment_id: str,
        crawl_session_id: str,
        crawled_url: str,
        crawl_depth: int,
    ) -> None:
        """internal handler for cookie consent detections."""
        logger.info("Cookie consent detected on %s", url)
        if self._on_cookie_accept_callback:
            event: CookieAcceptEvent = {
                "experiment_id": experiment_id,
                "crawl_session_id": crawl_session_id,
                "crawled_url": crawled_url,
                "url": url,
                "timestamp": timestamp,
                "crawl_depth": crawl_depth,
            }
            await self._on_cookie_accept_callback(event)

    async def start_browser_engine(self, headless: bool = True) -> None:
        """launch the browser instance."""
        if self.browser:
            return

        self.pw = await async_playwright().start()
        launcher = getattr(self.pw, self.config.browser_type)
        self.browser = await launcher.launch(headless=headless)

        logger.info(
            "Started %s browser engine (parallel contexts limit: %d)",
            self.config.browser_type,
            self.config.num_crawlers,
        )

    @_browser_operation
    async def open_new_crawl_context(
        self,
        experiment_id: str = "default",
        crawl_session_id: str = "",
        crawled_url: str = "",
        auto_accept_cookies: bool = True,
    ) -> Page:
        """provision a new browser context with telemetry hooks."""
        if not self.browser:
            raise RuntimeError(
                "Browser engine not started. Call start_browser_engine() first."
            )

        # new context is so that cookies are independent
        # which means we can run crawls with different cookie-settings in a single browser instance.
        context_args = {}

        # this is a fix for the following error that only happens to webkit:
        # Protocol error (Emulation.setDeviceMetricsOverride): Failed to resize window
        if self.config.browser_type.lower().strip() == "webkit":
            context_args["no_viewport"] = True

        context = None
        try:
            context = await self.browser.new_context(**context_args)

            if auto_accept_cookies:
                # this script tries to accept anything that looks like it is a cookie consent prompt.
                await context.add_init_script(COOKIE_ACCEPTOR_JS)

            page = await context.new_page()
        except Exception:
            logger.error("Failed to create new page. Closing context.", exc_info=True)
            if context:
                try:
                    await context.close()
                except Exception:
                    pass
            raise

        page._crawl_depth = 0
        page._experiment_id = experiment_id
        page._crawl_session_id = crawl_session_id

        # attach event listeners to requests and responses
        page.on(
            "request",
            lambda r, p=page, eid=experiment_id, sid=crawl_session_id, curl=crawled_url: (
                asyncio.create_task(
                    self._dispatch_request_event(
                        r, p.url, eid, sid, curl, getattr(p, "_crawl_depth", 0)
                    )
                )
            ),
        )
        page.on(
            "response",
            lambda r, p=page, eid=experiment_id, sid=crawl_session_id, curl=crawled_url: (
                asyncio.create_task(
                    self._dispatch_response_event(
                        r, eid, sid, curl, getattr(p, "_crawl_depth", 0)
                    )
                )
            ),
        )

        # expose cookie accept handler to the cookie-script
        await context.expose_binding(
            "handleCookieAccept",
            lambda source, url, ts, p=page, eid=experiment_id, sid=crawl_session_id, curl=crawled_url: (
                asyncio.create_task(
                    self._handle_cookie_accept_event(
                        url, ts, eid, sid, curl, getattr(p, "_crawl_depth", 0)
                    )
                )
            ),
        )

        return page

    async def browser_engine_dead(self):
        """check if browser engine dead by attempting to create a context."""
        if self.browser:
            if not self.browser.is_connected():
                return True
            try:
                async with asyncio.timeout(2.0):
                    context = await self.browser.new_context()
                    await context.close()
            except Exception:
                return True
        return False

    @_browser_operation
    async def close_crawl_context(self, page: Page) -> None:
        """safely terminate a browser page and its context."""
        context = page.context
        try:
            async with asyncio.timeout(10.0):
                await page.close()
                await context.close()
            logger.info("Page and context closed successfully")
        except asyncio.TimeoutError:
            logger.warning("Timeout during page closure - proceeding anyway")
        except Exception as exc:
            logger.debug("Error during page closure: %s", exc)

    def is_engine_ready(self) -> bool:
        """check if browser engine is operational."""
        return self.browser is not None

    async def stop_browser_engine(self) -> None:
        """terminate browser and playwright instance."""
        if self.browser:
            await self.browser.close()
        if self.pw:
            await self.pw.stop()

    @_browser_operation
    async def wait_on_page(self, duration_seconds: float, page: Page) -> None:
        """wait for a duration, raises BrowserCrashError if the browser dies."""
        await page.wait_for_timeout(duration_seconds * 1000)

    @_browser_operation
    async def execute_scrolling_sequence(
        self,
        scroll_steps: List[int],
        page: Page | None = None,
    ) -> None:
        """perform a series of scroll actions on the page."""
        if not page:
            return

        async with asyncio.timeout(30.0):
            for amount in scroll_steps:
                try:
                    await page.mouse.wheel(0, amount)
                    await asyncio.sleep(self.config.scroll_delay_seconds)
                except Exception:
                    if await self.browser_engine_dead():
                        raise
                    break

    @_browser_operation
    async def navigate_to_url(
        self, url: str, page: Page | None = None, crawl_depth: int = 0
    ) -> Optional[List[str]]:
        """navigate to a target URL and extract same-domain links."""
        if not page:
            return None

        page._crawl_depth = crawl_depth
        total_timeout_s = (self.config.goto_timeout_ms / 1000.0) + 15.0

        try:
            return await asyncio.wait_for(
                self._perform_navigation_and_extraction(url, page),
                timeout=total_timeout_s,
            )
        except (asyncio.TimeoutError, PWTimeoutError):
            if await self.browser_engine_dead():
                raise BrowserCrashError(f"Browser died during navigation to {url}")

            logger.warning("Navigation to %s timed out after %ds", url, total_timeout_s)
            return None
        except Exception:
            if await self.browser_engine_dead():
                raise BrowserCrashError(f"Browser died during navigation to {url}")

            logger.warning("Navigation to %s failed with error", url)
            return None

    async def _perform_navigation_and_extraction(
        self, url: str, page: Page
    ) -> List[str]:
        """internal navigation and link extraction logic."""
        logger.info("Navigating to %s (wait_until: domcontentloaded)", url)
        await page.goto(
            url, wait_until="domcontentloaded", timeout=self.config.goto_timeout_ms
        )

        try:
            # allow some time for network activity to settle
            await page.wait_for_load_state("networkidle", timeout=5000)
        except PWTimeoutError:
            logger.debug(
                "Network didn't settle in 5s for %s, proceeding to extraction", url
            )

        # extract same-domain links
        links = await page.evaluate("""
            () => {
                const links = Array.from(document.querySelectorAll('a[href]'));
                const currentDomain = window.location.hostname;
                return links
                    .map(a => a.href)
                    .filter(href => {
                        try {
                            const url = new URL(href);
                            return url.hostname === currentDomain &&
                                   (url.protocol === 'http:' || url.protocol === 'https:');
                        } catch (e) {
                            return false;
                        }
                    });
            }
        """)

        # extract homepage metadata (depth 0 only)
        if getattr(page, "_crawl_depth", -1) == 0:
            await self._extract_and_dispatch_metadata(page)

        return list(set(links))

    async def _extract_and_dispatch_metadata(self, page: Page) -> None:
        """extract page description and dispatch as metadata event."""
        try:
            description = await page.evaluate(
                '() => document.querySelector(\'meta[name="description"]\')?.content?.slice(0, 500) || ""'
            )

            if self._on_site_metadata_callback:
                event: SiteMetadataEvent = {
                    "crawl_session_id": getattr(page, "_crawl_session_id", ""),
                    "description": description,
                    "timestamp": str(time.time()),
                }
                exp_id = getattr(page, "_experiment_id", "default")
                await self._on_site_metadata_callback(event, exp_id)
        except BrowserCrashError:
            logger.warning("Browser crashed while extracting metadata for %s", page.url)
            raise
        except Exception as exc:
            logger.warning("Metadata extraction failed for %s: %s", page.url, exc)

    async def _dispatch_request_event(
        self,
        request,
        current_page_url: str,
        experiment_id: str,
        crawl_session_id: str,
        crawled_url: str,
        crawl_depth: int,
    ) -> None:
        """internal handler for outgoing request telemetry."""
        if self._on_request_callback:
            try:
                frame_url = ""
                try:
                    if request.frame:
                        frame_url = request.frame.url
                except Exception:
                    pass

                event: RequestEvent = {
                    "experiment_id": experiment_id,
                    "request_id": str(id(request)),
                    "worker_id": "",  # will be populated by the sink
                    "crawl_session_id": crawl_session_id,
                    "timestamp": str(time.time()),
                    "crawled_url": crawled_url,
                    "current_page_url": current_page_url,
                    "url": request.url,
                    "frame_url": frame_url,
                    "resource_type": request.resource_type,
                    "method": request.method,
                    "headers": json.dumps(request.headers),
                    "crawl_depth": crawl_depth,
                }
                await self._on_request_callback(event)
            except Exception as exc:
                logger.debug(
                    "Telemetry dispatch error (request) for %s: %s", request.url, exc
                )

    async def _dispatch_response_event(
        self,
        response,
        experiment_id: str,
        crawl_session_id: str,
        crawled_url: str,
        crawl_depth: int,
    ) -> None:
        """internal handler for incoming response telemetry."""
        if self._on_response_callback:
            try:
                timing = response.request.timing
                event: ResponseEvent = {
                    "experiment_id": experiment_id,
                    "request_id": str(id(response.request)),
                    "crawl_session_id": crawl_session_id,
                    "timestamp": str(
                        timing.get("startTime", 0) + timing.get("responseStart", 0)
                    ),
                    "crawled_url": crawled_url,
                    "url": response.url,
                    "status": response.status,
                    "headers": json.dumps(response.headers),
                    "crawl_depth": crawl_depth,
                    "cookies": json.dumps(await response.header_values("set-cookie")),
                }
                await self._on_response_callback(event)
            except Exception as exc:
                logger.debug(
                    "Telemetry dispatch error (response) for %s: %s", response.url, exc
                )
