"""browser engine protocol."""

from typing import Any, List, Optional, Protocol

from distcrawl.telemetry.protocol import CallbackSink

# page handle is any to avoid playwright dependency in protocol
PageHandle = Any


class BrowserEngine(Protocol):
    """browser backend interface.

    Raises:
        BrowserCrashError: when the browser process dies or becomes unresponsive.
    """

    async def browser_engine_dead(self) -> bool:
        """check if the browser process has died."""
        ...

    async def start_browser_engine(self, headless: bool = True) -> None: ...

    async def stop_browser_engine(self) -> None: ...

    def is_engine_ready(self) -> bool: ...

    def set_sink(self, sink: CallbackSink) -> None: ...

    async def open_new_crawl_context(
        self,
        experiment_id: str = "default",
        crawl_session_id: str = "",
        crawled_url: str = "",
        auto_accept_cookies: bool = True,
    ) -> PageHandle: ...

    async def close_crawl_context(self, page: PageHandle) -> None: ...

    async def navigate_to_url(
        self,
        url: str,
        page: PageHandle = None,
        crawl_depth: int = 0,
    ) -> Optional[List[str]]:
        """navigate to url and return same-domain links."""
        ...

    async def execute_scrolling_sequence(
        self,
        scroll_steps: List[int],
        page: PageHandle = None,
    ) -> None:
        """perform a series of scroll actions on the page."""
        ...

    async def wait_on_page(self, duration_seconds: float, page: PageHandle) -> None:
        """wait for a duration while keeping the page alive.

        Unlike asyncio.sleep, this must detect browser crashes during the wait.
        """
        ...
