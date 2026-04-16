import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from distcrawl.crawl.errors import BrowserCrashError
from distcrawl.engine.playwright import SUPPORTED_BROWSERS, PlaywrightEngine
from playwright.async_api import Error as PlaywrightError


def _make_mock_page():
    page = MagicMock()
    page.on = MagicMock()
    page.evaluate = AsyncMock(return_value=[])
    page.goto = AsyncMock()
    page.wait_for_load_state = AsyncMock()
    page.close = AsyncMock()
    page.mouse = MagicMock()
    page.mouse.wheel = AsyncMock()
    page.context = MagicMock()
    page.context.close = AsyncMock()
    page.context.add_init_script = AsyncMock()
    page.context.expose_binding = AsyncMock()
    return page


def _make_mock_context(page=None):
    ctx = MagicMock()
    page = page or _make_mock_page()
    ctx.new_page = AsyncMock(return_value=page)
    ctx.add_init_script = AsyncMock()
    ctx.expose_binding = AsyncMock()
    ctx.close = AsyncMock()
    page.context = ctx
    return ctx, page


def _make_mock_browser():
    ctx, page = _make_mock_context()
    browser = MagicMock()
    browser.new_context = AsyncMock(return_value=ctx)
    browser.close = AsyncMock()
    return browser, ctx, page


@pytest.fixture
def mock_config():
    cfg = MagicMock()
    cfg.browser_type = "chromium"
    cfg.num_crawlers = 10
    cfg.goto_timeout_ms = 30000
    cfg.scroll_delay_seconds = 0.1
    return cfg


class TestInit:
    def test_valid_browser_type(self):
        for bt in SUPPORTED_BROWSERS:
            cfg = MagicMock()
            cfg.browser_type = bt
            engine = PlaywrightEngine(config=cfg)
            assert engine.config.browser_type == bt

    def test_invalid_browser_type_raises(self):
        cfg = MagicMock()
        cfg.browser_type = "netscape"
        with pytest.raises(ValueError, match="Unsupported browser type"):
            PlaywrightEngine(config=cfg)

    def test_default_values(self, mock_config):
        engine = PlaywrightEngine(config=mock_config)
        assert engine.config.num_crawlers == 10
        assert engine.config.browser_type == "chromium"
        assert engine.browser is None
        assert engine.pw is None

    def test_custom_num_crawlers(self):
        cfg = MagicMock()
        cfg.num_crawlers = 3
        cfg.browser_type = "chromium"
        engine = PlaywrightEngine(config=cfg)
        assert engine.config.num_crawlers == 3


class TestIsReady:
    def test_not_ready_before_start(self, mock_config):
        engine = PlaywrightEngine(config=mock_config)
        assert engine.is_engine_ready() is False

    def test_ready_after_browser_set(self, mock_config):
        engine = PlaywrightEngine(config=mock_config)
        engine.browser = MagicMock()
        assert engine.is_engine_ready() is True


class TestStart:
    @pytest.mark.asyncio
    async def test_start_launches_browser(self, mock_config):
        engine = PlaywrightEngine(config=mock_config)

        mock_browser = MagicMock()
        mock_launcher = MagicMock()
        mock_launcher.launch = AsyncMock(return_value=mock_browser)

        mock_pw_instance = MagicMock()
        mock_pw_instance.chromium = mock_launcher

        mock_pw_ctx = MagicMock()
        mock_pw_ctx.start = AsyncMock(return_value=mock_pw_instance)

        with patch(
            "distcrawl.engine.playwright.async_playwright",
            return_value=mock_pw_ctx,
        ):
            await engine.start_browser_engine(headless=True)

        assert engine.browser is mock_browser
        assert engine.pw is mock_pw_instance
        mock_launcher.launch.assert_awaited_once_with(headless=True)

    @pytest.mark.asyncio
    async def test_start_is_idempotent(self, mock_config):
        engine = PlaywrightEngine(config=mock_config)
        engine.browser = MagicMock()  # already started

        with patch("distcrawl.engine.playwright.async_playwright") as mock_pw:
            await engine.start_browser_engine()
            mock_pw.assert_not_called()


class TestStop:
    @pytest.mark.asyncio
    async def test_stop_closes_browser_and_pw(self, mock_config):
        engine = PlaywrightEngine(config=mock_config)
        engine.browser = MagicMock()
        engine.browser.close = AsyncMock()
        engine.pw = MagicMock()
        engine.pw.stop = AsyncMock()

        await engine.stop_browser_engine()

        engine.browser.close.assert_awaited_once()
        engine.pw.stop.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_stop_handles_none(self, mock_config):
        engine = PlaywrightEngine(config=mock_config)
        # should not raise
        await engine.stop_browser_engine()


class TestSetSink:
    def test_wires_handlers(self, mock_config):
        engine = PlaywrightEngine(config=mock_config)
        sink = MagicMock()
        sink.on_cookie_accept = AsyncMock()
        sink.on_request = AsyncMock()
        sink.on_response = AsyncMock()

        engine.set_sink(sink)

        assert engine._on_cookie_accept_callback is sink.on_cookie_accept
        assert engine._on_request_callback is sink.on_request
        assert engine._on_response_callback is sink.on_response


class TestNewPage:
    @pytest.mark.asyncio
    async def test_creates_context_and_page(self, mock_config):
        browser, ctx, page = _make_mock_browser()
        engine = PlaywrightEngine(config=mock_config)
        engine.browser = browser

        result = await engine.open_new_crawl_context(
            experiment_id="exp1",
            crawl_session_id="sess1",
            crawled_url="http://example.com",
        )

        assert result is page
        browser.new_context.assert_awaited_once()
        ctx.new_page.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_registers_request_and_response_handlers(self, mock_config):
        browser, ctx, page = _make_mock_browser()
        engine = PlaywrightEngine(config=mock_config)
        engine.browser = browser

        await engine.open_new_crawl_context()

        # page.on should be called for "request" and "response"
        event_names = [call[0][0] for call in page.on.call_args_list]
        assert "request" in event_names
        assert "response" in event_names

    @pytest.mark.asyncio
    async def test_injects_init_script_and_exposes_binding(self, mock_config):
        browser, ctx, page = _make_mock_browser()
        engine = PlaywrightEngine(config=mock_config)
        engine.browser = browser

        await engine.open_new_crawl_context(auto_accept_cookies=True)

        ctx.add_init_script.assert_awaited_once()
        ctx.expose_binding.assert_awaited_once()
        # first arg to expose_binding should be the function name
        assert ctx.expose_binding.call_args[0][0] == "handleCookieAccept"

    @pytest.mark.asyncio
    async def test_does_not_inject_init_script_if_disabled(self, mock_config):
        browser, ctx, page = _make_mock_browser()
        engine = PlaywrightEngine(config=mock_config)
        engine.browser = browser

        await engine.open_new_crawl_context(auto_accept_cookies=False)

        ctx.add_init_script.assert_not_called()
        ctx.expose_binding.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_raises_if_browser_not_started(self, mock_config):
        engine = PlaywrightEngine(config=mock_config)

        with pytest.raises(RuntimeError, match="Browser engine not started"):
            await engine.open_new_crawl_context()


class TestClosePage:
    @pytest.mark.asyncio
    async def test_closes_page_and_context(self, mock_config):
        page = _make_mock_page()
        page.close = AsyncMock()
        page.context.close = AsyncMock()

        engine = PlaywrightEngine(config=mock_config)
        await engine.close_crawl_context(page)

        page.close.assert_awaited_once()
        page.context.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_swallows_exceptions(self, mock_config):
        page = _make_mock_page()
        page.close = AsyncMock(side_effect=RuntimeError("boom"))

        engine = PlaywrightEngine(config=mock_config)
        # should not raise
        await engine.close_crawl_context(page)


class TestPerformScroll:
    @pytest.mark.asyncio
    async def test_scrolls_each_amount(self, mock_config):
        page = _make_mock_page()
        mock_config.scroll_delay_seconds = 0
        engine = PlaywrightEngine(config=mock_config)

        await engine.execute_scrolling_sequence([100, 200, 300], page=page)

        assert page.mouse.wheel.await_count == 3
        page.mouse.wheel.assert_any_await(0, 100)
        page.mouse.wheel.assert_any_await(0, 200)
        page.mouse.wheel.assert_any_await(0, 300)

    @pytest.mark.asyncio
    async def test_noop_without_page(self, mock_config):
        engine = PlaywrightEngine(config=mock_config)
        # should not raise
        await engine.execute_scrolling_sequence([100], page=None)

    @pytest.mark.asyncio
    async def test_stops_on_exception(self, mock_config):
        page = _make_mock_page()
        page.mouse.wheel = AsyncMock(side_effect=RuntimeError("detached"))

        mock_config.scroll_delay_seconds = 0
        engine = PlaywrightEngine(config=mock_config)
        await engine.execute_scrolling_sequence([100, 200], page=page)

        # first one attempted then stopped
        assert page.mouse.wheel.await_count == 1


class TestGoto:
    @pytest.mark.asyncio
    async def test_returns_none_without_page(self, mock_config):
        engine = PlaywrightEngine(config=mock_config)
        result = await engine.navigate_to_url("http://example.com", page=None)
        assert result is None

    @pytest.mark.asyncio
    async def test_navigates_and_returns_links(self, mock_config):
        page = _make_mock_page()
        page.evaluate = AsyncMock(
            return_value=[
                "http://example.com/a",
                "http://example.com/b",
                "http://example.com/a",
            ]
        )
        page.goto = AsyncMock()

        mock_config.goto_timeout_ms = 1000
        engine = PlaywrightEngine(config=mock_config)
        links = await engine.navigate_to_url("http://example.com", page=page)

        page.goto.assert_awaited_once()
        assert links is not None
        # links are deduplicated
        assert set(links) == {"http://example.com/a", "http://example.com/b"}

    @pytest.mark.asyncio
    async def test_returns_none_on_navigation_error(self, mock_config):
        page = _make_mock_page()
        page.evaluate = AsyncMock(return_value=None)
        page.goto = AsyncMock(side_effect=TimeoutError("timed out"))

        mock_config.goto_timeout_ms = 1000
        engine = PlaywrightEngine(config=mock_config)
        result = await engine.navigate_to_url("http://bad.example", page=page)
        assert result is None


class TestRequestHandler:
    @pytest.mark.asyncio
    async def test_forwards_request_to_sink(self, mock_config):
        engine = PlaywrightEngine(config=mock_config)
        handler = AsyncMock()
        engine._on_request_callback = handler

        mock_request = MagicMock()
        mock_request.url = "http://example.com/script.js"
        mock_request.frame = MagicMock()
        mock_request.frame.url = "http://example.com"
        mock_request.resource_type = "script"
        mock_request.method = "GET"
        mock_request.headers = {"accept": "*/*"}

        await engine._dispatch_request_event(
            mock_request,
            "http://example.com/page1",
            "exp1",
            "sess1",
            "http://example.com",
            2,
        )

        handler.assert_awaited_once()
        event = handler.call_args[0][0]
        assert event["url"] == "http://example.com/script.js"
        assert event["current_page_url"] == "http://example.com/page1"
        assert event["experiment_id"] == "exp1"
        assert event["crawl_session_id"] == "sess1"
        assert event["crawled_url"] == "http://example.com"
        assert event["headers"] == '{"accept": "*/*"}'
        assert event["crawl_depth"] == 2
        assert "request_id" in event

    @pytest.mark.asyncio
    async def test_handles_missing_frame(self, mock_config):
        engine = PlaywrightEngine(config=mock_config)
        handler = AsyncMock()
        engine._on_request_callback = handler

        mock_request = MagicMock()
        mock_request.url = "http://example.com"
        mock_request.frame = None
        mock_request.resource_type = "document"
        mock_request.method = "GET"
        mock_request.headers = {}

        await engine._dispatch_request_event(
            mock_request, "http://example.com", "e", "s", "u", 0
        )

        event = handler.call_args[0][0]
        assert event["frame_url"] == ""
        assert event["headers"] == "{}"
        assert event["crawl_depth"] == 0

    @pytest.mark.asyncio
    async def test_noop_without_handler(self, mock_config):
        engine = PlaywrightEngine(config=mock_config)
        engine._on_request_callback = None

        # should not raise
        await engine._dispatch_request_event(
            MagicMock(), "http://example.com", "e", "s", "u", 0
        )

    @pytest.mark.asyncio
    async def test_swallows_handler_exception(self, mock_config):
        engine = PlaywrightEngine(config=mock_config)
        engine._on_request_callback = AsyncMock(side_effect=RuntimeError("oops"))

        mock_request = MagicMock()
        mock_request.url = "http://example.com"
        mock_request.frame = MagicMock()
        mock_request.frame.url = "http://example.com"
        mock_request.resource_type = "document"
        mock_request.method = "GET"
        mock_request.headers = {}

        # should not raise
        await engine._dispatch_request_event(
            mock_request, "http://example.com", "e", "s", "u", 0
        )


class TestResponseHandler:
    @pytest.mark.asyncio
    async def test_forwards_response_to_sink(self, mock_config):
        engine = PlaywrightEngine(config=mock_config)
        handler = AsyncMock()
        engine._on_response_callback = handler

        mock_response = MagicMock()
        mock_response.url = "http://example.com/page"
        mock_response.status = 200
        mock_response.headers = {"content-type": "text/html"}
        mock_response.request.timing = {"startTime": 100.0, "responseStart": 789.0}
        mock_response.header_values = AsyncMock(
            return_value=[
                "session_id=abc123xyz789_deadbeef456; Domain=.example.com; Path=/; Expires=Sun, 30 Mar 2025 00:00:00 GMT; HttpOnly; Secure; SameSite=Lax"
            ]
        )

        await engine._dispatch_response_event(
            mock_response, "exp1", "sess1", "http://example.com", 1
        )

        handler.assert_awaited_once()
        event = handler.call_args[0][0]
        assert event["status"] == 200
        assert event["experiment_id"] == "exp1"
        assert event["headers"] == '{"content-type": "text/html"}'
        assert event["crawl_depth"] == 1
        assert "request_id" in event
        assert "response_id" not in event
        assert event["timestamp"] == "889.0"

    @pytest.mark.asyncio
    async def test_noop_without_handler(self, mock_config):
        engine = PlaywrightEngine(config=mock_config)
        engine._on_response_callback = None
        await engine._dispatch_response_event(MagicMock(), "e", "s", "u", 0)

    @pytest.mark.asyncio
    async def test_swallows_handler_exception(self, mock_config):
        engine = PlaywrightEngine(config=mock_config)
        engine._on_response_callback = AsyncMock(side_effect=RuntimeError("oops"))

        mock_response = MagicMock()
        mock_response.url = "http://example.com"
        mock_response.status = 500
        mock_response.headers = {}
        mock_response.request.timing = {}
        mock_response.header_values = AsyncMock(return_value=[])

        await engine._dispatch_response_event(mock_response, "e", "s", "u", 0)


class TestCookieAcceptHandler:
    @pytest.mark.asyncio
    async def test_fires_event_to_sink(self, mock_config):
        engine = PlaywrightEngine(config=mock_config)
        handler = AsyncMock()
        engine._on_cookie_accept_callback = handler

        await engine._handle_cookie_accept_event(
            "http://example.com",
            "2024-01-01T00:00:00Z",
            "exp1",
            "sess1",
            "http://example.com",
            3,
        )

        handler.assert_awaited_once()
        event = handler.call_args[0][0]
        assert event["url"] == "http://example.com"
        assert event["timestamp"] == "2024-01-01T00:00:00Z"
        assert event["crawl_depth"] == 3

    @pytest.mark.asyncio
    async def test_noop_without_handler(self, mock_config):
        engine = PlaywrightEngine(config=mock_config)
        engine._on_cookie_accept_callback = None
        # should not raise
        await engine._handle_cookie_accept_event("u", "t", "e", "s", "c", 0)


def _make_dead_browser(**kwargs):
    """Create a browser mock whose new_context raises (simulating a crashed browser)."""
    browser = MagicMock(**kwargs)
    browser.new_context = AsyncMock(side_effect=Exception("browser crashed"))
    return browser


def _make_healthy_browser(**kwargs):
    """Create a browser mock whose new_context succeeds (health check passes)."""
    browser = MagicMock(**kwargs)
    mock_ctx = MagicMock()
    mock_ctx.close = AsyncMock()
    browser.new_context = AsyncMock(return_value=mock_ctx)
    return browser


class TestBrowserCrashDetection:
    @pytest.mark.asyncio
    async def test_disconnected_browser_during_navigation_raises_browser_crash(
        self, mock_config
    ):
        """Any error with a dead browser must become BrowserCrashError."""
        page = _make_mock_page()
        page.goto = AsyncMock(side_effect=PlaywrightError("Target closed"))

        mock_config.goto_timeout_ms = 1000
        engine = PlaywrightEngine(config=mock_config)
        engine.browser = _make_dead_browser()

        with pytest.raises(BrowserCrashError):
            await engine.navigate_to_url("http://example.com", page=page)

    @pytest.mark.asyncio
    async def test_disconnected_browser_during_context_creation_raises_browser_crash(
        self, mock_config
    ):
        """Error during new_page with a dead browser must become BrowserCrashError."""
        ctx = MagicMock()
        ctx.new_page = AsyncMock(side_effect=PlaywrightError("Target closed"))
        ctx.add_init_script = AsyncMock()
        ctx.close = AsyncMock()

        # First new_context call (in open_new_crawl_context) succeeds,
        # second call (health check in decorator) raises -> dead browser.
        browser = MagicMock()
        browser.new_context = AsyncMock(
            side_effect=[ctx, Exception("browser crashed")]
        )

        engine = PlaywrightEngine(config=mock_config)
        engine.browser = browser

        with pytest.raises(BrowserCrashError):
            await engine.open_new_crawl_context()

        ctx.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_connection_closed_while_reading_raises_browser_crash(
        self, mock_config
    ):
        """Reproduces production incident: 'Connection closed while reading from
        the driver' during new_context must become BrowserCrashError, not silently
        ack the task."""
        browser = MagicMock()
        browser.new_context = AsyncMock(
            side_effect=Exception(
                "Browser.new_context: Connection closed while reading from the driver"
            )
        )

        engine = PlaywrightEngine(config=mock_config)
        engine.browser = browser

        with pytest.raises(BrowserCrashError, match="Connection closed"):
            await engine.open_new_crawl_context()

    @pytest.mark.asyncio
    async def test_connected_browser_playwright_error_propagates_normally(
        self, mock_config
    ):
        """PlaywrightError with a still-connected browser should not become
        BrowserCrashError -- it is a regular page-level failure."""
        page = _make_mock_page()
        page.goto = AsyncMock(
            side_effect=PlaywrightError("net::ERR_NAME_NOT_RESOLVED")
        )

        mock_config.goto_timeout_ms = 1000
        engine = PlaywrightEngine(config=mock_config)
        engine.browser = _make_healthy_browser()
        result = await engine.navigate_to_url("http://bad.example", page=page)

        assert result is None

    @pytest.mark.asyncio
    async def test_normal_navigation_error_returns_none(self, mock_config):
        """Regular website errors should return None, not raise."""
        page = _make_mock_page()
        page.goto = AsyncMock(side_effect=RuntimeError("net::ERR_NAME_NOT_RESOLVED"))

        mock_config.goto_timeout_ms = 1000
        engine = PlaywrightEngine(config=mock_config)
        engine.browser = _make_healthy_browser()
        result = await engine.navigate_to_url("http://bad.example", page=page)

        assert result is None

    @pytest.mark.asyncio
    async def test_disconnected_browser_during_scroll_raises_browser_crash(
        self, mock_config
    ):
        """Scroll errors with a disconnected browser must become BrowserCrashError."""
        page = _make_mock_page()
        page.mouse.wheel = AsyncMock(side_effect=PlaywrightError("Target closed"))

        mock_config.scroll_delay_seconds = 0
        engine = PlaywrightEngine(config=mock_config)
        engine.browser = _make_dead_browser()

        with pytest.raises(BrowserCrashError):
            await engine.execute_scrolling_sequence([100], page=page)

    @pytest.mark.asyncio
    async def test_crash_during_wait_on_page_raises_browser_crash(
        self, mock_config
    ):
        """wait_on_page with a dead browser must raise BrowserCrashError."""
        page = _make_mock_page()
        page.wait_for_timeout = AsyncMock(side_effect=PlaywrightError("Target closed"))

        engine = PlaywrightEngine(config=mock_config)
        engine.browser = _make_dead_browser()

        with pytest.raises(BrowserCrashError):
            await engine.wait_on_page(5, page=page)

    @pytest.mark.asyncio
    async def test_crash_during_page_closure_detects_dead_browser(
        self, mock_config
    ):
        """close_crawl_context swallows exceptions internally, but when the
        browser is dead the error is logged and the method completes without raising."""
        page = _make_mock_page()
        page.close = AsyncMock(side_effect=PlaywrightError("Target closed"))
        page.context.close = AsyncMock()

        engine = PlaywrightEngine(config=mock_config)
        engine.browser = _make_dead_browser()

        # close_crawl_context catches all exceptions internally and should not raise.
        await engine.close_crawl_context(page)  # should not raise

    @pytest.mark.asyncio
    async def test_crash_detected_via_is_connected_false(self, mock_config):
        """browser_engine_dead should return True when is_connected() is False."""
        browser = MagicMock()
        browser.is_connected = MagicMock(return_value=False)

        engine = PlaywrightEngine(config=mock_config)
        engine.browser = browser

        result = await engine.browser_engine_dead()
        assert result is True

    @pytest.mark.asyncio
    async def test_crash_detected_via_probe_context_timeout(self, mock_config):
        """browser_engine_dead should return True when new_context() hangs past 2s."""
        browser = MagicMock()
        browser.is_connected = MagicMock(return_value=True)

        async def _hang_forever(*args, **kwargs):
            await asyncio.sleep(999)

        browser.new_context = AsyncMock(side_effect=_hang_forever)

        engine = PlaywrightEngine(config=mock_config)
        engine.browser = browser

        result = await engine.browser_engine_dead()
        assert result is True
