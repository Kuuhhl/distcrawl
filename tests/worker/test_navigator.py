from unittest.mock import AsyncMock, MagicMock

import pytest
from distcrawl.crawl.errors import BrowserCrashError
from distcrawl.crawl.navigator import CrawlNavigator


def _make_mock_engine(goto_links: list[list[str]] | None = None) -> MagicMock:
    """create mock browser engine."""
    engine = MagicMock()
    engine.config = MagicMock()
    engine.config.goto_timeout_ms = 30000
    engine.open_new_crawl_context = AsyncMock(return_value=MagicMock(name="page"))
    engine.close_crawl_context = AsyncMock()
    engine.execute_scrolling_sequence = AsyncMock()
    engine.wait_on_page = AsyncMock()

    if goto_links is None:
        engine.navigate_to_url = AsyncMock(return_value=[])
    else:
        engine.navigate_to_url = AsyncMock(side_effect=goto_links)

    return engine


class TestBasicExecution:
    @pytest.mark.asyncio
    async def test_successful_crawl(self, crawl_task_factory):
        engine = _make_mock_engine()
        nav = CrawlNavigator(engine=engine)
        task = crawl_task_factory()

        result = await nav.execute(task)

        assert result is True
        engine.open_new_crawl_context.assert_awaited_once()
        call_kwargs = engine.open_new_crawl_context.call_args[1]
        assert call_kwargs["experiment_id"] == "test_exp"
        assert call_kwargs["crawled_url"] == "https://www.wikipedia.org"
        # 32-char hex uuid
        assert len(call_kwargs["crawl_session_id"]) == 32

        engine.navigate_to_url.assert_awaited_once_with(
            "https://www.wikipedia.org",
            page=engine.open_new_crawl_context.return_value,
            crawl_depth=0,
        )
        engine.close_crawl_context.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_page_is_closed_on_success(self, crawl_task_factory):
        engine = _make_mock_engine()
        nav = CrawlNavigator(engine=engine)
        task = crawl_task_factory()

        await nav.execute(task)

        engine.close_crawl_context.assert_awaited_once_with(
            engine.open_new_crawl_context.return_value
        )

    @pytest.mark.asyncio
    async def test_page_is_closed_on_failure(self, crawl_task_factory):
        engine = _make_mock_engine()
        engine.navigate_to_url = AsyncMock(side_effect=RuntimeError("nav failed"))
        nav = CrawlNavigator(engine=engine)
        task = crawl_task_factory()

        result = await nav.execute(task)

        assert result is False
        engine.close_crawl_context.assert_awaited_once_with(
            engine.open_new_crawl_context.return_value
        )

    @pytest.mark.asyncio
    async def test_returns_false_on_goto_exception(self, crawl_task_factory):
        engine = _make_mock_engine()
        engine.navigate_to_url = AsyncMock(side_effect=RuntimeError("boom"))
        nav = CrawlNavigator(engine=engine)
        task = crawl_task_factory()

        result = await nav.execute(task)

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_on_new_page_exception(self, crawl_task_factory):
        engine = _make_mock_engine()
        engine.open_new_crawl_context = AsyncMock(
            side_effect=RuntimeError("no browser")
        )
        nav = CrawlNavigator(engine=engine)
        task = crawl_task_factory()

        result = await nav.execute(task)

        assert result is False

    @pytest.mark.asyncio
    async def test_close_page_not_called_when_page_never_created(
        self, crawl_task_factory
    ):
        engine = _make_mock_engine()
        engine.open_new_crawl_context = AsyncMock(
            side_effect=RuntimeError("no browser")
        )
        nav = CrawlNavigator(engine=engine)
        task = crawl_task_factory()

        await nav.execute(task)

        engine.close_crawl_context.assert_not_awaited()


class TestCookieAcceptance:
    @pytest.mark.asyncio
    async def test_auto_accept_cookies_passed_to_new_page(self, crawl_task_factory):
        engine = _make_mock_engine()
        nav = CrawlNavigator(engine=engine)

        # test true
        task_true = crawl_task_factory(auto_accept_cookies=True)
        await nav.execute(task_true)
        assert engine.open_new_crawl_context.call_args[1]["auto_accept_cookies"] is True

        # test false
        engine.open_new_crawl_context.reset_mock()
        task_false = crawl_task_factory(auto_accept_cookies=False)
        await nav.execute(task_false)
        assert (
            engine.open_new_crawl_context.call_args[1]["auto_accept_cookies"] is False
        )


class TestDwellTime:
    @pytest.mark.asyncio
    async def test_dwell_time_waits_on_page(self, crawl_task_factory):
        engine = _make_mock_engine()
        nav = CrawlNavigator(engine=engine)
        task = crawl_task_factory(dwell_time=5)

        await nav.execute(task)
        engine.wait_on_page.assert_awaited_once_with(
            5, page=engine.open_new_crawl_context.return_value
        )

    @pytest.mark.asyncio
    async def test_zero_dwell_time_does_not_wait(self, crawl_task_factory):
        engine = _make_mock_engine()
        nav = CrawlNavigator(engine=engine)
        task = crawl_task_factory(dwell_time=0)

        await nav.execute(task)
        engine.wait_on_page.assert_not_awaited()


class TestScrolling:
    @pytest.mark.asyncio
    async def test_scroll_amounts_delegated(self, crawl_task_factory):
        engine = _make_mock_engine()
        nav = CrawlNavigator(engine=engine)
        task = crawl_task_factory(scroll_amounts=[500, 300])

        await nav.execute(task)

        engine.execute_scrolling_sequence.assert_awaited_once_with(
            [500, 300],
            page=engine.open_new_crawl_context.return_value,
        )

    @pytest.mark.asyncio
    async def test_empty_scroll_amounts_skips_scroll(self, crawl_task_factory):
        engine = _make_mock_engine()
        nav = CrawlNavigator(engine=engine)
        task = crawl_task_factory(scroll_amounts=[])

        await nav.execute(task)

        engine.execute_scrolling_sequence.assert_not_awaited()


class TestSubpageNavigation:
    @pytest.mark.asyncio
    async def test_no_subpage_navigation_when_disabled(self, crawl_task_factory):
        engine = _make_mock_engine(
            goto_links=[
                ["https://www.wikipedia.org/page1", "https://www.wikipedia.org/page2"],
            ]
        )
        nav = CrawlNavigator(engine=engine)
        task = crawl_task_factory(navigate_subpages=False, max_depth=3)

        await nav.execute(task)

        assert engine.navigate_to_url.await_count == 1

    @pytest.mark.asyncio
    async def test_subpage_navigation_visits_one_link(self, crawl_task_factory):
        """test visit root then one random subpage."""
        engine = _make_mock_engine(
            goto_links=[
                ["https://www.wikipedia.org/sub1"],  # links from root
                [],  # links from sub1
            ]
        )
        nav = CrawlNavigator(engine=engine)
        task = crawl_task_factory(navigate_subpages=True, max_depth=2)

        result = await nav.execute(task)

        assert result is True
        assert engine.navigate_to_url.await_count == 2

        first_url = engine.navigate_to_url.call_args_list[0][0][0]
        assert first_url == "https://www.wikipedia.org"

        second_url = engine.navigate_to_url.call_args_list[1][0][0]
        assert second_url == "https://www.wikipedia.org/sub1"

    @pytest.mark.asyncio
    async def test_depth_limits_navigation(self, crawl_task_factory):
        """test max_depth limit on link following."""
        engine = _make_mock_engine(
            goto_links=[
                ["https://www.wikipedia.org/sub1"],  # depth 0
                ["https://www.wikipedia.org/sub2"],  # depth 1
            ]
        )
        nav = CrawlNavigator(engine=engine)
        task = crawl_task_factory(navigate_subpages=True, max_depth=1)

        await nav.execute(task)

        assert engine.navigate_to_url.await_count == 2

    @pytest.mark.asyncio
    async def test_no_duplicate_url_visits(self, crawl_task_factory):
        """avoid revisit root if subpage links back."""
        engine = _make_mock_engine(
            goto_links=[
                ["https://www.wikipedia.org"],  # circular link
            ]
        )
        nav = CrawlNavigator(engine=engine)
        task = crawl_task_factory(navigate_subpages=True, max_depth=3)

        await nav.execute(task)

        assert engine.navigate_to_url.await_count == 1

    @pytest.mark.asyncio
    async def test_no_subpage_when_goto_returns_empty_links(self, crawl_task_factory):
        engine = _make_mock_engine(goto_links=[[]])
        nav = CrawlNavigator(engine=engine)
        task = crawl_task_factory(navigate_subpages=True, max_depth=5)

        await nav.execute(task)

        assert engine.navigate_to_url.await_count == 1

    @pytest.mark.asyncio
    async def test_subpage_dwell_and_scroll_on_each_page(self, crawl_task_factory):
        """dwell and scroll on every page."""
        engine = _make_mock_engine(
            goto_links=[
                ["https://www.wikipedia.org/sub1"],
                [],
            ]
        )
        nav = CrawlNavigator(engine=engine)
        task = crawl_task_factory(
            navigate_subpages=True,
            max_depth=2,
            dwell_time=1,
            scroll_amounts=[100],
        )

        await nav.execute(task)

        assert engine.wait_on_page.await_count == 2
        assert engine.execute_scrolling_sequence.await_count == 2


class TestExperimentId:
    @pytest.mark.asyncio
    async def test_experiment_id_passed_to_new_page(self, crawl_task_factory):
        engine = _make_mock_engine()
        nav = CrawlNavigator(engine=engine)
        task = crawl_task_factory(experiment_id="my_custom_exp")

        await nav.execute(task)

        call_kwargs = engine.open_new_crawl_context.call_args[1]
        assert call_kwargs["experiment_id"] == "my_custom_exp"


class TestCrawlSessionId:
    @pytest.mark.asyncio
    async def test_crawl_session_id_is_unique_per_execution(self, crawl_task_factory):
        """fresh session id per execution."""
        engine = _make_mock_engine()
        nav = CrawlNavigator(engine=engine)
        task = crawl_task_factory()

        await nav.execute(task)
        first_session = engine.open_new_crawl_context.call_args[1]["crawl_session_id"]

        engine.open_new_crawl_context.reset_mock()
        await nav.execute(task)
        second_session = engine.open_new_crawl_context.call_args[1]["crawl_session_id"]

        assert first_session != second_session

    @pytest.mark.asyncio
    async def test_crawled_url_matches_task_url(self, crawl_task_factory):
        engine = _make_mock_engine()
        nav = CrawlNavigator(engine=engine)
        task = crawl_task_factory(url="https://test.org/page")

        await nav.execute(task)

        call_kwargs = engine.open_new_crawl_context.call_args[1]
        assert call_kwargs["crawled_url"] == "https://test.org/page"


class TestEdgeCases:
    @pytest.mark.asyncio
    async def test_close_page_exception_does_not_hide_success(self, crawl_task_factory):
        """true result even if close_page fails."""
        engine = _make_mock_engine()
        engine.close_crawl_context = AsyncMock(side_effect=RuntimeError("close failed"))
        nav = CrawlNavigator(engine=engine)
        task = crawl_task_factory()

        result = await nav.execute(task)

        assert result is True
        engine.close_crawl_context.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_scroll_exception_does_not_crash_crawl(self, crawl_task_factory):
        """return false gracefully if scroll fails."""
        engine = _make_mock_engine()
        engine.execute_scrolling_sequence = AsyncMock(
            side_effect=RuntimeError("scroll failed")
        )
        nav = CrawlNavigator(engine=engine)
        task = crawl_task_factory(scroll_amounts=[100])

        result = await nav.execute(task)

        assert result is False
        engine.close_crawl_context.assert_awaited_once()


class TestBrowserCrashDetection:
    @pytest.mark.asyncio
    async def test_browser_crash_during_navigation_propagates(self, crawl_task_factory):
        """BrowserCrashError from engine must propagate through navigator."""
        engine = _make_mock_engine()
        engine.navigate_to_url = AsyncMock(
            side_effect=BrowserCrashError("browser has been closed")
        )
        nav = CrawlNavigator(engine=engine)
        task = crawl_task_factory()

        with pytest.raises(BrowserCrashError):
            await nav.execute(task)

        engine.close_crawl_context.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_normal_error_returns_false(self, crawl_task_factory):
        """regular website errors should return False, not raise."""
        engine = _make_mock_engine()
        engine.navigate_to_url = AsyncMock(
            side_effect=RuntimeError("net::ERR_NAME_NOT_RESOLVED")
        )
        nav = CrawlNavigator(engine=engine)
        task = crawl_task_factory()

        result = await nav.execute(task)

        assert result is False

    @pytest.mark.asyncio
    async def test_browser_crash_during_context_creation_propagates(
        self, crawl_task_factory
    ):
        """BrowserCrashError during open_new_crawl_context must propagate."""
        engine = _make_mock_engine()
        engine.open_new_crawl_context = AsyncMock(
            side_effect=BrowserCrashError("browser has been closed")
        )
        nav = CrawlNavigator(engine=engine)
        task = crawl_task_factory()

        with pytest.raises(BrowserCrashError):
            await nav.execute(task)

        # page was never created, so close should not be called
        engine.close_crawl_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_browser_crash_during_dwell_propagates(self, crawl_task_factory):
        """BrowserCrashError from wait_on_page must propagate through navigator,
        and page should be closed in the finally block."""
        engine = _make_mock_engine()
        engine.wait_on_page = AsyncMock(
            side_effect=BrowserCrashError("browser died during dwell")
        )
        nav = CrawlNavigator(engine=engine)
        task = crawl_task_factory(dwell_time=5)

        with pytest.raises(BrowserCrashError):
            await nav.execute(task)

        engine.close_crawl_context.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_browser_crash_during_scroll_propagates(self, crawl_task_factory):
        """BrowserCrashError from execute_scrolling_sequence must propagate."""
        engine = _make_mock_engine()
        engine.execute_scrolling_sequence = AsyncMock(
            side_effect=BrowserCrashError("browser died during scroll")
        )
        nav = CrawlNavigator(engine=engine)
        task = crawl_task_factory(scroll_amounts=[100, 200])

        with pytest.raises(BrowserCrashError):
            await nav.execute(task)

        engine.close_crawl_context.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_browser_crash_during_subpage_propagates(self, crawl_task_factory):
        """BrowserCrashError on second navigate_to_url call (subpage) still
        propagates through the navigator."""
        engine = _make_mock_engine(
            goto_links=[
                ["https://www.wikipedia.org/sub1"],  # links from root
            ]
        )
        # First call succeeds, second call (subpage) crashes
        engine.navigate_to_url = AsyncMock(
            side_effect=[
                ["https://www.wikipedia.org/sub1"],  # root navigation
                BrowserCrashError("browser died on subpage"),  # subpage crash
            ]
        )
        nav = CrawlNavigator(engine=engine)
        task = crawl_task_factory(navigate_subpages=True, max_depth=2)

        with pytest.raises(BrowserCrashError):
            await nav.execute(task)

        engine.close_crawl_context.assert_awaited_once()



class TestDeterministicLinkSelection:
    @pytest.mark.asyncio
    async def test_same_links_produce_same_choice(self, crawl_task_factory):
        """same url and links must always pick the same subpage."""
        links = [
            "https://www.wikipedia.org/a",
            "https://www.wikipedia.org/b",
            "https://www.wikipedia.org/c",
        ]

        chosen = set()
        for _ in range(5):
            engine = _make_mock_engine(goto_links=[links, []])
            nav = CrawlNavigator(engine=engine)
            task = crawl_task_factory(navigate_subpages=True, max_depth=2)
            await nav.execute(task)
            second_url = engine.navigate_to_url.call_args_list[1][0][0]
            chosen.add(second_url)

        assert len(chosen) == 1, f"Expected deterministic choice, got {chosen}"

    @pytest.mark.asyncio
    async def test_different_seeds_can_differ(self, crawl_task_factory):
        """different seed urls should (generally) pick different subpages."""
        links = [f"https://example.com/{i}" for i in range(50)]
        choices = []
        for seed_url in [
            "https://site-a.com",
            "https://site-b.com",
            "https://site-c.com",
        ]:
            engine = _make_mock_engine(goto_links=[links, []])
            nav = CrawlNavigator(engine=engine)
            task = crawl_task_factory(
                url=seed_url, navigate_subpages=True, max_depth=2
            )
            await nav.execute(task)
            choices.append(engine.navigate_to_url.call_args_list[1][0][0])

        # with 50 links and 3 seeds, extremely unlikely all three match
        assert len(set(choices)) > 1
