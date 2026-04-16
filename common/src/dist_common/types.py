"""sink event types."""

from typing import TypedDict


class NodeInfo(TypedDict):
    """Metadata of the worker node."""

    country_code: str
    is_residential: bool
    browser_type: str
    is_headless: bool


class CookieAcceptEvent(TypedDict):
    """fired when cookie consent is clicked."""

    experiment_id: str
    crawl_session_id: str
    crawled_url: str
    url: str
    timestamp: str
    crawl_depth: int


class RequestEvent(TypedDict):
    """recorded browser request."""

    experiment_id: str
    request_id: str
    worker_id: str
    crawl_session_id: str
    crawled_url: str
    current_page_url: str
    url: str
    frame_url: str
    resource_type: str
    method: str
    headers: str
    timestamp: str
    crawl_depth: int


class ResponseEvent(TypedDict):
    """recorded browser response."""

    experiment_id: str
    request_id: str
    crawl_session_id: str
    crawled_url: str
    url: str
    status: int
    headers: str
    timestamp: str
    cookies: str
    crawl_depth: int


class SiteMetadataEvent(TypedDict):
    """website metadata."""

    crawl_session_id: str
    timestamp: str
    description: str
