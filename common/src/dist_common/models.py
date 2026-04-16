"""pydantic models for worker and scripts."""

from datetime import datetime
from typing import List, Literal

from pydantic import BaseModel, Field


class ExperimentParams(BaseModel):
    """crawl configuration parameters."""

    auto_accept_cookies: bool = True
    navigate_subpages: bool = False
    max_depth: int = 0
    dwell_time: int = 30
    scroll_amounts: List[int] = Field(
        default_factory=list
    )  # sequence of vertical scroll steps in pixels (e.g. [500, 500]).
    browser_type: Literal["chromium", "firefox", "webkit"] = "chromium"
    headless: bool = False


class ExperimentMetadata(BaseModel):
    """metadata stored in object store."""

    id: str
    timestamp: datetime
    total_urls: int
    params: ExperimentParams


class CrawlTask(ExperimentParams):
    """crawl task sent via nats."""

    url: str
    experiment_id: str


class NodeInfo(BaseModel):
    """Metadata of the worker node."""

    country_code: str = Field(alias="country_code")
    is_residential: bool = Field(alias="is_residential")
    browser_type: str = Field(alias="browser_type")
    is_headless: bool = Field(alias="is_headless")
