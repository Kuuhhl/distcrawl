"""worker configuration."""

from typing import Literal
from urllib.parse import urlparse, urlunparse

from dist_common.config import BaseCrawlSettings
from pydantic import Field, field_validator


class WorkerSettings(BaseCrawlSettings):
    """worker settings from env."""

    enable_otel: bool = Field(default=False, validation_alias="ENABLE_OTEL")
    otel_collector_endpoint: str = Field(
        default="http://localhost:4318", validation_alias="OTEL_ENDPOINT"
    )
    otel_auth_header: str = Field(default="", validation_alias="OTEL_AUTH_HEADER")

    @field_validator("otel_auth_header", mode="before")
    @classmethod
    def _strip_otel_auth_header(cls, value):
        if isinstance(value, str):
            return value.strip().strip("\"'")
        return value

    def _otlp_endpoint(self, signal_path: str) -> str:
        """append a signal-specific path to the configured otlp base url."""
        parsed = urlparse(self.otel_collector_endpoint)
        path = parsed.path.rstrip("/") + "/" + signal_path
        return urlunparse(parsed._replace(path=path))

    @property
    def otel_logs_endpoint(self) -> str:
        return self._otlp_endpoint("v1/logs")

    @property
    def otel_traces_endpoint(self) -> str:
        return self._otlp_endpoint("v1/traces")

    @property
    def otel_metrics_endpoint(self) -> str:
        return self._otlp_endpoint("v1/metrics")

    num_crawlers: int = Field(default=2, validation_alias="NUM_CRAWLERS")
    persistence_batch_size: int = Field(
        default=1000, validation_alias="PERSISTENCE_BATCH_SIZE"
    )
    fetch_timeout_seconds: float = Field(default=5.0, validation_alias="FETCH_TIMEOUT")
    flush_threshold: int = Field(default=10, validation_alias="FLUSH_THRESHOLD")
    idle_sleep_seconds: float = Field(default=5.0, validation_alias="IDLE_SLEEP")

    browser_type: Literal["chromium", "firefox", "webkit"] = Field(
        default="chromium", validation_alias="BROWSER_TYPE"
    )
    headless: bool = Field(default=False, validation_alias="HEADLESS")
    goto_timeout_ms: int = Field(default=30000, validation_alias="GOTO_TIMEOUT_MS")

    @property
    def subject_pattern(self) -> str:
        """pattern for matching lane-based routing."""
        mode = "headless" if self.headless else "headed"
        # Pattern: crawl.urls.<browser>.<mode>.>
        return f"{self.subject_prefix}.{self.browser_type}.{mode}.>"

    @property
    def consumer_name(self) -> str:
        """stable consumer name so all workers in the same category share one consumer."""
        mode = "headless" if self.headless else "headed"
        return f"{self.stream_name}_{self.browser_type}_{mode}"

    scroll_delay_seconds: float = Field(
        default=0.2, validation_alias="SCROLL_DELAY_SECONDS"
    )
    watchdog_timeout_seconds: float = Field(
        default=600.0, validation_alias="WATCHDOG_TIMEOUT_SECONDS"
    )
    only_allow_residential_connections: bool = Field(
        default=False, validation_alias="ONLY_ALLOW_RESIDENTIAL_CONNECTIONS"
    )
