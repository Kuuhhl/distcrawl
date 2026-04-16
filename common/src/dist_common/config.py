"""shared infrastructure configuration."""

from pathlib import Path
from typing import Literal, Optional, Union

from obstore.store import LocalStore, MemoryStore, S3Store
from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ObjectStore = Union[S3Store, MemoryStore, LocalStore]


class BaseCrawlSettings(BaseSettings):
    """infra config from env vars or .env."""

    logging_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO", validation_alias="LOGGING_LEVEL"
    )
    nats_url: str = Field(
        default="ws://localhost:8080", validation_alias="NATS_URL"
    )  # infra urls
    nats_token: Optional[str] = Field(
        default=None, validation_alias="NATS_TOKEN"
    )  # token auth

    stream_name: str = Field(
        default="CRAWL", validation_alias="NATS_STREAM"
    )  # nats jetstream
    results_bucket: str = Field(
        default="crawl-results", validation_alias="RESULTS_BUCKET_NAME"
    )
    subject_prefix: str = Field(
        default="crawl.urls", validation_alias="NATS_SUBJECT_PREFIX"
    )
    seed_publish_batch_size: int = Field(  # operational knobs
        default=100, validation_alias="SEED_PUBLISH_BATCH_SIZE"
    )
    nats_connect_timeout: float = Field(
        default=5.0, validation_alias="NATS_CONNECT_TIMEOUT"
    )
    nats_reconnect_wait: float = Field(
        default=2.0, validation_alias="NATS_RECONNECT_WAIT"
    )
    queue_retry_delay_seconds: float = Field(
        default=5.0, validation_alias="QUEUE_RETRY_DELAY_SECONDS"
    )
    nats_ack_wait_seconds: int = Field(
        default=45, validation_alias="NATS_ACK_WAIT_SECONDS"
    )
    nats_heartbeat_seconds: float = Field(
        default=15.0, validation_alias="HEARTBEAT_SECONDS"
    )
    nats_max_retries: int = Field(default=3, validation_alias="MAX_RETRIES")

    s3_endpoint_url: Optional[str] = Field(
        default=None, validation_alias="S3_ENDPOINT_URL"
    )
    s3_access_key: Optional[str] = Field(default=None, validation_alias="S3_ACCESS_KEY")
    s3_secret_key: Optional[str] = Field(default=None, validation_alias="S3_SECRET_KEY")

    storage_type: Literal["s3", "memory", "local"] = Field(
        default="memory", validation_alias="STORAGE_TYPE"
    )
    local_storage_path: str = Field(
        default="/tmp/crawler_results", validation_alias="LOCAL_STORAGE_PATH"
    )

    @model_validator(mode="after")
    def validate_s3_creds(self):
        """if an endpoint is provided, keys should also be provided."""
        if self.s3_endpoint_url:
            if not self.s3_access_key or not self.s3_secret_key:
                raise ValueError("S3_ENDPOINT_URL is set, but keys are missing.")
        return self

    @property
    def use_s3(self) -> bool:
        """determine storage backend."""
        return self.storage_type == "s3"

    @property
    def subject_pattern(self) -> str:
        """pattern for matching all subjects under the prefix."""
        return f"{self.subject_prefix}.>"

    def get_storage(self) -> ObjectStore:
        """create an object store for storing results."""
        if self.storage_type == "s3":
            return S3Store(
                self.results_bucket,
                access_key_id=self.s3_access_key,
                secret_access_key=self.s3_secret_key,
                endpoint=self.s3_endpoint_url,
            )
        elif self.storage_type == "local":
            Path(self.local_storage_path).mkdir(parents=True, exist_ok=True)
            return LocalStore(self.local_storage_path)
        else:
            return MemoryStore()

    model_config = SettingsConfigDict(
        env_file=Path(__file__).parent / ".env",
        extra="ignore",
        env_ignore_empty=True,
        populate_by_name=True,
    )
