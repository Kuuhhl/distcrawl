"""config for cli scripts."""

from dist_common import BaseCrawlSettings
from pydantic_settings import SettingsConfigDict


class ScriptSettings(BaseCrawlSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
        env_ignore_empty=True,
        populate_by_name=True,
    )
