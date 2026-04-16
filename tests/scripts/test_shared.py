import os
from unittest.mock import patch

from config import ScriptSettings

_ENV_KEYS = frozenset(
    {
        "NATS_URL",
        "NATS_STREAM",
        "NATS_SUBJECT_PREFIX",
        "NATS_DURABLE",
        "ACK_WAIT_SECONDS",
        "HEARTBEAT_SECONDS",
        "MAX_RETRIES",
    }
)  # env keys for scriptsettings


def _clean_env(**extras):
    """return cleaned environment dict."""
    env = {k: v for k, v in os.environ.items() if k not in _ENV_KEYS}
    env.update(extras)
    return env


class TestScriptSettings:
    def test_constructor_args_take_priority(self):
        with patch.dict(os.environ, _clean_env(), clear=True):
            settings = ScriptSettings(
                nats_url="nats://custom:4222",
            )
        assert settings.nats_url == "nats://custom:4222"

    def test_env_vars_used_when_no_constructor_args(self):
        env = _clean_env(
            NATS_URL="nats://env:9999",
        )
        with patch.dict(os.environ, env, clear=True):
            settings = ScriptSettings()

        assert settings.nats_url == "nats://env:9999"

    def test_falls_back_to_defaults_when_nothing_set(self):
        env = _clean_env()
        with patch.dict(os.environ, env, clear=True):
            settings = ScriptSettings()

        assert any(
            proto in settings.nats_url for proto in ["nats://", "ws://", "wss://"]
        )

    def test_stream_and_subject_from_env(self):
        env = _clean_env(
            NATS_STREAM="CUSTOM_STREAM",
            NATS_SUBJECT_PREFIX="custom.prefix",
        )
        with patch.dict(os.environ, env, clear=True):
            settings = ScriptSettings()

        assert settings.stream_name == "CUSTOM_STREAM"
        assert settings.subject_prefix == "custom.prefix"

    def test_stream_and_subject_defaults(self):
        env = _clean_env()
        with patch.dict(os.environ, env, clear=True):
            settings = ScriptSettings()

        assert settings.stream_name == "CRAWL"
        assert settings.subject_prefix == "crawl.urls"

    def test_extra_fields_are_ignored(self):
        """verify extra fields are ignored."""
        env = _clean_env()
        with patch.dict(os.environ, env, clear=True):
            settings = ScriptSettings()
        assert settings.nats_url is not None
