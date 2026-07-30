"""fatal error types for the crawl subsystem."""


class BrowserCrashError(Exception):
    """raised when the browser process has died and cannot recover."""


class TelemetryIncompleteError(Exception):
    """raised when telemetry can not be recorded completely."""
