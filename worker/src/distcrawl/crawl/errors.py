"""fatal error types for the crawl subsystem."""


class BrowserCrashError(Exception):
    """raised when the browser process has died and cannot recover."""
