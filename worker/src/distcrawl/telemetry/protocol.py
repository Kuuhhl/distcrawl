"""telemetry and data batching protocols."""

from typing import Any, Mapping, Protocol

from dist_common import CookieAcceptEvent, RequestEvent, ResponseEvent, SiteMetadataEvent


class DataBatcher(Protocol):
    """batches records and flushes to storage."""

    async def append(
        self, exp_id: str, data_type: str, item: Mapping[str, Any]
    ) -> None: ...

    async def flush_buffer(self, exp_id: str, data_type: str) -> None: ...

    async def flush_all(self) -> None: ...


class CallbackSink(Protocol):
    """callback sink interface for browser events."""

    async def on_cookie_accept(self, event_data: CookieAcceptEvent) -> None: ...

    async def on_request(self, event_data: RequestEvent) -> None: ...

    async def on_response(self, event_data: ResponseEvent) -> None: ...

    async def on_site_metadata(
        self, event_data: SiteMetadataEvent, exp_id: str
    ) -> None: ...

    async def flush(self) -> None: ...
