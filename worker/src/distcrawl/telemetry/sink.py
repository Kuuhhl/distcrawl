"""telemetry data sink."""

from dist_common import (
    CookieAcceptEvent,
    RequestEvent,
    ResponseEvent,
    SiteMetadataEvent,
    NodeInfo,
)
from distcrawl.telemetry.protocol import CallbackSink, DataBatcher


class TelemetrySink(CallbackSink):
    """sink for browser telemetry events."""

    def __init__(
        self, batcher: DataBatcher, worker_id: str, node_info: NodeInfo
    ) -> None:
        self.batcher = batcher
        self.worker_id = worker_id
        self.node_info = node_info

    async def on_cookie_accept(self, event_data: CookieAcceptEvent) -> None:
        exp_id = event_data.get("experiment_id", "default")
        await self.batcher.append(exp_id, "cookie_warning_consents", event_data)

    async def on_request(self, event_data: RequestEvent) -> None:
        event_data["worker_id"] = self.worker_id
        exp_id = event_data.get("experiment_id", "default")
        await self.batcher.append(exp_id, "requests", event_data)

    async def on_response(self, event_data: ResponseEvent) -> None:
        exp_id = event_data.get("experiment_id", "default")
        await self.batcher.append(exp_id, "responses", event_data)

    async def on_site_metadata(
        self, event_data: SiteMetadataEvent, exp_id: str
    ) -> None:
        await self.batcher.append(exp_id, "site_metadata", event_data)

    async def flush(self) -> None:
        await self.batcher.flush_all()
