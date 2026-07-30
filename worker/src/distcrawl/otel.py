"""open telemetry bootstrap."""

import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from opentelemetry import metrics, trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.metrics import Counter, UpDownCounter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from dist_common import NodeInfo
from distcrawl.config import WorkerSettings


@dataclass
class DistCrawlMetrics:
    """holds the open telemetry instruments used by the worker."""

    workers_active: UpDownCounter
    tasks_in_flight: UpDownCounter
    tasks_acked: Counter
    worker_attributes: Dict[str, Any]

    def worker_active(self, delta: int) -> None:
        self.workers_active.add(delta, self.worker_attributes)

    def task_started(self) -> None:
        self.tasks_in_flight.add(1, self.worker_attributes)

    def task_finished(self) -> None:
        self.tasks_in_flight.add(-1, self.worker_attributes)

    def task_acked(self, experiment_id: str, crawl_success: bool) -> None:
        self.tasks_acked.add(
            1,
            {
                **self.worker_attributes,
                "experiment.id": experiment_id,
                "crawl.success": crawl_success,
            },
        )


def setup_otel(
    worker_id: str, node_info: NodeInfo, settings: WorkerSettings
) -> Tuple[
    Optional[LoggerProvider],
    Optional[TracerProvider],
    Optional[MeterProvider],
    Optional[DistCrawlMetrics],
]:
    if not settings.enable_otel:
        return None, None, None, None

    resource = Resource.create(
        {
            "service.name": "distcrawl-worker",
            "worker.id": worker_id,
            "worker.country_code": node_info.country_code,
            "worker.browser_type": node_info.browser_type,
            "worker.headless": node_info.is_headless,
            "worker.is_residential": node_info.is_residential,
        }
    )

    worker_attributes = {
        "worker.country_code": node_info.country_code,
        "worker.browser_type": node_info.browser_type,
        "worker.headless": node_info.is_headless,
    }

    headers = {}
    if settings.otel_auth_header:
        headers["Authorization"] = settings.otel_auth_header

    # logs
    logger_provider = LoggerProvider(resource=resource)
    set_logger_provider(logger_provider)
    logger_provider.add_log_record_processor(
        BatchLogRecordProcessor(
            OTLPLogExporter(endpoint=settings.otel_logs_endpoint, headers=headers)
        )
    )
    logging.getLogger().addHandler(
        LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)
    )

    # traces
    tracer_provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(tracer_provider)
    tracer_provider.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(endpoint=settings.otel_traces_endpoint, headers=headers)
        )
    )

    # metrics
    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[
            PeriodicExportingMetricReader(
                OTLPMetricExporter(
                    endpoint=settings.otel_metrics_endpoint, headers=headers
                )
            )
        ],
    )
    metrics.set_meter_provider(meter_provider)

    meter = metrics.get_meter("distcrawl-worker")
    dist_metrics = DistCrawlMetrics(
        workers_active=meter.create_up_down_counter(
            "distcrawl.workers.active",
            unit="1",
            description="Number of active crawl workers.",
        ),
        tasks_in_flight=meter.create_up_down_counter(
            "distcrawl.crawl.tasks.in_flight",
            unit="1",
            description="Number of crawl tasks currently being processed.",
        ),
        tasks_acked=meter.create_counter(
            "distcrawl.crawl.tasks.acked",
            unit="1",
            description="Number of crawl tasks acknowledged after processing.",
        ),
        worker_attributes=worker_attributes,
    )

    return logger_provider, tracer_provider, meter_provider, dist_metrics


def shutdown_otel(
    logger_provider: Optional[LoggerProvider],
    tracer_provider: Optional[TracerProvider],
    meter_provider: Optional[MeterProvider],
) -> None:
    if logger_provider is not None:
        logger_provider.shutdown()
    if tracer_provider is not None:
        tracer_provider.shutdown()
    if meter_provider is not None:
        meter_provider.shutdown()
