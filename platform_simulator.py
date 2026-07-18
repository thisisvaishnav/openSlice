#!/usr/bin/env python3
"""
platform_simulator.py

A 7-service e-commerce backend simulator that generates realistic OpenTelemetry
traces and metrics for SigNoz. Each service runs in its own thread with its own
TracerProvider / MeterProvider (the standard OTel separation pattern).

Run it standalone:
    python platform_simulator.py

Optional flags:
    --interval SECONDS   How often each service fires a request (default 0.3)
    --rate               Requests per second per service (overrides --interval)
    --once               Run a single request per service, then exit
    --endpoint URL       OTLP HTTP endpoint (default http://localhost:4318)
    --no-chaos           Disable all anomaly/incident injection

Design rationale:
    - Each service gets its own TracerProvider and MeterProvider because SigNoz
      (and most OTel backends) uses the resource's service.name to group spans
      and metrics into separate service entities. Sharing providers would merge
      them into one.
    - Span context is passed from api-gateway -> downstream: the gateway creates
      a root span and injects it into an HTTP-style headers dict, which each
      child service extracts as its parent. This models real distributed tracing.
    - Services communicate via "in-memory HTTP simulation" (dict pass-through)
      to avoid real network dependencies while keeping the OTel propagation path
      realistic.
"""

from __future__ import annotations

import argparse
import random
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Optional

from opentelemetry import trace, metrics
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.propagate import inject, extract
from opentelemetry.trace import SpanKind, Status, StatusCode

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SERVICES = [
    "api-gateway",
    "auth-service",
    "product-catalog",
    "cart-service",
    "order-service",
    "payment-service",
    "notification",
]

STATUS_INTERVAL = 10  # seconds between terminal-table refreshes

# ---------------------------------------------------------------------------
# Incident definitions for payment-service
# ---------------------------------------------------------------------------

INCIDENT_NONE = "HEALTHY"


@dataclass
class IncidentState:
    active: bool = False
    incident_type: str = INCIDENT_NONE
    cooldown_until: float = 0.0  # time.time() before which no new incident starts
    duration: float = 0.0
    started_at: float = 0.0

    def description(self) -> str:
        if not self.active:
            return "[payment-service] HEALTHY"
        return f"[payment-service] INCIDENT: {self.incident_type}"


@dataclass
class IncidentScenario:
    name: str
    duration_min: float
    duration_max: float
    cooldown_min: float
    cooldown_max: float


INCIDENTS = [
    IncidentScenario("DEGRADED", 30, 90, 60, 120),
    IncidentScenario("PARTIAL_OUTAGE", 20, 40, 60, 120),
    IncidentScenario("PROVIDER_FAILURE", 30, 60, 60, 120),
    IncidentScenario("MEMORY_PRESSURE", 45, 45, 60, 120),
]

# ---------------------------------------------------------------------------
# Rolling statistics tracker
# ---------------------------------------------------------------------------


class RollingStats:
    """
    Tracks the last N request results per service for the live status table
    and the error-rate gauge metric.
    """
    def __init__(self, window: int = 10):
        self.window = window
        self.records: deque = deque(maxlen=window)
        self._lock = threading.Lock()

    def record(self, latency_ms: float, is_error: bool) -> None:
        with self._lock:
            self.records.append((latency_ms, is_error))

    def snapshot(self) -> dict:
        with self._lock:
            total = len(self.records)
            if total == 0:
                return {"err_count": 0, "total": 0, "p99": 0.0, "avg_latency": 0.0}
            err_count = sum(1 for _, err in self.records if err)
            latencies = sorted(l for l, _ in self.records)
            p99_idx = max(0, int(0.99 * total) - 1)
            return {
                "err_count": err_count,
                "total": total,
                "p99": latencies[p99_idx],
                "avg_latency": sum(latencies) / total,
            }


# ---------------------------------------------------------------------------
# Helpers for "in-memory HTTP" propagation
# ---------------------------------------------------------------------------

def make_headers() -> dict:
    return {}


def inject_context(headers: dict) -> dict:
    inject(headers)
    return headers


def extract_context(headers: dict) -> object:
    return extract(headers)


# ---------------------------------------------------------------------------
# The heart of every service: a thread that generates spans + metrics
# ---------------------------------------------------------------------------

class ServiceThread(threading.Thread):
    """
    A single service in the e-commerce topology.

    Each ServiceThread owns its own TracerProvider, MeterProvider, and Resource.
    This is intentional: in a real deployment each service is a separate process,
    so they MUST NOT share providers. OTel resources carry the service.name that
    SigNoz uses to separate services in its UI.

    The thread loops: sleeps for an interval, then simulates processing one
    request. The api-gateway also calls downstream services via _handle_downstream,
    passing span context through in-memory headers so the resulting spans form
    a trace waterfall.
    """

    def __init__(
        self,
        name: str,
        downstream: list[ServiceThread],
        endpoint: str,
        interval: float,
        chaos_enabled: bool,
        stats: dict[str, RollingStats],
        incident_state: IncidentState,
        stop_event: threading.Event,
    ):
        super().__init__(name=name, daemon=True)
        self.service_name = name
        self.downstream = downstream
        self.interval = interval
        self.chaos_enabled = chaos_enabled
        self.stats = stats
        self.incident_state = incident_state
        self.stop_event = stop_event

        # Each service gets its own Resource with a unique service.name.
        # This is the primary dimension SigNoz uses to separate services.
        self.resource = Resource.create({
            "service.name": self.service_name,
            "service.version": "1.0.0",
            "deployment.environment": "production",
        })

        # --- Own TracerProvider + Tracer ---
        trace_exporter = OTLPSpanExporter(endpoint=f"{endpoint}/v1/traces")
        span_processor = BatchSpanProcessor(trace_exporter)
        self.tracer_provider = TracerProvider(resource=self.resource)
        self.tracer_provider.add_span_processor(span_processor)
        self.tracer = self.tracer_provider.get_tracer(self.service_name)

        # --- Own MeterProvider + instruments ---
        metric_exporter = OTLPMetricExporter(endpoint=f"{endpoint}/v1/metrics")
        metric_reader = PeriodicExportingMetricReader(metric_exporter)
        self.meter_provider = MeterProvider(
            resource=self.resource,
            metric_readers=[metric_reader],
        )
        self.meter = self.meter_provider.get_meter(self.service_name)

        self.request_counter = self.meter.create_counter(
            name=f"{self.service_name}.requests",
            description=f"Total requests handled by {self.service_name}",
            unit="1",
        )
        self.error_counter = self.meter.create_counter(
            name=f"{self.service_name}.errors",
            description=f"Total errors in {self.service_name}",
            unit="1",
        )
        self.latency_histogram = self.meter.create_histogram(
            name=f"{self.service_name}.latency",
            description=f"Request latency for {self.service_name}",
            unit="ms",
        )

        self.request_count = 0
        self.error_count = 0

    def run(self) -> None:
        while not self.stop_event.is_set():
            if self.service_name == "api-gateway":
                self._handle_as_gateway()
            else:
                self._handle_as_leaf()
            time.sleep(self.interval)

    # ------------------------------------------------------------------
    # Simulation methods
    # ------------------------------------------------------------------

    def _get_baseline_latency(self) -> tuple[float, float, Optional[float]]:
        """
        Return (latency_ms, error_rate, cold_start_multiplier).

        Checks for active incidents, baseline variance, and cold starts.
        Cold-start multiplier is applied by callers so the span can set the
        "cold_start" attribute at the right time.
        """
        latency: float = 0.0
        error_rate: float = 0.0

        if self.service_name == "api-gateway":
            latency = random.uniform(10, 30)
            error_rate = 0.02
        elif self.service_name == "auth-service":
            latency = random.uniform(20, 60)
            error_rate = 0.03
        elif self.service_name == "product-catalog":
            latency = random.uniform(30, 80)
            error_rate = 0.02
        elif self.service_name == "cart-service":
            latency = random.uniform(30, 70)
            error_rate = 0.03
        elif self.service_name == "order-service":
            latency = random.uniform(50, 120)
            error_rate = 0.04
        elif self.service_name == "payment-service":
            latency = random.uniform(200, 600)
            error_rate = 0.05
        elif self.service_name == "notification":
            latency = random.uniform(50, 100)
            error_rate = 0.01

        if self.service_name == "payment-service" and self.chaos_enabled:
            latency, error_rate = self._apply_payment_incident(latency, error_rate)

        if self.service_name == "order-service" and self.chaos_enabled:
            latency, error_rate = self._apply_cascading_failure(latency, error_rate)

        # Cold-start spike: 2.5% chance, multiplies latency by 3-5x
        cold_start_multiplier: Optional[float] = None
        if random.random() < 0.025:
            cold_start_multiplier = random.uniform(3.0, 5.0)

        # product-catalog: 1% chance of a "full table scan" (high latency)
        if self.service_name == "product-catalog" and random.random() < 0.01:
            latency = random.uniform(800, 1200)

        return latency, error_rate, cold_start_multiplier

    def _apply_payment_incident(
        self, baseline_latency: float, baseline_error_rate: float
    ) -> tuple[float, float]:
        """
        Modify payment-service's latency/error-rate based on the active incident.
        For PROVIDER_FAILURE the actual error decision is made in the span handler
        (stripe vs non-stripe), so this always returns 0 error rate.
        """
        inc = self.incident_state
        now = time.time()

        if not inc.active:
            if inc.cooldown_until == 0.0:
                # Wait 8 seconds before first incident so SigNoz sees healthy baseline
                inc.cooldown_until = now + 8.0
                return baseline_latency, baseline_error_rate

            if now >= inc.cooldown_until:
                scenario = random.choice(INCIDENTS)
                inc.active = True
                inc.incident_type = scenario.name
                inc.duration = random.uniform(scenario.duration_min, scenario.duration_max)
                inc.started_at = now
                print(inc.description())
                return baseline_latency, baseline_error_rate

            return baseline_latency, baseline_error_rate

        elapsed = now - inc.started_at

        if elapsed >= inc.duration:
            inc.active = False
            inc.incident_type = INCIDENT_NONE
            cooldown = random.uniform(60.0, 120.0)
            inc.cooldown_until = now + cooldown
            print("[payment-service] HEALTHY (incident recovered)")
            return baseline_latency, baseline_error_rate

        if inc.incident_type == "DEGRADED":
            return random.uniform(2000, 5000), 0.30

        elif inc.incident_type == "PARTIAL_OUTAGE":
            if random.random() < 0.60:
                return baseline_latency, 1.0
            return baseline_latency, baseline_error_rate

        elif inc.incident_type == "PROVIDER_FAILURE":
            # Error handling is done in the span handler via _is_stripe_request()
            # so we return 0 error rate here.
            return baseline_latency, 0.0

        elif inc.incident_type == "MEMORY_PRESSURE":
            progress = min(elapsed / 45.0, 1.0)
            ramp_latency = baseline_latency + (3000.0 - baseline_latency) * progress
            return ramp_latency, baseline_error_rate

        return baseline_latency, baseline_error_rate

    def _apply_cascading_failure(
        self, baseline_latency: float, baseline_error_rate: float
    ) -> tuple[float, float]:
        """
        When payment-service is in PARTIAL_OUTAGE or PROVIDER_FAILURE,
        order-service also degrades because orders can't complete without payment.
        """
        inc = self.incident_state
        if inc.active and inc.incident_type in ("PARTIAL_OUTAGE", "PROVIDER_FAILURE"):
            latency = baseline_latency * random.uniform(1.5, 2.5)
            error_rate = 0.20
            return latency, error_rate
        return baseline_latency, baseline_error_rate

    def _is_stripe_request(self) -> bool:
        return random.random() < 0.50

    # ------------------------------------------------------------------
    # Span handlers
    # ------------------------------------------------------------------

    def _handle_as_gateway(self) -> None:
        """
        api-gateway creates a root span, simulates some processing, then
        calls each downstream service in sequence. The downstream calls
        pass span context via headers so they appear as child spans.
        """
        latency, error_rate, cold_start_mult = self._get_baseline_latency()
        start = time.perf_counter()

        with self.tracer.start_as_current_span(
            f"{self.service_name}/handle_request",
            kind=SpanKind.SERVER,
        ) as gateway_span:
            gateway_span.set_attribute("service.name", self.service_name)

            if cold_start_mult is not None:
                latency *= cold_start_mult
                gateway_span.set_attribute("cold_start", True)

            is_error = random.random() < error_rate

            # Gateway does ~30% of its latency as local processing before fanning out
            simulated_work(latency * 0.3)

            for downstream_service in self.downstream:
                headers = make_headers()
                inject_context(headers)
                downstream_service._handle_downstream(headers)

            total_latency = (time.perf_counter() - start) * 1000

            if is_error:
                gateway_span.set_status(Status(StatusCode.ERROR, "upstream timeout"))
            else:
                gateway_span.set_status(Status(StatusCode.OK))

        self._record_and_emit(total_latency, is_error, cold_start_mult is not None)

    def _handle_as_leaf(self) -> None:
        """
        Leaf services process work without downstream calls.
        In a real system they'd be invoked via queue or gRPC; here they
        create their own root spans.
        """
        latency, error_rate, cold_start_mult = self._get_baseline_latency()
        start = time.perf_counter()

        with self.tracer.start_as_current_span(
            f"{self.service_name}/process",
            kind=SpanKind.SERVER,
        ) as span:
            span.set_attribute("service.name", self.service_name)

            if cold_start_mult is not None:
                latency *= cold_start_mult
                span.set_attribute("cold_start", True)

            self._apply_span_attributes(span, latency)

            is_error = random.random() < error_rate
            is_error = self._apply_provider_failure_override(is_error, span)

            simulated_work(latency)
            total_latency = (time.perf_counter() - start) * 1000

            if is_error:
                span.set_status(Status(StatusCode.ERROR, "internal error"))
            else:
                span.set_status(Status(StatusCode.OK))

        self._record_and_emit(total_latency, is_error, cold_start_mult is not None)

    def _handle_downstream(self, parent_headers: dict) -> None:
        """
        Called by an upstream service (e.g., gateway -> order, order -> payment).
        The parent span context is extracted from headers so this span is a child
        in the trace waterfall.
        """
        ctx = extract_context(parent_headers)
        latency, error_rate, cold_start_mult = self._get_baseline_latency()
        start = time.perf_counter()

        with self.tracer.start_as_current_span(
            f"{self.service_name}/process",
            context=ctx,
            kind=SpanKind.SERVER,
        ) as span:
            span.set_attribute("service.name", self.service_name)

            if cold_start_mult is not None:
                latency *= cold_start_mult
                span.set_attribute("cold_start", True)

            self._apply_span_attributes(span, latency)

            is_error = random.random() < error_rate
            is_error = self._apply_provider_failure_override(is_error, span)

            # order-service fans out to payment-service (and payment -> notification)
            if self.service_name == "order-service":
                for svc in self.downstream:
                    child_headers = make_headers()
                    inject_context(child_headers)
                    svc._handle_downstream(child_headers)

            simulated_work(latency)
            total_latency = (time.perf_counter() - start) * 1000

            if is_error:
                span.set_status(Status(StatusCode.ERROR, f"error in {self.service_name}"))
            else:
                span.set_status(Status(StatusCode.OK))

        self._record_and_emit(total_latency, is_error, cold_start_mult is not None)

    def _apply_span_attributes(self, span, latency: float) -> None:
        """
        Set service-specific span attributes for visibility in SigNoz.
        Separated from the main handler to avoid duplication across
        _handle_as_leaf and _handle_downstream.
        """
        if self.service_name == "payment-service" and self.incident_state.active:
            span.set_attribute("incident.type", self.incident_state.incident_type)

        if self.service_name == "order-service" and self.chaos_enabled:
            inc = self.incident_state
            if inc.active and inc.incident_type in ("PARTIAL_OUTAGE", "PROVIDER_FAILURE"):
                span.set_attribute("degraded_by", "payment-service")

        if self.service_name == "product-catalog" and latency > 800:
            span.set_attribute("query.type", "full_scan")

    def _apply_provider_failure_override(self, is_error: bool, span) -> bool:
        """
        During PROVIDER_FAILURE incidents, stripe requests always fail.
        We override the error flag and add the payment.method attribute
        so SigNoz users can filter by payment method.
        """
        if (
            self.service_name == "payment-service"
            and self.incident_state.active
            and self.incident_state.incident_type == "PROVIDER_FAILURE"
            and self._is_stripe_request()
        ):
            span.set_attribute("payment.method", "stripe")
            return True
        return is_error

    # ------------------------------------------------------------------
    # Metrics recording
    # ------------------------------------------------------------------

    def _record_and_emit(
        self, latency_ms: float, is_error: bool, was_cold: bool = False
    ) -> None:
        self.stats[self.service_name].record(latency_ms, is_error)

        self.request_count += 1
        self.request_counter.add(1)
        if is_error:
            self.error_count += 1
            self.error_counter.add(1)
        self.latency_histogram.record(latency_ms)

    def shutdown(self) -> None:
        self.tracer_provider.shutdown()
        self.meter_provider.shutdown()


# ---------------------------------------------------------------------------
# Helper: simulate blocking work
# ---------------------------------------------------------------------------

def simulated_work(latency_ms: float) -> None:
    if latency_ms > 0:
        time.sleep(latency_ms / 1000.0)


# ---------------------------------------------------------------------------
# Terminal status table — printed every STATUS_INTERVAL seconds
# ---------------------------------------------------------------------------

def print_status_table(
    service_threads: list[ServiceThread],
    stats: dict[str, RollingStats],
    incident_state: IncidentState,
) -> None:
    header = (
        f"{'Service':<20} {'State':<16} {'Req/s':<8} {'Err%':<8} {'P99 (last 10)':<15}"
    )
    sep = "-" * len(header)

    lines = [header, sep]

    for st in service_threads:
        name = st.service_name
        snap = stats[name].snapshot()

        if name == "payment-service" and incident_state.active:
            state = incident_state.incident_type
        elif (
            name == "order-service"
            and incident_state.active
            and incident_state.incident_type in ("PARTIAL_OUTAGE", "PROVIDER_FAILURE")
        ):
            state = "DEGRADED"
        else:
            state = "HEALTHY"

        req_s = snap["total"] / STATUS_INTERVAL if snap["total"] > 0 else 0.0
        err_pct = (snap["err_count"] / snap["total"] * 100) if snap["total"] > 0 else 0.0
        p99 = snap["p99"]

        lines.append(
            f"{name:<20} {state:<16} {req_s:<8.1f} {err_pct:<8.0f}% {p99:<15.0f}ms"
        )

    sys.stdout.write("\033[2J\033[H")  # Clear screen, move cursor home
    sys.stdout.write("\n".join(lines) + "\n")
    sys.stdout.flush()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="E-commerce platform simulator with OpenTelemetry tracing"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.3,
        help="Interval in seconds between requests per service (default: 0.3)",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=None,
        help="Requests per second (overrides --interval)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single request per service and exit",
    )
    parser.add_argument(
        "--endpoint",
        type=str,
        default="http://localhost:4318",
        help="OTLP HTTP endpoint (default: http://localhost:4318)",
    )
    parser.add_argument(
        "--no-chaos",
        action="store_true",
        help="Disable anomaly injection — all services run at healthy baseline",
    )

    args = parser.parse_args()

    interval = args.interval
    if args.rate is not None:
        interval = 1.0 / args.rate

    chaos_enabled = not args.no_chaos

    stats: dict[str, RollingStats] = {s: RollingStats(window=10) for s in SERVICES}
    incident_state = IncidentState()
    stop_event = threading.Event()

    # Build the service topology bottom-up so downstream refs exist.
    # Topology:
    #   api-gateway -> auth-service -> product-catalog
    #               -> cart-service
    #               -> order-service -> payment-service -> notification
    #               -> product-catalog
    notification = ServiceThread(
        name="notification",
        downstream=[],
        endpoint=args.endpoint,
        interval=interval,
        chaos_enabled=chaos_enabled,
        stats=stats,
        incident_state=incident_state,
        stop_event=stop_event,
    )

    payment = ServiceThread(
        name="payment-service",
        downstream=[notification],
        endpoint=args.endpoint,
        interval=interval,
        chaos_enabled=chaos_enabled,
        stats=stats,
        incident_state=incident_state,
        stop_event=stop_event,
    )

    product_catalog = ServiceThread(
        name="product-catalog",
        downstream=[],
        endpoint=args.endpoint,
        interval=interval,
        chaos_enabled=chaos_enabled,
        stats=stats,
        incident_state=incident_state,
        stop_event=stop_event,
    )

    auth = ServiceThread(
        name="auth-service",
        downstream=[product_catalog],
        endpoint=args.endpoint,
        interval=interval,
        chaos_enabled=chaos_enabled,
        stats=stats,
        incident_state=incident_state,
        stop_event=stop_event,
    )

    cart = ServiceThread(
        name="cart-service",
        downstream=[],
        endpoint=args.endpoint,
        interval=interval,
        chaos_enabled=chaos_enabled,
        stats=stats,
        incident_state=incident_state,
        stop_event=stop_event,
    )

    # order calls payment; payment calls notification.
    # order does NOT call notification directly — that would double-call it.
    order = ServiceThread(
        name="order-service",
        downstream=[payment],
        endpoint=args.endpoint,
        interval=interval,
        chaos_enabled=chaos_enabled,
        stats=stats,
        incident_state=incident_state,
        stop_event=stop_event,
    )

    gateway = ServiceThread(
        name="api-gateway",
        downstream=[auth, cart, order, product_catalog],
        endpoint=args.endpoint,
        interval=interval,
        chaos_enabled=chaos_enabled,
        stats=stats,
        incident_state=incident_state,
        stop_event=stop_event,
    )

    all_services = [gateway, auth, product_catalog, cart, order, payment, notification]

    # --- Set up observable gauges for rolling error rates ---
    # Each gauge reads from the RollingStats and emits a value when polled by
    # the PeriodicExportingMetricReader. This creates a <service>.error_rate
    # metric in SigNoz that can be plotted directly.
    for st in all_services:
        svc_stats = stats[st.service_name]

        def make_callback(svc_name: str, s: RollingStats):
            def callback(options):
                snap = s.snapshot()
                err_rate = snap["err_count"] / max(snap["total"], 1)
                yield metrics.Observation(err_rate, {"service.name": svc_name})
            return callback

        st.meter.create_observable_gauge(
            name=f"{st.service_name}.error_rate",
            callbacks=[make_callback(st.service_name, svc_stats)],
            description=f"Rolling error rate for {st.service_name} (last 10 requests)",
            unit="1",
        )

    if not chaos_enabled:
        print("Chaos mode DISABLED — all services run at healthy baseline")
    else:
        print(
            "Chaos mode ENABLED — payment-service will experience incidents. "
            "Watch the terminal output for state changes."
        )

    for svc in all_services:
        svc.start()

    last_status_print = time.monotonic()

    try:
        if args.once:
            time.sleep(2)
        else:
            while not stop_event.is_set():
                now = time.monotonic()
                if now - last_status_print >= STATUS_INTERVAL:
                    print_status_table(all_services, stats, incident_state)
                    last_status_print = now
                time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        stop_event.set()
        for svc in all_services:
            svc.join(timeout=3)
            svc.shutdown()


if __name__ == "__main__":
    main()
