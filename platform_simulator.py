#!/usr/bin/env python3
"""
Multi-service E-commerce Platform Simulator for SigNoz.

Simulates an e-commerce platform with 7 internal services. Each service
has its own OTel TracerProvider, MeterProvider, and Resource — the key
pattern that lets SigNoz separate services in its Services tab.

Sends traces and metrics via OTLP HTTP to SigNoz at localhost:4318.

Includes realistic anomaly injection for observability training:
- payment-service incident scenarios (DEGRADED, PARTIAL_OUTAGE, etc.)
- Cascading failure from payment-service to order-service
- Cold-start spikes and full-table-scan events
- Per-service rolling error rate gauge metric

Usage:
    pip install opentelemetry-sdk opentelemetry-exporter-otlp-proto-http
    python platform_simulator.py
    python platform_simulator.py --no-chaos   # healthy baseline only
"""

import argparse
import random
import sys
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Optional

# ── OpenTelemetry imports ──────────────────────────────────────────────
try:
    from opentelemetry import trace, metrics
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
except ImportError as e:
    print("[!] Missing OTel SDK. Install with:  pip install opentelemetry-sdk opentelemetry-exporter-otlp-proto-http")
    print(f"    Error: {e}")
    sys.exit(1)


# ── Configuration ───────────────────────────────────────────────────────

OTLP_ENDPOINT = "http://localhost:4318"
ENVIRONMENT = "staging"

SERVICES_CONFIG = {
    "api-gateway": {
        "latency_ms": (5, 30),
        "error_rate": 0.01,
    },
    "auth-service": {
        "latency_ms": (20, 80),
        "error_rate": 0.03,
    },
    "product-catalog": {
        "latency_ms": (30, 150),
        "error_rate": 0.02,
    },
    "cart-service": {
        "latency_ms": (10, 50),
        "error_rate": 0.01,
    },
    "order-service": {
        "latency_ms": (50, 200),
        "error_rate": 0.05,
    },
    "payment-service": {
        "latency_ms": (200, 1500),
        "error_rate": 0.10,
    },
    "notification": {
        "latency_ms": (30, 300),
        "error_rate": 0.04,
    },
}

USER_ACTIONS = [
    "view_homepage", "search_products", "view_product", "add_to_cart",
    "checkout", "view_orders", "update_profile",
]

PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "stripe"]
PRODUCT_IDS = [f"PROD-{i}" for i in range(1, 51)]
USER_IDS = [f"USER-{i}" for i in range(1, 101)]


# ── Incident Types ─────────────────────────────────────────────────────

class IncidentType(Enum):
    DEGRADED = "DEGRADED"
    PARTIAL_OUTAGE = "PARTIAL_OUTAGE"
    PROVIDER_FAILURE = "PROVIDER_FAILURE"
    MEMORY_PRESSURE = "MEMORY_PRESSURE"


@dataclass
class Incident:
    type: IncidentType
    started_at: float          # time.monotonic()
    duration: float            # total seconds the incident lasts
    ramp_duration: float = 45.0  # used by MEMORY_PRESSURE for linear climb


# ── Chaos Engine ────────────────────────────────────────────────────────

class ChaosEngine:
    """
    Drives payment-service incident scenarios on a background thread.

    Every 60–120 seconds a random incident type is triggered for a set
    duration.  current_state() is called per-request to check whether a
    service should deviate from its healthy baseline.

    WHY a background thread: incidents need to start and end on their own
    schedule without blocking the main request loop.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._incident: Optional[Incident] = None
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self):
        while self._running:
            time.sleep(random.uniform(60, 120))
            if not self._running:
                break

            incident_type = random.choice(list(IncidentType))
            durations = {
                IncidentType.DEGRADED: random.uniform(30, 90),
                IncidentType.PARTIAL_OUTAGE: random.uniform(20, 40),
                IncidentType.PROVIDER_FAILURE: random.uniform(30, 60),
                # 45s ramp-up + 30s peak plateau
                IncidentType.MEMORY_PRESSURE: 75.0,
            }
            incident = Incident(
                type=incident_type,
                started_at=time.monotonic(),
                duration=durations[incident_type],
            )
            with self._lock:
                self._incident = incident
            print(f"\n[payment-service] >>> INCIDENT START: {incident_type.value}")

            # Sleep in 1s chunks so we can be stopped cleanly via Ctrl-C
            deadline = time.monotonic() + incident.duration
            while time.monotonic() < deadline and self._running:
                time.sleep(1)

            with self._lock:
                self._incident = None
            print(f"\n[payment-service] <<< INCIDENT END: {incident_type.value}")

    def stop(self):
        self._running = False

    def current_state(self, service_name: str) -> dict:
        """
        Return a dict of override values for *service_name* based on the
        active incident, or an empty dict for healthy baseline.

        This is called for EVERY service on EVERY request, so it must be
        fast and not block for long.
        """
        with self._lock:
            if self._incident is None:
                return {}
            inc = self._incident
            now = time.monotonic()
            elapsed = now - inc.started_at

            if service_name == "payment-service":
                return self._payment_overrides(inc, elapsed)

            # ── Cascading failure ──
            # When payment is in PARTIAL_OUTAGE or PROVIDER_FAILURE, orders
            # cannot complete, so order-service degrades too.
            if service_name == "order-service" and inc.type in (
                IncidentType.PARTIAL_OUTAGE,
                IncidentType.PROVIDER_FAILURE,
            ):
                return {"error_rate": 0.20, "degraded_by": "payment-service"}

            return {}

    def _payment_overrides(self, inc: Incident, elapsed: float) -> dict:
        result = {"incident_type": inc.type.value}

        if inc.type == IncidentType.DEGRADED:
            result["latency_ms"] = (2000, 5000)
            result["error_rate"] = 0.30

        elif inc.type == IncidentType.PARTIAL_OUTAGE:
            result["latency_ms"] = (200, 1500)
            result["error_rate"] = 0.60
            result["error_msg"] = "Circuit breaker open"

        elif inc.type == IncidentType.PROVIDER_FAILURE:
            # Only stripe requests fail; other methods at baseline error
            result["stripe_only"] = True

        elif inc.type == IncidentType.MEMORY_PRESSURE:
            peak_latency = 3000.0
            base_latency = 350.0
            if elapsed < inc.ramp_duration:
                fraction = elapsed / inc.ramp_duration
                curr = base_latency + (peak_latency - base_latency) * fraction
                result["latency_ms"] = (curr * 0.8, curr * 1.2)
                result["error_rate"] = 0.10
            elif elapsed < inc.duration:
                result["latency_ms"] = (2500, 3500)
                result["error_rate"] = 0.15
            else:
                result["latency_ms"] = (200, 1500)
                result["error_rate"] = 0.10

        return result

    @property
    def active_incident(self) -> Optional[Incident]:
        with self._lock:
            return self._incident


# ── Service wrapper ────────────────────────────────────────────────────

class PlatformService:
    """
    A simulated microservice with its own OTel TracerProvider, MeterProvider,
    and Resource.

    WHY each service gets its own providers: SigNoz (and other OTel backends)
    group telemetry by service.name in the resource.  If all services shared
    a single provider, they'd appear as one service in the UI.
    """

    def __init__(self, name: str, config: dict, otlp_endpoint: str):
        self.name = name
        self.config = config
        self.overrides: dict = {}

        # Each service gets a unique resource so SigNoz separates them
        resource = Resource.create({
            "service.name": name,
            "service.version": "1.0.0",
            "deployment.environment": ENVIRONMENT,
            "service.instance.id": f"{name}-{uuid.uuid4().hex[:8]}",
            "telemetry.sdk.language": "python",
            "telemetry.sdk.name": "opentelemetry",
        })

        span_processor = BatchSpanProcessor(
            OTLPSpanExporter(endpoint=f"{otlp_endpoint}/v1/traces")
        )
        self.tracer_provider = TracerProvider(resource=resource)
        self.tracer_provider.add_span_processor(span_processor)

        metric_reader = PeriodicExportingMetricReader(
            OTLPMetricExporter(endpoint=f"{otlp_endpoint}/v1/metrics"),
            export_interval_millis=5000,
        )
        self.meter_provider = MeterProvider(
            resource=resource,
            metric_readers=[metric_reader],
        )

        self.tracer = self.tracer_provider.get_tracer(name)
        self.meter = self.meter_provider.get_meter(name)

        self.request_histogram = self.meter.create_histogram(
            name=f"{name}.request.duration",
            unit="ms",
        )
        self.request_counter = self.meter.create_counter(
            name=f"{name}.request.count",
            unit="1",
        )
        self.error_counter = self.meter.create_counter(
            name=f"{name}.error.count",
            unit="1",
        )
        self.active_requests = self.meter.create_up_down_counter(
            name=f"{name}.requests.active",
            unit="1",
        )

        # Rolling windows for error rate gauge.
        # We track the last 10 results so we can compute a stable rate
        # without heavy storage.
        self._recent_results: deque[bool] = deque(maxlen=10)
        self._recent_latencies: deque[float] = deque(maxlen=10)
        self.error_rate_gauge = self.meter.create_gauge(
            name=f"{name}.error_rate",
            description=f"Rolling error rate (last 10 requests) for {name}",
            unit="1",
        )

    def set_overrides(self, overrides: dict):
        self.overrides = overrides

    def _get_effective_config(self):
        """
        Merge healthy-baseline config with current chaos overrides.

        Returns (latency_range, error_rate, extra_span_attrs).
        """
        latency_range = self.config["latency_ms"]
        error_rate = self.config["error_rate"]
        extra_attrs = {}

        if self.overrides:
            if "latency_ms" in self.overrides:
                latency_range = self.overrides["latency_ms"]
            if "error_rate" in self.overrides:
                error_rate = self.overrides["error_rate"]
            if "incident_type" in self.overrides:
                extra_attrs["incident.type"] = self.overrides["incident_type"]
            if "degraded_by" in self.overrides:
                extra_attrs["degraded_by"] = self.overrides["degraded_by"]

        return latency_range, error_rate, extra_attrs

    def simulate_request(
        self,
        user_id: str,
        action: str,
        parent_context: Optional[object] = None,
    ) -> tuple[bool, float]:
        """
        Simulate one request through this service.

        Returns (was_error, latency_ms) so the orchestrator can track
        per-service health for the live status table.
        """
        latency_range, error_rate, extra_attrs = self._get_effective_config()
        latency_ms = random.uniform(*latency_range)

        # ── Realistic baseline variance ──

        # Cold-start spike: 2.5% chance, 3-5x normal latency, no error.
        # This mimics a container starting up or a cache miss on first request.
        if random.random() < 0.025:
            latency_ms *= random.uniform(3.0, 5.0)
            extra_attrs["cold_start"] = "true"

        # Full table scan: product-catalog only, 1% chance.
        # Simulates a query that skips the index.
        if self.name == "product-catalog" and random.random() < 0.01:
            latency_ms = random.uniform(800, 1200)
            extra_attrs["query.type"] = "full_scan"

        # ── Span attributes ──
        attrs = {
            "user.id": user_id,
            "user.action": action,
            "service.name": self.name,
        }

        if self.name == "payment-service":
            attrs["payment.method"] = random.choice(PAYMENT_METHODS)
        if self.name in ("product-catalog", "cart-service"):
            attrs["product.id"] = random.choice(PRODUCT_IDS)

        attrs.update(extra_attrs)

        # ── Error determination ──
        is_error = random.random() < error_rate

        # PROVIDER_FAILURE: stripe always fails, other methods are fine.
        if self.overrides.get("stripe_only"):
            pm = attrs.get("payment.method")
            if pm == "stripe":
                is_error = True

        if is_error:
            latency_ms *= random.uniform(1.3, 2.0)

        # ── Create / continue the trace span ──
        ctx = trace.set_span_in_context(parent_context) if parent_context else None

        with self.tracer.start_as_current_span(
            f"{self.name}.handle",
            context=ctx,
            attributes=attrs,
        ) as span:
            self.active_requests.add(1, {"service": self.name})
            self.request_counter.add(1, {"service": self.name})

            if is_error:
                error_msg = self.overrides.get("error_msg") or random.choice([
                    "Connection timeout",
                    "Database unavailable",
                    "Rate limit exceeded",
                    "Invalid request data",
                    "External API failure",
                ])
                span.set_attribute("error.message", error_msg)
                span.set_status(trace.Status(trace.StatusCode.ERROR, error_msg))
                self.error_counter.add(1, {"service": self.name})

            # Simulate processing time
            time.sleep(latency_ms / 1000.0)

            self.request_histogram.record(latency_ms, {"service": self.name})
            self.active_requests.add(-1, {"service": self.name})

        # ── Update rolling windows and emit gauge ──
        self._recent_results.append(is_error)
        self._recent_latencies.append(latency_ms)

        if self._recent_results:
            err_rate = sum(self._recent_results) / len(self._recent_results)
            self.error_rate_gauge.record(err_rate, {"service": self.name})

        return is_error, latency_ms

    @property
    def rolling_error_rate(self) -> float:
        if not self._recent_results:
            return 0.0
        return sum(self._recent_results) / len(self._recent_results)

    @property
    def rolling_p99(self) -> float:
        """Approximate P99 over the last 10 responses."""
        if not self._recent_latencies:
            return 0.0
        arr = sorted(self._recent_latencies)
        idx = min(int(len(arr) * 0.99), len(arr) - 1)
        return arr[idx]

    @property
    def requests_per_second(self) -> float:
        """Approximate throughput based on recent latencies."""
        if not self._recent_latencies:
            return 0.0
        total_sec = sum(self._recent_latencies) / 1000.0
        return len(self._recent_latencies) / total_sec if total_sec > 0 else 0.0

    def shutdown(self):
        self.tracer_provider.shutdown()
        self.meter_provider.shutdown()


# ── Orchestrator ───────────────────────────────────────────────────────

class MonolithPlatform:
    """
    Drives the full request flow across all services and periodically
    prints a live status table to the terminal.

    The api-gateway span is created first and its context is passed to
    downstream services so the entire request appears as a single trace
    waterfall in SigNoz.
    """

    def __init__(self, otlp_endpoint: str = OTLP_ENDPOINT, chaos_enabled: bool = True):
        self.chaos_enabled = chaos_enabled
        self.chaos_engine = ChaosEngine() if chaos_enabled else None
        self.services: dict[str, PlatformService] = {}
        for name, cfg in SERVICES_CONFIG.items():
            self.services[name] = PlatformService(name, cfg, otlp_endpoint)
        self.gateway = self.services["api-gateway"]
        self._running = True

    def _update_overrides(self):
        """Push current chaos state into every service before each request."""
        if not self.chaos_engine:
            for svc in self.services.values():
                svc.set_overrides({})
            return
        for name, svc in self.services.items():
            svc.set_overrides(self.chaos_engine.current_state(name))

    def simulate_user_request(self, user_id: str, action: str):
        """
        Simulate a full user request through every relevant service.

        The gateway span is the root; all downstream spans link to it
        via parent_context, creating a trace tree.
        """
        self._update_overrides()

        with self.gateway.tracer.start_as_current_span(
            "api-gateway.route",
            attributes={
                "user.id": user_id,
                "user.action": action,
                "service.name": "api-gateway",
            },
        ) as gateway_span:

            # ── Auth ──
            # Manually created span (not using simulate_request) to show
            # an alternative pattern — useful for readers learning OTel.
            auth_span = self.services["auth-service"].tracer.start_span(
                "auth-service.handle",
                context=trace.set_span_in_context(gateway_span),
                attributes={"user.id": user_id, "service.name": "auth-service"},
            )
            with auth_span:
                auth_latency = random.uniform(
                    *self.services["auth-service"].config["latency_ms"]
                )
                # Auth also gets cold-start spikes for realism
                if random.random() < 0.025:
                    auth_latency *= random.uniform(3.0, 5.0)
                    auth_span.set_attribute("cold_start", "true")
                if random.random() < 0.03:
                    auth_span.set_status(
                        trace.Status(trace.StatusCode.ERROR, "Token expired")
                    )
                time.sleep(auth_latency / 1000.0)

            # ── Downstream services ──
            if action in ("search_products", "view_product"):
                self.services["product-catalog"].simulate_request(
                    user_id, action, gateway_span
                )
            if action in ("add_to_cart", "checkout"):
                self.services["cart-service"].simulate_request(
                    user_id, action, gateway_span
                )
            if action == "checkout":
                self.services["order-service"].simulate_request(
                    user_id, action, gateway_span
                )
                self.services["payment-service"].simulate_request(
                    user_id, action, gateway_span
                )
                self.services["notification"].simulate_request(
                    user_id, action, gateway_span
                )

    def _display_loop(self):
        """Background thread: print a live status table every 10 seconds."""
        while self._running:
            time.sleep(10)
            if not self._running:
                break
            self._print_status()

    def _print_status(self):
        print("\n" + "─" * 80)
        print(f"  {'Service':<20} {'State':<22} {'Req/s':<8} {'Err%':<8} {'P99(last 10)':<14}")
        print("─" * 80)
        for name, svc in self.services.items():
            o = svc.overrides
            if o.get("incident_type"):
                state = o["incident_type"]
            elif o.get("degraded_by"):
                state = f"CASCADED({o['degraded_by']})"
            else:
                state = "HEALTHY"
            rps = svc.requests_per_second
            err = svc.rolling_error_rate * 100
            p99 = svc.rolling_p99
            print(f"  {name:<20} {state:<22} {rps:<8.1f} {err:<8.0f}% {p99:<14.0f}ms")
        print("─" * 80)
        if self.chaos_engine and self.chaos_engine.active_incident:
            inc = self.chaos_engine.active_incident
            rem = max(0, inc.duration - (time.monotonic() - inc.started_at))
            print(f"  >>> payment-service INCIDENT: {inc.type.value}  ({rem:.0f}s remaining)")
        print()

    def run_loop(self, interval: float = 2.0, rate: int = 5):
        """Continuously simulate user requests until Ctrl-C."""
        print(f"[*] Monolith Platform Simulator")
        print(f"[*] OTLP Endpoint: {OTLP_ENDPOINT}")
        print(f"[*] Services: {', '.join(SERVICES_CONFIG.keys())}")
        print(f"[*] Interval: {interval}s batch (up to {rate} req/batch)")
        print(f"[*] Environment: {ENVIRONMENT}")
        print(f"[*] Chaos: {'ENABLED' if self.chaos_enabled else 'DISABLED'}")
        print("─" * 50)

        threading.Thread(target=self._display_loop, daemon=True).start()

        try:
            while True:
                for _ in range(random.randint(1, rate)):
                    self.simulate_user_request(
                        random.choice(USER_IDS),
                        random.choice(USER_ACTIONS),
                    )
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\n[*] Shutting down...")
        finally:
            self._running = False
            if self.chaos_engine:
                self.chaos_engine.stop()
            for svc in self.services.values():
                svc.shutdown()
            print("[*] Done.")

    def run_once(self):
        """Send one complete batch per user action and exit."""
        user = random.choice(USER_IDS)
        for action in USER_ACTIONS:
            self.simulate_user_request(user, action)
        for svc in self.services.values():
            svc.shutdown()
        print("[*] Single batch complete.")


# ── CLI ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Multi-service E-commerce Platform Simulator for SigNoz"
    )
    parser.add_argument(
        "-e", "--endpoint",
        default=OTLP_ENDPOINT,
        help=f"SigNoz OTLP HTTP endpoint (default: {OTLP_ENDPOINT})",
    )
    parser.add_argument(
        "-i", "--interval",
        type=float,
        default=2.0,
        help="Seconds between request batches (default: 2.0)",
    )
    parser.add_argument(
        "-r", "--rate",
        type=int,
        default=5,
        help="Max requests per batch (default: 5)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Send one batch and exit",
    )
    parser.add_argument(
        "--no-chaos",
        action="store_false",
        dest="chaos",
        default=True,
        help="Disable anomaly injection — all services run at healthy baseline",
    )
    args = parser.parse_args()

    platform = MonolithPlatform(
        otlp_endpoint=args.endpoint,
        chaos_enabled=args.chaos,
    )

    if args.once:
        platform.run_once()
    else:
        platform.run_loop(interval=args.interval, rate=args.rate)


if __name__ == "__main__":
    main()
