# openSlice — E-Commerce Observability Simulator

A Python simulator that mimics a real 7-service e-commerce backend and sends
traces + metrics to [SigNoz](https://signoz.io) via OpenTelemetry.

No real shopping website. No Docker Compose. No databases.  
Just one Python file that generates realistic distributed traces so you can
practice finding performance problems in SigNoz.

---

## What It Simulates

```
api-gateway (5–30ms, 1% error)
├── auth-service (20–80ms, 3% error)          ← always runs
├── product-catalog (30–150ms, 2% error)      ← on search requests
├── cart-service (10–50ms, 1% error)          ← on checkout
├── order-service (50–200ms, 5% error)        ← on checkout
├── payment-service (200–1500ms, 10% error)   ← THE SLOW ONE
└── notification (30–300ms, 4% error)         ← after payment
```

`payment-service` is deliberately broken — highest latency, highest error rate.
This is the "patient zero" you'll hunt down in SigNoz.

---

## Prerequisites

- Python 3.9+
- SigNoz running locally (OTLP HTTP on port 4318)

Get SigNoz: https://signoz.io/docs/install/

---

## Quickstart

```bash
git clone https://github.com/YOUR_USERNAME/openSlice.git
cd openSlice

python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt

python platform_simulator.py
```

Open SigNoz → `http://localhost:8080` → **Services tab**.  
All 7 services appear within 10–15 seconds.

---

## CLI Options

```bash
# Run continuously (default: 5 req/batch every 2s)
python platform_simulator.py

# Custom rate and interval
python platform_simulator.py --rate 10 --interval 1

# Send one batch and exit (good for testing)
python platform_simulator.py --once

# Point at a remote SigNoz instance
python platform_simulator.py --endpoint http://your-signoz-host:4318
```

---

## What to Look For in SigNoz

1. **Services tab** — all 7 services listed. `payment-service` has the highest P99 and error rate.
2. **Dashboards** — build a latency bar chart (`*.request.duration` P99 per service). One bar will tower over the rest.
3. **Traces** — filter by `service.name = payment-service`, sort by duration desc. Open the slowest trace. The waterfall shows exactly which span is the bottleneck.
4. **Span attributes** — click a `payment-service.handle` span → see `error.message` and `payment.method` attributes. These turn "payment is slow" into "Stripe is failing."
5. **Alerts** — set `payment-service.request.duration` P99 > 1400ms → get notified when incidents happen.

See [`evidence-pack/signoz-dashboard-guide.md`](evidence-pack/signoz-dashboard-guide.md) for exact query builder steps.

---

## Metrics Emitted

Each service sends:

| Metric | Type | Description |
|---|---|---|
| `<service>.request.duration` | Histogram | Latency in ms |
| `<service>.request.count` | Counter | Total requests |
| `<service>.error.count` | Counter | Total errors |
| `<service>.requests.active` | UpDownCounter | Concurrent requests |

---

## Tech Stack

- [OpenTelemetry Python SDK](https://opentelemetry.io/docs/instrumentation/python/) `1.44.0`
- [SigNoz](https://signoz.io) — traces, metrics, dashboards, alerts
- OTLP HTTP exporter → `localhost:4318`
