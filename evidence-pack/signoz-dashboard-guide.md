# SigNoz Dashboard Guide — Monolith Platform

## What This Platform Sends

The simulator generates 7 services visible in SigNoz via `service.name`:

| Service | Latency (ms) | Error Rate | What to watch |
|---|---|---|---|
| `api-gateway` | 5-30 | 1% | Entry point, always called |
| `auth-service` | 20-80 | 3% | Token validation |
| `product-catalog` | 30-150 | 2% | Search queries |
| `cart-service` | 10-50 | 1% | Add/remove items |
| `order-service` | 50-200 | 5% | Order creation |
| `payment-service` | 200-1500 | 10% | **Slowest, most errors** |
| `notification` | 30-300 | 4% | Email/SMS dispatch |

Each service has its own metrics:
- `<service>.request.duration` — histogram (ms)
- `<service>.request.count` — counter
- `<service>.error.count` — counter

## Step 1: Find Which Service is Slow

In SigNoz **Trace** view:
- Filter by `service.name` one at a time, compare latencies
- `payment-service` should consistently show the highest latencies (200-1500ms)

In SigNoz **Metrics** → `payment-service.request.duration`:
- P50 ~900ms, P95 ~1425ms, P99 ~1500ms
- Compare with `auth-service.request.duration`: P50 ~50ms, P95 ~76ms

## Step 2: Dashboard Panels to Build

Open SigNoz → **Dashboards** → **New Dashboard** → **Add Panel**

### Panel 1: Service Latency Comparison (bar chart)
- Query: `api-gateway.request.duration` P99
- Query: `auth-service.request.duration` P99
- Query: `payment-service.request.duration` P99
- Query: `notification.request.duration` P99
- **This instantly shows which service is the bottleneck**

### Panel 2: Error Rate by Service (bar chart)
- Formula: `error.count / request.count * 100`
- Query A: `<service>.error.count` (sum)
- Query B: `<service>.request.count` (sum)
- Formula: `A / B * 100`
- Legend per service → shows `payment-service` with ~10%

### Panel 3: Payment Service Latency Timeline (time series)
- Query: `payment-service.request.duration` P50, P95, P99
- Spot sudden spikes that indicate degradation

### Panel 4: Auth Service Latency Timeline (time series)
- Query: `auth-service.request.duration` P50, P95, P99
- Should stay flat and low — if it spikes, auth is the problem

### Panel 5: Request Count per Service (bar chart)
- Query: `<service>.request.count` grouped by `service.name`
- Shows traffic distribution

## Step 3: Traces — Root Cause Drilldown

When a trace shows high end-to-end latency (>2s):

1. Open the trace in SigNoz **Traces** tab
2. Look at the span list — spans are sorted by duration
3. The slowest child span is the bottleneck
4. Click the span → check `service.name` attribute → that's the offending service
5. If it's `payment-service`, the problem is the payment provider
6. If it's `product-catalog`, the problem is the database query

## Step 4: Alerts (Optional)

Set up an alert in SigNoz:
- **Condition**: `payment-service.request.duration` P99 > 1400ms for 2 minutes
- **Severity**: Warning
- **Channel**: Slack/Email

## Key Queries for SigNoz Query Builder

### "Which services had errors in the last 5 minutes?"
```
Metric: error.count
Aggregator: Sum
Group By: service.name
Time: Last 5 min
```

### "What's the P95 latency per service?"
```
Metric: *.request.duration
Aggregator: P95
Group By: service.name
Time: Last 15 min
```

### "Find traces where payment was slow"
```
In Trace tab:
Filter: service.name = payment-service
Filter: duration > 1000ms
Sort By: duration (desc)
```

## Visual Pattern: Healthy vs Unhealthy

**Healthy**: All service latencies flat, error rates < 2%, no sudden spikes.

**Unhealthy**: `payment-service` latency suddenly doubles, errors jump to 20%.
→ Check `payment.method` attribute — is one provider failing?
→ Drill into trace → find `error.message` → "External API failure"
→ The payment provider is down.

## Quick Start

1. Run the simulator: `python platform_simulator.py`
2. Go to SigNoz → **Services** tab → see all 7 services listed
3. Click `payment-service` → see its metrics and traces
4. Build the dashboard above
5. Stop the simulator → see data freeze → run it again and watch the dashboard update
