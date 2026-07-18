#!/usr/bin/env python3
"""Send simple OTLP/HTTP smoke telemetry to a local SigNoz.

This script sends traces, logs, and a metric to SigNoz's OTLP HTTP endpoint:
  http://localhost:4318

It is meant for hackathon prep / debugging and should be safe to run repeatedly.
"""

import json
import random
import time
import urllib.request

OTLP_HTTP = "http://localhost:4318"
SERVICE_NAME = "hackathon-toy-app"


def now_ns() -> int:
    return time.time_ns()


def hex_id(length: int) -> str:
    return "".join(random.choice("0123456789abcdef") for _ in range(length))


def post(path: str, payload: dict) -> None:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        f"{OTLP_HTTP}{path}",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as res:
        print(f"{path}: HTTP {res.status}")


def resource() -> dict:
    return {
        "attributes": [
            {"key": "service.name", "value": {"stringValue": SERVICE_NAME}},
            {"key": "deployment.environment", "value": {"stringValue": "local"}},
            {"key": "hackathon.block", "value": {"stringValue": "block-3"}},
        ]
    }


def send_trace() -> None:
    start = now_ns()
    latency_ms = random.randint(45, 260)
    end = start + latency_ms * 1_000_000
    errored = random.random() < 0.2

    payload = {
        "resourceSpans": [
            {
                "resource": resource(),
                "scopeSpans": [
                    {
                        "scope": {"name": "send-telemetry"},
                        "spans": [
                            {
                                "traceId": hex_id(32),
                                "spanId": hex_id(16),
                                "name": "e2e-smoke-test",
                                "kind": 2,
                                "startTimeUnixNano": str(start),
                                "endTimeUnixNano": str(end),
                                "attributes": [
                                    {"key": "demo.step", "value": {"stringValue": "send-trace"}},
                                    {"key": "ai.agent.name", "value": {"stringValue": "prep-agent"}},
                                    {"key": "llm.token.count", "value": {"intValue": str(random.randint(20, 180))}},
                                    {"key": "demo.latency_ms", "value": {"intValue": str(latency_ms)}},
                                    {"key": "demo.error", "value": {"boolValue": errored}},
                                ],
                                "status": {"code": 2 if errored else 1},
                            }
                        ],
                    }
                ],
            }
        ]
    }
    post("/v1/traces", payload)


def send_log() -> None:
    messages = [
        "Agent selected search tool for user request.",
        "Agent retried after slow downstream response.",
        "Agent completed answer with supporting telemetry.",
        "Agent detected missing context and asked for clarification.",
        "Agent warning: token usage higher than expected.",
    ]
    payload = {
        "resourceLogs": [
            {
                "resource": resource(),
                "scopeLogs": [
                    {
                        "scope": {"name": "send-telemetry"},
                        "logRecords": [
                            {
                                "timeUnixNano": str(now_ns()),
                                "severityText": "INFO",
                                "severityNumber": 9,
                                "body": {"stringValue": random.choice(messages)},
                                "attributes": [
                                    {"key": "demo.step", "value": {"stringValue": "send-log"}},
                                    {"key": "event.name", "value": {"stringValue": "first_log"}},
                                ],
                            }
                        ],
                    }
                ],
            }
        ]
    }
    post("/v1/logs", payload)


def send_metric() -> None:
    latency_ms = float(random.randint(45, 260))
    payload = {
        "resourceMetrics": [
            {
                "resource": resource(),
                "scopeMetrics": [
                    {
                        "scope": {"name": "send-telemetry"},
                        "metrics": [
                            {
                                "name": "hackathon.toy_agent.latency_ms",
                                "description": "Toy metric for SigNoz hackathon prep",
                                "unit": "ms",
                                "gauge": {
                                    "dataPoints": [
                                        {
                                            "timeUnixNano": str(now_ns()),
                                            "asDouble": latency_ms,
                                            "attributes": [
                                                {"key": "demo.step", "value": {"stringValue": "send-metric"}}
                                            ],
                                        }
                                    ]
                                },
                            }
                        ],
                    }
                ],
            }
        ]
    }
    post("/v1/metrics", payload)


if __name__ == "__main__":
    loop = "--loop" in __import__("sys").argv
    interval = 5

    while True:
        send_trace()
        send_log()
        send_metric()
        print(f"Sent telemetry for service.name={SERVICE_NAME}")
        if not loop:
            break
        time.sleep(interval)

