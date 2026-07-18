#!/usr/bin/env python3
"""
Custom telemetry generator for SigNoz.
Generates traces, logs, and metrics for a mock AI agent workflow.
Zero dependencies (uses standard library urllib).
"""

import argparse
import json
import random
import sys
import time
import urllib.request
from typing import Dict, Any, List

DEFAULT_ENDPOINT = "http://localhost:4318"
DEFAULT_SERVICE = "ai-agent-service"

# Predefined logs for realistic agent workflows
AGENT_LOG_MESSAGES = [
    ("INFO", "Received user query: 'How do I query my metrics in SigNoz?'"),
    ("INFO", "Initiating semantic search over documentation vector index..."),
    ("INFO", "Vector search complete. Found 3 relevant chunks in 24ms."),
    ("INFO", "Sending prompt to LLM (model: gpt-4o)..."),
    ("WARNING", "LLM request latency is high (1.4s), waiting for response..."),
    ("INFO", "Received LLM response (124 tokens generated)."),
    ("INFO", "Executing tool 'signoz_query_api' with arguments: {'query': 'avg:latency_ms'}"),
    ("INFO", "Tool executed successfully. Returning formatted response to user."),
]

def get_now_ns() -> int:
    return time.time_ns()

def generate_hex_id(length: int) -> str:
    return "".join(random.choice("0123456789abcdef") for _ in range(length))

def build_resource(service_name: str) -> Dict[str, Any]:
    return {
        "attributes": [
            {"key": "service.name", "value": {"stringValue": service_name}},
            {"key": "deployment.environment", "value": {"stringValue": "development"}},
            {"key": "telemetry.sdk.language", "value": {"stringValue": "python"}},
            {"key": "telemetry.sdk.name", "value": {"stringValue": "signoz-agent-generator"}},
        ]
    }

def post_payload(endpoint: str, path: str, payload: Dict[str, Any]) -> bool:
    url = f"{endpoint.rstrip('/')}{path}"
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as res:
            if res.status in (200, 201, 202):
                return True
            print(f"[-] Failed to send to {path}: HTTP Status {res.status}", file=sys.stderr)
            return False
    except Exception as e:
        print(f"[-] Connection error sending to {url}: {e}", file=sys.stderr)
        return False

def send_traces(endpoint: str, service_name: str) -> None:
    """
    Sends a structured trace representing an agent run:
      - Parent: agent_run (overall request)
        - Child 1: vector_db_search
        - Child 2: llm_generation
        - Child 3: tool_execution
    """
    trace_id = generate_hex_id(32)
    parent_span_id = generate_hex_id(16)
    
    start_time = get_now_ns()
    
    # Latencies in milliseconds
    vector_search_ms = random.randint(15, 60)
    llm_ms = random.randint(800, 1800)
    tool_ms = random.randint(100, 300)
    total_ms = vector_search_ms + llm_ms + tool_ms + random.randint(10, 30)
    
    end_time = start_time + (total_ms * 1_000_000)
    
    # Sub-span timestamps
    t1_start = start_time + 2_000_000
    t1_end = t1_start + (vector_search_ms * 1_000_000)
    
    t2_start = t1_end + 5_000_000
    t2_end = t2_start + (llm_ms * 1_000_000)
    
    t3_start = t2_end + 5_000_000
    t3_end = t3_start + (tool_ms * 1_000_000)
    
    # We occasionally simulate an error in the tool or LLM
    is_error = random.random() < 0.15
    tool_status_code = 2 if is_error else 1  # 2 is Error, 1 is Ok
    
    spans = [
        # Parent span: Agent Request
        {
            "traceId": trace_id,
            "spanId": parent_span_id,
            "name": "agent.request_workflow",
            "kind": 1,  # Server
            "startTimeUnixNano": str(start_time),
            "endTimeUnixNano": str(end_time),
            "attributes": [
                {"key": "agent.workflow", "value": {"stringValue": "support_agent"}},
                {"key": "user.question_type", "value": {"stringValue": "documentation"}},
                {"key": "llm.total_cost", "value": {"doubleValue": 0.0042}},
            ],
            "status": {"code": tool_status_code},
        },
        # Child 1: Vector DB Search
        {
            "traceId": trace_id,
            "spanId": generate_hex_id(16),
            "parentSpanId": parent_span_id,
            "name": "db.vector_search",
            "kind": 3,  # Client
            "startTimeUnixNano": str(t1_start),
            "endTimeUnixNano": str(t1_end),
            "attributes": [
                {"key": "db.system", "value": {"stringValue": "qdrant"}},
                {"key": "db.operation", "value": {"stringValue": "search"}},
                {"key": "db.search.top_k", "value": {"intValue": "3"}},
            ],
            "status": {"code": 1},
        },
        # Child 2: LLM Generation
        {
            "traceId": trace_id,
            "spanId": generate_hex_id(16),
            "parentSpanId": parent_span_id,
            "name": "llm.generation",
            "kind": 3,  # Client
            "startTimeUnixNano": str(t2_start),
            "endTimeUnixNano": str(t2_end),
            "attributes": [
                {"key": "llm.model", "value": {"stringValue": "gpt-4o"}},
                {"key": "llm.provider", "value": {"stringValue": "openai"}},
                {"key": "llm.prompt_tokens", "value": {"intValue": str(random.randint(150, 450))}},
                {"key": "llm.completion_tokens", "value": {"intValue": str(random.randint(50, 200))}},
            ],
            "status": {"code": 1},
        },
        # Child 3: Tool Execution
        {
            "traceId": trace_id,
            "spanId": generate_hex_id(16),
            "parentSpanId": parent_span_id,
            "name": "tool.signoz_query_api",
            "kind": 3,  # Client
            "startTimeUnixNano": str(t3_start),
            "endTimeUnixNano": str(t3_end),
            "attributes": [
                {"key": "tool.name", "value": {"stringValue": "signoz_query_api"}},
                {"key": "tool.type", "value": {"stringValue": "http_api"}},
            ],
            "status": {"code": tool_status_code},
        }
    ]
    
    if is_error:
        spans[3]["attributes"].append({"key": "error.message", "value": {"stringValue": "Rate limit exceeded on target API"}})
    
    payload = {
        "resourceSpans": [
            {
                "resource": build_resource(service_name),
                "scopeSpans": [
                    {
                        "scope": {"name": "agent-tracer", "version": "1.0.0"},
                        "spans": spans
                    }
                ]
            }
        ]
    }
    
    if post_payload(endpoint, "/v1/traces", payload):
        print(f"[+] Trace sent: ID={trace_id[:8]}... (4 spans)")

def send_logs(endpoint: str, service_name: str) -> None:
    severity, body = random.choice(AGENT_LOG_MESSAGES)
    
    # Map severity to OTel numbers (9=INFO, 13=WARN, 17=ERROR)
    sev_num = 9
    if severity == "WARNING":
        sev_num = 13
    elif severity == "ERROR":
        sev_num = 17

    payload = {
        "resourceLogs": [
            {
                "resource": build_resource(service_name),
                "scopeLogs": [
                    {
                        "scope": {"name": "agent-logger", "version": "1.0.0"},
                        "logRecords": [
                            {
                                "timeUnixNano": str(get_now_ns()),
                                "severityText": severity,
                                "severityNumber": sev_num,
                                "body": {"stringValue": body},
                                "attributes": [
                                    {"key": "log.source", "value": {"stringValue": "agent_execution_loop"}},
                                    {"key": "thread.id", "value": {"intValue": "1"}},
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
    }
    
    if post_payload(endpoint, "/v1/logs", payload):
        print(f"[+] Log sent: [{severity}] {body}")

def send_metrics(endpoint: str, service_name: str) -> None:
    """
    Sends two metrics:
      - agent.execution_latency (Gauge)
      - agent.token_count (Sum / Counter)
    """
    now = get_now_ns()
    latency_val = float(random.randint(900, 2200))
    token_val = random.randint(200, 600)
    
    payload = {
        "resourceMetrics": [
            {
                "resource": build_resource(service_name),
                "scopeMetrics": [
                    {
                        "scope": {"name": "agent-metrics", "version": "1.0.0"},
                        "metrics": [
                            # Metric 1: Gauge for Execution Latency
                            {
                                "name": "agent.execution_latency",
                                "description": "Total agent loop execution latency in milliseconds",
                                "unit": "ms",
                                "gauge": {
                                    "dataPoints": [
                                        {
                                            "timeUnixNano": str(now),
                                            "asDouble": latency_val,
                                            "attributes": [
                                                {"key": "agent.model", "value": {"stringValue": "gpt-4o"}}
                                            ]
                                        }
                                    ]
                                }
                            },
                            # Metric 2: Sum for Token Count
                            {
                                "name": "agent.token_usage",
                                "description": "Cumulative tokens used by the agent",
                                "unit": "1",
                                "sum": {
                                    "dataPoints": [
                                        {
                                            "startTimeUnixNano": str(now - 5_000_000_000),
                                            "timeUnixNano": str(now),
                                            "asInt": str(token_val),
                                            "attributes": [
                                                {"key": "token.type", "value": {"stringValue": "total"}}
                                            ]
                                        }
                                    ],
                                    "aggregationTemporality": 2, # Delta
                                    "isMonotonic": True
                                }
                            }
                        ]
                    }
                ]
            }
        ]
    }
    
    if post_payload(endpoint, "/v1/metrics", payload):
        print(f"[+] Metrics sent: agent.execution_latency={latency_val}ms, agent.token_usage={token_val}")

def main() -> None:
    parser = argparse.ArgumentParser(description="Send OpenTelemetry data to SigNoz OTLP HTTP receiver.")
    parser.add_argument("-e", "--endpoint", default=DEFAULT_ENDPOINT, help=f"SigNoz OTLP endpoint (default: {DEFAULT_ENDPOINT})")
    parser.add_argument("-s", "--service", default=DEFAULT_SERVICE, help=f"Service name to report (default: {DEFAULT_SERVICE})")
    parser.add_argument("-i", "--interval", type=float, default=5.0, help="Interval in seconds between loops (default: 5.0)")
    parser.add_argument("-o", "--once", action="store_true", help="Send once and exit immediately")
    parser.add_argument("--traces-only", action="store_true", help="Only send trace telemetry")
    parser.add_argument("--metrics-only", action="store_true", help="Only send metric telemetry")
    parser.add_argument("--logs-only", action="store_true", help="Only send log telemetry")
    
    args = parser.parse_args()
    
    endpoint = args.endpoint
    service = args.service
    
    # Determine what to send
    send_all = not (args.traces_only or args.metrics_only or args.logs_only)
    
    print(f"[*] Starting telemetry generator...")
    print(f"[*] Endpoint: {endpoint}")
    print(f"[*] Service:  {service}")
    print(f"[*] Interval: {args.interval}s")
    print(f"[*] Mode:     {'Single Run' if args.once else 'Continuous Loop'}")
    print("--------------------------------------------------")
    
    try:
        while True:
            if send_all or args.traces_only:
                send_traces(endpoint, service)
            if send_all or args.logs_only:
                send_logs(endpoint, service)
            if send_all or args.metrics_only:
                send_metrics(endpoint, service)
            
            if args.once:
                break
                
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\n[*] Exiting telemetry generator.")
        sys.exit(0)

if __name__ == "__main__":
    main()
