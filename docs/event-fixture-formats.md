# Event Fixture Formats

This document defines the concrete event shapes we should use for fixture templates and loadtest generation.

The primary example tenant is **Codex**, with coding, tool-use, canvas, retrieval, and agent-orchestration workflows. Some fixture families still include generic product/state examples so Nanotrace can exercise analytics read paths beyond pure agent telemetry.

Assume the loadtest generator mutates these templates deterministically with `run_id + sequence`, but the field contract should stay stable.

## Common Envelope

Every fixture is a JSON object with this envelope:

```json
{
  "event_id": "fixture-name",
  "timestamp": "2026-05-08T01:23:45.123Z",
  "observed_timestamp": "2026-05-08T01:23:45.130Z",
  "data": {
    "tenant_id": "fixture",
    "event_type": "string",
    "service": "string",
    "environment": "prod"
  }
}
```

The loader/server own or normalize:

- `event_id`
- `timestamp`
- `observed_timestamp`
- `source_file`
- `source_offset`
- `source_length`

Fixtures should keep those fields present for shape validation, but loadtest rewrites them.

## Shared Fields

Use these consistently across event families.

Low-cardinality facet fields:

- `service`
- `environment`
- `event_type`
- `signal`
- `http.method`
- `http.route`
- `http.status_code`
- `account.plan`
- `account.risk_tier`
- `user.lifecycle`
- `order.status`
- `model`
- `tool.name`
- `retrieval.index`
- `eval.name`

High-cardinality lookup fields:

- `trace_id`
- `span_id`
- `parent_span_id`
- `request_id`
- `session_id`
- `user_id`
- `account.id`
- `order.id`
- `conversation.id`
- `thread_id`
- `run_id`

Numeric measure fields:

- `duration_ms`
- `revenue`
- `credits_used`
- `input_tokens`
- `output_tokens`
- `total_tokens`
- `cost_usd`
- `queue_depth`
- `retrieval.top_k`
- `retrieval.hit_count`
- `retrieval.max_score`
- `eval.score`

Longitudinal state fields:

- `account.plan`
- `account.risk_tier`
- `user.lifecycle`
- `order.status`
- `agent.workflow_state`

## Format 1: Product Event

Purpose:

- Product analytics
- Funnels
- Cohorts
- Retention
- Revenue summaries
- Field facets and lookup IDs

Examples:

- `user.signup`
- `workspace.created`
- `watchlist.created`
- `order.submitted`
- `order.filled`
- `checkout.started`
- `checkout.completed`
- `feature.used`

Template:

```json
{
  "event_id": "fixture-product-checkout-completed",
  "timestamp": "2026-05-08T01:23:45.123Z",
  "observed_timestamp": "2026-05-08T01:23:45.130Z",
  "data": {
    "tenant_id": "fixture",
    "event_type": "checkout.completed",
    "signal": "analytics",
    "service": "billing",
    "environment": "prod",
    "request_id": "req_000001",
    "session_id": "sess_000001",
    "user_id": "usr_000001",
    "account": {
      "id": "acct_000001",
      "plan": "pro",
      "risk_tier": "medium"
    },
    "checkout": {
      "id": "chk_000001",
      "currency": "USD",
      "amount": 49.0,
      "payment_method": "card"
    },
    "revenue": 49.0,
    "feature": "pro_upgrade"
  }
}
```

ClickHouse coverage:

- `field_index`: `event_type`, `service`, `account.plan`, `account.risk_tier`, `feature`
- `event_measures`: `revenue`
- `field_counts_5m`: top plans/features/services
- `report_results`: revenue by plan after report materialization

## Format 2: State Transition Event

Purpose:

- Longitudinal entity-state analytics
- As-of state reconstruction
- Conversion/churn analysis

Examples:

- `account.plan_changed`
- `account.risk_tier_changed`
- `user.lifecycle_changed`
- `order.status_changed`
- `agent.workflow_state_changed`

Template:

```json
{
  "event_id": "fixture-state-account-plan-changed",
  "timestamp": "2026-05-08T01:23:45.123Z",
  "observed_timestamp": "2026-05-08T01:23:45.130Z",
  "data": {
    "tenant_id": "fixture",
    "event_type": "account.plan_changed",
    "signal": "analytics",
    "service": "billing",
    "environment": "prod",
    "user_id": "usr_000001",
    "account": {
      "id": "acct_000001",
      "plan": "enterprise",
      "previous_plan": "pro",
      "risk_tier": "medium"
    },
    "change": {
      "field": "account.plan",
      "from": "pro",
      "to": "enterprise",
      "reason": "sales_upgrade"
    }
  }
}
```

Recommended state definition:

```json
{
  "kind": "state",
  "name": "account.plan",
  "mode": "state_transition",
  "config": {
    "path": "account.plan",
    "entity_type": "account",
    "entity_id_path": "account.id",
    "value_type": "string"
  }
}
```

ClickHouse coverage:

- `entity_state_updates`: account plan history
- `field_index`: exact account/user lookup
- `report_results`: plan conversion reports

## Format 3: HTTP Span Start

Purpose:

- Trace flamegraph start marker
- Event timeline
- Service/route activity

Template:

```json
{
  "event_id": "fixture-span-start-http",
  "timestamp": "2026-05-08T01:23:45.000Z",
  "observed_timestamp": "2026-05-08T01:23:45.002Z",
  "data": {
    "tenant_id": "fixture",
    "event_type": "span_start",
    "signal": "trace",
    "service": "trading-api",
    "environment": "prod",
    "trace_id": "tr_000001",
    "span_id": "sp_000001",
    "parent_span_id": "",
    "name": "POST /orders",
    "span_kind": "server",
    "start_time": "2026-05-08T01:23:45.000Z",
    "http.method": "POST",
    "http.route": "/orders"
  }
}
```

ClickHouse coverage:

- `event_index`: timeline/flamegraph lane
- `field_index`: `trace_id`, `span_id`, `service`, `http.route`

## Format 4: HTTP Span End

Purpose:

- Trace flamegraph duration
- Route latency
- Error rate
- Rollups by service/route/status

Template:

```json
{
  "event_id": "fixture-span-end-http",
  "timestamp": "2026-05-08T01:23:45.231Z",
  "observed_timestamp": "2026-05-08T01:23:45.233Z",
  "data": {
    "tenant_id": "fixture",
    "event_type": "span_end",
    "signal": "trace",
    "service": "trading-api",
    "environment": "prod",
    "trace_id": "tr_000001",
    "span_id": "sp_000001",
    "parent_span_id": "",
    "name": "POST /orders",
    "span_kind": "server",
    "span_status_code": "ok",
    "start_time": "2026-05-08T01:23:45.000Z",
    "end_time": "2026-05-08T01:23:45.231Z",
    "duration_ms": 231,
    "is_error": 0,
    "http.method": "POST",
    "http.route": "/orders",
    "http.status_code": 200,
    "request_id": "req_000001",
    "user_id": "usr_000001",
    "account": {
      "id": "acct_000001",
      "plan": "pro"
    }
  }
}
```

ClickHouse coverage:

- `event_index`: duration, error, route, source pointers
- `event_measures`: `duration_ms`
- `event_rollups_5m`: counts/errors/duration by service/route/status
- `measure_rollups`: p95 duration by service or route

## Format 5: Agent Root Span

Purpose:

- Deep agent trace root
- User intent analysis
- Agent outcome tracking
- Link product event to agent execution tree

Examples:

- `agent.request`
- `agent.run`
- `agent.turn`

Template:

```json
{
  "event_id": "fixture-agent-root",
  "timestamp": "2026-05-08T01:23:45.000Z",
  "observed_timestamp": "2026-05-08T01:23:45.002Z",
  "data": {
    "tenant_id": "fixture",
    "event_type": "span_start",
    "signal": "trace",
    "service": "support-agent",
    "environment": "prod",
    "trace_id": "tr_agent_000001",
    "span_id": "sp_root",
    "parent_span_id": "",
    "name": "agent.request",
    "span_kind": "internal",
    "conversation": {
      "id": "conv_000001",
      "channel": "web_chat",
      "intent": "ach_transfer_failed",
      "language": "en"
    },
    "agent": {
      "name": "Atlas Support Agent",
      "workflow": "transfer_support",
      "workflow_state": "planning",
      "prompt_version": "transfer-support-v17"
    },
    "user_id": "usr_000001",
    "account": {
      "id": "acct_000001",
      "plan": "pro",
      "risk_tier": "high"
    }
  }
}
```

ClickHouse coverage:

- `event_index`: trace tree root
- `field_index`: `conversation.intent`, `agent.workflow`, `account.risk_tier`
- `entity_state_updates`: optional `agent.workflow_state`

## Format 6: Agent Decision Span

Purpose:

- Agent behavior analysis
- Loop/cycle detection
- Planner quality
- Reason-code faceting

Template:

```json
{
  "event_id": "fixture-agent-decision",
  "timestamp": "2026-05-08T01:23:45.100Z",
  "observed_timestamp": "2026-05-08T01:23:45.101Z",
  "data": {
    "tenant_id": "fixture",
    "event_type": "span_end",
    "signal": "trace",
    "service": "support-agent",
    "environment": "prod",
    "trace_id": "tr_agent_000001",
    "span_id": "sp_decide_001",
    "parent_span_id": "sp_root",
    "name": "agent.decide_next_action",
    "span_kind": "internal",
    "start_time": "2026-05-08T01:23:45.050Z",
    "end_time": "2026-05-08T01:23:45.100Z",
    "duration_ms": 50,
    "is_error": 0,
    "agent": {
      "workflow": "transfer_support",
      "workflow_state": "tool_selection",
      "cycle_count": 2,
      "decision": "call_tool",
      "reason_code": "needs_transfer_status"
    }
  }
}
```

ClickHouse coverage:

- `event_index`: deep trace lane
- `field_index`: `agent.decision`, `agent.reason_code`
- `event_measures`: `duration_ms`

## Format 7: LLM Call Span

Purpose:

- LLM cost/latency/token dashboards
- Model comparison
- Prompt-version regression analysis
- Finish reason/error analysis

Template:

```json
{
  "event_id": "fixture-llm-call",
  "timestamp": "2026-05-08T01:23:45.200Z",
  "observed_timestamp": "2026-05-08T01:23:45.201Z",
  "data": {
    "tenant_id": "fixture",
    "event_type": "span_end",
    "signal": "trace",
    "service": "support-agent",
    "environment": "prod",
    "trace_id": "tr_agent_000001",
    "span_id": "sp_llm_001",
    "parent_span_id": "sp_root",
    "name": "llm.call",
    "span_kind": "client",
    "start_time": "2026-05-08T01:23:45.100Z",
    "end_time": "2026-05-08T01:23:45.900Z",
    "duration_ms": 800,
    "is_error": 0,
    "model": "claude-sonnet-4-6",
    "provider": "anthropic",
    "prompt_version": "transfer-support-v17",
    "temperature": 0.2,
    "finish_reason": "tool_call",
    "input_tokens": 1842,
    "output_tokens": 291,
    "total_tokens": 2133,
    "cost_usd": 0.014,
    "llm": {
      "prompt_hash": "prh_000001",
      "response_hash": "rsh_000001",
      "cached_input_tokens": 1400
    }
  }
}
```

ClickHouse coverage:

- `field_index`: `model`, `provider`, `prompt_version`, `finish_reason`
- `event_measures`: `input_tokens`, `output_tokens`, `total_tokens`, `cost_usd`, `duration_ms`
- `measure_rollups`: cost/tokens/latency by model or prompt version

## Format 8: Tool Call Span

Purpose:

- Tool reliability
- Tool latency
- Retry/error analysis
- Agent flamegraph depth

Template:

```json
{
  "event_id": "fixture-tool-call",
  "timestamp": "2026-05-08T01:23:46.000Z",
  "observed_timestamp": "2026-05-08T01:23:46.001Z",
  "data": {
    "tenant_id": "fixture",
    "event_type": "span_end",
    "signal": "trace",
    "service": "support-agent",
    "environment": "prod",
    "trace_id": "tr_agent_000001",
    "span_id": "sp_tool_001",
    "parent_span_id": "sp_llm_001",
    "name": "tool.call banking.get_transfer_status",
    "span_kind": "client",
    "start_time": "2026-05-08T01:23:45.900Z",
    "end_time": "2026-05-08T01:23:46.083Z",
    "duration_ms": 183,
    "is_error": 0,
    "tool": {
      "name": "banking.get_transfer_status",
      "category": "internal_api",
      "status": "ok",
      "retry_count": 1
    },
    "http.status_code": 200
  }
}
```

ClickHouse coverage:

- `field_index`: `tool.name`, `tool.category`, `tool.status`
- `event_measures`: `duration_ms`, `tool.retry_count`
- `measure_rollups`: tool latency by tool name

## Format 9: Retrieval Step Span

Purpose:

- RAG quality
- Stale-doc detection
- Retrieval latency
- Context-size debugging

Template:

```json
{
  "event_id": "fixture-retrieval-step",
  "timestamp": "2026-05-08T01:23:46.200Z",
  "observed_timestamp": "2026-05-08T01:23:46.201Z",
  "data": {
    "tenant_id": "fixture",
    "event_type": "span_end",
    "signal": "trace",
    "service": "support-agent",
    "environment": "prod",
    "trace_id": "tr_agent_000001",
    "span_id": "sp_retrieval_001",
    "parent_span_id": "sp_root",
    "name": "retrieval.policy_docs",
    "span_kind": "client",
    "start_time": "2026-05-08T01:23:46.100Z",
    "end_time": "2026-05-08T01:23:46.250Z",
    "duration_ms": 150,
    "is_error": 0,
    "retrieval": {
      "index": "policy_docs",
      "query_class": "ach_failure",
      "top_k": 8,
      "hit_count": 8,
      "max_score": 0.87,
      "stale_doc_count": 1,
      "context_tokens": 3200
    }
  }
}
```

ClickHouse coverage:

- `field_index`: `retrieval.index`, `retrieval.query_class`
- `event_measures`: `retrieval.top_k`, `retrieval.hit_count`, `retrieval.max_score`, `retrieval.context_tokens`, `duration_ms`
- reports: stale-doc rate by index/query class

## Format 10: Evaluation / Feedback Event

Purpose:

- Agent quality trends
- Human feedback
- Online evaluator results
- Regression detection by model/prompt version

Examples:

- `eval.score`
- `feedback.user`
- `feedback.human_review`

Template:

```json
{
  "event_id": "fixture-eval-score",
  "timestamp": "2026-05-08T01:23:47.000Z",
  "observed_timestamp": "2026-05-08T01:23:47.001Z",
  "data": {
    "tenant_id": "fixture",
    "event_type": "eval.score",
    "signal": "analytics",
    "service": "eval-worker",
    "environment": "prod",
    "trace_id": "tr_agent_000001",
    "span_id": "sp_eval_001",
    "parent_span_id": "sp_root",
    "conversation": {
      "id": "conv_000001",
      "intent": "ach_transfer_failed"
    },
    "model": "claude-sonnet-4-6",
    "prompt_version": "transfer-support-v17",
    "eval": {
      "name": "policy_compliance",
      "score": 0.94,
      "label": "pass",
      "evaluator": "llm_judge_v3"
    },
    "feedback": {
      "source": "online_evaluator",
      "accepted": true
    }
  }
}
```

ClickHouse coverage:

- `field_index`: `eval.name`, `eval.label`, `feedback.source`, `model`, `prompt_version`
- `event_measures`: `eval.score`
- `report_results`: quality trend by model/prompt version

## Format 11: Security / Safety Event

Purpose:

- Prompt-injection monitoring
- PII detection/redaction
- Safety policy trend dashboards

Examples:

- `safety.pii_detected`
- `safety.prompt_injection_detected`
- `safety.output_blocked`

Template:

```json
{
  "event_id": "fixture-safety-event",
  "timestamp": "2026-05-08T01:23:47.100Z",
  "observed_timestamp": "2026-05-08T01:23:47.101Z",
  "data": {
    "tenant_id": "fixture",
    "event_type": "safety.prompt_injection_detected",
    "signal": "analytics",
    "service": "support-agent",
    "environment": "prod",
    "trace_id": "tr_agent_000001",
    "conversation": {
      "id": "conv_000001",
      "intent": "ach_transfer_failed"
    },
    "safety": {
      "category": "prompt_injection",
      "severity": "high",
      "action": "blocked",
      "pii_detected": false,
      "score": 0.91
    }
  }
}
```

ClickHouse coverage:

- `field_index`: `safety.category`, `safety.severity`, `safety.action`
- `event_measures`: `safety.score`
- reports: unsafe output rate by model/prompt version

## Format 12: Log Event

Purpose:

- Operational logging
- Error list
- Correlated trace debugging

Template:

```json
{
  "event_id": "fixture-log-error",
  "timestamp": "2026-05-08T01:23:47.200Z",
  "observed_timestamp": "2026-05-08T01:23:47.201Z",
  "data": {
    "tenant_id": "fixture",
    "event_type": "log",
    "signal": "log",
    "service": "payments",
    "environment": "prod",
    "trace_id": "tr_000001",
    "span_id": "sp_000009",
    "name": "payment.processor_error",
    "severity_number": 17,
    "severity_text": "ERROR",
    "is_error": 1,
    "message": "Payment processor returned transient decline",
    "caller": "src/payments/processor.ts:314",
    "request_id": "req_000001",
    "account": {
      "id": "acct_000001",
      "plan": "pro"
    }
  }
}
```

ClickHouse coverage:

- `event_index`: recent errors
- `field_index`: service/severity/account lookup
- `field_counts_5m`: error counts by service

## Format 13: Metric Counter

Purpose:

- Request count
- Business count
- Queue throughput

Template:

```json
{
  "event_id": "fixture-metric-counter",
  "timestamp": "2026-05-08T01:23:48.000Z",
  "observed_timestamp": "2026-05-08T01:23:48.001Z",
  "data": {
    "tenant_id": "fixture",
    "event_type": "metric",
    "signal": "metric",
    "service": "trading-api",
    "environment": "prod",
    "metric_name": "http.server.requests",
    "metric_type": "counter",
    "metric_value": 1,
    "metric_unit": "1",
    "http.method": "POST",
    "http.route": "/orders",
    "http.status_code": 200,
    "metric.temporality": "delta",
    "metric.is_monotonic": true
  }
}
```

## Format 14: Metric Gauge

Purpose:

- Runtime resource state
- Queue depth
- Worker lag

Template:

```json
{
  "event_id": "fixture-metric-gauge",
  "timestamp": "2026-05-08T01:23:48.100Z",
  "observed_timestamp": "2026-05-08T01:23:48.101Z",
  "data": {
    "tenant_id": "fixture",
    "event_type": "metric",
    "signal": "metric",
    "service": "loader",
    "environment": "prod",
    "metric_name": "runtime.queue.depth",
    "metric_type": "gauge",
    "metric_value": 128,
    "metric_unit": "1",
    "queue.name": "s3-loader",
    "partition": "p=0007"
  }
}
```

## Format 15: Metric Histogram

Purpose:

- Latency distributions
- Token/cost distributions
- Report worker durations

Template:

```json
{
  "event_id": "fixture-metric-histogram",
  "timestamp": "2026-05-08T01:23:48.200Z",
  "observed_timestamp": "2026-05-08T01:23:48.201Z",
  "data": {
    "tenant_id": "fixture",
    "event_type": "metric",
    "signal": "metric",
    "service": "support-agent",
    "environment": "prod",
    "metric_name": "llm.duration",
    "metric_type": "histogram",
    "metric_value": 840,
    "metric_unit": "ms",
    "model": "claude-sonnet-4-6",
    "bucket_counts": [12, 31, 4],
    "explicit_bounds": [250, 500, 1000],
    "count": 47,
    "sum": 39480
  }
}
```

## Format 16: Processor / Pipeline Event

Purpose:

- Processor observability
- Loader health
- Backfill/report runner visibility

Examples:

- `processor.build_completed`
- `processor.run_failed`
- `backfill.completed`
- `report.materialized`

Template:

```json
{
  "event_id": "fixture-pipeline-event",
  "timestamp": "2026-05-08T01:23:49.000Z",
  "observed_timestamp": "2026-05-08T01:23:49.001Z",
  "data": {
    "tenant_id": "fixture",
    "event_type": "report.materialized",
    "signal": "analytics",
    "service": "report-worker",
    "environment": "prod",
    "report": {
      "id": "rep_weekly_revenue",
      "kind": "summary",
      "status": "completed",
      "rows_written": 450,
      "events_scanned": 864000000,
      "duration_ms": 92000
    },
    "pipeline": {
      "component": "report-worker",
      "job_id": "job_000001",
      "priority": "normal"
    }
  }
}
```

ClickHouse coverage:

- `event_index`: pipeline event list
- `event_measures`: `report.rows_written`, `report.events_scanned`, `report.duration_ms`
- `pipeline_metrics`: operational metric rows when emitted directly by components

## Format 17: Edge / Schema Stress Event

Purpose:

- Type drift
- Missing fields
- High-cardinality lookups
- Nested JSON stress
- Field extraction safety

Template:

```json
{
  "event_id": "fixture-schema-stress",
  "timestamp": "2026-05-08T01:23:50.000Z",
  "observed_timestamp": "2026-05-08T01:23:50.001Z",
  "data": {
    "tenant_id": "fixture",
    "event_type": "schema.stress",
    "signal": "analytics",
    "service": "edge-gateway",
    "environment": "prod",
    "request_id": "req_high_cardinality_000001",
    "session_id": "sess_high_cardinality_000001",
    "account": {
      "id": "acct_000001",
      "plan": "free",
      "risk_tier": "unknown"
    },
    "experimental": {
      "string_number": "42",
      "number_string": 42,
      "bool_string": "true",
      "large_text": "redacted large text payload",
      "array_values": ["a", "b", "c"],
      "nested": {
        "version": 3,
        "variant": "schema_stress"
      }
    }
  }
}
```

Use sparingly. This should test extraction behavior without dominating normal dashboard examples.

## Recommended Fixture Files

Add these files under `fixtures/events`:

- `product_checkout_completed.json`
- `product_order_filled.json`
- `state_account_plan_changed.json`
- `state_account_risk_tier_changed.json`
- `span_start_http.json`
- `span_end_http.json`
- `agent_root.json`
- `agent_decision.json`
- `llm_call.json`
- `tool_call.json`
- `retrieval_step.json`
- `eval_score.json`
- `safety_event.json`
- `log_error.json`
- `metric_counter.json`
- `metric_gauge.json`
- `metric_histogram.json`
- `pipeline_report_materialized.json`
- `schema_stress.json`

## Recommended Loadtest Mixes

`atlas_mixed`:

- 20% product events
- 12% HTTP spans
- 18% agent/LLM/tool/retrieval spans
- 10% logs
- 12% metrics
- 8% state transitions
- 8% eval/feedback/safety
- 7% pipeline/report events
- 5% schema stress

`atlas_agent_deep_trace`:

- 5% product context events
- 65% agent root/decision/LLM/tool/retrieval spans
- 10% eval/feedback/safety
- 10% logs
- 10% metrics

`atlas_fintech_state`:

- 35% product events
- 25% state transitions
- 15% HTTP spans
- 10% logs
- 10% metrics
- 5% schema stress

## Deep Trace Shape

A realistic deep agent trace should contain 12-40 spans/events:

```text
agent.request
  classify_intent
  retrieve_account_profile
  retrieve_recent_transfers
  retrieval.policy_docs
  llm.plan
  agent.decide_next_action
  tool.call risk_service.get_account_risk
  tool.call banking.get_transfer_status
  llm.decide_next_action
  tool.call ticketing.create_case
  llm.generate_response
  safety.prompt_injection_check
  safety.pii_scan
  eval.policy_compliance
  eval.helpfulness
  feedback.user
```

The generator should keep these correlated across:

- `trace_id`
- `conversation.id`
- `user_id`
- `account.id`
- `session_id`
- `prompt_version`
- `model`

This is what makes the flamegraph, state transitions, reports, and dashboard queries meaningful.
