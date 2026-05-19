---
name: nanotrace-topn-entity-promotion
description: Use when a Nanotrace product or query needs high-cardinality top-N leaderboards, such as top users or accounts by errors, revenue, latency, usage, or other metrics, and should be served from bounded report-style materialization.
---

# Nanotrace Top-N Entity Promotion

## Trigger This Skill When

Use this promotion for high-cardinality leaderboard questions over entities such as users, accounts, organizations, sessions, customers, or API keys.

Common scenarios:

- "Top users by errors."
- "Top accounts by revenue impact."
- "Customers with the most failed requests."
- "Largest token consumers in the last hour."
- "Highest-latency endpoints per account."
- A dashboard repeatedly groups raw events by a high-cardinality field and sorts by an aggregate.

## Observation Signals

Look for these signals in product flows, traces, SQL, or dashboard definitions:

- Raw queries perform `GROUP BY user_id`, `account_id`, `organization_id`, or another large entity key.
- Results are immediately sorted and limited to top K.
- The product only needs the leaderboard rows, not a complete all-entity cube.
- Query cost grows with tenant size or event volume.
- The same top-N list is refreshed on an interval or shown on shared dashboards.
- Users ask to compare "who/which entity is causing the most X" across a broad population.

## Promotion Action

Promote the leaderboard into a bounded scheduled report result. Use `report_results` only after verifying the current code has a worker that materializes this definition; otherwise the promotion includes adding that executor. Materialize only the bounded top K required by the product, with explicit window, bucket, metric, dimensions, filters, and retention.

Config sketch:

```json
{
  "kind": "topn_entity",
  "name": "top_accounts_by_checkout_errors",
  "source": "events",
  "entity": {
    "path": "account_id",
    "label_path": "account.name"
  },
  "filter": {
    "event": "checkout.failed",
    "env": "prod"
  },
  "window": "1h",
  "bucket": "5m",
  "metric": {
    "op": "count"
  },
  "limit": 50,
  "result_table": "report_results"
}
```

Use bounded top-K maintenance instead of materializing every entity unless the product has an explicit need for complete per-entity rollups.

## Preferred Serving Path

Serve dashboards, leaderboards, summary panels, and alert context from `report_results` when its materializer exists for this report type.

Use raw events for:

- Clicking a row to inspect that entity's timeline.
- Debugging why an entity ranked highly.
- Rebuilds, backfills, and validation.
- Low-volume or one-off exploratory queries before promotion is justified.

## Caveats

- Top-N is not the same as complete group-by coverage; make that limitation explicit.
- Do not imply complete all-entity aggregation when only a bounded top K is persisted.
- Set a practical `limit`, retention policy, and refresh cadence.
- Decide how missing entity IDs are handled. Prefer skipping missing entity keys for entity leaderboards unless product semantics require an "unknown" bucket.
- Avoid 1-second buckets for high-cardinality entity ranking unless there is a strong product reason and quota controls.
- Be careful with revenue or impact metrics: define units, deduplication, and late event handling before promotion.
