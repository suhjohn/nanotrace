---
name: nanotrace-cohort-retention-promotion
description: Promote Nanotrace queries that ask for cohort membership, retention, or activity-window analysis into cohort membership and report-style materialization.
---

# Nanotrace Cohort Retention Promotion

Use this skill when a Nanotrace request is really asking "which entities belong to a cohort, and what did they do in later activity windows?"

## Trigger When Observed

- The query defines an initial cohort from an event, span, trace attribute, user/account id, experiment arm, plan, region, or timestamp window.
- The answer depends on later activity windows such as D1/D7/D30 retention, reactivation, churn, repeat usage, expansion, or follow-up conversion.
- The user asks for per-cohort counts, rates, or breakdowns over time rather than raw trace inspection.
- The same cohort definition will likely be reused across dashboard tiles, scheduled reports, or drilldowns.
- The current ad hoc query would repeatedly scan large trace/event windows to rebuild the same membership set.

## Observation Signals

- Phrases like "users who first did X", "accounts active after signup", "retained", "returned", "cohort", "activation", "conversion after", "churned", or "same set over time".
- Joins from a first-touch or membership window to one or more later activity windows.
- Repeated `distinct user_id`, `account_id`, `tenant_id`, or entity key extraction before aggregating later behavior.
- Dashboard parameters that change the cohort date range, segment, or activity horizon.

## Action Sketch

- Materialize cohort identity into `cohort_memberships` when the current code has that worker; otherwise add the membership materializer as part of the promotion.
- Store the promoted report output in `report_results` only when a report executor exists for this definition shape.
- Use stable entity keys such as user, account, workspace, org, or service id.
- Capture cohort definition, cohort window, entity key, segment dimensions, and activity windows in the report config.
- Precompute membership once, then compute activity-window metrics against the membership table.

Example config shape:

```yaml
promotion: cohort_retention
membership_source: traces_or_events
membership_table: cohort_memberships
result_table: report_results
entity_key: user_id
cohort_window:
  start_param: start_date
  end_param: end_date
activity_windows: [d1, d7, d30]
dimensions: [plan, region, experiment_arm]
metrics: [members, active_members, retention_rate]
```

## Preferred Serving Path

- Serve dashboards and recurring analyses from `report_results` when the materializer exists.
- Use `cohort_memberships` for drilldowns into member sets and follow-up activity windows when membership materialization is implemented.
- Fall back to raw trace/event queries only for validating a sample, debugging promotion correctness, or handling one-off exploratory slices.

## Caveats

- Require a stable entity id. Do not promote if identity is inferred from volatile free-form fields.
- Separate schema/design intent from implemented serving paths; add missing executors explicitly.
- Keep cohort definitions versioned so result changes are explainable.
- Bound activity windows explicitly; open-ended windows can become unbounded scans.
- Avoid using this promotion for arbitrary JSON regex searches, full lifecycle replay, or cross-tenant analytics.
