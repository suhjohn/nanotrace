---
name: nanotrace-deployment-lifecycle
description: Run Nanotrace deployment as a multi-turn guided lifecycle from first cloud bootstrap through repeatable CI deploys and operational hardening. Use when Codex is asked how to deploy Nanotrace, bootstrap staging or prod, move deployment from a laptop to CI, configure Pulumi/AWS/DNS/Kafka/ClickHouse/Iceberg/Postgres, guide the user through intermediate deployment steps, resume from returned command output or process state, write or review deployment runbooks, add deployment automation, or tighten post-deploy observability and rollback practices.
---

# Nanotrace Deployment Lifecycle

Use this skill to operate deployment as a guided, multi-turn process. Prefer the repo's existing Pulumi and script surface over inventing a new platform.

## First Checks

- Read root `AGENTS.md`; Node/npm commands must load NVM and use Node `22.17.1`:

```sh
export NVM_DIR="$HOME/.nvm"; . "$NVM_DIR/nvm.sh"; nvm use 22.17.1
```

- Inspect current deployment files before advising or editing:
  - `.env.example`
  - `package.json`
  - `scripts/pulumi-nanotrace.mjs`
  - `scripts/deploy-roll-pulumi.mjs`
  - `scripts/e2e-pulumi.mjs`
  - `deploy/pulumi/nanotrace/index.ts`
  - `deploy/pulumi/nanotrace/iam/README.md`
- Treat `npm run deploy:roll` as the canonical deploy command unless the user asks to redesign deployment.
- Do not run live deploy, destroy, scale, DNS, or production-mutating commands unless the user explicitly asks for that action in the current turn.

## Multi-Turn Protocol

Deployment is multi-turn by default. Do not dump the whole lifecycle unless the user explicitly asks for a full plan.

Each turn should:

1. Identify the current phase and checkpoint.
2. State what is known, what is missing, and the next smallest useful action.
3. Give the user one bounded command group or one external-service action to perform.
4. Tell the user exactly what output or state to bring back.
5. Stop before the next cloud-mutating or externally-owned step.

Use this response shape for active guidance:

```text
Phase: <Bootstrap | Working Deploy | CI Ownership | Ops Tightening | Incident/Recovery>
Checkpoint: <short current checkpoint>
Next step: <one bounded action>
Run/do:
  <commands or external-console action>
Bring back:
  <specific command output, screenshot facts, IDs, URLs, or error text>
Do not continue to:
  <next risky step that should wait>
```

When the user returns with output, parse it, update the checkpoint, and either advance to the next step or troubleshoot the failing layer. If output is incomplete, ask for the minimum missing detail.

## Phase Decision

Classify the request into one phase and answer from that phase forward:

- **Bootstrap**: first working staging/prod, laptop acceptable, external services selected, Pulumi stack configured.
- **Working Deploy**: deploy succeeds repeatably, app rolls through ASGs, UI publishes, E2E passes.
- **CI Ownership**: GitHub Actions or similar runs preview/deploy with OIDC and environment-scoped secrets.
- **Ops Tightening**: alarms, dashboards, backup, rollback, runbooks, access boundaries, cost and capacity checks.
- **Incident/Recovery**: failed deploy, broken DNS/cert/email, failing ASG refresh, missing data in ClickHouse, Kafka/materializer lag.

For checkpoint ordering and what the user should bring back after each step, read `references/lifecycle.md`.

## Default Architecture

Use this as the recommended target unless repo code or user constraints say otherwise:

- AWS Pulumi stack for VPC, ALB, EC2 Auto Scaling Groups, ECR, S3, CloudFront, optional RDS, IAM, ACM, Route53/Cloudflare records, and SES email identity.
- External managed Kafka.
- External ClickHouse Cloud; the stack applies schema but does not provision the service.
- Iceberg REST catalog plus S3 warehouse.
- Pulumi-managed RDS Postgres by default; external Postgres only when there is an existing managed service with a clear owner.
- Separate Pulumi stacks for `staging` and `prod`.
- CI deploys with AWS OIDC and immutable build IDs; laptop deploys only for bootstrap or break-glass operations.

## Canonical Commands

Use NVM first, then:

```sh
npm run deploy:preview
npm run deploy:roll -- --build-id "$GIT_SHA"
npm run e2e:pulumi
```

For initial laptop bootstrap, if no commit SHA is available:

```sh
npm run deploy:roll -- --build-id "manual-$(date +%Y%m%d%H%M%S)"
```

## Guidance Rules

- Recommend staging before prod.
- Advance one checkpoint at a time when actively guiding deployment.
- Prefer GitHub Actions OIDC over long-lived AWS keys.
- Keep `cleanup.json` separate from normal deploy permissions.
- Require manual approval for prod deploys.
- Keep deploy secrets in CI environments, not committed Pulumi YAML or shell history.
- Use immutable image/build IDs; avoid `latest` for real deploys.
- Make `npm run e2e:pulumi` the minimum post-deploy gate.
- When writing automation, preserve the existing deploy scripts unless replacing them is explicitly justified.
- When answering "what now?", provide only the next checkpoint unless the user asks for a broader roadmap.

## Validation Expectations

For deployment documentation or workflow edits, run:

```sh
python3 /Users/johnsuh/.codex/skills/.system/skill-creator/scripts/quick_validate.py skills/nanotrace-deployment-lifecycle
```

For code changes touching deployment scripts or Pulumi config, also run focused checks when practical:

```sh
export NVM_DIR="$HOME/.nvm"; . "$NVM_DIR/nvm.sh"; nvm use 22.17.1
npm run typecheck
```

Do not run `pulumi up`, `deploy:roll`, `deploy:destroy`, or cloud-changing scripts as validation without explicit user intent.
