# Nanotrace Deployment Lifecycle Reference

Use this reference when a deployment question needs step-by-step guidance.

## Operating Rule

Guide one checkpoint at a time. The user may leave to create cloud resources, run commands, approve DNS changes, or inspect consoles, then return with partial output. Resume from the latest returned state instead of restarting the lifecycle.

At the end of each active guidance turn, say what to bring back. Good return artifacts include:

- Exact command output.
- Pulumi stack name and selected stack.
- Provider IDs, URLs, hosted zone names, and DNS records.
- Error text.
- API health response status.
- E2E script final lines.
- AWS/GitHub console facts stated in text.

Do not ask the user to run a deploy, destroy, scale, or prod-changing command unless they have explicitly authorized that kind of action in the current thread.

## Checkpoint Map

Use this map to order the multi-turn flow:

1. Local prerequisites: Node, AWS auth, Pulumi CLI, Docker/buildx, selected repo branch.
2. External services: Kafka, ClickHouse, Iceberg catalog, DNS, email, Postgres decision.
3. Staging stack config: Pulumi stack, environment variables, secrets source.
4. Preview: review expected resources and failures.
5. First staging deploy: run roll only after explicit approval.
6. Staging verification: health, readiness, login/email, E2E, query.
7. CI identity: GitHub OIDC role and scoped AWS policies.
8. CI secrets/environments: staging first, prod protected.
9. CI staging deploy: clean checkout deploy and E2E.
10. Prod stack: configure, preview, manually approve, deploy, E2E.
11. Ops hardening: alarms, dashboards, backups, runbooks, rollback drills.

## Phase 1: Bootstrap

Goal: create one working environment, usually `staging`, even if the first deploy is from a laptop.

Checkpoint 1: local prerequisites.

Ask the user to run/read:

```sh
export NVM_DIR="$HOME/.nvm"; . "$NVM_DIR/nvm.sh"; nvm use 22.17.1
node -v
npm -v
pulumi version
aws sts get-caller-identity
docker version
git status --short
```

Ask them to bring back versions, AWS account ID, active branch, and whether the worktree has unrelated changes.

Checkpoint 2: external service choices.

- Confirm external service choices:
  - Kafka: WarpStream, Confluent Cloud, or MSK.
  - ClickHouse: ClickHouse Cloud.
  - Iceberg: REST catalog plus S3 warehouse.
  - DNS: Cloudflare or Route53.
  - Postgres: Pulumi-managed RDS unless an existing managed Postgres is clearly preferred.
  - Email: SES for magic-link login.

Ask the user to bring back service endpoints and whether credentials are available in a secret manager or shell environment. Do not ask them to paste secrets.

Checkpoint 3: staging stack config.

- Create/select Pulumi stack:

```sh
cd deploy/pulumi/nanotrace
pulumi stack init staging
pulumi stack select staging
```

- Populate environment from `.env.example`; minimum important variables:
  - `AWS_REGION`
  - `NANOTRACE_DOMAIN_NAME`
  - `NANOTRACE_ADMIN_EMAILS`
  - `CLICKHOUSE_URL`
  - `CLICKHOUSE_USER`
  - `CLICKHOUSE_PASSWORD`
  - `NANOTRACE_KAFKA_BROKERS`
  - Kafka TLS/SASL variables when required.
  - `NANOTRACE_ICEBERG_REST_URI`
  - DNS provider credentials such as `CLOUDFLARE_API_TOKEN` and `CLOUDFLARE_ZONE_ID` when using Cloudflare.

Ask them to bring back `pulumi stack --show-name`, the non-secret environment variable names they set, and any missing required values.

Checkpoint 4: staging preview.

- Preview before any apply:

```sh
export NVM_DIR="$HOME/.nvm"; . "$NVM_DIR/nvm.sh"; nvm use 22.17.1
npm run deploy:preview
```

Ask them to bring back the final preview summary and any errors. Review the diff before moving to deploy.

Checkpoint 5: first staging deploy.

- Deploy only after the user explicitly wants to mutate cloud resources:

```sh
npm run deploy:roll -- --build-id "manual-$(date +%Y%m%d%H%M%S)"
npm run e2e:pulumi
```

Ask them to bring back the deploy roll final status, exported URLs, ASG refresh status if shown, and E2E final lines.

Completion criteria:

- ALB/API URL is reachable.
- UI URL is reachable.
- Login email/DNS path is understood or verified.
- `POST /v1/events` returns `202` with a valid key.
- E2E event reaches ClickHouse and query paths work.

## Phase 2: Working Deploy

Goal: make deploy repeatable and unsurprising.

Checkpoint 1: immutable deploy.

- Keep `scripts/deploy-roll-pulumi.mjs` as the roll primitive. It sets image build/tag config, runs `pulumi up`, starts ASG instance refreshes, and waits for app/query groups.
- Use immutable build IDs:

```sh
npm run deploy:roll -- --build-id "$GIT_SHA"
```

Ask the user to bring back the image build ID and instance refresh result.

Checkpoint 2: outputs and verification.

- Confirm Pulumi outputs after deploy:

```sh
pulumi -C deploy/pulumi/nanotrace stack output
```

- Run post-deploy verification:

```sh
npm run e2e:pulumi
```

Ask the user to bring back the output names/URLs, not secret values, plus E2E pass/fail and any first error.

Checkpoint 3: UI-only path, only if relevant.

- If UI-only changes need separate handling, use the existing UI deploy path only after checking Pulumi output env vars:
  - `NANOTRACE_UI_BUCKET`
  - `NANOTRACE_UI_DISTRIBUTION_ID`

Completion criteria:

- Re-running preview after deploy is clean or only contains expected drift.
- Instance refresh succeeds for ingest/app and query ASGs.
- E2E passes without manual ClickHouse or Kafka intervention.

## Phase 3: CI Ownership

Goal: make CI the normal deployer and keep laptop deployment as break-glass.

Checkpoint 1: GitHub environments and permissions.

- Create GitHub environments:
  - `staging`
  - `prod`
- Require manual approval for `prod`.
- Configure AWS OIDC role assumption; do not store long-lived AWS keys in GitHub secrets.
- Attach normal deploy policies from:
  - `deploy/pulumi/nanotrace/iam/deploy-storage.json`
  - `deploy/pulumi/nanotrace/iam/deploy-compute.json`
  - `deploy/pulumi/nanotrace/iam/deploy-image.json`
  - `deploy/pulumi/nanotrace/iam/deploy-iam.json`
- Do not attach `cleanup.json` to routine CI deploy roles.

Ask the user to bring back the role ARN, allowed repository/branch conditions, and which policies are attached.

Checkpoint 2: secrets and config.

- Store environment-specific secrets in GitHub Environments:
  - Pulumi backend/access token.
  - ClickHouse URL/user/password.
  - Kafka brokers and credentials.
  - Iceberg REST URI and warehouse settings if externally provided.
  - DNS provider credentials.
  - Nanotrace domain/admin/email settings.

Ask the user to bring back only the secret names configured, not secret values.

Checkpoint 3: workflows.

- Add workflows:
  - PR checks: Rust fmt/clippy, npm typecheck/build.
  - PR or main preview: `npm run deploy:preview`.
  - Staging deploy on merge: `npm run deploy:roll -- --build-id "$GITHUB_SHA"` then `npm run e2e:pulumi`.
  - Manual prod deploy: same deploy and E2E, with protected environment approval.

Minimum CI command skeleton:

```sh
export NVM_DIR="$HOME/.nvm"; . "$NVM_DIR/nvm.sh"; nvm use 22.17.1
npm ci
npm run deploy:preview
npm run deploy:roll -- --build-id "$GITHUB_SHA"
npm run e2e:pulumi
```

Ask the user to bring back the workflow run URL/status and the first failing step if any.

Completion criteria:

- A clean checkout can deploy staging without local shell state.
- Prod requires approval.
- Every deploy has a unique build ID visible in Pulumi/ECR/ASG rollout history.

## Phase 4: Ops Tightening

Goal: make production diagnosable, recoverable, and bounded.

Add monitoring and alarms for:

- ALB target health, 5xx rate, latency, and request volume.
- EC2 ASG instance refresh failure and unhealthy instance count.
- Server `/healthz`, `/readyz`, and `/metrics` availability.
- Kafka broker availability, produce errors, consumer lag for normalizer and alerts.
- ClickHouse insert errors, query failures, slow queries, bytes read limits, and storage growth.
- Materializer/rebuild loop freshness and serving watermarks.
- RDS CPU, storage, connections, and backup status.
- S3/Iceberg warehouse object growth and small-file pressure.
- SES bounce/complaint rate and login email delivery failures.

Create runbooks for:

- Failed deploy or ASG refresh.
- Rollback to a previous image build ID.
- DNS/certificate validation failure.
- Kafka ingest accepted but no ClickHouse row appears.
- ClickHouse reachable but `/v1/query` fails.
- Magic-link login email not delivered.
- RDS restore or credential rotation.
- Iceberg catalog/warehouse outage.

Access hardening:

- Separate bootstrap, deploy, observe, and cleanup roles.
- Keep cleanup permissions out of deploy CI.
- Use SSO/OIDC where possible.
- Rotate provider tokens and Kafka/ClickHouse credentials.
- Document who owns each external managed service.

Completion criteria:

- On-call can answer "is ingest healthy?", "is normalization caught up?", "is serving fresh?", and "what build is running?" without SSH.
- There is a known rollback path.
- Backups and retention are tested, not just configured.

## Incident/Recovery Triage

Start with the symptom and isolate the layer:

- API unreachable: DNS, TLS, ALB listener, target health, security groups, ASG.
- `POST /v1/events` not `202`: auth/API key, server config, Kafka brokers/credentials/topic.
- `202` accepted but no row: normalizer lag/error, Iceberg commit, materializer, ClickHouse insert.
- Query broken but rows exist: query ASG, ClickHouse credentials, schema drift, freshness/watermark checks.
- UI broken but API works: S3 UI bucket, CloudFront distribution, invalidation, `VITE_NANOTRACE_URL`, CORS/session settings.
- Login broken: SES identity/DKIM/Mail From, DNS records, admin/allowed email config, session cookie settings.

Prefer read-only commands first: `pulumi stack output`, AWS describe APIs, ClickHouse SELECTs, Kafka lag tools, and HTTP health checks. Escalate to deploy/rollback only after identifying the failing layer.
