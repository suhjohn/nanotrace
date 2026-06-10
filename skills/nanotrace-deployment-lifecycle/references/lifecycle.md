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
2. Environment checklist: required deploy env names present, no obvious placeholders, no secrets pasted.
3. External services: WarpStream Kafka topics, ClickHouse, DNS, email, PlanetScale Postgres.
4. Preview: review expected resources and failures.
5. First deploy: run roll only after explicit approval.
6. Post-deploy outputs: bucket, URLs, DNS/email outputs, ASG names.
7. WarpStream Tableflow: configure destination bucket, source topic, schema, agent env, and S3 access.
8. Verification: health, readiness, login/email, event ingest, Tableflow, E2E/query.
9. CI identity: GitHub OIDC role and scoped AWS policies.
10. CI secrets/environments: staging first, prod protected.
11. CI staging deploy: clean checkout deploy and E2E.
12. Prod stack: configure, preview, manually approve, deploy, E2E.
13. Ops hardening: alarms, dashboards, backups, runbooks, rollback drills.

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

Checkpoint 2: environment checklist.

Validate `.env` or the active secret-manager environment by variable name only. Never print secret values.

Required deploy names:

- `AWS_REGION`
- One AWS auth method: `AWS_PROFILE` or `AWS_ACCESS_KEY_ID` plus `AWS_SECRET_ACCESS_KEY`.
- `NANOTRACE_DOMAIN_NAME`
- `DATABASE_URL`
- `PLANETSCALE_PRIVATELINK_SERVICE_NAME`
- `CLICKHOUSE_URL`
- `CLICKHOUSE_USER`
- `CLICKHOUSE_PASSWORD`
- `CLICKHOUSE_DATABASE`
- `NANOTRACE_KAFKA_BROKERS`
- `NANOTRACE_KAFKA_SECURITY_PROTOCOL`
- `NANOTRACE_KAFKA_SASL_MECHANISM`
- `NANOTRACE_KAFKA_SASL_USERNAME`
- `NANOTRACE_KAFKA_SASL_PASSWORD`

Recommended or conditional:

- `NANOTRACE_AWS_ACCOUNT_ID`
- DNS is manual. No DNS provider credentials are required by deploy.
- UI custom-domain TLS is a two-pass flow: first deploy outputs an ACM validation CNAME; after the user creates it and ACM issues the certificate, rerun deploy so CloudFront gets the custom domain alias.
- `NANOTRACE_EMAIL_FROM` only when overriding the default `login@mail.<domain>`.
- `NANOTRACE_GOOGLE_OAUTH_CLIENT_ID`, `NANOTRACE_GOOGLE_OAUTH_CLIENT_SECRET`, and optionally `NANOTRACE_GOOGLE_OAUTH_REDIRECT_URI` when enabling Google login.
- `NANOTRACE_KAFKA_TABLEFLOW_TOPIC` only when overriding `events.tableflow.batches.v1`.
- `NANOTRACE_KAFKA_ICEBERG_TOPIC` only when overriding `events.iceberg.rows.v1`.

Ask the user to bring back only missing variable names, placeholder warnings, and selected DNS provider.

Checkpoint 3: external service setup.

- Confirm external service choices:
  - Kafka: WarpStream.
  - Iceberg: WarpStream Tableflow managed table over the Tableflow Kafka topic.
  - ClickHouse: ClickHouse Cloud.
  - DNS: manual.
  - Postgres: PlanetScale Postgres over PrivateLink.
  - Email: SES for magic-link login.
- In WarpStream Kafka, confirm topics exist:
  - `events.ingest.v1`
  - `events.normalized.v1`
  - `events.invalid.v1`
  - `events.tableflow.batches.v1`
  - `events.iceberg.rows.v1`
- If WarpStream ACLs are disabled, no ACL rules are needed. If enabling ACLs, configure topic and consumer-group ACLs before enabling; otherwise non-superuser clients will be blocked.
- Create a WarpStream Tableflow cluster, but expect to finish the Tableflow destination bucket after Nanotrace deploy outputs the S3 bucket.

Ask the user to bring back service endpoints and whether credentials are available in a secret manager or shell environment. Do not ask them to paste secrets.

Checkpoint 4: stack config.

- Create/select Pulumi stack:

```sh
cd deploy/pulumi/nanotrace
pulumi stack init staging
pulumi stack select staging
```

- Populate environment from `.env.example`; minimum important variables:
  - Use the names from checkpoint 2.
  - Deploy commands read process env, not `.env` automatically:

```sh
set -a
source .env
set +a
```

Ask them to bring back `pulumi stack --show-name`, the non-secret environment variable names they set, and any missing required values.

Checkpoint 5: preview.

- Preview before any apply:

```sh
export NVM_DIR="$HOME/.nvm"; . "$NVM_DIR/nvm.sh"; nvm use 22.17.1
npm run deploy:preview
```

Ask them to bring back the final preview summary and any errors. Review the diff before moving to deploy.

Checkpoint 6: first deploy.

- Deploy only after the user explicitly wants to mutate cloud resources:

```sh
npm run deploy:roll -- --build-id "manual-$(date +%Y%m%d%H%M%S)"
npm run e2e:pulumi
```

Ask them to bring back the deploy roll final status, exported URLs, ASG refresh status if shown, and E2E final lines.

Checkpoint 7: post-deploy outputs.

Collect outputs needed by external systems:

```sh
pulumi -C deploy/pulumi/nanotrace stack output
pulumi -C deploy/pulumi/nanotrace stack output bucketName
pulumi -C deploy/pulumi/nanotrace stack output manualDnsRecordsOutput
```

Ask the user to bring back output names and non-secret values such as URLs, bucket name, DNS record names, and current email verification status.

Checkpoint 8: WarpStream Tableflow configuration.

In the Tableflow Configuration editor, define the source cluster, source topic, table schema, and destination bucket. Use the bucket from checkpoint 7.

Current canonical source topic:

```yaml
source_topic: events.iceberg.rows.v1
```

Minimal YAML shape:

```yaml
source_clusters:
  - name: nanotrace_prod
    bootstrap_brokers:
      - hostname: <warpstream-bootstrap-host>
        port: 9092

tables:
  - source_cluster_name: nanotrace_prod
    source_topic: events.iceberg.rows.v1
    source_format: json
    schema_mode: inline
    input_schema: |
      {
        "type": "object",
        "required": [
          "schema_version",
          "batch_id",
          "tenant_id",
          "organization_id",
          "received_at",
          "ingest_source_topic",
          "ingest_source_partition",
          "ingest_source_offset",
          "event_index",
          "event_id",
          "timestamp",
          "data_json"
        ],
        "properties": {
          "schema_version": { "type": "integer" },
          "batch_id": { "type": "string" },
          "tenant_id": { "type": "string" },
          "organization_id": { "type": "string" },
          "received_at": { "type": "string" },
          "ingest_source_topic": { "type": "string" },
          "ingest_source_partition": { "type": "integer" },
          "ingest_source_offset": { "type": "integer" },
          "event_index": { "type": "integer" },
          "event_id": { "type": "string" },
          "timestamp": { "type": "string" },
          "observed_timestamp": { "type": "string" },
          "ingested_timestamp": { "type": "string" },
          "data_json": { "type": "string" }
        }
      }
    partitioning_scheme: hour
    dlq_mode: stop
    compression: zstd

destination_bucket_url: s3://<bucketName>?region=<AWS_REGION>
```

Important operational notes:

- If Tableflow connects through Nanotrace's internal WarpStream Kafka NLB, no SASL credentials are required in the source cluster config.
- Tableflow agents need S3 access to `s3://<bucketName>/warpstream/_tableflow/*` and relevant bucket list/read/write permissions.
- Nanotrace emits one JSON object per Kafka record to `events.iceberg.rows.v1`; `data_json` contains the original normalized event payload as a JSON string.

Ask the user to bring back the Tableflow preview/save result, agent health, and any schema or permissions error.

Completion criteria:

- ALB/API URL is reachable.
- UI URL is reachable.
- Login email/DNS path is understood or verified.
- `POST /v1/events` returns `202` with a valid key.
- Normalizer publishes batch records to `events.tableflow.batches.v1` for the
  ClickHouse materializer and per-event records to `events.iceberg.rows.v1` for
  WarpStream Tableflow.
- Tableflow agents are healthy and writing Iceberg files under the configured bucket prefix.
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
  - WarpStream Tableflow cluster/configuration credentials if managed through CI.
  - Nanotrace domain, email sender, OAuth, and session settings.

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
- WarpStream Tableflow lag and bad-record/error status.
- ClickHouse insert errors, query failures, slow queries, bytes read limits, and storage growth.
- Materializer/rebuild loop freshness and serving watermarks.
- PlanetScale connection health and backup status.
- Tableflow object-storage growth and managed Iceberg table health.
- SES bounce/complaint rate and login email delivery failures.

Create runbooks for:

- Failed deploy or ASG refresh.
- Rollback to a previous image build ID.
- DNS/certificate validation failure.
- Kafka ingest accepted but no ClickHouse row appears.
- ClickHouse reachable but `/v1/query` fails.
- Magic-link login email not delivered.
- PlanetScale restore or credential rotation.
- WarpStream Tableflow or object-storage outage.

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
- `202` accepted but no row: normalizer lag/error, Tableflow topic lag, materializer, ClickHouse insert.
- Query broken but rows exist: query ASG, ClickHouse credentials, schema drift, freshness/watermark checks.
- UI broken but API works: S3 UI bucket, CloudFront distribution, invalidation, `VITE_NANOTRACE_URL`, CORS/session settings.
- Login broken: SES identity/DKIM/Mail From, DNS records, database auth state, session cookie settings.

Prefer read-only commands first: `pulumi stack output`, AWS describe APIs, ClickHouse SELECTs, Kafka lag tools, and HTTP health checks. Escalate to deploy/rollback only after identifying the failing layer.
