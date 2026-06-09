---
name: bootstrap
description: "Guide a user through first-time Nanotrace Pulumi bootstrap. Use when Codex is asked to bootstrap Nanotrace, set up a Pulumi stack, configure staging/prod infrastructure, prepare environment variables for deploy, run Pulumi preview, or walk through cloud setup decision points. Emphasizes sequential guidance: execute safe local inspection automatically, but pause for user choices, secrets, external-service setup, DNS, production approval, and any cloud-mutating Pulumi action."
---

# Bootstrap

Use this skill for first-time Nanotrace Pulumi setup. The job is to guide the
user through one decision point at a time, not dump the whole deployment manual.

Use `nanotrace-deployment-lifecycle` as the companion skill when the request
moves beyond bootstrap into repeatable deploy, CI ownership, production rollout,
or incident recovery.

## Operating Contract

- Read root `AGENTS.md` first. For Node/npm commands, load NVM and use Node
  `22.17.1`:

```sh
export NVM_DIR="$HOME/.nvm"; . "$NVM_DIR/nvm.sh"; nvm use 22.17.1
```

- Execute safe local inspection without asking when it helps: file reads, git
  status, tool versions, package script discovery, and non-secret env-name
  checks.
- Ask before commands that mutate cloud resources or external services:
  `pulumi up`, `npm run deploy:roll`, `npm run deploy:destroy`, scaling, DNS
  edits, SES verification changes, WarpStream changes, ClickHouse changes, or
  production deploys.
- Never ask the user to paste secret values. Ask for variable names present,
  placeholder status, account IDs, resource IDs, URLs, or error text.
- Keep each turn bounded. End active guidance with exactly what to run/do next
  and what output to bring back.
- Prefer `staging` as the first stack unless the user explicitly chooses prod.
- Treat `.env.example` as the checklist. Deploy commands read process env; `.env`
  is not loaded automatically.

## Decision Protocol

Classify each step as one of these:

- **Execute**: safe local read-only checks Codex can run directly.
- **Ask**: user preference or external account choice is needed.
- **Instruct**: user must perform an external-console or secret-manager action.
- **Approve**: cloud mutation or production action requires explicit approval.
- **Resume**: user returned output; parse it and advance one checkpoint.

Use this response shape while guiding:

```text
Phase: Pulumi Bootstrap
Checkpoint: <short checkpoint>
Decision: <Execute | Ask | Instruct | Approve | Resume>
Known: <short facts>
Need: <specific missing choice/output>
Next step:
  <one command group or external action>
Bring back:
  <exact output/facts needed>
Stop before:
  <next risky or dependent step>
```

If the user asks Codex to "do as much as possible," execute only `Execute`
steps, then stop at the next `Ask`, `Instruct`, or `Approve` step.

## Checkpoints

### 1. Local Readiness

Decision: **Execute**.

Run safe checks:

```sh
git status --short
git branch --show-current
command -v pulumi || true
command -v aws || true
command -v docker || true
export NVM_DIR="$HOME/.nvm"; . "$NVM_DIR/nvm.sh"; nvm use 22.17.1
node -v
npm -v
```

If credentials are already configured, `aws sts get-caller-identity` is safe
read-only and useful. If it fails, ask the user which AWS auth method they want
to use.

Stop when any required local tool is missing or AWS identity is unclear.

### 2. Target Stack

Decision: **Ask**.

Ask the user to choose:

- `staging` for the first bootstrap.
- `prod` only if staging already works and they explicitly want production.
- A custom stack name only if they need a separate sandbox.

Do not initialize or select a stack until the target is clear.

### 3. External Service Choices

Decision: **Ask**.

Confirm choices before writing config or previewing:

- AWS region.
- Domain name and DNS provider. DNS is manual.
- Postgres provider; expected default is PlanetScale Postgres over PrivateLink.
- Kafka provider; expected default is WarpStream.
- Iceberg/Tableflow path; expected default is WarpStream Tableflow into the
  Pulumi-created S3 bucket.
- ClickHouse provider; expected default is ClickHouse Cloud.
- Login method: magic-link email only, or magic-link plus Google OAuth.

If the user accepts defaults, proceed with the default checklist. If they choose
a different provider, inspect repo scripts and Pulumi config before advising.

### 4. Environment Checklist

Decision: **Execute** for names only, then **Ask/Instruct** for missing values.

Inspect `.env.example`, `package.json`, and Pulumi scripts. If `.env` exists,
check only variable names and placeholder-looking values; do not print secrets.

Required names:

```text
AWS_REGION
NANOTRACE_DOMAIN_NAME
DATABASE_URL
PLANETSCALE_PRIVATELINK_SERVICE_NAME
CLICKHOUSE_URL
CLICKHOUSE_USER
CLICKHOUSE_PASSWORD
CLICKHOUSE_DATABASE
NANOTRACE_KAFKA_BROKERS
NANOTRACE_KAFKA_SECURITY_PROTOCOL
NANOTRACE_KAFKA_SASL_MECHANISM
NANOTRACE_KAFKA_SASL_USERNAME
NANOTRACE_KAFKA_SASL_PASSWORD
```

Required AWS auth is either:

```text
AWS_PROFILE
```

or:

```text
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
```

Conditional names:

```text
NANOTRACE_EMAIL_FROM
NANOTRACE_GOOGLE_OAUTH_CLIENT_ID
NANOTRACE_GOOGLE_OAUTH_CLIENT_SECRET
NANOTRACE_GOOGLE_OAUTH_REDIRECT_URI
NANOTRACE_KAFKA_TABLEFLOW_TOPIC
```

Ask the user to set missing values through their shell, `.env`, or secret
manager. Do not continue to preview until required names are present.

### 5. Pulumi Stack Setup

Decision: **Ask** before creating a new stack; **Execute** for read-only stack
inspection.

Safe inspection:

```sh
pulumi -C deploy/pulumi/nanotrace stack ls
pulumi -C deploy/pulumi/nanotrace stack --show-name
```

If the target stack does not exist, ask before running:

```sh
pulumi -C deploy/pulumi/nanotrace stack init <stack>
```

Selecting an existing stack is low risk, but still state what is being selected:

```sh
pulumi -C deploy/pulumi/nanotrace stack select <stack>
```

### 6. Preview

Decision: **Execute** if env is loaded and target stack is selected.

Use the repo command:

```sh
export NVM_DIR="$HOME/.nvm"; . "$NVM_DIR/nvm.sh"; nvm use 22.17.1
npm run deploy:preview
```

Summarize only the important preview result: resources to create/change/delete,
missing config, auth failures, provider failures, and any risky deletes.

Stop after preview. Do not run deploy from the same step unless the user already
explicitly approved cloud mutation.

### 7. First Deploy

Decision: **Approve**.

Ask for explicit approval before running:

```sh
export NVM_DIR="$HOME/.nvm"; . "$NVM_DIR/nvm.sh"; nvm use 22.17.1
npm run deploy:roll -- --build-id "manual-$(date +%Y%m%d%H%M%S)"
```

After deploy, collect outputs:

```sh
pulumi -C deploy/pulumi/nanotrace stack output
pulumi -C deploy/pulumi/nanotrace stack output manualDnsRecordsOutput
```

Stop for DNS, certificate, SES, or Tableflow configuration actions as needed.

### 8. Manual DNS and Email

Decision: **Instruct**.

Ask the user to create the DNS records from `manualDnsRecordsOutput` in their
DNS provider. For UI custom domains, expect a two-pass flow:

1. First deploy outputs ACM validation records.
2. User creates DNS validation records.
3. ACM issues the certificate.
4. User approves a second deploy/roll so CloudFront can attach the alias.

For SES, ask for verification status only. Do not ask for mailbox credentials.

### 9. WarpStream Tableflow

Decision: **Instruct**.

Use the Pulumi S3 bucket output as the Tableflow destination bucket. Confirm the
source topic, normally:

```text
events.tableflow.batches.v1
```

Tell the user to bring back Tableflow agent health, save/preview status, and any
schema or S3 permissions error. If Tableflow fails, inspect IAM/Pulumi outputs
and the Tableflow error before suggesting changes.

### 10. Verification

Decision: **Execute** for read-only/local verification, **Approve** if it posts
to a live prod endpoint.

For staging, run:

```sh
export NVM_DIR="$HOME/.nvm"; . "$NVM_DIR/nvm.sh"; nvm use 22.17.1
npm run e2e:pulumi
```

Bootstrap is complete when:

- API/ALB URL is reachable.
- UI URL is reachable.
- Auth path is understood or verified.
- Events ingest with `202`.
- Normalizer publishes to Kafka/Tableflow topic.
- ClickHouse query path returns the test event.
- DNS/certificate/email remaining tasks are either complete or explicitly
  tracked as follow-ups.

## Handoff

End with:

- Stack name and AWS account/region.
- What was created or changed.
- Current checkpoint.
- Commands run by Codex versus actions performed by the user.
- Required next action, if any.
- Known blockers and exact output needed to continue.
