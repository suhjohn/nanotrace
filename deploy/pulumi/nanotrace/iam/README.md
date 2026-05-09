# Nanotrace IAM Policies

These policies support the Pulumi EC2/EBS/S3/SQS/ECR deployment in
`deploy/pulumi/nanotrace`.

- `bootstrap.json`: for an administrator to create and attach the Nanotrace
  deploy identities and managed policies.
- `deploy-storage.json`, `deploy-compute.json`, `deploy-image.json`, and
  `deploy-iam.json`: attach all four to the user or CI role that runs
  `pulumi up` and pushes the server image to ECR. These are split because AWS
  managed policies have a 6144 non-whitespace character limit.
- `observe.json`: read-only inspection plus enough S3/SQS access to run the
  deploy-aware E2E.
- `cleanup.json`: for `pulumi destroy` and cleanup of partially-created
  Nanotrace resources.

The resource scope intentionally uses `nanotrace-*` names because this stack
creates unique physical names with Pulumi suffixes, for example
`nanotrace-prod-events-035300c`.

`deploy-iam.json` includes `iam:PassRole` only for the Nanotrace instance-role
pattern and only when passed to `ec2.amazonaws.com`. The stack needs that
because EC2 launch templates pass the instance role to launched instances.

Remaining `Resource: "*"` grants are limited to APIs that either do not support
resource-level permissions or are read/list discovery calls Pulumi uses during
refresh:

- EC2, Auto Scaling, and ELB `Describe*`
- SQS `ListQueues`
- ECR `GetAuthorizationToken`
- account/caller identity reads
