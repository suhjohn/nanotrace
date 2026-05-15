# Nanotrace IAM Policies

These policies support the Pulumi EC2/EBS/S3/SQS/ECR/RDS/KMS deployment in
`deploy/pulumi/nanotrace`. ClickHouse Cloud service creation is handled by the
ClickHouse Cloud API credentials, not AWS IAM.

- `bootstrap.json`: for an administrator to create and attach the Nanotrace
  deploy identities and managed policies.
- `deploy-storage.json`, `deploy-compute.json`, `deploy-image.json`, and
  `deploy-iam.json`: attach all four to the user or CI role that runs
  `pulumi up` and pushes the server image to ECR. These are split because AWS
  managed policies have a 6144 non-whitespace character limit.
- `observe.json`: read-only inspection plus S3/SQS/SSM access to run
  deploy-aware E2E and live diagnostics on Nanotrace instances.
- `cleanup.json`: for `pulumi destroy` and cleanup of partially-created
  Nanotrace resources.

The resource scope intentionally uses `nanotrace-*` names because this stack
creates unique physical names with Pulumi suffixes, for example
`nanotrace-prod-events-035300c`.

`deploy-iam.json` includes `iam:PassRole` only for the Nanotrace instance-role
pattern and only when passed to `ec2.amazonaws.com`. The stack needs that
because EC2 launch templates pass the instance role to launched instances.
It also allows attaching or detaching only the AWS-managed
`AmazonSSMManagedInstanceCore` policy to Nanotrace instance roles, so live SSM
diagnostics can be enabled temporarily without granting broad IAM mutation.

The compute policy also grants ACM certificate management for Nanotrace-tagged
certificates and Route 53 hosted-zone record changes so the stack can provision
HTTPS for the deployer-provided domain name.

Remaining `Resource: "*"` grants are limited to APIs that either do not support
resource-level permissions or are read/list discovery calls Pulumi uses during
refresh:

- EC2, Auto Scaling, and ELB `Describe*`
- ACM certificate read/list APIs
- Route 53 hosted zone and record discovery APIs
- SQS `ListQueues`
- ECR `GetAuthorizationToken`
- KMS read/list APIs
- account/caller identity reads

`deploy-storage.json` also includes Nanotrace-scoped KMS key and alias
management. These permissions are needed only when the stack is configured with
`createDataPlaneKmsKey=true`; otherwise the deployment can run with existing
AWS-managed/default encryption or a provided `dataPlaneKmsKeyArn`.
