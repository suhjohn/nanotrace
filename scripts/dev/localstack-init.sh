#!/bin/sh
set -eu

bucket="${NANOTRACE_DEV_S3_BUCKET:-nanotrace-dev-events}"
queue="${NANOTRACE_DEV_SQS_QUEUE:-nanotrace-dev-events}"
queue_url="http://localstack:4566/000000000000/${queue}"

awslocal s3 mb "s3://${bucket}" >/dev/null 2>&1 || true
printf '{"processors":[]}\n' | awslocal s3 cp - "s3://${bucket}/processors/index.json" >/dev/null
awslocal sqs create-queue --queue-name "${queue}" >/dev/null
queue_arn="$(awslocal sqs get-queue-attributes \
  --queue-url "${queue_url}" \
  --attribute-names QueueArn \
  --query 'Attributes.QueueArn' \
  --output text)"

awslocal s3api put-bucket-notification-configuration \
  --bucket "${bucket}" \
  --notification-configuration "{\"QueueConfigurations\":[{\"QueueArn\":\"${queue_arn}\",\"Events\":[\"s3:ObjectCreated:*\"],\"Filter\":{\"Key\":{\"FilterRules\":[{\"Name\":\"prefix\",\"Value\":\"events/\"}]}}}]}"

echo "localstack resources ready"
