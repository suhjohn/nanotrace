import * as aws from '@pulumi/aws'
import * as command from '@pulumi/command'
import * as pulumi from '@pulumi/pulumi'
import { readFileSync } from 'node:fs'
import { createHash } from 'node:crypto'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const repoRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  '../../..'
)
loadEnvFile(process.env.NANOTRACE_ENV_FILE)

const cfg = new pulumi.Config()
const awsCfg = new pulumi.Config('aws')

const deploymentId =
  cfg.get('deploymentId') ?? process.env.NANOTRACE_DEPLOYMENT_ID ?? 'dev'
const name = cfg.get('name') ?? `nanotrace-${deploymentId}`
const prefix =
  cfg.get('objectPrefix') ??
  process.env.S3_PREFIX ??
  process.env.NANOTRACE_OBJECT_PREFIX ??
  'events'
const normalizedPrefix = prefix.replace(/^\/+|\/+$/g, '')
const region = awsCfg.get('region') ?? process.env.AWS_REGION ?? 'us-west-1'
const port = cfg.getNumber('port') ?? 18473
const secretKey =
  cfg.getSecret('secretKey') ?? pulumi.secret(requireEnv('SECRET_KEY'))
const clickhouseUrl = cfg.get('clickhouseUrl') ?? requireEnv('CLICKHOUSE_URL')
const clickhouseUser =
  cfg.get('clickhouseUser') ?? requireEnv('CLICKHOUSE_USER')
const clickhousePassword =
  cfg.getSecret('clickhousePassword') ??
  pulumi.secret(requireEnv('CLICKHOUSE_PASSWORD'))
const modalTokenId =
  cfg.getSecret('modalTokenId') ?? pulumi.secret(process.env.MODAL_TOKEN_ID ?? '')
const modalTokenSecret =
  cfg.getSecret('modalTokenSecret') ??
  pulumi.secret(process.env.MODAL_TOKEN_SECRET ?? '')
const modalServerApiKey =
  cfg.getSecret('modalServerApiKey') ??
  pulumi.secret(process.env.MODAL_SERVER_API_KEY ?? '')
const clickhouseDatabase =
  cfg.get('clickhouseDatabase') ??
  process.env.CLICKHOUSE_DATABASE ??
  'observatory'
const clickhouseTable =
  cfg.get('clickhouseTable') ?? process.env.CLICKHOUSE_TABLE ?? 'events'

const instanceType = cfg.get('instanceType') ?? 'c7g.large'
const queryInstanceType =
  cfg.get('queryInstanceType') ?? cfg.get('instanceType') ?? 'c7g.large'
const cpuArchitecture = cfg.get('cpuArchitecture') ?? 'arm64'
const minSize = cfg.getNumber('minSize') ?? 1
const maxSize = cfg.getNumber('maxSize') ?? 8
const desiredCapacity = cfg.getNumber('desiredCapacity') ?? minSize
const queryMinSize = cfg.getNumber('queryMinSize') ?? 1
const queryMaxSize = cfg.getNumber('queryMaxSize') ?? 4
const queryDesiredCapacity =
  cfg.getNumber('queryDesiredCapacity') ?? queryMinSize
const dataVolumeSizeGb = cfg.getNumber('dataVolumeSizeGb') ?? 64
const dataVolumeType = cfg.get('dataVolumeType') ?? 'gp3'
const dataVolumeIops = cfg.getNumber('dataVolumeIops') ?? 3000
const dataVolumeThroughput = cfg.getNumber('dataVolumeThroughput') ?? 250
const localDataDir = cfg.get('dataDir') ?? '/data/events'
const partMaxBytes = cfg.getNumber('partMaxBytes') ?? 64 * 1024 * 1024
const partMaxAgeSecs =
  cfg.getNumber('partMaxAgeSecs') ?? numberEnv('NANOTRACE_PART_MAX_AGE_SECS', 1)
const uploadPollIntervalMs =
  cfg.getNumber('uploadPollIntervalMs') ??
  numberEnv('UPLOAD_POLL_INTERVAL_MS', 500)
const doneRetentionMins =
  cfg.getNumber('doneRetentionMins') ??
  nonNegativeNumberEnv('NANOTRACE_DONE_RETENTION_MINS', 60)
const doneCleanupIntervalSecs =
  cfg.getNumber('doneCleanupIntervalSecs') ??
  numberEnv('NANOTRACE_DONE_CLEANUP_INTERVAL_SECS', 60)
const maxRequestBytes = cfg.getNumber('maxRequestBytes') ?? 209_715_200
const maxEventBytes =
  cfg.getNumber('maxEventBytes') ??
  numberEnv('MAX_EVENT_BYTES', maxRequestBytes)
const writerLanes =
  cfg.getNumber('writerLanes') ?? numberEnv('NANOTRACE_WRITER_LANES', 4)
const writerQueueCapacity =
  cfg.getNumber('writerQueueCapacity') ??
  numberEnv('NANOTRACE_WRITER_QUEUE_CAPACITY', 8192)
const writerFlushIntervalMs =
  cfg.getNumber('writerFlushIntervalMs') ??
  numberEnv('NANOTRACE_WRITER_FLUSH_INTERVAL_MS', 10)
const writerFlushBytes =
  cfg.getNumber('writerFlushBytes') ??
  numberEnv('NANOTRACE_WRITER_FLUSH_BYTES', 1024 * 1024)
const compactBatchReceipts =
  cfg.getBoolean('compactBatchReceipts') ??
  booleanEnv('NANOTRACE_COMPACT_BATCH_RECEIPTS', false)
const imageUriOverride = cfg.get('imageUri')
const buildImage = cfg.getBoolean('buildImage') ?? !imageUriOverride
const schemaHash = createHash('sha256')
  .update(
    readFileSync(path.join(repoRoot, 'deploy/clickhouse/schema.sql'), 'utf8')
  )
  .digest('hex')
const schemaScriptHash = createHash('sha256')
  .update(
    readFileSync(
      path.join(repoRoot, 'scripts/apply-clickhouse-schema.mjs'),
      'utf8'
    )
  )
  .digest('hex')

const tags = {
  Project: 'nanotrace',
  Deployment: deploymentId
}

const azs = aws.getAvailabilityZonesOutput({ state: 'available' })

const vpc = new aws.ec2.Vpc(`${name}-vpc`, {
  cidrBlock: '10.42.0.0/16',
  enableDnsHostnames: true,
  enableDnsSupport: true,
  tags: { ...tags, Name: `${name}-vpc` }
})

const igw = new aws.ec2.InternetGateway(`${name}-igw`, {
  vpcId: vpc.id,
  tags: { ...tags, Name: `${name}-igw` }
})

const routeTable = new aws.ec2.RouteTable(`${name}-public-rt`, {
  vpcId: vpc.id,
  routes: [{ cidrBlock: '0.0.0.0/0', gatewayId: igw.id }],
  tags: { ...tags, Name: `${name}-public-rt` }
})

const subnets = [0, 1].map(i => {
  const subnet = new aws.ec2.Subnet(`${name}-public-${i}`, {
    vpcId: vpc.id,
    availabilityZone: azs.names.apply(names => names[i]),
    cidrBlock: `10.42.${i}.0/24`,
    mapPublicIpOnLaunch: true,
    tags: { ...tags, Name: `${name}-public-${i}` }
  })

  new aws.ec2.RouteTableAssociation(`${name}-public-${i}`, {
    subnetId: subnet.id,
    routeTableId: routeTable.id
  })

  return subnet
})

const bucket = new aws.s3.BucketV2(`${name}-events`, {
  forceDestroy: cfg.getBoolean('forceDestroyBucket') ?? false,
  tags
})

new aws.s3.BucketPublicAccessBlock(`${name}-events-public-access`, {
  bucket: bucket.id,
  blockPublicAcls: true,
  blockPublicPolicy: true,
  ignorePublicAcls: true,
  restrictPublicBuckets: true
})

new aws.s3.BucketVersioningV2(`${name}-events-versioning`, {
  bucket: bucket.id,
  versioningConfiguration: { status: 'Enabled' }
})

const loaderQueue = new aws.sqs.Queue(`${name}-loader-events`, {
  messageRetentionSeconds: 345_600,
  visibilityTimeoutSeconds: 300,
  tags
})

const loaderQueuePolicy = new aws.sqs.QueuePolicy(
  `${name}-loader-events-policy`,
  {
    queueUrl: loaderQueue.url,
    policy: pulumi
      .all([loaderQueue.arn, bucket.arn])
      .apply(([queueArn, bucketArn]) =>
        JSON.stringify({
          Version: '2012-10-17',
          Statement: [
            {
              Sid: 'AllowS3EventNotifications',
              Effect: 'Allow',
              Principal: { Service: 's3.amazonaws.com' },
              Action: 'sqs:SendMessage',
              Resource: queueArn,
              Condition: { ArnEquals: { 'aws:SourceArn': bucketArn } }
            }
          ]
        })
      )
  }
)

new aws.s3.BucketNotification(
  `${name}-events-notifications`,
  {
    bucket: bucket.id,
    queues: [
      {
        queueArn: loaderQueue.arn,
        events: ['s3:ObjectCreated:*'],
        filterPrefix: `${normalizedPrefix}/`,
        filterSuffix: '.ndjson'
      }
    ]
  },
  {
    dependsOn: [loaderQueuePolicy]
  }
)

const repository = new aws.ecr.Repository(`${name}-server`, {
  forceDelete: cfg.getBoolean('forceDeleteRepository') ?? false,
  imageScanningConfiguration: { scanOnPush: true },
  tags
})

const imageUri = imageUriOverride
  ? pulumi.output(imageUriOverride)
  : pulumi.interpolate`${repository.repositoryUrl}:${
      cfg.get('imageTag') ?? 'latest'
    }`

const imageBuild = buildImage
  ? new command.local.Command(
      `${name}-image`,
      {
        create: pulumi.interpolate`aws ecr get-login-password --region ${region} | docker login --username AWS --password-stdin ${repository.repositoryUrl.apply(
          value => value.split('/')[0]
        )} && docker build --platform linux/${cpuArchitecture} -t ${imageUri} . && docker push ${imageUri}`,
        update: pulumi.interpolate`aws ecr get-login-password --region ${region} | docker login --username AWS --password-stdin ${repository.repositoryUrl.apply(
          value => value.split('/')[0]
        )} && docker build --platform linux/${cpuArchitecture} -t ${imageUri} . && docker push ${imageUri}`,
        delete: `true`,
        dir: repoRoot,
        triggers: [cfg.get('imageBuildId') ?? cfg.get('imageTag') ?? 'latest']
      },
      {
        dependsOn: [repository]
      }
    )
  : undefined

const role = new aws.iam.Role(`${name}-instance-role`, {
  assumeRolePolicy: JSON.stringify({
    Version: '2012-10-17',
    Statement: [
      {
        Effect: 'Allow',
        Principal: { Service: 'ec2.amazonaws.com' },
        Action: 'sts:AssumeRole'
      }
    ]
  }),
  tags
})

const instancePolicy = new aws.iam.RolePolicy(`${name}-instance-policy`, {
  role: role.id,
  policy: pulumi
    .all([bucket.arn, repository.arn, loaderQueue.arn])
    .apply(([bucketArn, repositoryArn, queueArn]) =>
      JSON.stringify({
        Version: '2012-10-17',
        Statement: [
          {
            Sid: 'WriteEventObjects',
            Effect: 'Allow',
            Action: ['s3:PutObject', 's3:AbortMultipartUpload'],
            Resource: `${bucketArn}/${normalizedPrefix}/*`
          },
          {
            Sid: 'ReadEventObjects',
            Effect: 'Allow',
            Action: 's3:GetObject',
            Resource: `${bucketArn}/${normalizedPrefix}/*`
          },
          {
            Sid: 'ReadWriteProcessorObjects',
            Effect: 'Allow',
            Action: ['s3:GetObject', 's3:PutObject'],
            Resource: `${bucketArn}/processors/*`
          },
          {
            Sid: 'ReadObjectNotifications',
            Effect: 'Allow',
            Action: [
              'sqs:ReceiveMessage',
              'sqs:DeleteMessage',
              'sqs:GetQueueAttributes'
            ],
            Resource: queueArn
          },
          {
            Sid: 'ReadEcrAuth',
            Effect: 'Allow',
            Action: 'ecr:GetAuthorizationToken',
            Resource: '*'
          },
          {
            Sid: 'PullServerImage',
            Effect: 'Allow',
            Action: [
              'ecr:BatchCheckLayerAvailability',
              'ecr:BatchGetImage',
              'ecr:GetDownloadUrlForLayer'
            ],
            Resource: repositoryArn
          }
        ]
      })
    )
})

const instanceProfile = new aws.iam.InstanceProfile(
  `${name}-instance-profile`,
  {
    role: role.name,
    tags
  }
)

const clickHouseSchema = new command.local.Command(
  `${name}-clickhouse-schema`,
  {
    create: 'node scripts/apply-clickhouse-schema.mjs',
    update: 'node scripts/apply-clickhouse-schema.mjs',
    delete: 'true',
    dir: repoRoot,
    environment: {
      CLICKHOUSE_URL: clickhouseUrl,
      CLICKHOUSE_USER: clickhouseUser,
      CLICKHOUSE_PASSWORD: clickhousePassword,
      CLICKHOUSE_DATABASE: clickhouseDatabase,
      CLICKHOUSE_TABLE: clickhouseTable,
      CLICKHOUSE_SCHEMA_PATH: 'deploy/clickhouse/schema.sql'
    },
    triggers: [
      schemaHash,
      schemaScriptHash,
      clickhouseUrl,
      clickhouseDatabase,
      clickhouseTable
    ]
  },
  {
    additionalSecretOutputs: ['environment']
  }
)

const albSg = new aws.ec2.SecurityGroup(`${name}-alb-sg`, {
  vpcId: vpc.id,
  ingress: [
    {
      protocol: 'tcp',
      fromPort: 80,
      toPort: 80,
      cidrBlocks: ['0.0.0.0/0']
    }
  ],
  egress: [
    {
      protocol: '-1',
      fromPort: 0,
      toPort: 0,
      cidrBlocks: ['0.0.0.0/0']
    }
  ],
  tags: { ...tags, Name: `${name}-alb-sg` }
})

const instanceSg = new aws.ec2.SecurityGroup(`${name}-instance-sg`, {
  vpcId: vpc.id,
  ingress: [
    {
      protocol: 'tcp',
      fromPort: port,
      toPort: port,
      securityGroups: [albSg.id]
    }
  ],
  egress: [
    {
      protocol: '-1',
      fromPort: 0,
      toPort: 0,
      cidrBlocks: ['0.0.0.0/0']
    }
  ],
  tags: { ...tags, Name: `${name}-instance-sg` }
})

const lb = new aws.lb.LoadBalancer(`${name}-alb`, {
  loadBalancerType: 'application',
  securityGroups: [albSg.id],
  subnets: subnets.map(subnet => subnet.id),
  tags
})

const targetGroup = new aws.lb.TargetGroup(`${name}-tg`, {
  vpcId: vpc.id,
  targetType: 'instance',
  protocol: 'HTTP',
  port,
  deregistrationDelay: cfg.getNumber('deregistrationDelaySecs') ?? 15,
  healthCheck: {
    enabled: true,
    path: '/healthz',
    matcher: '200',
    healthyThreshold: 2,
    unhealthyThreshold: 3,
    interval: 15,
    timeout: 5
  },
  tags
})

const queryTargetGroup = new aws.lb.TargetGroup(`${name}-query-tg`, {
  vpcId: vpc.id,
  targetType: 'instance',
  protocol: 'HTTP',
  port,
  deregistrationDelay: cfg.getNumber('deregistrationDelaySecs') ?? 15,
  healthCheck: {
    enabled: true,
    path: '/healthz',
    matcher: '200',
    healthyThreshold: 2,
    unhealthyThreshold: 3,
    interval: 15,
    timeout: 5
  },
  tags: { ...tags, Service: 'query' }
})

const listener = new aws.lb.Listener(`${name}-http`, {
  loadBalancerArn: lb.arn,
  port: 80,
  protocol: 'HTTP',
  defaultActions: [{ type: 'forward', targetGroupArn: targetGroup.arn }]
})

new aws.lb.ListenerRule(`${name}-query-route`, {
  listenerArn: listener.arn,
  priority: 10,
  conditions: [
    { pathPattern: { values: ['/query'] } },
    { httpRequestMethod: { values: ['POST'] } }
  ],
  actions: [{ type: 'forward', targetGroupArn: queryTargetGroup.arn }]
})

new aws.lb.ListenerRule(`${name}-event-read-route`, {
  listenerArn: listener.arn,
  priority: 20,
  conditions: [
    { pathPattern: { values: ['/events/*'] } },
    { httpRequestMethod: { values: ['GET'] } }
  ],
  actions: [{ type: 'forward', targetGroupArn: queryTargetGroup.arn }]
})

const ami = aws.ec2.getAmiOutput({
  mostRecent: true,
  owners: ['amazon'],
  filters: [
    {
      name: 'name',
      values: [
        `al2023-ami-2023.*-${cpuArchitecture === 'arm64' ? 'arm64' : 'x86_64'}`
      ]
    },
    {
      name: 'architecture',
      values: [cpuArchitecture === 'arm64' ? 'arm64' : 'x86_64']
    },
    { name: 'root-device-type', values: ['ebs'] },
    { name: 'virtualization-type', values: ['hvm'] }
  ]
})

const userData = pulumi
  .all([
    secretKey,
    bucket.bucket,
    imageUri,
    clickhousePassword,
    loaderQueue.url,
    modalTokenId,
    modalTokenSecret,
    modalServerApiKey
  ])
  .apply(
    ([
      resolvedSecretKey,
      bucketName,
      resolvedImageUri,
      resolvedClickhousePassword,
      loaderQueueUrl,
      resolvedModalTokenId,
      resolvedModalTokenSecret,
      resolvedModalServerApiKey
    ]) =>
      renderUserData({
        bucketName,
        clickhouseDatabase,
        clickhousePassword: resolvedClickhousePassword,
        clickhouseTable,
        clickhouseUrl,
        clickhouseUser,
        imageUri: resolvedImageUri,
        loaderQueueUrl,
        modalServerApiKey: resolvedModalServerApiKey,
        modalTokenId: resolvedModalTokenId,
        modalTokenSecret: resolvedModalTokenSecret,
        localDataDir,
        doneCleanupIntervalSecs,
        doneRetentionMins,
        maxEventBytes,
        maxRequestBytes,
        partMaxAgeSecs,
        partMaxBytes,
        port,
        prefix,
        region,
        secretKey: resolvedSecretKey,
        uploadPollIntervalMs,
        writerFlushBytes,
        writerFlushIntervalMs,
        writerLanes,
        writerQueueCapacity,
        compactBatchReceipts
      })
  )

const queryUserData = pulumi
  .all([secretKey, bucket.bucket, imageUri, clickhousePassword])
  .apply(([resolvedSecretKey, bucketName, resolvedImageUri, resolvedClickhousePassword]) =>
    renderQueryUserData({
      bucketName,
      clickhouseDatabase,
      clickhousePassword: resolvedClickhousePassword,
      clickhouseTable,
      clickhouseUrl,
      clickhouseUser,
      imageUri: resolvedImageUri,
      maxRequestBytes,
      port,
      prefix,
      region,
      secretKey: resolvedSecretKey
    })
  )

const launchTemplate = new aws.ec2.LaunchTemplate(
  `${name}-lt`,
  {
    imageId: ami.id,
    instanceType,
    iamInstanceProfile: { arn: instanceProfile.arn },
    vpcSecurityGroupIds: [instanceSg.id],
    userData: userData.apply(value => Buffer.from(value).toString('base64')),
    blockDeviceMappings: [
      {
        deviceName: '/dev/xvda',
        ebs: {
          volumeSize: cfg.getNumber('rootVolumeSizeGb') ?? 16,
          volumeType: 'gp3',
          deleteOnTermination: 'true',
          encrypted: 'true'
        }
      },
      {
        deviceName: '/dev/xvdb',
        ebs: {
          volumeSize: dataVolumeSizeGb,
          volumeType: dataVolumeType,
          iops: dataVolumeType === 'gp3' ? dataVolumeIops : undefined,
          throughput:
            dataVolumeType === 'gp3' ? dataVolumeThroughput : undefined,
          deleteOnTermination: 'true',
          encrypted: 'true'
        }
      }
    ],
    tagSpecifications: [
      { resourceType: 'instance', tags: { ...tags, Name: `${name}-server` } },
      { resourceType: 'volume', tags }
    ],
    tags
  },
  {
    dependsOn: imageBuild ? [imageBuild] : []
  }
)

const queryLaunchTemplate = new aws.ec2.LaunchTemplate(
  `${name}-query-lt`,
  {
    imageId: ami.id,
    instanceType: queryInstanceType,
    iamInstanceProfile: { arn: instanceProfile.arn },
    vpcSecurityGroupIds: [instanceSg.id],
    userData: queryUserData.apply(value => Buffer.from(value).toString('base64')),
    blockDeviceMappings: [
      {
        deviceName: '/dev/xvda',
        ebs: {
          volumeSize: cfg.getNumber('queryRootVolumeSizeGb') ?? 16,
          volumeType: 'gp3',
          deleteOnTermination: 'true',
          encrypted: 'true'
        }
      }
    ],
    tagSpecifications: [
      { resourceType: 'instance', tags: { ...tags, Name: `${name}-query` } },
      { resourceType: 'volume', tags: { ...tags, Service: 'query' } }
    ],
    tags: { ...tags, Service: 'query' }
  },
  {
    dependsOn: imageBuild ? [imageBuild] : []
  }
)

const asg = new aws.autoscaling.Group(`${name}-asg`, {
  vpcZoneIdentifiers: subnets.map(subnet => subnet.id),
  minSize,
  maxSize,
  desiredCapacity,
  healthCheckType: 'ELB',
  healthCheckGracePeriod: 120,
  targetGroupArns: [targetGroup.arn],
  launchTemplate: {
    id: launchTemplate.id,
    version: '$Latest'
  },
  tags: [
    { key: 'Project', value: tags.Project, propagateAtLaunch: true },
    { key: 'Deployment', value: tags.Deployment, propagateAtLaunch: true },
    { key: 'Name', value: `${name}-server`, propagateAtLaunch: true }
  ]
})

const queryAsg = new aws.autoscaling.Group(`${name}-query-asg`, {
  vpcZoneIdentifiers: subnets.map(subnet => subnet.id),
  minSize: queryMinSize,
  maxSize: queryMaxSize,
  desiredCapacity: queryDesiredCapacity,
  healthCheckType: 'ELB',
  healthCheckGracePeriod: 120,
  targetGroupArns: [queryTargetGroup.arn],
  launchTemplate: {
    id: queryLaunchTemplate.id,
    version: '$Latest'
  },
  tags: [
    { key: 'Project', value: tags.Project, propagateAtLaunch: true },
    { key: 'Deployment', value: tags.Deployment, propagateAtLaunch: true },
    { key: 'Name', value: `${name}-query`, propagateAtLaunch: true },
    { key: 'Service', value: 'query', propagateAtLaunch: true }
  ]
})

export const albDnsName = lb.dnsName
export const ingestUrl = pulumi.interpolate`http://${lb.dnsName}`
export const queryTargetGroupArn = queryTargetGroup.arn
export const ingestAutoScalingGroupName = asg.name
export const queryAutoScalingGroupName = queryAsg.name
export const bucketName = bucket.bucket
export const objectPrefix = prefix
export const loaderSqsQueueUrl = loaderQueue.url
export const loaderSqsQueueArn = loaderQueue.arn
export const clickhouseDatabaseOutput = clickhouseDatabase
export const clickhouseTableOutput = clickhouseTable
export const ecrRepositoryUrl = repository.repositoryUrl
export const serverImageUri = imageUri

interface UserDataArgs {
  bucketName: string
  clickhouseDatabase: string
  clickhousePassword: string
  clickhouseTable: string
  clickhouseUrl: string
  clickhouseUser: string
  imageUri: string
  loaderQueueUrl: string
  modalServerApiKey: string
  modalTokenId: string
  modalTokenSecret: string
  localDataDir: string
  doneCleanupIntervalSecs: number
  doneRetentionMins: number
  maxEventBytes: number
  maxRequestBytes: number
  partMaxAgeSecs: number
  partMaxBytes: number
  port: number
  prefix: string
  region: string
  secretKey: string
  uploadPollIntervalMs: number
  writerFlushBytes: number
  writerFlushIntervalMs: number
  writerLanes: number
  writerQueueCapacity: number
  compactBatchReceipts: boolean
}

interface QueryUserDataArgs {
  bucketName: string
  clickhouseDatabase: string
  clickhousePassword: string
  clickhouseTable: string
  clickhouseUrl: string
  clickhouseUser: string
  imageUri: string
  maxRequestBytes: number
  port: number
  prefix: string
  region: string
  secretKey: string
}

function renderUserData (args: UserDataArgs): string {
  const debugPrefix = `${args.prefix.replace(/^\/+|\/+$/g, '')}/_debug`
  return `#!/bin/bash
set -uo pipefail

LOG=/var/log/nanotrace-bootstrap.log
exec > >(tee -a "$LOG") 2>&1

TOKEN="$(curl -sS --max-time 2 -X PUT 'http://169.254.169.254/latest/api/token' -H 'X-aws-ec2-metadata-token-ttl-seconds: 300' || echo)"
INSTANCE_ID="$(curl -sS --max-time 2 -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-id || echo unknown)"
S3_DEBUG_PREFIX="s3://${args.bucketName}/${debugPrefix}/$INSTANCE_ID"

upload_debug() {
  local rc=$?
  echo "=== bootstrap exit rc=$rc ==="
  (docker ps -a 2>&1 || true) > /tmp/docker-ps.txt
  (docker logs nanotrace-server 2>&1 || true) > /tmp/docker-logs.txt
  (docker logs nanotrace-loader 2>&1 || true) > /tmp/docker-loader-logs.txt
  (docker inspect nanotrace-server 2>&1 || true) > /tmp/docker-inspect.txt
  (docker inspect nanotrace-loader 2>&1 || true) > /tmp/docker-loader-inspect.txt
  (journalctl -u docker --no-pager 2>&1 || true) > /tmp/docker-journal.txt
  (cat /var/log/cloud-init-output.log 2>&1 || true) > /tmp/cloud-init-output.log
  for f in "$LOG" /tmp/docker-ps.txt /tmp/docker-logs.txt /tmp/docker-inspect.txt /tmp/docker-journal.txt /tmp/cloud-init-output.log; do
    aws s3 cp "$f" "$S3_DEBUG_PREFIX/$(basename "$f")" --region ${shellQuote(
      args.region
    )} || true
  done
}
trap upload_debug EXIT

set -e
dnf update -y
dnf install -y docker awscli xfsprogs
systemctl enable --now docker

mkdir -p /data
ROOT_SOURCE="$(findmnt -no SOURCE / || true)"
ROOT_DEVICE="$(readlink -f "$ROOT_SOURCE" || true)"
ROOT_PARENT=""
if [ -n "$ROOT_DEVICE" ]; then
  ROOT_PARENT="$(lsblk -no PKNAME "$ROOT_DEVICE" 2>/dev/null | head -n1 || true)"
  if [ -z "$ROOT_PARENT" ]; then
    ROOT_PARENT="$(basename "$ROOT_DEVICE")"
  fi
fi

DATA_DEVICE=""
while read -r NAME TYPE; do
  if [ "$TYPE" != "disk" ] || [ "$NAME" = "$ROOT_PARENT" ]; then
    continue
  fi
  if ! lsblk -nr "/dev/$NAME" -o MOUNTPOINT | grep -q '/'; then
    DATA_DEVICE="/dev/$NAME"
    break
  fi
done < <(lsblk -ndo NAME,TYPE)

if [ -n "$DATA_DEVICE" ]; then
  if ! blkid "$DATA_DEVICE" >/dev/null 2>&1; then
    mkfs.xfs -f "$DATA_DEVICE"
  fi
  if ! grep -q " /data " /proc/mounts; then
    mount "$DATA_DEVICE" /data
  fi
  UUID="$(blkid -s UUID -o value "$DATA_DEVICE")"
  if [ -n "$UUID" ] && ! grep -q "$UUID" /etc/fstab; then
    echo "UUID=$UUID /data xfs defaults,nofail 0 2" >> /etc/fstab
  fi
fi

mkdir -p ${shellQuote(args.localDataDir)}
aws ecr get-login-password --region ${shellQuote(
    args.region
  )} | docker login --username AWS --password-stdin "$(echo ${shellQuote(
    args.imageUri
  )} | cut -d/ -f1)"
docker pull ${shellQuote(args.imageUri)}
docker rm -f nanotrace-server >/dev/null 2>&1 || true
docker rm -f nanotrace-loader >/dev/null 2>&1 || true
docker run -d --name nanotrace-server --restart unless-stopped \\
  -p ${args.port}:${args.port} \\
  -v ${shellQuote(args.localDataDir)}:${shellQuote(args.localDataDir)} \\
  -e AWS_REGION=${shellQuote(args.region)} \\
  -e SECRET_KEY=${shellQuote(args.secretKey)} \\
  -e PORT=${args.port} \\
  -e NANOTRACE_DATA_DIR=${shellQuote(args.localDataDir)} \\
  -e NANOTRACE_S3_BUCKET=${shellQuote(args.bucketName)} \\
  -e S3_PREFIX=${shellQuote(args.prefix)} \\
  -e CLICKHOUSE_URL=${shellQuote(args.clickhouseUrl)} \\
  -e CLICKHOUSE_USER=${shellQuote(args.clickhouseUser)} \\
  -e CLICKHOUSE_PASSWORD=${shellQuote(args.clickhousePassword)} \\
  -e CLICKHOUSE_DATABASE=${shellQuote(args.clickhouseDatabase)} \\
  -e CLICKHOUSE_TABLE=${shellQuote(args.clickhouseTable)} \\
  -e MAX_REQUEST_BYTES=${args.maxRequestBytes} \\
  -e MAX_EVENT_BYTES=${args.maxEventBytes} \\
  -e NANOTRACE_PART_MAX_BYTES=${args.partMaxBytes} \\
  -e NANOTRACE_PART_MAX_AGE_SECS=${args.partMaxAgeSecs} \\
  -e UPLOAD_POLL_INTERVAL_MS=${args.uploadPollIntervalMs} \\
  -e NANOTRACE_DONE_RETENTION_MINS=${args.doneRetentionMins} \\
  -e NANOTRACE_DONE_CLEANUP_INTERVAL_SECS=${args.doneCleanupIntervalSecs} \\
  -e NANOTRACE_WRITER_LANES=${args.writerLanes} \\
  -e NANOTRACE_WRITER_QUEUE_CAPACITY=${args.writerQueueCapacity} \\
  -e NANOTRACE_WRITER_FLUSH_INTERVAL_MS=${args.writerFlushIntervalMs} \\
  -e NANOTRACE_WRITER_FLUSH_BYTES=${args.writerFlushBytes} \\
  -e NANOTRACE_COMPACT_BATCH_RECEIPTS=${
    args.compactBatchReceipts ? 'true' : 'false'
  } \\
  -e MODAL_TOKEN_ID=${shellQuote(args.modalTokenId)} \\
  -e MODAL_TOKEN_SECRET=${shellQuote(args.modalTokenSecret)} \\
  -e MODAL_SERVER_API_KEY=${shellQuote(args.modalServerApiKey)} \\
  -e PROCESSOR_S3_BUCKET=${shellQuote(args.bucketName)} \\
  -e PROCESSOR_POLL_INTERVAL_SECS=30 \\
  -e PROCESSOR_BUILDER_CMD=${shellQuote('python3 /usr/local/bin/modal_processor_builder.py')} \\
  -e PROCESSOR_TARGET=${shellQuote('aarch64-unknown-linux-gnu')} \\
  ${shellQuote(args.imageUri)}
docker run -d --name nanotrace-loader --restart unless-stopped \\
  -e AWS_REGION=${shellQuote(args.region)} \\
  -e LOADER_SQS_QUEUE_URL=${shellQuote(args.loaderQueueUrl)} \\
  -e NANOTRACE_S3_BUCKET=${shellQuote(args.bucketName)} \\
  -e PROCESSOR_S3_BUCKET=${shellQuote(args.bucketName)} \\
  -e PROCESSOR_POLL_INTERVAL_SECS=30 \\
  -e CLICKHOUSE_URL=${shellQuote(args.clickhouseUrl)} \\
  -e CLICKHOUSE_USER=${shellQuote(args.clickhouseUser)} \\
  -e CLICKHOUSE_PASSWORD=${shellQuote(args.clickhousePassword)} \\
  -e CLICKHOUSE_DATABASE=${shellQuote(args.clickhouseDatabase)} \\
  -e CLICKHOUSE_TABLE=${shellQuote(args.clickhouseTable)} \\
  ${shellQuote(args.imageUri)} \\
  /usr/local/bin/nanotrace-loader
`
}

function renderQueryUserData (args: QueryUserDataArgs): string {
  const debugPrefix = `${args.prefix.replace(/^\/+|\/+$/g, '')}/_debug-query`
  return `#!/bin/bash
set -uo pipefail

LOG=/var/log/nanotrace-query-bootstrap.log
exec > >(tee -a "$LOG") 2>&1

TOKEN="$(curl -sS --max-time 2 -X PUT 'http://169.254.169.254/latest/api/token' -H 'X-aws-ec2-metadata-token-ttl-seconds: 300' || echo)"
INSTANCE_ID="$(curl -sS --max-time 2 -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-id || echo unknown)"
S3_DEBUG_PREFIX="s3://${args.bucketName}/${debugPrefix}/$INSTANCE_ID"

upload_debug() {
  local rc=$?
  echo "=== query bootstrap exit rc=$rc ==="
  (docker ps -a 2>&1 || true) > /tmp/docker-query-ps.txt
  (docker logs nanotrace-query 2>&1 || true) > /tmp/docker-query-logs.txt
  (docker inspect nanotrace-query 2>&1 || true) > /tmp/docker-query-inspect.txt
  (journalctl -u docker --no-pager 2>&1 || true) > /tmp/docker-query-journal.txt
  (cat /var/log/cloud-init-output.log 2>&1 || true) > /tmp/query-cloud-init-output.log
  for f in "$LOG" /tmp/docker-query-ps.txt /tmp/docker-query-logs.txt /tmp/docker-query-inspect.txt /tmp/docker-query-journal.txt /tmp/query-cloud-init-output.log; do
    aws s3 cp "$f" "$S3_DEBUG_PREFIX/$(basename "$f")" --region ${shellQuote(
      args.region
    )} || true
  done
}
trap upload_debug EXIT

set -e
dnf update -y
dnf install -y docker awscli
systemctl enable --now docker

aws ecr get-login-password --region ${shellQuote(
    args.region
  )} | docker login --username AWS --password-stdin "$(echo ${shellQuote(
    args.imageUri
  )} | cut -d/ -f1)"
docker pull ${shellQuote(args.imageUri)}
docker rm -f nanotrace-query >/dev/null 2>&1 || true
docker run -d --name nanotrace-query --restart unless-stopped \\
  -p ${args.port}:${args.port} \\
  -e AWS_REGION=${shellQuote(args.region)} \\
  -e SECRET_KEY=${shellQuote(args.secretKey)} \\
  -e PORT=${args.port} \\
  -e NANOTRACE_S3_BUCKET=${shellQuote(args.bucketName)} \\
  -e CLICKHOUSE_URL=${shellQuote(args.clickhouseUrl)} \\
  -e CLICKHOUSE_USER=${shellQuote(args.clickhouseUser)} \\
  -e CLICKHOUSE_PASSWORD=${shellQuote(args.clickhousePassword)} \\
  -e CLICKHOUSE_DATABASE=${shellQuote(args.clickhouseDatabase)} \\
  -e CLICKHOUSE_TABLE=${shellQuote(args.clickhouseTable)} \\
  -e MAX_REQUEST_BYTES=${args.maxRequestBytes} \\
  ${shellQuote(args.imageUri)} \\
  /usr/local/bin/nanotrace-query
`
}

function shellQuote (value: unknown): string {
  return `'${String(value).replaceAll("'", "'\\''")}'`
}

function requireEnv (key: string): string {
  const value = process.env[key]
  if (!value) {
    throw new Error(
      `Missing required Pulumi config "secretKey" or ${key} environment variable`
    )
  }
  return value
}

function numberEnv (key: string, fallback: number): number {
  const value = process.env[key]
  if (!value) {
    return fallback
  }
  const parsed = Number(value)
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${key} must be a positive number`)
  }
  return parsed
}

function booleanEnv (key: string, fallback: boolean): boolean {
  const value = process.env[key]
  if (!value) {
    return fallback
  }
  switch (value.toLowerCase()) {
    case '1':
    case 'true':
    case 'yes':
    case 'on':
      return true
    case '0':
    case 'false':
    case 'no':
    case 'off':
      return false
    default:
      throw new Error(`${key} must be a boolean`)
  }
}

function nonNegativeNumberEnv (key: string, fallback: number): number {
  const value = process.env[key]
  if (!value) {
    return fallback
  }
  const parsed = Number(value)
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(`${key} must be a non-negative number`)
  }
  return parsed
}

function loadEnvFile (file: string | undefined): void {
  if (!file) {
    return
  }

  const envPath = path.resolve(repoRoot, file)
  try {
    const text = readFileSync(envPath, 'utf8')
    for (const line of text.split(/\r?\n/)) {
      const trimmed = line.trim()
      if (!trimmed || trimmed.startsWith('#')) {
        continue
      }
      const match = trimmed.match(
        /^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$/
      )
      if (!match) {
        continue
      }
      const [, key, rawValue] = match
      if (process.env[key] !== undefined) {
        continue
      }
      process.env[key] = parseEnvValue(rawValue)
    }
  } catch (error) {
    const nodeError = error as NodeJS.ErrnoException
    if (nodeError.code !== 'ENOENT') {
      throw error
    }
  }
}

function parseEnvValue (value: string): string {
  const trimmed = value.trim()
  if (
    (trimmed.startsWith('"') && trimmed.endsWith('"')) ||
    (trimmed.startsWith("'") && trimmed.endsWith("'"))
  ) {
    return trimmed.slice(1, -1)
  }
  return trimmed
}
