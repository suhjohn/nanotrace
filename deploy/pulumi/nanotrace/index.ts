import * as aws from '@pulumi/aws'
import * as cloudflare from '@pulumi/cloudflare'
import * as command from '@pulumi/command'
import * as pulumi from '@pulumi/pulumi'
import * as random from '@pulumi/random'
import { readFileSync } from 'node:fs'
import { createHash } from 'node:crypto'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const repoRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  '../../..'
)

const cfg = new pulumi.Config()
const awsCfg = new pulumi.Config('aws')
const usEast1 = new aws.Provider('useast1', { region: 'us-east-1' })

const deploymentId = cfg.get('deploymentId') ?? pulumi.getStack()
const name = cfg.get('name') ?? `nanotrace-${deploymentId}`
const prefix =
  cfg.get('objectPrefix') ??
  process.env.S3_PREFIX ??
  process.env.NANOTRACE_OBJECT_PREFIX ??
  'events'
const normalizedPrefix = prefix.replace(/^\/+|\/+$/g, '')
const dataPlaneOrganizationId =
  cfg.get('dataPlaneOrganizationId') ??
  process.env.NANOTRACE_DATA_PLANE_ORGANIZATION_ID ??
  ''
const isDataPlaneOnly = dataPlaneOrganizationId.trim() !== ''
const createLoginEmailResources = !isDataPlaneOnly
const dataPlaneSharedSecret =
  cfg.getSecret('dataPlaneSharedSecret') ??
  pulumi.secret(process.env.NANOTRACE_DATA_PLANE_SHARED_SECRET ?? '')
const processorPrefix =
  cfg.get('processorPrefix') ??
  process.env.PROCESSOR_PREFIX ??
  (dataPlaneOrganizationId
    ? `organizations/${dataPlaneOrganizationId}/processors`
    : 'organizations/org_default/processors')
const region = awsCfg.get('region') ?? process.env.AWS_REGION ?? 'us-west-1'
const expectedAwsAccountId =
  cfg.get('awsAccountId') ??
  process.env.NANOTRACE_AWS_ACCOUNT_ID ??
  ''
if (expectedAwsAccountId) {
  const caller = await aws.getCallerIdentity({})
  if (caller.accountId !== expectedAwsAccountId) {
    throw new Error(
      `AWS account mismatch: active credentials are for ${caller.accountId}, expected ${expectedAwsAccountId}`
    )
  }
}
const port = cfg.getNumber('port') ?? 18473
const clickhouseMode = normalizeClickhouseMode(
  process.env.NANOTRACE_CLICKHOUSE_MODE ??
  (!process.env.CLICKHOUSE_URL &&
  (process.env.CLICKHOUSE_CLOUD_API_KEY ?? process.env.CLICKHOUSE_CLOUD_API_KEY_ID) &&
  (process.env.CLICKHOUSE_CLOUD_API_SECRET ?? process.env.CLICKHOUSE_CLOUD_API_KEY_SECRET) &&
  process.env.CLICKHOUSE_CLOUD_ORG_ID
    ? 'shared-service'
    : 'external')
)
const createDefaultClickhouseCloudService =
  clickhouseMode === 'dedicated-service' ||
  (clickhouseMode === 'shared-service' && !isDataPlaneOnly)
const clickhouseCloudOrgId = process.env.CLICKHOUSE_CLOUD_ORG_ID ?? ''
const clickhouseCloudApiKey =
  process.env.CLICKHOUSE_CLOUD_API_KEY ??
  process.env.CLICKHOUSE_CLOUD_API_KEY_ID ??
  ''
const clickhouseCloudApiSecret =
  ((process.env.CLICKHOUSE_CLOUD_API_SECRET ?? process.env.CLICKHOUSE_CLOUD_API_KEY_SECRET)
    ? pulumi.secret(process.env.CLICKHOUSE_CLOUD_API_SECRET ?? process.env.CLICKHOUSE_CLOUD_API_KEY_SECRET!)
    : undefined)
const clickhouseCloudProvider =
  process.env.CLICKHOUSE_CLOUD_PROVIDER ??
  'aws'
const clickhouseCloudRegion =
  process.env.CLICKHOUSE_CLOUD_REGION ??
  region
const defaultClickhouseServiceName =
  process.env.NANOTRACE_DEFAULT_CLICKHOUSE_SERVICE_NAME ??
  (clickhouseMode === 'shared-service'
    ? `${name}-default`
    : `${name}-clickhouse`)
const defaultClickhouseTier = process.env.NANOTRACE_DEFAULT_CLICKHOUSE_TIER
const defaultClickhouseIdleScaling =
  booleanEnv('NANOTRACE_DEFAULT_CLICKHOUSE_IDLE_SCALING', true)
const defaultClickhouseIdleTimeoutMinutes =
  numberEnv('NANOTRACE_DEFAULT_CLICKHOUSE_IDLE_TIMEOUT_MINUTES', 15)
const defaultClickhouseMinTotalMemoryGb =
  numberEnv('NANOTRACE_DEFAULT_CLICKHOUSE_MIN_TOTAL_MEMORY_GB', 24)
const defaultClickhouseMaxTotalMemoryGb =
  numberEnv('NANOTRACE_DEFAULT_CLICKHOUSE_MAX_TOTAL_MEMORY_GB', 24)
const defaultClickhouseNumReplicas = optionalNumberEnv('NANOTRACE_DEFAULT_CLICKHOUSE_NUM_REPLICAS')
const defaultClickhouseIpAccess = parseClickhouseIpAccess(
  process.env.NANOTRACE_DEFAULT_CLICKHOUSE_IP_ACCESS ??
  '0.0.0.0/0'
)
const modalTokenId =
  cfg.getSecret('modalTokenId') ?? pulumi.secret(process.env.MODAL_TOKEN_ID ?? '')
const modalTokenSecret =
  cfg.getSecret('modalTokenSecret') ??
  pulumi.secret(process.env.MODAL_TOKEN_SECRET ?? '')
const modalServerApiKey =
  cfg.getSecret('modalServerApiKey') ??
  pulumi.secret(process.env.MODAL_SERVER_API_KEY ?? '')
const clickhouseDatabase =
  process.env.CLICKHOUSE_DATABASE ??
  (dataPlaneOrganizationId
    ? clickhouseDatabaseName(dataPlaneOrganizationId)
    : 'observatory')
const clickhouseTable = process.env.CLICKHOUSE_TABLE ?? 'events'
const clickhouseFacetsTable =
  process.env.CLICKHOUSE_FACETS_TABLE ??
  'event_facets'
const clickhouseEventIndexTable =
  process.env.CLICKHOUSE_EVENT_INDEX_TABLE ??
  'event_facet_index'
const clickhouseHotDimensionsTable =
  process.env.CLICKHOUSE_HOT_DIMENSIONS_TABLE ??
  'hot_dimensions'
const clickhouseMaxBytesToRead =
  numberEnv('CLICKHOUSE_MAX_BYTES_TO_READ', 1_000_000_000_000)

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
const partMaxBytes = cfg.getNumber('partMaxBytes') ?? 1024 * 1024
const partMaxAgeSecs = cfg.getNumber('partMaxAgeSecs') ?? 1
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
const postgresMode =
  cfg.get('postgresMode') ??
  process.env.NANOTRACE_POSTGRES_MODE ??
  'managed'
if (postgresMode !== 'managed' && postgresMode !== 'external') {
  throw new Error('nanotrace:postgresMode must be managed or external')
}
const postgresPrivateConnect =
  cfg.get('postgresPrivateConnect') ??
  process.env.NANOTRACE_POSTGRES_PRIVATE_CONNECT ??
  'none'
if (postgresPrivateConnect !== 'none' && postgresPrivateConnect !== 'aws-privatelink') {
  throw new Error('nanotrace:postgresPrivateConnect must be none or aws-privatelink')
}
const externalPostgresUrl =
  cfg.getSecret('postgresUrl') ??
  (process.env.NANOTRACE_POSTGRES_URL
    ? pulumi.secret(process.env.NANOTRACE_POSTGRES_URL)
    : undefined)
const postgresPrivateLinkServiceName =
  cfg.get('postgresPrivateLinkServiceName') ??
  process.env.NANOTRACE_POSTGRES_PRIVATELINK_SERVICE_NAME ??
  ''
const databaseName = cfg.get('databaseName') ?? 'nanotrace'
const databaseUsername = cfg.get('databaseUsername') ?? 'nanotrace'
const databaseInstanceClass = cfg.get('databaseInstanceClass') ?? 'db.t4g.micro'
const databaseAllocatedStorageGb =
  cfg.getNumber('databaseAllocatedStorageGb') ?? 20
const databaseBackupRetentionDays =
  cfg.getNumber('databaseBackupRetentionDays') ?? 1
const databaseSkipFinalSnapshot =
  cfg.getBoolean('databaseSkipFinalSnapshot') ?? true
const databaseDeletionProtection =
  cfg.getBoolean('databaseDeletionProtection') ?? false
const allowedEmails =
  cfg.get('allowedEmails') ??
  process.env.NANOTRACE_ALLOWED_EMAILS ??
  ''
const adminEmails =
  cfg.get('adminEmails') ?? process.env.NANOTRACE_ADMIN_EMAILS ?? ''
const domainName = normalizeDomainName(
  requireConfigOrEnv('domainName', 'NANOTRACE_DOMAIN_NAME')
)
const configuredEmailFrom = cfg.get('emailFrom') ?? process.env.NANOTRACE_EMAIL_FROM
const emailFrom = configuredEmailFrom?.trim() || `login@mail.${domainName}`
const loginEmailIdentityDomain = normalizeDomainName(domainFromEmail(emailFrom))
const loginEmailMailFromDomain = normalizeDomainName(`bounce.${loginEmailIdentityDomain}`)
const manageLoginEmailDns =
  !isDataPlaneOnly &&
  emailFrom.trim() !== '' &&
  booleanEnv('NANOTRACE_MANAGE_LOGIN_EMAIL_DNS', cfg.getBoolean('manageLoginEmailDns') ?? true)
const apiDomainName = normalizeDomainName(
  cfg.get('apiDomainName') ??
    process.env.NANOTRACE_API_DOMAIN_NAME ??
    `api.${domainName}`
)
const publicDnsDomains = Array.from(new Set([domainName, apiDomainName]))
const appBaseUrl =
  cfg.get('appBaseUrl') ??
  process.env.NANOTRACE_APP_BASE_URL ??
  `https://${domainName}`
const apiBaseUrl =
  cfg.get('apiBaseUrl') ??
  process.env.NANOTRACE_API_BASE_URL ??
  `https://${apiDomainName}`
const uiApiBaseUrl =
  cfg.get('uiApiBaseUrl') ??
  process.env.VITE_NANOTRACE_URL ??
  apiBaseUrl
const corsAllowedOrigins =
  cfg.get('corsAllowedOrigins') ??
  process.env.NANOTRACE_CORS_ALLOWED_ORIGINS ??
  [
    appBaseUrl,
    'http://localhost:5174',
    'http://127.0.0.1:5174'
  ].join(',')
const hostedZoneName = normalizeDomainName(
  cfg.get('hostedZoneName') ??
    process.env.NANOTRACE_HOSTED_ZONE_NAME ??
    domainName
)
const dnsProvider =
  cfg.get('dnsProvider') ??
  process.env.NANOTRACE_DNS_PROVIDER ??
  (process.env.CLOUDFLARE_API_TOKEN ? 'cloudflare' : 'route53')
if (dnsProvider !== 'cloudflare' && dnsProvider !== 'route53' && dnsProvider !== 'external') {
  throw new Error('nanotrace:dnsProvider must be cloudflare, route53, or external')
}
const edgeTlsMode =
  cfg.get('edgeTlsMode') ??
  process.env.NANOTRACE_EDGE_TLS_MODE ??
  (dnsProvider === 'route53'
    ? 'alb'
    : dnsProvider === 'cloudflare'
      ? 'cloudflare-flexible'
      : 'edge-flexible')
if (edgeTlsMode !== 'alb' && edgeTlsMode !== 'cloudflare-flexible' && edgeTlsMode !== 'edge-flexible') {
  throw new Error('nanotrace:edgeTlsMode must be alb, cloudflare-flexible, or edge-flexible')
}
if (dnsProvider === 'external' && edgeTlsMode === 'alb') {
  throw new Error('nanotrace:edgeTlsMode=alb requires managed DNS for ACM validation; use edge-flexible with nanotrace:dnsProvider=external')
}
const hostedZoneIdOverride =
  cfg.get('hostedZoneId') ?? process.env.NANOTRACE_HOSTED_ZONE_ID
const cloudflareZoneIdOverride =
  cfg.get('cloudflareZoneId') ?? process.env.CLOUDFLARE_ZONE_ID
const usesEdgeFlexibleTls = edgeTlsMode === 'cloudflare-flexible' || edgeTlsMode === 'edge-flexible'
const manageDns = edgeTlsMode === 'alb'
const usesRoute53Dns = dnsProvider === 'route53' && (manageDns || manageLoginEmailDns || dnsProvider === 'route53')
const usesCloudflareDns = dnsProvider === 'cloudflare' && (
  manageDns ||
  usesEdgeFlexibleTls ||
  manageLoginEmailDns
)
const cloudflareProvider = usesCloudflareDns
  ? new cloudflare.Provider(`${name}-cloudflare`, {
    apiToken: requireConfigOrEnv('cloudflareApiToken', 'CLOUDFLARE_API_TOKEN')
  })
  : undefined
const sessionSecure =
  cfg.getBoolean('sessionSecure') ??
  booleanEnv('NANOTRACE_SESSION_SECURE', true)
const imageUriOverride = cfg.get('imageUri')
const buildImage = cfg.getBoolean('buildImage') ?? !imageUriOverride
const imageBuildId = cfg.get('imageBuildId') ?? cfg.get('imageTag') ?? 'latest'
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
const clickhouseCloudScriptHash = createHash('sha256')
  .update(
    readFileSync(
      path.join(repoRoot, 'scripts/clickhouse-cloud-service.mjs'),
      'utf8'
    )
  )
  .digest('hex')

const tags = {
  Project: 'nanotrace',
  Deployment: deploymentId
}
const managedLoginEmailIdentity = createLoginEmailResources && emailFrom.trim()
  ? new aws.sesv2.EmailIdentity(`${name}-login-email`, {
    dkimSigningAttributes: {
      nextSigningKeyLength: 'RSA_2048_BIT'
    },
    emailIdentity: loginEmailIdentityDomain,
    tags
  })
  : undefined
const managedLoginEmailMailFrom = managedLoginEmailIdentity
  ? new aws.sesv2.EmailIdentityMailFromAttributes(`${name}-login-email-mail-from`, {
    behaviorOnMxFailure: 'REJECT_MESSAGE',
    emailIdentity: managedLoginEmailIdentity.emailIdentity,
    mailFromDomain: loginEmailMailFromDomain
  })
  : undefined

const databasePassword = isDataPlaneOnly
  ? (postgresMode === 'managed'
      ? (cfg.getSecret('databasePassword') ??
        new random.RandomPassword(`${name}-database-password`, {
          length: 32,
          special: false
        }).result)
      : pulumi.secret(''))
  : postgresMode === 'managed'
    ? (cfg.getSecret('databasePassword') ??
      new random.RandomPassword(`${name}-database-password`, {
        length: 32,
        special: false
      }).result)
    : pulumi.secret('')
const generatedBootstrapApiKey = new random.RandomPassword(
  `${name}-bootstrap-api-key`,
  {
    length: 43,
    special: false
  }
)
const bootstrapApiKey = cfg.getSecret('bootstrapApiKey') ??
  (process.env.NANOTRACE_BOOTSTRAP_API_KEY
    ? pulumi.secret(process.env.NANOTRACE_BOOTSTRAP_API_KEY)
    : pulumi.interpolate`ntak_${generatedBootstrapApiKey.result}`)
const configuredDataPlaneKmsKeyArn =
  cfg.get('dataPlaneKmsKeyArn') ??
  process.env.NANOTRACE_DATA_PLANE_KMS_KEY_ARN ??
  ''
const createDataPlaneKmsKey =
  cfg.getBoolean('createDataPlaneKmsKey') ??
  booleanEnv('NANOTRACE_CREATE_DATA_PLANE_KMS_KEY', false)

const azs = aws.getAvailabilityZonesOutput({ state: 'available' })

if (createDefaultClickhouseCloudService) {
  if (!clickhouseCloudOrgId || !clickhouseCloudApiKey || !clickhouseCloudApiSecret) {
    throw new Error(
      `NANOTRACE_CLICKHOUSE_MODE=${clickhouseMode} requires CLICKHOUSE_CLOUD_ORG_ID, CLICKHOUSE_CLOUD_API_KEY, and CLICKHOUSE_CLOUD_API_SECRET`
    )
  }
}

const defaultClickhousePassword = createDefaultClickhouseCloudService
  ? new random.RandomPassword(`${name}-clickhouse-default-password`, {
      length: 32,
      special: true,
      overrideSpecial: '_-'
    }).result
  : undefined

const defaultClickhouseService = createDefaultClickhouseCloudService
  ? new command.local.Command(
      `${name}-clickhouse-default`,
      {
        create: 'node scripts/clickhouse-cloud-service.mjs create',
        update: 'node scripts/clickhouse-cloud-service.mjs create',
        delete: 'true',
        dir: repoRoot,
        environment: {
          CLICKHOUSE_CLOUD_API_KEY: clickhouseCloudApiKey,
          CLICKHOUSE_CLOUD_API_SECRET: clickhouseCloudApiSecret!,
          CLICKHOUSE_CLOUD_ORG_ID: clickhouseCloudOrgId,
          CLICKHOUSE_CLOUD_PROVIDER: clickhouseCloudProvider,
          CLICKHOUSE_CLOUD_REGION: clickhouseCloudRegion,
          CLICKHOUSE_CLOUD_SERVICE_NAME: defaultClickhouseServiceName,
          CLICKHOUSE_CLOUD_PASSWORD: defaultClickhousePassword!,
          CLICKHOUSE_CLOUD_IDLE_SCALING: String(defaultClickhouseIdleScaling),
          CLICKHOUSE_CLOUD_IDLE_TIMEOUT_MINUTES: String(
            defaultClickhouseIdleTimeoutMinutes
          ),
          CLICKHOUSE_CLOUD_IP_ACCESS: formatClickhouseIpAccess(
            defaultClickhouseIpAccess
          ),
          CLICKHOUSE_CLOUD_TIER: defaultClickhouseTier ?? '',
          CLICKHOUSE_CLOUD_MIN_TOTAL_MEMORY_GB:
            defaultClickhouseTier === 'production'
              ? String(defaultClickhouseMinTotalMemoryGb)
              : '',
          CLICKHOUSE_CLOUD_MAX_TOTAL_MEMORY_GB:
            defaultClickhouseTier === 'production'
              ? String(defaultClickhouseMaxTotalMemoryGb)
              : '',
          CLICKHOUSE_CLOUD_NUM_REPLICAS:
            defaultClickhouseTier === 'production' &&
            defaultClickhouseNumReplicas !== undefined
              ? String(defaultClickhouseNumReplicas)
              : '',
          CLICKHOUSE_CLOUD_STATE_FILE: path.join(
            '.nanotrace',
            'clickhouse-cloud',
            `${deploymentId}-${defaultClickhouseServiceName}.json`
          )
        },
        triggers: [
          clickhouseCloudScriptHash,
          clickhouseCloudProvider,
          clickhouseCloudRegion,
          defaultClickhouseServiceName,
          defaultClickhouseTier ?? '',
          defaultClickhouseIdleScaling,
          defaultClickhouseIdleTimeoutMinutes,
          formatClickhouseIpAccess(defaultClickhouseIpAccess),
          defaultClickhouseMinTotalMemoryGb,
          defaultClickhouseMaxTotalMemoryGb,
          defaultClickhouseNumReplicas ?? '',
          defaultClickhousePassword!
        ]
      },
      {
        additionalSecretOutputs: ['stdout', 'stderr']
      }
    )
  : undefined

const defaultClickhouseServiceState = defaultClickhouseService?.stdout.apply(
  value => JSON.parse(value) as ClickhouseCloudServiceState
)
const defaultClickhouseHttpsEndpoint =
  defaultClickhouseServiceState?.apply(state => state.url)

const clickhouseUrl =
  defaultClickhouseHttpsEndpoint ??
  requireEnv('CLICKHOUSE_URL')
const clickhouseUser =
  createDefaultClickhouseCloudService
    ? 'default'
    : requireEnv('CLICKHOUSE_USER')
const clickhousePassword =
  defaultClickhousePassword ??
  pulumi.secret(requireEnv('CLICKHOUSE_PASSWORD'))
const clickhouseCloudServiceId =
  defaultClickhouseServiceState?.apply(state => state.id) ?? ''

const managedDataPlaneKmsKey = createDataPlaneKmsKey
  ? new aws.kms.Key(`${name}-data-key`, {
      description: dataPlaneOrganizationId
        ? `Nanotrace data-plane key for ${dataPlaneOrganizationId}`
        : `Nanotrace data key for ${deploymentId}`,
      deletionWindowInDays: cfg.getNumber('kmsDeletionWindowDays') ?? 7,
      enableKeyRotation: true
    })
  : undefined

const dataPlaneKmsKeyArn = configuredDataPlaneKmsKeyArn
  ? pulumi.output(configuredDataPlaneKmsKeyArn)
  : managedDataPlaneKmsKey?.arn

if (managedDataPlaneKmsKey) {
  new aws.kms.Alias(`${name}-data-key-alias`, {
    name: dataPlaneOrganizationId
      ? `alias/nanotrace/${dataPlaneOrganizationId}`
      : `alias/nanotrace/${deploymentId}`,
    targetKeyId: managedDataPlaneKmsKey.keyId
  })
}

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
  const subnet = new aws.ec2.Subnet(
    `${name}-public-${i}`,
    {
      vpcId: vpc.id,
      availabilityZone: azs.names.apply(names => names[i]),
      cidrBlock: `10.42.${i}.0/24`,
      mapPublicIpOnLaunch: true,
      tags: { ...tags, Name: `${name}-public-${i}` }
    },
    { ignoreChanges: ['availabilityZone'] }
  )

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

new aws.s3.BucketServerSideEncryptionConfigurationV2(`${name}-events-encryption`, {
  bucket: bucket.id,
  rules: [
    {
      applyServerSideEncryptionByDefault: dataPlaneKmsKeyArn
        ? {
            kmsMasterKeyId: dataPlaneKmsKeyArn,
            sseAlgorithm: 'aws:kms'
          }
        : {
            sseAlgorithm: 'AES256'
          },
      bucketKeyEnabled: dataPlaneKmsKeyArn ? true : undefined
    }
  ]
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
  kmsMasterKeyId: dataPlaneKmsKeyArn,
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

const imageTag = cfg.get('imageTag') ?? imageBuildId
const imageUri = imageUriOverride
  ? pulumi.output(imageUriOverride)
  : pulumi.interpolate`${repository.repositoryUrl}:${imageTag}`

const imageBuild = buildImage
  ? new command.local.Command(
      `${name}-image`,
      {
        create: pulumi.interpolate`mkdir -p .pulumi-docker && ECR_PASSWORD="$(aws ecr get-login-password --region ${region})" && ECR_AUTH="$(printf 'AWS:%s' "$ECR_PASSWORD" | base64 | tr -d '\n')" && printf '{"auths":{"${repository.repositoryUrl.apply(
          value => value.split('/')[0]
        )}":{"auth":"%s"}}}\n' "$ECR_AUTH" > .pulumi-docker/config.json && docker --config .pulumi-docker build --platform linux/${cpuArchitecture} -t ${imageUri} . && docker --config .pulumi-docker push ${imageUri}`,
        update: pulumi.interpolate`mkdir -p .pulumi-docker && ECR_PASSWORD="$(aws ecr get-login-password --region ${region})" && ECR_AUTH="$(printf 'AWS:%s' "$ECR_PASSWORD" | base64 | tr -d '\n')" && printf '{"auths":{"${repository.repositoryUrl.apply(
          value => value.split('/')[0]
        )}":{"auth":"%s"}}}\n' "$ECR_AUTH" > .pulumi-docker/config.json && docker --config .pulumi-docker build --platform linux/${cpuArchitecture} -t ${imageUri} . && docker --config .pulumi-docker push ${imageUri}`,
        delete: `true`,
        dir: repoRoot,
        triggers: [imageBuildId]
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
    .all([
      bucket.arn,
      repository.arn,
      loaderQueue.arn,
      dataPlaneKmsKeyArn ?? pulumi.output('')
    ])
    .apply(([bucketArn, repositoryArn, queueArn, kmsKeyArn]) =>
      JSON.stringify(
        {
          Version: '2012-10-17',
          Statement: [
            ...(kmsKeyArn
              ? [
                  {
                    Sid: 'UseDataPlaneKmsKey',
                    Effect: 'Allow',
                    Action: [
                      'kms:Decrypt',
                      'kms:Encrypt',
                      'kms:GenerateDataKey',
                      'kms:DescribeKey'
                    ],
                    Resource: kmsKeyArn
                  }
                ]
              : []),
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
              Resource: `${bucketArn}/${processorPrefix}/*`
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
              Sid: 'SendLoginEmail',
              Effect: 'Allow',
              Action: ['ses:SendEmail', 'ses:SendRawEmail'],
              Resource: '*'
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
        }
      )
    )
})

new aws.iam.RolePolicyAttachment(`${name}-instance-ssm`, {
  role: role.name,
  policyArn: 'arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore'
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
      CLICKHOUSE_FACETS_TABLE: clickhouseFacetsTable,
      CLICKHOUSE_EVENT_INDEX_TABLE: clickhouseEventIndexTable,
      CLICKHOUSE_HOT_DIMENSIONS_TABLE: clickhouseHotDimensionsTable,
      CLICKHOUSE_SCHEMA_PATH: 'deploy/clickhouse/schema.sql'
    },
    triggers: [
      schemaHash,
      schemaScriptHash,
      clickhouseUrl,
      clickhouseDatabase,
      clickhouseTable,
      clickhouseFacetsTable,
      clickhouseEventIndexTable,
      clickhouseHotDimensionsTable
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
    },
    ...(edgeTlsMode === 'alb' ? [{
      protocol: 'tcp',
      fromPort: 443,
      toPort: 443,
      cidrBlocks: ['0.0.0.0/0']
    }] : [])
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
}, { ignoreChanges: ['ingress'] })

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

const createManagedPostgres = postgresMode === 'managed'
const createPostgresPrivateLink =
  postgresMode === 'external' && postgresPrivateConnect === 'aws-privatelink'
if (createPostgresPrivateLink && !postgresPrivateLinkServiceName.trim()) {
  throw new Error('NANOTRACE_POSTGRES_PRIVATELINK_SERVICE_NAME is required for aws-privatelink')
}
if (postgresMode === 'external' && !externalPostgresUrl) {
  throw new Error('NANOTRACE_POSTGRES_URL is required when NANOTRACE_POSTGRES_MODE=external')
}

const databaseSg = !createManagedPostgres
  ? undefined
  : new aws.ec2.SecurityGroup(`${name}-postgres-sg`, {
      vpcId: vpc.id,
      ingress: [
        {
          protocol: 'tcp',
          fromPort: 5432,
          toPort: 5432,
          securityGroups: [instanceSg.id]
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
      tags: { ...tags, Name: `${name}-postgres-sg` }
    })

const postgresEndpointSg = !createPostgresPrivateLink
  ? undefined
  : new aws.ec2.SecurityGroup(`${name}-postgres-endpoint-sg`, {
      vpcId: vpc.id,
      ingress: [
        {
          protocol: 'tcp',
          fromPort: 5432,
          toPort: 5432,
          securityGroups: [instanceSg.id]
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
      tags: { ...tags, Name: `${name}-postgres-endpoint-sg` }
    })

const postgresPrivateLinkEndpoint = !createPostgresPrivateLink
  ? undefined
  : new aws.ec2.VpcEndpoint(`${name}-postgres-privatelink`, {
      privateDnsEnabled: true,
      securityGroupIds: [postgresEndpointSg!.id],
      serviceName: postgresPrivateLinkServiceName,
      subnetIds: subnets.map(subnet => subnet.id),
      vpcEndpointType: 'Interface',
      vpcId: vpc.id,
      tags: { ...tags, Name: `${name}-postgres-privatelink` }
    })

const databaseSubnetGroup = !createManagedPostgres
  ? undefined
  : new aws.rds.SubnetGroup(`${name}-postgres-subnets`, {
      subnetIds: subnets.map(subnet => subnet.id),
      tags
    })

const database = !createManagedPostgres
  ? undefined
  : new aws.rds.Instance(`${name}-postgres`, {
      allocatedStorage: databaseAllocatedStorageGb,
      autoMinorVersionUpgrade: true,
      backupRetentionPeriod: databaseBackupRetentionDays,
      dbName: databaseName,
      dbSubnetGroupName: databaseSubnetGroup!.name,
      deletionProtection: databaseDeletionProtection,
      engine: 'postgres',
      identifier: `${name}-postgres`,
      instanceClass: databaseInstanceClass,
      kmsKeyId: dataPlaneKmsKeyArn,
      multiAz: false,
      password: databasePassword,
      publiclyAccessible: false,
      skipFinalSnapshot: databaseSkipFinalSnapshot,
      storageEncrypted: true,
      username: databaseUsername,
      vpcSecurityGroupIds: [databaseSg!.id],
      tags
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

const hostedZone = usesRoute53Dns && !hostedZoneIdOverride
  ? new aws.route53.Zone(`${name}-zone`, {
    name: hostedZoneName,
    tags
  })
  : undefined
const hostedZoneId = hostedZoneIdOverride ?? hostedZone?.zoneId
const cloudflareZone = usesCloudflareDns && !cloudflareZoneIdOverride
  ? cloudflare.getZoneOutput(
    { filter: { name: hostedZoneName } },
    { provider: cloudflareProvider }
  )
  : undefined
const cloudflareZoneId = cloudflareZoneIdOverride ?? cloudflareZone?.zoneId

type ManualDnsRecord = {
  name: string | pulumi.Output<string>
  type: string | pulumi.Output<string>
  value: string | pulumi.Output<string>
  ttl?: number
  priority?: number
  proxied?: boolean
  purpose: string
}
const manualDnsRecords: ManualDnsRecord[] = []

type ClickhouseCloudServiceState = {
  id: string
  url: string
}

const uiBucket = new aws.s3.BucketV2(`${name}-ui`, {
  forceDestroy: cfg.getBoolean('forceDestroyUiBucket') ?? false,
  tags: { ...tags, Service: 'ui' }
})

new aws.s3.BucketPublicAccessBlock(`${name}-ui-public-access`, {
  bucket: uiBucket.id,
  blockPublicAcls: true,
  blockPublicPolicy: true,
  ignorePublicAcls: true,
  restrictPublicBuckets: true
})

new aws.s3.BucketServerSideEncryptionConfigurationV2(`${name}-ui-encryption`, {
  bucket: uiBucket.id,
  rules: [
    {
      applyServerSideEncryptionByDefault: {
        sseAlgorithm: 'AES256'
      }
    }
  ]
})

const uiCertificate = dnsProvider === 'external'
  ? undefined
  : new aws.acm.Certificate(`${name}-ui-certificate`, {
      domainName,
      validationMethod: 'DNS',
      tags
    }, { provider: usEast1 })

const uiCertificateDomainValidationOption =
  uiCertificate?.domainValidationOptions.apply(options => {
    return options?.[0] ?? {
      domainName,
      resourceRecordName: `_pending-validation.${domainName}`,
      resourceRecordType: 'CNAME',
      resourceRecordValue: 'pending-validation'
    }
  })

const uiCertificateValidationRecordFqdn = !uiCertificateDomainValidationOption
  ? undefined
  : dnsProvider === 'cloudflare'
    ? new cloudflare.Record(`${name}-ui-certificate-validation`, {
        content: uiCertificateDomainValidationOption.resourceRecordValue,
        name: uiCertificateDomainValidationOption.resourceRecordName,
        proxied: false,
        ttl: 1,
        type: 'CNAME',
        zoneId: cloudflareZoneId
      }, { provider: cloudflareProvider }).name
    : new aws.route53.Record(`${name}-ui-certificate-validation`, {
        allowOverwrite: true,
        name: uiCertificateDomainValidationOption.resourceRecordName,
        records: [uiCertificateDomainValidationOption.resourceRecordValue],
        ttl: 60,
        type: uiCertificateDomainValidationOption.resourceRecordType,
        zoneId: hostedZoneId!
      }).fqdn

const uiCertificateValidation = uiCertificate && uiCertificateValidationRecordFqdn
  ? new aws.acm.CertificateValidation(`${name}-ui-certificate-validation`, {
      certificateArn: uiCertificate.arn,
      validationRecordFqdns: [uiCertificateValidationRecordFqdn]
    }, { provider: usEast1 })
  : undefined

const uiOriginAccessControl = new aws.cloudfront.OriginAccessControl(`${name}-ui-oac`, {
  description: `Private S3 access for ${domainName}`,
  originAccessControlOriginType: 's3',
  signingBehavior: 'always',
  signingProtocol: 'sigv4'
})

const uiOriginId = `${name}-ui-origin`
const uiDistribution = new aws.cloudfront.Distribution(`${name}-ui`, {
  aliases: uiCertificateValidation ? [domainName] : [],
  comment: `${name} UI`,
  defaultCacheBehavior: {
    allowedMethods: ['GET', 'HEAD', 'OPTIONS'],
    cachedMethods: ['GET', 'HEAD'],
    compress: true,
    forwardedValues: {
      cookies: { forward: 'none' },
      queryString: false
    },
    maxTtl: 31_536_000,
    minTtl: 0,
    defaultTtl: 60,
    targetOriginId: uiOriginId,
    viewerProtocolPolicy: 'redirect-to-https'
  },
  customErrorResponses: [
    {
      errorCode: 403,
      responseCode: 200,
      responsePagePath: '/index.html',
      errorCachingMinTtl: 0
    },
    {
      errorCode: 404,
      responseCode: 200,
      responsePagePath: '/index.html',
      errorCachingMinTtl: 0
    }
  ],
  defaultRootObject: 'index.html',
  enabled: true,
  isIpv6Enabled: true,
  origins: [
    {
      domainName: uiBucket.bucketRegionalDomainName,
      originAccessControlId: uiOriginAccessControl.id,
      originId: uiOriginId
    }
  ],
  priceClass: cfg.get('uiCloudFrontPriceClass') ?? 'PriceClass_100',
  restrictions: {
    geoRestriction: {
      restrictionType: 'none'
    }
  },
  viewerCertificate: uiCertificateValidation
    ? {
        acmCertificateArn: uiCertificateValidation.certificateArn,
        minimumProtocolVersion: 'TLSv1.2_2021',
        sslSupportMethod: 'sni-only'
      }
    : {
        cloudfrontDefaultCertificate: true
      },
  tags: { ...tags, Service: 'ui' }
})

new aws.s3.BucketPolicy(`${name}-ui-policy`, {
  bucket: uiBucket.id,
  policy: pulumi.all([uiBucket.arn, uiDistribution.arn]).apply(([bucketArn, distributionArn]) =>
    JSON.stringify({
      Version: '2012-10-17',
      Statement: [
        {
          Sid: 'AllowCloudFrontRead',
          Effect: 'Allow',
          Principal: { Service: 'cloudfront.amazonaws.com' },
          Action: 's3:GetObject',
          Resource: `${bucketArn}/*`,
          Condition: {
            StringEquals: {
              'AWS:SourceArn': distributionArn
            }
          }
        }
      ]
    })
  )
})

if (dnsProvider === 'external') {
  manualDnsRecords.push(
    {
      name: domainName,
      purpose: 'Point the Nanotrace UI domain at the CloudFront distribution. Use ALIAS/ANAME where your DNS provider does not allow CNAME records at the zone apex.',
      ttl: 60,
      type: domainName === hostedZoneName ? 'CNAME/ALIAS' : 'CNAME',
      value: uiDistribution.domainName
    },
    {
      name: apiDomainName,
      purpose: 'Point the Nanotrace API domain at the application load balancer. Use ALIAS/ANAME where your DNS provider does not allow CNAME records at the zone apex.',
      ttl: 60,
      type: apiDomainName === hostedZoneName ? 'CNAME/ALIAS' : 'CNAME',
      value: lb.dnsName
    }
  )
}

if (dnsProvider === 'cloudflare' && usesEdgeFlexibleTls) {
  new cloudflare.Record(`${name}-api-cloudflare-flexible-alias`, {
    content: lb.dnsName,
    name: cloudflareRecordName(apiDomainName, hostedZoneName),
    proxied: true,
    ttl: 1,
    type: 'CNAME',
    zoneId: cloudflareZoneId
  }, { provider: cloudflareProvider })
}

if (dnsProvider === 'cloudflare' && uiCertificateValidation) {
  new cloudflare.Record(`${name}-ui-alias`, {
    content: uiDistribution.domainName,
    name: cloudflareRecordName(domainName, hostedZoneName),
    proxied: false,
    ttl: 1,
    type: 'CNAME',
    zoneId: cloudflareZoneId
  }, { provider: cloudflareProvider })
} else if (dnsProvider === 'route53' && uiCertificateValidation) {
  new aws.route53.Record(`${name}-ui-alias`, {
    aliases: [
      {
        evaluateTargetHealth: false,
        name: uiDistribution.domainName,
        zoneId: uiDistribution.hostedZoneId
      }
    ],
    name: domainName,
    type: 'A',
    zoneId: hostedZoneId!
  })
}

if (manageLoginEmailDns && dnsProvider === 'cloudflare' && managedLoginEmailIdentity) {
  for (const index of [0, 1, 2]) {
    const dkimName = managedLoginEmailIdentity.dkimSigningAttributes.tokens.apply(tokens =>
      `${tokens[index]}._domainkey.${loginEmailIdentityDomain}`
    )
    const dkimTarget = managedLoginEmailIdentity.dkimSigningAttributes.tokens.apply(tokens =>
      `${tokens[index]}.dkim.amazonses.com`
    )
    new cloudflare.Record(`${name}-login-email-dkim-${index}`, {
      content: dkimTarget,
      name: dkimName,
      proxied: false,
      ttl: 1,
      type: 'CNAME',
      zoneId: cloudflareZoneId
    }, { provider: cloudflareProvider })
  }

  new cloudflare.Record(`${name}-login-email-mail-from-mx`, {
    content: `feedback-smtp.${region}.amazonses.com`,
    name: loginEmailMailFromDomain,
    priority: 10,
    proxied: false,
    ttl: 1,
    type: 'MX',
    zoneId: cloudflareZoneId
  }, { provider: cloudflareProvider, dependsOn: managedLoginEmailMailFrom ? [managedLoginEmailMailFrom] : [] })

  new cloudflare.Record(`${name}-login-email-mail-from-spf`, {
    content: 'v=spf1 include:amazonses.com -all',
    name: loginEmailMailFromDomain,
    proxied: false,
    ttl: 1,
    type: 'TXT',
    zoneId: cloudflareZoneId
  }, { provider: cloudflareProvider })

  new cloudflare.Record(`${name}-login-email-dmarc`, {
    content: 'v=DMARC1; p=none',
    name: `_dmarc.${loginEmailIdentityDomain}`,
    proxied: false,
    ttl: 1,
    type: 'TXT',
    zoneId: cloudflareZoneId
  }, { provider: cloudflareProvider })
} else if (manageLoginEmailDns && dnsProvider === 'route53' && managedLoginEmailIdentity) {
  for (const index of [0, 1, 2]) {
    const dkimName = managedLoginEmailIdentity.dkimSigningAttributes.tokens.apply(tokens =>
      `${tokens[index]}._domainkey.${loginEmailIdentityDomain}`
    )
    const dkimTarget = managedLoginEmailIdentity.dkimSigningAttributes.tokens.apply(tokens =>
      `${tokens[index]}.dkim.amazonses.com`
    )
    new aws.route53.Record(`${name}-login-email-dkim-${index}`, {
      allowOverwrite: true,
      name: dkimName,
      records: [dkimTarget],
      ttl: 60,
      type: 'CNAME',
      zoneId: hostedZoneId!
    })
  }

  new aws.route53.Record(`${name}-login-email-mail-from-mx`, {
    allowOverwrite: true,
    name: loginEmailMailFromDomain,
    records: [`10 feedback-smtp.${region}.amazonses.com`],
    ttl: 60,
    type: 'MX',
    zoneId: hostedZoneId!
  }, { dependsOn: managedLoginEmailMailFrom ? [managedLoginEmailMailFrom] : [] })

  new aws.route53.Record(`${name}-login-email-mail-from-spf`, {
    allowOverwrite: true,
    name: loginEmailMailFromDomain,
    records: ['v=spf1 include:amazonses.com -all'],
    ttl: 60,
    type: 'TXT',
    zoneId: hostedZoneId!
  })

  new aws.route53.Record(`${name}-login-email-dmarc`, {
    allowOverwrite: true,
    name: `_dmarc.${loginEmailIdentityDomain}`,
    records: ['v=DMARC1; p=none'],
    ttl: 60,
    type: 'TXT',
    zoneId: hostedZoneId!
  })
} else if (manageLoginEmailDns && dnsProvider === 'external' && managedLoginEmailIdentity) {
  for (const index of [0, 1, 2]) {
    const dkimName = managedLoginEmailIdentity.dkimSigningAttributes.tokens.apply(tokens =>
      `${tokens[index]}._domainkey.${loginEmailIdentityDomain}`
    )
    const dkimTarget = managedLoginEmailIdentity.dkimSigningAttributes.tokens.apply(tokens =>
      `${tokens[index]}.dkim.amazonses.com`
    )
    manualDnsRecords.push({
      name: dkimName,
      purpose: 'Verify the SES domain identity for login email DKIM signing.',
      ttl: 60,
      type: 'CNAME',
      value: dkimTarget
    })
  }

  manualDnsRecords.push(
    {
      name: loginEmailMailFromDomain,
      priority: 10,
      purpose: 'Configure the SES custom MAIL FROM bounce domain for login email.',
      ttl: 60,
      type: 'MX',
      value: `feedback-smtp.${region}.amazonses.com`
    },
    {
      name: loginEmailMailFromDomain,
      purpose: 'Allow SES to send login email for the custom MAIL FROM domain.',
      ttl: 60,
      type: 'TXT',
      value: 'v=spf1 include:amazonses.com -all'
    },
    {
      name: `_dmarc.${loginEmailIdentityDomain}`,
      purpose: 'Publish a baseline DMARC policy for login email.',
      ttl: 60,
      type: 'TXT',
      value: 'v=DMARC1; p=none'
    }
  )
}

let certificateValidation: aws.acm.CertificateValidation | undefined
let certificateArn: pulumi.Output<string> | undefined
if (edgeTlsMode === 'alb') {
  const certificate = new aws.acm.Certificate(`${name}-certificate`, {
    domainName: apiDomainName,
    validationMethod: 'DNS',
    tags
  })
  const certificateDomainValidationOption =
    certificate.domainValidationOptions.apply(options => {
      return options?.[0] ?? {
        domainName: apiDomainName,
        resourceRecordName: `_pending-validation.${apiDomainName}`,
        resourceRecordType: 'CNAME',
        resourceRecordValue: 'pending-validation'
      }
    })

  const certificateValidationRecordFqdn =
    dnsProvider === 'cloudflare'
      ? new cloudflare.Record(`${name}-certificate-validation`, {
        content: certificateDomainValidationOption.resourceRecordValue,
        name: certificateDomainValidationOption.resourceRecordName,
        proxied: false,
        ttl: 1,
        type: 'CNAME',
        zoneId: cloudflareZoneId
      }, { provider: cloudflareProvider }).name
      : new aws.route53.Record(
        `${name}-certificate-validation`,
        {
          allowOverwrite: true,
          name: certificateDomainValidationOption.resourceRecordName,
          records: [
            certificateDomainValidationOption.resourceRecordValue
          ],
          ttl: 60,
          type: certificateDomainValidationOption.resourceRecordType,
          zoneId: hostedZoneId!
        }
      ).fqdn

  certificateValidation = new aws.acm.CertificateValidation(
    `${name}-certificate-validation`,
    {
      certificateArn: certificate.arn,
      validationRecordFqdns: [certificateValidationRecordFqdn]
    }
  )
  certificateArn = certificateValidation.certificateArn

  if (dnsProvider === 'cloudflare') {
    new cloudflare.Record(`${name}-api-alias`, {
      content: lb.dnsName,
      name: cloudflareRecordName(apiDomainName, hostedZoneName),
      proxied: false,
      ttl: 1,
      type: 'CNAME',
      zoneId: cloudflareZoneId
    }, { provider: cloudflareProvider })
  } else if (dnsProvider === 'route53') {
    new aws.route53.Record(`${name}-api-alias`, {
      aliases: [
        {
          evaluateTargetHealth: true,
          name: lb.dnsName,
          zoneId: lb.zoneId
        }
      ],
      name: apiDomainName,
      type: 'A',
      zoneId: hostedZoneId!
    })
  }
}

const httpListener = new aws.lb.Listener(`${name}-http`, {
  loadBalancerArn: lb.arn,
  port: 80,
  protocol: 'HTTP',
  defaultActions: edgeTlsMode === 'alb'
    ? [
      {
        type: 'redirect',
        redirect: {
          port: '443',
          protocol: 'HTTPS',
          statusCode: 'HTTP_301'
        }
      }
    ]
    : [{ type: 'forward', targetGroupArn: targetGroup.arn }]
})

const listener = edgeTlsMode === 'alb'
  ? new aws.lb.Listener(`${name}-https`, {
    certificateArn: certificateArn!,
    loadBalancerArn: lb.arn,
    port: 443,
    protocol: 'HTTPS',
    sslPolicy: 'ELBSecurityPolicy-TLS13-1-2-2021-06',
    defaultActions: [{ type: 'forward', targetGroupArn: targetGroup.arn }]
  })
  : httpListener

const publicBaseUrl =
  cfg.get('publicBaseUrl') ??
  process.env.NANOTRACE_PUBLIC_BASE_URL ??
  appBaseUrl
const databaseUrl = database
  ? pulumi.interpolate`postgres://${databaseUsername}:${databasePassword}@${database.address}:5432/${databaseName}`
  : externalPostgresUrl ?? pulumi.output('')

new aws.lb.ListenerRule(`${name}-query-route`, {
  listenerArn: listener.arn,
  priority: 10,
  conditions: [
    { pathPattern: { values: ['/query'] } },
    { httpRequestMethod: { values: ['POST'] } }
  ],
  actions: [{ type: 'forward', targetGroupArn: queryTargetGroup.arn }]
})

new aws.lb.ListenerRule(`${name}-internal-query-route`, {
  listenerArn: listener.arn,
  priority: 11,
  conditions: [
    { pathPattern: { values: ['/internal/query'] } },
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

new aws.lb.ListenerRule(`${name}-internal-event-read-route`, {
  listenerArn: listener.arn,
  priority: 21,
  conditions: [
    { pathPattern: { values: ['/internal/events/*'] } },
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
    bucket.bucket,
    imageUri,
    imageBuildId,
    clickhouseUrl,
    clickhousePassword,
    databaseUrl,
    bootstrapApiKey,
    loaderQueue.url,
    modalTokenId,
    modalTokenSecret,
    modalServerApiKey,
    dataPlaneSharedSecret,
    publicBaseUrl
  ])
  .apply(
    ([
      bucketName,
      resolvedImageUri,
      resolvedImageBuildId,
      resolvedClickhouseUrl,
      resolvedClickhousePassword,
      resolvedDatabaseUrl,
      resolvedBootstrapApiKey,
      loaderQueueUrl,
      resolvedModalTokenId,
      resolvedModalTokenSecret,
      resolvedModalServerApiKey,
      resolvedDataPlaneSharedSecret,
      resolvedPublicBaseUrl
    ]) =>
      renderUserData({
        bucketName,
        adminEmails,
        bootstrapApiKey: resolvedBootstrapApiKey,
        clickhouseDatabase,
        clickhouseEventIndexTable,
        clickhousePassword: resolvedClickhousePassword,
        clickhouseFacetsTable,
        clickhouseHotDimensionsTable,
        clickhouseTable,
        clickhouseUrl: resolvedClickhouseUrl,
        clickhouseUser,
        clickhouseMaxBytesToRead,
        imageUri: resolvedImageUri,
        imageBuildId: resolvedImageBuildId,
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
        uploadPollIntervalMs,
        writerFlushBytes,
        writerFlushIntervalMs,
        writerLanes,
        writerQueueCapacity,
        compactBatchReceipts,
        dataPlaneOrganizationId,
        dataPlaneSharedSecret: resolvedDataPlaneSharedSecret,
        databaseUrl: resolvedDatabaseUrl,
        allowedEmails,
        appBaseUrl,
        corsAllowedOrigins,
        emailFrom,
        processorPrefix,
        publicBaseUrl: resolvedPublicBaseUrl,
        sessionSecure
      })
  )

const queryUserData = pulumi
  .all([bucket.bucket, imageUri, imageBuildId, clickhouseUrl, clickhousePassword, databaseUrl, bootstrapApiKey, dataPlaneSharedSecret, publicBaseUrl])
  .apply(([bucketName, resolvedImageUri, resolvedImageBuildId, resolvedClickhouseUrl, resolvedClickhousePassword, resolvedDatabaseUrl, resolvedBootstrapApiKey, resolvedDataPlaneSharedSecret, resolvedPublicBaseUrl]) =>
    renderQueryUserData({
      bucketName,
      bootstrapApiKey: resolvedBootstrapApiKey,
      clickhouseDatabase,
      clickhousePassword: resolvedClickhousePassword,
      clickhouseTable,
      clickhouseUrl: resolvedClickhouseUrl,
      clickhouseUser,
      clickhouseMaxBytesToRead,
      imageUri: resolvedImageUri,
      imageBuildId: resolvedImageBuildId,
      maxRequestBytes,
      port,
      prefix,
      region,
      databaseUrl: resolvedDatabaseUrl,
      appBaseUrl,
      dataPlaneOrganizationId,
      dataPlaneSharedSecret: resolvedDataPlaneSharedSecret,
      publicBaseUrl: resolvedPublicBaseUrl,
      corsAllowedOrigins,
      sessionSecure
    })
  )

const launchTemplate = new aws.ec2.LaunchTemplate(
  `${name}-lt`,
  {
    imageId: ami.id,
    instanceType,
    iamInstanceProfile: { arn: instanceProfile.arn },
    metadataOptions: {
      httpEndpoint: 'enabled',
      httpTokens: 'required',
      httpPutResponseHopLimit: 2
    },
    vpcSecurityGroupIds: [instanceSg.id],
    userData: userData.apply(value => Buffer.from(value).toString('base64')),
    blockDeviceMappings: [
      {
        deviceName: '/dev/xvda',
        ebs: {
          volumeSize: cfg.getNumber('rootVolumeSizeGb') ?? 16,
          volumeType: 'gp3',
          deleteOnTermination: 'true',
          encrypted: 'true',
          kmsKeyId: dataPlaneKmsKeyArn
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
          encrypted: 'true',
          kmsKeyId: dataPlaneKmsKeyArn
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
    dependsOn: imageBuild ? [imageBuild, clickHouseSchema] : [clickHouseSchema]
  }
)

const queryLaunchTemplate = new aws.ec2.LaunchTemplate(
  `${name}-query-lt`,
  {
    imageId: ami.id,
    instanceType: queryInstanceType,
    iamInstanceProfile: { arn: instanceProfile.arn },
    metadataOptions: {
      httpEndpoint: 'enabled',
      httpTokens: 'required',
      httpPutResponseHopLimit: 2
    },
    vpcSecurityGroupIds: [instanceSg.id],
    userData: queryUserData.apply(value => Buffer.from(value).toString('base64')),
    blockDeviceMappings: [
      {
        deviceName: '/dev/xvda',
        ebs: {
          volumeSize: cfg.getNumber('queryRootVolumeSizeGb') ?? 16,
          volumeType: 'gp3',
          deleteOnTermination: 'true',
          encrypted: 'true',
          kmsKeyId: dataPlaneKmsKeyArn
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
    dependsOn: imageBuild ? [imageBuild, clickHouseSchema] : [clickHouseSchema]
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
export const domainNameOutput = domainName
export const apiDomainNameOutput = apiDomainName
export const appBaseUrlOutput = appBaseUrl
export const apiBaseUrlOutput = apiBaseUrl
export const dataPlaneOrganizationIdOutput = dataPlaneOrganizationId
export const dataPlaneKmsKeyArnOutput = dataPlaneKmsKeyArn ?? ''
export const processorPrefixOutput = processorPrefix
export const dnsProviderOutput = dnsProvider
export const edgeTlsModeOutput = edgeTlsMode
export const hostedZoneNameOutput = hostedZoneName
export const hostedZoneNameServers = hostedZone
  ? hostedZone.nameServers
  : []
export const manualDnsRecordsOutput = manualDnsRecords
export const uiBucketName = uiBucket.bucket
export const uiCloudFrontDistributionId = uiDistribution.id
export const uiCloudFrontDomainName = uiDistribution.domainName
export const uiUrl = appBaseUrl
export const ingestUrl = apiBaseUrl
export const queryTargetGroupArn = queryTargetGroup.arn
export const ingestAutoScalingGroupName = asg.name
export const queryAutoScalingGroupName = queryAsg.name
export const bucketName = bucket.bucket
export const objectPrefix = prefix
export const loaderSqsQueueUrl = loaderQueue.url
export const loaderSqsQueueArn = loaderQueue.arn
export const clickhouseModeOutput = clickhouseMode
export const clickhouseCloudServiceIdOutput = clickhouseCloudServiceId
export const clickhouseUrlOutput = clickhouseUrl
export const clickhouseUserOutput = clickhouseUser
export const clickhouseDatabaseOutput = clickhouseDatabase
export const clickhouseTableOutput = clickhouseTable
export const clickhouseEventIndexTableOutput = clickhouseEventIndexTable
export const clickhouseHotDimensionsTableOutput = clickhouseHotDimensionsTable
export const databaseEndpoint = database ? database.address : ''
export const postgresModeOutput = postgresMode
export const postgresPrivateConnectOutput = postgresPrivateConnect
export const postgresPrivateLinkEndpointId = postgresPrivateLinkEndpoint
  ? postgresPrivateLinkEndpoint.id
  : ''
export const loginEmailIdentity = managedLoginEmailIdentity
  ? managedLoginEmailIdentity.emailIdentity
  : ''
export const loginEmailFrom = emailFrom
export const loginEmailMailFromDomainOutput = managedLoginEmailMailFrom
  ? managedLoginEmailMailFrom.mailFromDomain
  : loginEmailMailFromDomain
export const loginEmailDkimTokens = managedLoginEmailIdentity
  ? managedLoginEmailIdentity.dkimSigningAttributes.tokens
  : []
export const loginEmailVerifiedForSending = managedLoginEmailIdentity
  ? managedLoginEmailIdentity.verifiedForSendingStatus
  : false
export const bootstrapApiKeyOutput = pulumi.secret(bootstrapApiKey)
export const ecrRepositoryUrl = repository.repositoryUrl
export const serverImageUri = imageUri

interface UserDataArgs {
  bucketName: string
  adminEmails: string
  bootstrapApiKey: string
  clickhouseDatabase: string
  clickhouseEventIndexTable: string
  clickhousePassword: string
  clickhouseFacetsTable: string
  clickhouseHotDimensionsTable: string
  clickhouseTable: string
  clickhouseUrl: string
  clickhouseUser: string
  clickhouseMaxBytesToRead: number
  imageUri: string
  imageBuildId: string
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
  databaseUrl: string
  allowedEmails: string
  appBaseUrl: string
  corsAllowedOrigins: string
  emailFrom: string
  publicBaseUrl: string
  sessionSecure: boolean
  uploadPollIntervalMs: number
  writerFlushBytes: number
  writerFlushIntervalMs: number
  writerLanes: number
  writerQueueCapacity: number
  compactBatchReceipts: boolean
  dataPlaneOrganizationId: string
  dataPlaneSharedSecret: string
  processorPrefix: string
}

interface QueryUserDataArgs {
  bucketName: string
  bootstrapApiKey: string
  clickhouseDatabase: string
  clickhousePassword: string
  clickhouseTable: string
  clickhouseUrl: string
  clickhouseUser: string
  clickhouseMaxBytesToRead: number
  imageUri: string
  imageBuildId: string
  maxRequestBytes: number
  port: number
  prefix: string
  region: string
  databaseUrl: string
  appBaseUrl: string
  dataPlaneOrganizationId: string
  dataPlaneSharedSecret: string
  publicBaseUrl: string
  corsAllowedOrigins: string
  sessionSecure: boolean
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
  for f in "$LOG" /tmp/docker-ps.txt /tmp/docker-logs.txt /tmp/docker-loader-logs.txt /tmp/docker-inspect.txt /tmp/docker-loader-inspect.txt /tmp/docker-journal.txt /tmp/cloud-init-output.log; do
    aws s3 cp "$f" "$S3_DEBUG_PREFIX/$(basename "$f")" --region ${shellQuote(
      args.region
    )} || true
  done
}
trap upload_debug EXIT

set -e
dnf update -y
dnf install -y docker awscli xfsprogs amazon-ssm-agent
systemctl enable --now docker
systemctl enable --now amazon-ssm-agent || true

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
  -e PORT=${args.port} \\
  -e NANOTRACE_IMAGE_BUILD_ID=${shellQuote(args.imageBuildId)} \\
  -e NANOTRACE_POSTGRES_URL=${shellQuote(args.databaseUrl)} \\
  -e NANOTRACE_BOOTSTRAP_API_KEY=${shellQuote(args.bootstrapApiKey)} \\
  -e NANOTRACE_DATA_PLANE_ORGANIZATION_ID=${shellQuote(args.dataPlaneOrganizationId)} \\
  -e NANOTRACE_DATA_PLANE_SHARED_SECRET=${shellQuote(args.dataPlaneSharedSecret)} \\
  -e NANOTRACE_PUBLIC_BASE_URL=${shellQuote(args.publicBaseUrl)} \\
  -e NANOTRACE_APP_BASE_URL=${shellQuote(args.appBaseUrl)} \\
  -e NANOTRACE_SESSION_SECURE=${args.sessionSecure ? 'true' : 'false'} \\
  -e NANOTRACE_EMAIL_FROM=${shellQuote(args.emailFrom)} \\
  -e NANOTRACE_ALLOWED_EMAILS=${shellQuote(args.allowedEmails)} \\
  -e NANOTRACE_ADMIN_EMAILS=${shellQuote(args.adminEmails)} \\
  -e NANOTRACE_CORS_ALLOWED_ORIGINS=${shellQuote(args.corsAllowedOrigins)} \\
  -e NANOTRACE_DATA_DIR=${shellQuote(args.localDataDir)} \\
  -e NANOTRACE_S3_BUCKET=${shellQuote(args.bucketName)} \\
  -e S3_PREFIX=${shellQuote(args.prefix)} \\
  -e CLICKHOUSE_URL=${shellQuote(args.clickhouseUrl)} \\
  -e CLICKHOUSE_USER=${shellQuote(args.clickhouseUser)} \\
  -e CLICKHOUSE_PASSWORD=${shellQuote(args.clickhousePassword)} \\
  -e CLICKHOUSE_DATABASE=${shellQuote(args.clickhouseDatabase)} \\
  -e CLICKHOUSE_TABLE=${shellQuote(args.clickhouseTable)} \\
  -e CLICKHOUSE_HOT_DIMENSIONS_TABLE=${shellQuote(args.clickhouseHotDimensionsTable)} \\
  -e CLICKHOUSE_MAX_BYTES_TO_READ=${args.clickhouseMaxBytesToRead} \\
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
  -e PROCESSOR_PREFIX=${shellQuote(args.processorPrefix)} \\
  -e PROCESSOR_BUILDER_CMD=${shellQuote('python3 /usr/local/bin/modal_processor_builder.py')} \\
  ${shellQuote(args.imageUri)}
docker run -d --name nanotrace-loader --restart unless-stopped \\
  -e AWS_REGION=${shellQuote(args.region)} \\
  -e NANOTRACE_IMAGE_BUILD_ID=${shellQuote(args.imageBuildId)} \\
  -e LOADER_SQS_QUEUE_URL=${shellQuote(args.loaderQueueUrl)} \\
  -e PROCESSOR_S3_BUCKET=${shellQuote(args.bucketName)} \\
  -e PROCESSOR_PREFIX=${shellQuote(args.processorPrefix)} \\
  -e CLICKHOUSE_URL=${shellQuote(args.clickhouseUrl)} \\
  -e CLICKHOUSE_USER=${shellQuote(args.clickhouseUser)} \\
  -e CLICKHOUSE_PASSWORD=${shellQuote(args.clickhousePassword)} \\
  -e CLICKHOUSE_DATABASE=${shellQuote(args.clickhouseDatabase)} \\
  -e CLICKHOUSE_TABLE=${shellQuote(args.clickhouseTable)} \\
  -e CLICKHOUSE_FACETS_TABLE=${shellQuote(args.clickhouseFacetsTable)} \\
  -e CLICKHOUSE_EVENT_INDEX_TABLE=${shellQuote(args.clickhouseEventIndexTable)} \\
  -e CLICKHOUSE_HOT_DIMENSIONS_TABLE=${shellQuote(args.clickhouseHotDimensionsTable)} \\
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
dnf install -y docker awscli amazon-ssm-agent
systemctl enable --now docker
systemctl enable --now amazon-ssm-agent || true

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
  -e PORT=${args.port} \\
  -e NANOTRACE_IMAGE_BUILD_ID=${shellQuote(args.imageBuildId)} \\
  -e NANOTRACE_POSTGRES_URL=${shellQuote(args.databaseUrl)} \\
  -e NANOTRACE_BOOTSTRAP_API_KEY=${shellQuote(args.bootstrapApiKey)} \\
  -e NANOTRACE_DATA_PLANE_ORGANIZATION_ID=${shellQuote(args.dataPlaneOrganizationId)} \\
  -e NANOTRACE_DATA_PLANE_SHARED_SECRET=${shellQuote(args.dataPlaneSharedSecret)} \\
  -e NANOTRACE_PUBLIC_BASE_URL=${shellQuote(args.publicBaseUrl)} \\
  -e NANOTRACE_APP_BASE_URL=${shellQuote(args.appBaseUrl)} \\
  -e NANOTRACE_SESSION_SECURE=${args.sessionSecure ? 'true' : 'false'} \\
  -e NANOTRACE_CORS_ALLOWED_ORIGINS=${shellQuote(args.corsAllowedOrigins)} \\
  -e NANOTRACE_S3_BUCKET=${shellQuote(args.bucketName)} \\
  -e CLICKHOUSE_URL=${shellQuote(args.clickhouseUrl)} \\
  -e CLICKHOUSE_USER=${shellQuote(args.clickhouseUser)} \\
  -e CLICKHOUSE_PASSWORD=${shellQuote(args.clickhousePassword)} \\
  -e CLICKHOUSE_DATABASE=${shellQuote(args.clickhouseDatabase)} \\
  -e CLICKHOUSE_TABLE=${shellQuote(args.clickhouseTable)} \\
  -e CLICKHOUSE_MAX_BYTES_TO_READ=${args.clickhouseMaxBytesToRead} \\
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
    throw new Error(`Missing required Pulumi config or ${key} environment variable`)
  }
  return value
}

function requireConfigOrEnv (configKey: string, envKey: string): string {
  const value = cfg.get(configKey) ?? process.env[envKey]
  if (!value) {
    throw new Error(
      `Missing required Pulumi config nanotrace:${configKey} or ${envKey} environment variable`
    )
  }
  return value
}

function normalizeDomainName (value: string): string {
  const normalized = value.trim().replace(/\.$/, '')
  if (!normalized || normalized.includes('/') || normalized.includes(':')) {
    throw new Error(`Invalid domain name: ${value}`)
  }
  return normalized
}

function domainFromEmail (value: string): string {
  const trimmed = value.trim()
  const at = trimmed.lastIndexOf('@')
  if (at <= 0 || at === trimmed.length - 1) {
    throw new Error(`Invalid email sender: ${value}`)
  }
  return trimmed.slice(at + 1)
}

function cloudflareRecordName (recordName: string, zoneName: string): string {
  if (recordName === zoneName) {
    return '@'
  }
  const suffix = `.${zoneName}`
  if (!recordName.endsWith(suffix)) {
    throw new Error(`${recordName} is not in Cloudflare zone ${zoneName}`)
  }
  return recordName.slice(0, -suffix.length)
}

function normalizeClickhouseMode (value: string): 'shared-service' | 'dedicated-service' | 'external' {
  const normalized = value.trim()
  if (
    normalized === 'shared-service' ||
    normalized === 'dedicated-service' ||
    normalized === 'external'
  ) {
    return normalized
  }
  throw new Error(
    'NANOTRACE_CLICKHOUSE_MODE must be shared-service, dedicated-service, or external'
  )
}

function clickhouseDatabaseName (organizationId: string): string {
  const normalized = organizationId
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]+/g, '_')
    .replace(/^_+|_+$/g, '')
  if (!normalized) {
    return 'observatory'
  }
  return /^[a-z_]/.test(normalized)
    ? normalized
    : `org_${normalized}`
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

function optionalNumberEnv (key: string): number | undefined {
  const value = process.env[key]
  if (!value) {
    return undefined
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

function parseClickhouseIpAccess (
  value: string
): { source: string; description?: string }[] {
  const entries = value
    .split(',')
    .map(entry => entry.trim())
    .filter(Boolean)
  if (entries.length === 0) {
    throw new Error('ClickHouse Cloud IP access list must not be empty')
  }
  return entries.map(entry => {
    const [source, description] = entry.split(':', 2)
    if (!source) {
      throw new Error(`Invalid ClickHouse Cloud IP access entry: ${entry}`)
    }
    return {
      source,
      description: description || `Nanotrace access ${source}`
    }
  })
}

function formatClickhouseIpAccess (
  entries: { source: string; description?: string }[]
): string {
  return entries
    .map(entry => `${entry.source}:${entry.description ?? ''}`)
    .join(',')
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
