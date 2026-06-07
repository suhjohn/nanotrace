import * as aws from '@pulumi/aws'
import * as cloudflare from '@pulumi/cloudflare'
import * as command from '@pulumi/command'
import * as pulumi from '@pulumi/pulumi'
import { readdirSync, readFileSync, statSync } from 'node:fs'
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
if (cfg.get('bootstrapApiKey') || process.env.NANOTRACE_BOOTSTRAP_API_KEY) {
  throw new Error('bootstrap API keys are not supported in cloud deployments; use magic-link admin login and created API keys instead')
}

const deploymentId = cfg.get('deploymentId') ?? pulumi.getStack()
const name = cfg.get('name') ?? `nanotrace-${deploymentId}`
const prefix =
  cfg.get('objectPrefix') ??
  process.env.S3_PREFIX ??
  process.env.NANOTRACE_OBJECT_PREFIX ??
  'ops'
const normalizedPrefix = prefix.replace(/^\/+|\/+$/g, '')
const createLoginEmailResources = true
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
const clickhouseDatabase =
  process.env.CLICKHOUSE_DATABASE ??
  'observatory'
const clickhouseTable = process.env.CLICKHOUSE_TABLE ?? 'events'
const clickhouseSchemaPath =
  cfg.get('clickhouseSchemaPath') ??
  process.env.CLICKHOUSE_SCHEMA_PATH ??
  'deploy/clickhouse/schema.sql'
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
const maxRequestBytes = cfg.getNumber('maxRequestBytes') ?? 209_715_200
const maxEventBytes =
  cfg.getNumber('maxEventBytes') ??
  numberEnv('MAX_EVENT_BYTES', maxRequestBytes)
const kafkaBrokers = (
  cfg.get('kafkaBrokers') ??
  process.env.NANOTRACE_KAFKA_BROKERS ??
  ''
).trim()
if (!kafkaBrokers) {
  throw new Error('kafkaBrokers or NANOTRACE_KAFKA_BROKERS is required')
}
const kafkaIngestTopic =
  cfg.get('kafkaIngestTopic') ??
  process.env.NANOTRACE_KAFKA_INGEST_TOPIC ??
  'events.ingest.v1'
const kafkaNormalizedTopic =
  cfg.get('kafkaNormalizedTopic') ??
  process.env.NANOTRACE_KAFKA_NORMALIZED_TOPIC ??
  'events.normalized.v1'
const kafkaTableflowTopic =
  cfg.get('kafkaTableflowTopic') ??
  process.env.NANOTRACE_KAFKA_TABLEFLOW_TOPIC ??
  'events.tableflow.batches.v1'
const kafkaInvalidTopic =
  cfg.get('kafkaInvalidTopic') ??
  process.env.NANOTRACE_KAFKA_INVALID_TOPIC ??
  'events.invalid.v1'
const kafkaServerClientId =
  cfg.get('kafkaServerClientId') ??
  process.env.NANOTRACE_KAFKA_CLIENT_ID ??
  `${name}-server`
const kafkaSecurityProtocol =
  cfg.get('kafkaSecurityProtocol') ??
  process.env.NANOTRACE_KAFKA_SECURITY_PROTOCOL ??
  ''
const kafkaSaslMechanism =
  cfg.get('kafkaSaslMechanism') ??
  process.env.NANOTRACE_KAFKA_SASL_MECHANISM ??
  ''
const kafkaSaslUsername =
  cfg.getSecret('kafkaSaslUsername') ??
  (process.env.NANOTRACE_KAFKA_SASL_USERNAME
    ? pulumi.secret(process.env.NANOTRACE_KAFKA_SASL_USERNAME)
    : pulumi.secret(''))
const kafkaSaslPassword =
  cfg.getSecret('kafkaSaslPassword') ??
  (process.env.NANOTRACE_KAFKA_SASL_PASSWORD
    ? pulumi.secret(process.env.NANOTRACE_KAFKA_SASL_PASSWORD)
    : pulumi.secret(''))
const normalizerGroupId =
  cfg.get('normalizerGroupId') ??
  process.env.NANOTRACE_NORMALIZER_GROUP_ID ??
  `${name}-normalizer`
const normalizerClientId =
  cfg.get('normalizerClientId') ??
  process.env.NANOTRACE_NORMALIZER_CLIENT_ID ??
  `${name}-normalizer`
const tableflowMaterializerGroupId =
  cfg.get('tableflowMaterializerGroupId') ??
  process.env.NANOTRACE_TABLEFLOW_MATERIALIZER_GROUP_ID ??
  `${name}-tableflow-materializer`
const tableflowMaterializerClientId =
  cfg.get('tableflowMaterializerClientId') ??
  process.env.NANOTRACE_TABLEFLOW_MATERIALIZER_CLIENT_ID ??
  `${name}-tableflow-materializer`
const databaseUrl =
  cfg.getSecret('databaseUrl') ??
  (process.env.DATABASE_URL
    ? pulumi.secret(process.env.DATABASE_URL)
    : undefined)
if (!databaseUrl) {
  throw new Error('DATABASE_URL is required')
}
const planetScalePrivateLinkServiceName =
  cfg.get('planetScalePrivateLinkServiceName') ??
  process.env.PLANETSCALE_PRIVATELINK_SERVICE_NAME ??
  ''
if (!planetScalePrivateLinkServiceName.trim()) {
  throw new Error('PLANETSCALE_PRIVATELINK_SERVICE_NAME is required')
}
const domainName = normalizeDomainName(
  requireConfigOrEnv('domainName', 'NANOTRACE_DOMAIN_NAME')
)
const configuredEmailFrom = cfg.get('emailFrom') ?? process.env.NANOTRACE_EMAIL_FROM
const emailFrom = configuredEmailFrom?.trim() || `login@mail.${domainName}`
const loginEmailIdentityDomain = normalizeDomainName(domainFromEmail(emailFrom))
const loginEmailMailFromDomain = normalizeDomainName(`bounce.${loginEmailIdentityDomain}`)
const manageLoginEmailDns =
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
const googleOauthClientId =
  cfg.get('googleOauthClientId') ??
  process.env.NANOTRACE_GOOGLE_OAUTH_CLIENT_ID ??
  ''
const configuredGoogleOauthClientSecret = cfg.getSecret('googleOauthClientSecret')
const googleOauthClientSecret =
  configuredGoogleOauthClientSecret ??
  (process.env.NANOTRACE_GOOGLE_OAUTH_CLIENT_SECRET
    ? pulumi.secret(process.env.NANOTRACE_GOOGLE_OAUTH_CLIENT_SECRET)
    : pulumi.secret(''))
const googleOauthClientSecretConfigured =
  Boolean(configuredGoogleOauthClientSecret) ||
  Boolean(process.env.NANOTRACE_GOOGLE_OAUTH_CLIENT_SECRET)
if (Boolean(googleOauthClientId.trim()) !== googleOauthClientSecretConfigured) {
  throw new Error('Google OAuth deploy config requires both NANOTRACE_GOOGLE_OAUTH_CLIENT_ID and NANOTRACE_GOOGLE_OAUTH_CLIENT_SECRET')
}
const googleOauthRedirectUri =
  cfg.get('googleOauthRedirectUri') ??
  process.env.NANOTRACE_GOOGLE_OAUTH_REDIRECT_URI ??
  ''
const apiBaseUrl =
  cfg.get('apiBaseUrl') ??
  process.env.NANOTRACE_API_BASE_URL ??
  `https://${apiDomainName}`
const uiApiBaseUrl =
  cfg.get('uiApiBaseUrl') ??
  process.env.VITE_NANOTRACE_URL ??
  apiBaseUrl
const buildUi =
  cfg.getBoolean('buildUi') ??
  booleanEnv('NANOTRACE_BUILD_UI', true)
const corsAllowedOrigins =
  cfg.get('corsAllowedOrigins') ??
  process.env.NANOTRACE_CORS_ALLOWED_ORIGINS ??
  [
    appBaseUrl,
    'http://localhost:41233',
    'http://127.0.0.1:41233',
    'http://localhost:41234',
    'http://127.0.0.1:41234',
    'http://localhost:5173',
    'http://127.0.0.1:5173',
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
const sessionSameSite =
  cfg.get('sessionSameSite') ??
  process.env.NANOTRACE_SESSION_SAME_SITE ??
  'Lax'
const magicLinkTtlSecs =
  cfg.getNumber('magicLinkTtlSecs') ??
  numberEnv('NANOTRACE_MAGIC_LINK_TTL_SECS', 60 * 60)
const imageUriOverride = cfg.get('imageUri')
const buildImage = cfg.getBoolean('buildImage') ?? !imageUriOverride
const imageBuildId = cfg.get('imageBuildId') ?? cfg.get('imageTag') ?? 'latest'
const schemaHash = createHash('sha256')
  .update(
    readFileSync(path.join(repoRoot, clickhouseSchemaPath), 'utf8')
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
const uiSourceHash = createHash('sha256')
  .update(hashDirectory(path.join(repoRoot, 'apps/ui')))
  .update(readFileSync(path.join(repoRoot, 'package.json'), 'utf8'))
  .update(readFileSync(path.join(repoRoot, 'package-lock.json'), 'utf8'))
  .update(readFileSync(path.join(repoRoot, 'scripts/deploy-ui.mjs'), 'utf8'))
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

const configuredDataKmsKeyArn =
  cfg.get('dataKmsKeyArn') ??
  process.env.NANOTRACE_DATA_KMS_KEY_ARN ??
  ''
const createDataKmsKey =
  cfg.getBoolean('createDataKmsKey') ??
  booleanEnv('NANOTRACE_CREATE_DATA_KMS_KEY', false)

const azs = aws.getAvailabilityZonesOutput({ state: 'available' })

const clickhouseUrl = requireEnv('CLICKHOUSE_URL')
const clickhouseUser = requireEnv('CLICKHOUSE_USER')
const clickhousePassword = pulumi.secret(requireEnv('CLICKHOUSE_PASSWORD'))

const managedDataKmsKey = createDataKmsKey
  ? new aws.kms.Key(`${name}-data-key`, {
      description: `Nanotrace data key for ${deploymentId}`,
      deletionWindowInDays: cfg.getNumber('kmsDeletionWindowDays') ?? 7,
      enableKeyRotation: true
    })
  : undefined

const dataKmsKeyArn = configuredDataKmsKeyArn
  ? pulumi.output(configuredDataKmsKeyArn)
  : managedDataKmsKey?.arn

if (managedDataKmsKey) {
  new aws.kms.Alias(`${name}-data-key-alias`, {
    name: `alias/nanotrace/${deploymentId}`,
    targetKeyId: managedDataKmsKey.keyId
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
      applyServerSideEncryptionByDefault: dataKmsKeyArn
        ? {
            kmsMasterKeyId: dataKmsKeyArn,
            sseAlgorithm: 'aws:kms'
          }
        : {
            sseAlgorithm: 'AES256'
          },
      bucketKeyEnabled: dataKmsKeyArn ? true : undefined
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

const repository = new aws.ecr.Repository(`${name}-server`, {
  forceDelete: cfg.getBoolean('forceDeleteRepository') ?? false,
  imageScanningConfiguration: { scanOnPush: true },
  tags
})

const imageTag = cfg.get('imageTag') ?? imageBuildId
const imageUri = imageUriOverride
  ? pulumi.output(imageUriOverride)
  : pulumi.interpolate`${repository.repositoryUrl}:${imageTag}`
const imageBuildCommand = pulumi
  .all([repository.repositoryUrl, imageUri])
  .apply(([repositoryUrl, resolvedImageUri]) => {
    const registry = repositoryUrl.split('/')[0]
    const platform = `linux/${cpuArchitecture}`
    const localCache = '.pulumi-docker/buildx-cache'
    const nextLocalCache = '.pulumi-docker/buildx-cache-next'
    const cacheImage = `${repositoryUrl}:buildcache`
    return [
      'set -eu',
      'mkdir -p .pulumi-docker',
      `ECR_PASSWORD="$(aws ecr get-login-password --region ${shellQuote(region)})"`,
      'ECR_AUTH="$(printf \'AWS:%s\' "$ECR_PASSWORD" | base64 | tr -d \'\\n\')"',
      `printf '{"auths":{"${registry}":{"auth":"%s"}}}\\n' "$ECR_AUTH" > .pulumi-docker/config.json`,
      'if docker --config .pulumi-docker buildx version >/dev/null 2>&1; then',
      '  if ! docker --config .pulumi-docker buildx inspect nanotrace-builder >/dev/null 2>&1; then',
      '    docker --config .pulumi-docker buildx create --name nanotrace-builder --driver docker-container >/dev/null',
      '  fi',
      '  docker --config .pulumi-docker buildx inspect nanotrace-builder --bootstrap >/dev/null',
      `  mkdir -p ${shellQuote(localCache)}`,
      `  rm -rf ${shellQuote(nextLocalCache)}`,
      '  CACHE_FROM_ARGS=""',
      `  if docker --config .pulumi-docker manifest inspect ${shellQuote(cacheImage)} >/dev/null 2>&1; then`,
      `    CACHE_FROM_ARGS="$CACHE_FROM_ARGS --cache-from type=registry,ref=${shellQuote(cacheImage)}"`,
      '  fi',
      `  CACHE_FROM_ARGS="$CACHE_FROM_ARGS --cache-from type=local,src=${shellQuote(localCache)}"`,
      '  if ! docker --config .pulumi-docker buildx build \\',
      '    --builder nanotrace-builder \\',
      `    --platform ${shellQuote(platform)} \\`,
      '    $CACHE_FROM_ARGS \\',
      `    --cache-to type=local,dest=${shellQuote(nextLocalCache)},mode=max \\`,
      `    --cache-to type=registry,ref=${shellQuote(cacheImage)},mode=max,ignore-error=true \\`,
      `    -t ${shellQuote(resolvedImageUri)} \\`,
      '    --push .; then',
      '    echo "buildx cached build failed; falling back to docker build without external cache" >&2',
      `    DOCKER_BUILDKIT=1 docker --config .pulumi-docker build --platform ${shellQuote(platform)} -t ${shellQuote(resolvedImageUri)} .`,
      `    docker --config .pulumi-docker push ${shellQuote(resolvedImageUri)}`,
      '  fi',
      `  if [ -d ${shellQuote(nextLocalCache)} ]; then`,
      `    rm -rf ${shellQuote(localCache)}`,
      `    mv ${shellQuote(nextLocalCache)} ${shellQuote(localCache)}`,
      '  fi',
      'else',
      `  DOCKER_BUILDKIT=1 docker --config .pulumi-docker build --platform ${shellQuote(platform)} -t ${shellQuote(resolvedImageUri)} .`,
      `  docker --config .pulumi-docker push ${shellQuote(resolvedImageUri)}`,
      'fi'
    ].join('\n')
  })

const imageBuild = buildImage
  ? new command.local.Command(
      `${name}-image`,
      {
        create: imageBuildCommand,
        update: imageBuildCommand,
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
      dataKmsKeyArn ?? pulumi.output('')
    ])
    .apply(([bucketArn, repositoryArn, kmsKeyArn]) =>
      JSON.stringify(
        {
          Version: '2012-10-17',
          Statement: [
            ...(kmsKeyArn
              ? [
                  {
                    Sid: 'UseDataKmsKey',
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
              Sid: 'WriteBootstrapDebugObjects',
              Effect: 'Allow',
              Action: ['s3:PutObject', 's3:AbortMultipartUpload'],
              Resource: `${bucketArn}/${normalizedPrefix}/_debug*/*`
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
      CLICKHOUSE_SCHEMA_PATH: clickhouseSchemaPath
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

const planetScaleEndpointSg = new aws.ec2.SecurityGroup(`${name}-planetscale-endpoint-sg`, {
  vpcId: vpc.id,
  ingress: [
    {
      protocol: 'tcp',
      fromPort: 5432,
      toPort: 5432,
      securityGroups: [instanceSg.id]
    },
    {
      protocol: 'tcp',
      fromPort: 6432,
      toPort: 6432,
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
  tags: { ...tags, Name: `${name}-planetscale-endpoint-sg` }
})

const planetScalePrivateLinkEndpoint = new aws.ec2.VpcEndpoint(`${name}-planetscale-privatelink`, {
  privateDnsEnabled: true,
  securityGroupIds: [planetScaleEndpointSg.id],
  serviceName: planetScalePrivateLinkServiceName,
  subnetIds: subnets.map(subnet => subnet.id),
  vpcEndpointType: 'Interface',
  vpcId: vpc.id,
  tags: { ...tags, Name: `${name}-planetscale-privatelink` }
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

const uiBucketPolicy = new aws.s3.BucketPolicy(`${name}-ui-policy`, {
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

const uiBuild = buildUi
  ? new command.local.Command(
      `${name}-ui-build`,
      {
        create: 'node scripts/deploy-ui.mjs',
        update: 'node scripts/deploy-ui.mjs',
        delete: 'true',
        dir: repoRoot,
        environment: {
          AWS_REGION: region,
          NANOTRACE_UI_BUCKET: uiBucket.bucket,
          NANOTRACE_UI_DISTRIBUTION_ID: uiDistribution.id,
          VITE_NANOTRACE_URL: uiApiBaseUrl
        },
        triggers: [
          uiSourceHash,
          uiApiBaseUrl,
          uiBucket.bucket,
          uiDistribution.id
        ]
      },
      {
        dependsOn: [uiBucketPolicy, uiDistribution]
      }
    )
  : undefined

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
  apiBaseUrl
new aws.lb.ListenerRule(`${name}-query-route`, {
  listenerArn: listener.arn,
  priority: 10,
  conditions: [
    { pathPattern: { values: ['/v1/query'] } },
    { httpRequestMethod: { values: ['POST'] } }
  ],
  actions: [{ type: 'forward', targetGroupArn: queryTargetGroup.arn }]
})

new aws.lb.ListenerRule(`${name}-event-read-route`, {
  listenerArn: listener.arn,
  priority: 20,
  conditions: [
    { pathPattern: { values: ['/v1/events/*'] } },
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
    publicBaseUrl,
    kafkaSaslUsername,
    kafkaSaslPassword,
    googleOauthClientSecret
  ])
  .apply(
    ([
      bucketName,
      resolvedImageUri,
      resolvedImageBuildId,
      resolvedClickhouseUrl,
      resolvedClickhousePassword,
      resolvedDatabaseUrl,
      resolvedPublicBaseUrl,
      resolvedKafkaSaslUsername,
      resolvedKafkaSaslPassword,
      resolvedGoogleOauthClientSecret
    ]) =>
      renderUserData({
        bucketName,
        clickhouseDatabase,
        clickhousePassword: resolvedClickhousePassword,
        clickhouseTable,
        clickhouseUrl: resolvedClickhouseUrl,
        clickhouseUser,
        clickhouseMaxBytesToRead,
        imageUri: resolvedImageUri,
        imageBuildId: resolvedImageBuildId,
        kafkaBrokers,
        kafkaIngestTopic,
        kafkaInvalidTopic,
        kafkaTableflowTopic,
        kafkaSaslMechanism,
        kafkaSaslPassword: resolvedKafkaSaslPassword,
        kafkaSaslUsername: resolvedKafkaSaslUsername,
        kafkaSecurityProtocol,
        kafkaNormalizedTopic,
        kafkaServerClientId,
        normalizerGroupId,
        normalizerClientId,
        tableflowMaterializerGroupId,
        tableflowMaterializerClientId,
        maxEventBytes,
        maxRequestBytes,
        port,
        prefix,
        region,
        databaseUrl: resolvedDatabaseUrl,
        appBaseUrl,
        corsAllowedOrigins,
        emailFrom,
        googleOauthClientId,
        googleOauthClientSecret: resolvedGoogleOauthClientSecret,
        googleOauthRedirectUri,
        publicBaseUrl: resolvedPublicBaseUrl,
        sessionSecure,
        sessionSameSite,
        magicLinkTtlSecs
      })
  )

const queryUserData = pulumi
  .all([bucket.bucket, imageUri, imageBuildId, clickhouseUrl, clickhousePassword, databaseUrl, publicBaseUrl])
  .apply(([bucketName, resolvedImageUri, resolvedImageBuildId, resolvedClickhouseUrl, resolvedClickhousePassword, resolvedDatabaseUrl, resolvedPublicBaseUrl]) =>
    renderQueryUserData({
      bucketName,
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
      publicBaseUrl: resolvedPublicBaseUrl,
      corsAllowedOrigins,
      sessionSecure,
      sessionSameSite,
      magicLinkTtlSecs
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
          kmsKeyId: dataKmsKeyArn
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
          kmsKeyId: dataKmsKeyArn
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
export const dataKmsKeyArnOutput = dataKmsKeyArn ?? ''
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
export const kafkaBrokersOutput = kafkaBrokers
export const kafkaIngestTopicOutput = kafkaIngestTopic
export const kafkaNormalizedTopicOutput = kafkaNormalizedTopic
export const kafkaInvalidTopicOutput = kafkaInvalidTopic
export const kafkaTableflowTopicOutput = kafkaTableflowTopic
export const clickhouseUrlOutput = clickhouseUrl
export const clickhouseUserOutput = clickhouseUser
export const clickhouseDatabaseOutput = clickhouseDatabase
export const clickhouseTableOutput = clickhouseTable
export const planetScalePrivateLinkEndpointId = planetScalePrivateLinkEndpoint.id
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
export const ecrRepositoryUrl = repository.repositoryUrl
export const serverImageUri = imageUri

interface UserDataArgs {
  bucketName: string
  clickhouseDatabase: string
  clickhousePassword: string
  clickhouseTable: string
  clickhouseUrl: string
  clickhouseUser: string
  clickhouseMaxBytesToRead: number
  imageUri: string
  imageBuildId: string
  kafkaBrokers: string
  kafkaIngestTopic: string
  kafkaInvalidTopic: string
  kafkaNormalizedTopic: string
  kafkaTableflowTopic: string
  kafkaSaslMechanism: string
  kafkaSaslPassword: string
  kafkaSaslUsername: string
  kafkaServerClientId: string
  kafkaSecurityProtocol: string
  normalizerGroupId: string
  normalizerClientId: string
  tableflowMaterializerGroupId: string
  tableflowMaterializerClientId: string
  maxEventBytes: number
  maxRequestBytes: number
  port: number
  prefix: string
  region: string
  databaseUrl: string
  appBaseUrl: string
  corsAllowedOrigins: string
  emailFrom: string
  googleOauthClientId: string
  googleOauthClientSecret: string
  googleOauthRedirectUri: string
  publicBaseUrl: string
  sessionSecure: boolean
  sessionSameSite: string
  magicLinkTtlSecs: number
}

interface QueryUserDataArgs {
  bucketName: string
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
  publicBaseUrl: string
  corsAllowedOrigins: string
  sessionSecure: boolean
  sessionSameSite: string
  magicLinkTtlSecs: number
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
  (docker logs nanotrace-normalizer 2>&1 || true) > /tmp/docker-normalizer-logs.txt
  (docker logs nanotrace-materializer 2>&1 || true) > /tmp/docker-materializer-logs.txt
  (docker inspect nanotrace-server 2>&1 || true) > /tmp/docker-inspect.txt
  (docker inspect nanotrace-normalizer 2>&1 || true) > /tmp/docker-normalizer-inspect.txt
  (docker inspect nanotrace-materializer 2>&1 || true) > /tmp/docker-materializer-inspect.txt
  (journalctl -u docker --no-pager 2>&1 || true) > /tmp/docker-journal.txt
  (cat /var/log/cloud-init-output.log 2>&1 || true) > /tmp/cloud-init-output.log
  for f in "$LOG" /tmp/docker-ps.txt /tmp/docker-logs.txt /tmp/docker-normalizer-logs.txt /tmp/docker-materializer-logs.txt /tmp/docker-inspect.txt /tmp/docker-normalizer-inspect.txt /tmp/docker-materializer-inspect.txt /tmp/docker-journal.txt /tmp/cloud-init-output.log; do
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
docker rm -f nanotrace-server >/dev/null 2>&1 || true
docker rm -f nanotrace-normalizer >/dev/null 2>&1 || true
docker rm -f nanotrace-materializer >/dev/null 2>&1 || true
docker run -d --name nanotrace-server --restart unless-stopped \\
  -p ${args.port}:${args.port} \\
  -e AWS_REGION=${shellQuote(args.region)} \\
  -e PORT=${args.port} \\
  -e NANOTRACE_IMAGE_BUILD_ID=${shellQuote(args.imageBuildId)} \\
  -e DATABASE_URL=${shellQuote(args.databaseUrl)} \\
  -e NANOTRACE_PUBLIC_BASE_URL=${shellQuote(args.publicBaseUrl)} \\
  -e NANOTRACE_APP_BASE_URL=${shellQuote(args.appBaseUrl)} \\
  -e NANOTRACE_SESSION_SECURE=${args.sessionSecure ? 'true' : 'false'} \\
  -e NANOTRACE_SESSION_SAME_SITE=${shellQuote(args.sessionSameSite)} \\
  -e NANOTRACE_MAGIC_LINK_TTL_SECS=${args.magicLinkTtlSecs} \\
  -e NANOTRACE_EMAIL_FROM=${shellQuote(args.emailFrom)} \\
  -e NANOTRACE_GOOGLE_OAUTH_CLIENT_ID=${shellQuote(args.googleOauthClientId)} \\
  -e NANOTRACE_GOOGLE_OAUTH_CLIENT_SECRET=${shellQuote(args.googleOauthClientSecret)} \\
  -e NANOTRACE_GOOGLE_OAUTH_REDIRECT_URI=${shellQuote(args.googleOauthRedirectUri)} \\
  -e NANOTRACE_CORS_ALLOWED_ORIGINS=${shellQuote(args.corsAllowedOrigins)} \\
  -e NANOTRACE_KAFKA_BROKERS=${shellQuote(args.kafkaBrokers)} \\
  -e NANOTRACE_KAFKA_INGEST_TOPIC=${shellQuote(args.kafkaIngestTopic)} \\
  -e NANOTRACE_KAFKA_TABLEFLOW_TOPIC=${shellQuote(args.kafkaTableflowTopic)} \\
  -e NANOTRACE_KAFKA_CLIENT_ID=${shellQuote(args.kafkaServerClientId)} \\
  -e NANOTRACE_KAFKA_SECURITY_PROTOCOL=${shellQuote(args.kafkaSecurityProtocol)} \\
  -e NANOTRACE_KAFKA_SASL_MECHANISM=${shellQuote(args.kafkaSaslMechanism)} \\
  -e NANOTRACE_KAFKA_SASL_USERNAME=${shellQuote(args.kafkaSaslUsername)} \\
  -e NANOTRACE_KAFKA_SASL_PASSWORD=${shellQuote(args.kafkaSaslPassword)} \\
  -e CLICKHOUSE_URL=${shellQuote(args.clickhouseUrl)} \\
  -e CLICKHOUSE_USER=${shellQuote(args.clickhouseUser)} \\
  -e CLICKHOUSE_PASSWORD=${shellQuote(args.clickhousePassword)} \\
  -e CLICKHOUSE_DATABASE=${shellQuote(args.clickhouseDatabase)} \\
  -e CLICKHOUSE_TABLE=${shellQuote(args.clickhouseTable)} \\
  -e CLICKHOUSE_MAX_BYTES_TO_READ=${args.clickhouseMaxBytesToRead} \\
  -e MAX_REQUEST_BYTES=${args.maxRequestBytes} \\
  ${shellQuote(args.imageUri)}
docker run -d --name nanotrace-normalizer --restart unless-stopped \\
  -e AWS_REGION=${shellQuote(args.region)} \\
  -e NANOTRACE_IMAGE_BUILD_ID=${shellQuote(args.imageBuildId)} \\
  -e NANOTRACE_KAFKA_BROKERS=${shellQuote(args.kafkaBrokers)} \\
  -e NANOTRACE_KAFKA_INGEST_TOPIC=${shellQuote(args.kafkaIngestTopic)} \\
  -e NANOTRACE_KAFKA_NORMALIZED_TOPIC=${shellQuote(args.kafkaNormalizedTopic)} \\
  -e NANOTRACE_KAFKA_TABLEFLOW_TOPIC=${shellQuote(args.kafkaTableflowTopic)} \\
  -e NANOTRACE_KAFKA_INVALID_TOPIC=${shellQuote(args.kafkaInvalidTopic)} \\
  -e NANOTRACE_NORMALIZER_GROUP_ID=${shellQuote(args.normalizerGroupId)} \\
  -e NANOTRACE_NORMALIZER_CLIENT_ID=${shellQuote(args.normalizerClientId)} \\
  -e NANOTRACE_KAFKA_SECURITY_PROTOCOL=${shellQuote(args.kafkaSecurityProtocol)} \\
  -e NANOTRACE_KAFKA_SASL_MECHANISM=${shellQuote(args.kafkaSaslMechanism)} \\
  -e NANOTRACE_KAFKA_SASL_USERNAME=${shellQuote(args.kafkaSaslUsername)} \\
  -e NANOTRACE_KAFKA_SASL_PASSWORD=${shellQuote(args.kafkaSaslPassword)} \\
  -e CLICKHOUSE_URL=${shellQuote(args.clickhouseUrl)} \\
  -e CLICKHOUSE_USER=${shellQuote(args.clickhouseUser)} \\
  -e CLICKHOUSE_PASSWORD=${shellQuote(args.clickhousePassword)} \\
  -e CLICKHOUSE_DATABASE=${shellQuote(args.clickhouseDatabase)} \\
  -e CLICKHOUSE_TABLE=${shellQuote(args.clickhouseTable)} \\
  -e MAX_EVENT_BYTES=${args.maxEventBytes} \\
  ${shellQuote(args.imageUri)} \\
  /usr/local/bin/nanotrace-normalizer
docker run -d --name nanotrace-materializer --restart unless-stopped \\
  -e AWS_REGION=${shellQuote(args.region)} \\
  -e NANOTRACE_IMAGE_BUILD_ID=${shellQuote(args.imageBuildId)} \\
  -e NANOTRACE_KAFKA_BROKERS=${shellQuote(args.kafkaBrokers)} \\
  -e NANOTRACE_KAFKA_TABLEFLOW_TOPIC=${shellQuote(args.kafkaTableflowTopic)} \\
  -e NANOTRACE_KAFKA_SECURITY_PROTOCOL=${shellQuote(args.kafkaSecurityProtocol)} \\
  -e NANOTRACE_KAFKA_SASL_MECHANISM=${shellQuote(args.kafkaSaslMechanism)} \\
  -e NANOTRACE_KAFKA_SASL_USERNAME=${shellQuote(args.kafkaSaslUsername)} \\
  -e NANOTRACE_KAFKA_SASL_PASSWORD=${shellQuote(args.kafkaSaslPassword)} \\
  -e NANOTRACE_TABLEFLOW_MATERIALIZER_GROUP_ID=${shellQuote(args.tableflowMaterializerGroupId)} \\
  -e NANOTRACE_TABLEFLOW_MATERIALIZER_CLIENT_ID=${shellQuote(args.tableflowMaterializerClientId)} \\
  -e NANOTRACE_TABLEFLOW_MATERIALIZE_LOOP=true \\
  -e CLICKHOUSE_URL=${shellQuote(args.clickhouseUrl)} \\
  -e CLICKHOUSE_USER=${shellQuote(args.clickhouseUser)} \\
  -e CLICKHOUSE_PASSWORD=${shellQuote(args.clickhousePassword)} \\
  -e CLICKHOUSE_DATABASE=${shellQuote(args.clickhouseDatabase)} \\
  -e CLICKHOUSE_TABLE=${shellQuote(args.clickhouseTable)} \\
  ${shellQuote(args.imageUri)} \\
  /usr/local/bin/nanotrace-lakehouse-rebuild
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
  -e DATABASE_URL=${shellQuote(args.databaseUrl)} \\
  -e NANOTRACE_PUBLIC_BASE_URL=${shellQuote(args.publicBaseUrl)} \\
  -e NANOTRACE_APP_BASE_URL=${shellQuote(args.appBaseUrl)} \\
  -e NANOTRACE_SESSION_SECURE=${args.sessionSecure ? 'true' : 'false'} \\
  -e NANOTRACE_SESSION_SAME_SITE=${shellQuote(args.sessionSameSite)} \\
  -e NANOTRACE_MAGIC_LINK_TTL_SECS=${args.magicLinkTtlSecs} \\
  -e NANOTRACE_CORS_ALLOWED_ORIGINS=${shellQuote(args.corsAllowedOrigins)} \\
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

function hashDirectory (directory: string): string {
  const hash = createHash('sha256')
  for (const file of listFiles(directory)) {
    const relative = path.relative(repoRoot, file)
    hash.update(relative)
    hash.update('\0')
    hash.update(readFileSync(file))
    hash.update('\0')
  }
  return hash.digest('hex')
}

function listFiles (directory: string): string[] {
  const entries = readdirSync(directory)
    .filter(entry => entry !== 'dist' && entry !== 'node_modules')
    .sort()
  const files: string[] = []
  for (const entry of entries) {
    const absolute = path.join(directory, entry)
    const stat = statSync(absolute)
    if (stat.isDirectory()) {
      files.push(...listFiles(absolute))
    } else if (stat.isFile()) {
      files.push(absolute)
    }
  }
  return files
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
