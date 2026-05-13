#!/usr/bin/env node
import { readFileSync } from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";

const root = path.resolve(new URL("..", import.meta.url).pathname);
const pulumiCwd = path.join(root, "deploy/pulumi/nanotrace");

const args = process.argv.slice(2);
if (args.includes("--help") || args.includes("-h")) {
    usage();
    process.exit(0);
}

const target = Number(args[0]);
if (!Number.isInteger(target) || target < 0) {
    usage();
    throw new Error("capacity must be a non-negative integer");
}

const wait = !args.includes("--no-wait");
const strictWait = args.includes("--strict");
const allowMixedLaunchTemplates = args.includes("--allow-mixed-launch-templates");
const envFile = optionValue("--env") ?? process.env.NANOTRACE_ENV_FILE;
const pollMs = numberOption("--poll-ms", numberEnv("NANOTRACE_SCALE_POLL_MS", 10_000));
const waitMs = numberOption("--wait-ms", numberEnv("NANOTRACE_SCALE_WAIT_MS", 15 * 60_000));

loadEnvFile(envFile);

const region = requiredEnv("AWS_REGION");
const deploymentId = process.env.NANOTRACE_DEPLOYMENT_ID ?? "prod";
const baseName = process.env.NANOTRACE_NAME ?? `nanotrace-${deploymentId}`;
const asgName = process.env.NANOTRACE_ASG_NAME ?? await findAutoScalingGroup(`${baseName}-asg`);
const pulumiEnv = {
    ...process.env,
    PULUMI_CONFIG_PASSPHRASE: process.env.PULUMI_CONFIG_PASSPHRASE ?? "",
};

console.log(`asg=${asgName}`);
console.log(`region=${region}`);
console.log(`target=${target}`);

await run("pulumi", ["config", "set", "minSize", String(target)], {
    cwd: pulumiCwd,
    env: pulumiEnv,
});
await run("pulumi", ["config", "set", "desiredCapacity", String(target)], {
    cwd: pulumiCwd,
    env: pulumiEnv,
});

await run("aws", [
    "autoscaling",
    "update-auto-scaling-group",
    "--region",
    region,
    "--auto-scaling-group-name",
    asgName,
    "--min-size",
    String(target),
    "--desired-capacity",
    String(target),
]);

if (wait) {
    await waitForCapacity(asgName, target, waitMs, pollMs);
}

console.log(`scale complete: ${asgName} desired=${target}`);

async function findAutoScalingGroup(namePrefix) {
    const result = await run("aws", [
        "autoscaling",
        "describe-auto-scaling-groups",
        "--region",
        region,
        "--query",
        `AutoScalingGroups[?starts_with(AutoScalingGroupName, \`${namePrefix}\`)].AutoScalingGroupName`,
        "--output",
        "json",
    ]);
    const names = JSON.parse(result.stdout);
    if (names.length !== 1) {
        throw new Error(`expected one ASG starting with ${namePrefix}, found ${names.length}: ${names.join(", ")}`);
    }
    return names[0];
}

async function waitForCapacity(name, capacity, timeoutMs, intervalMs) {
    const deadline = Date.now() + timeoutMs;
    let latestVersion = null;
    let lastStatus = "";

    while (Date.now() < deadline) {
        const group = await describeAutoScalingGroup(name);
        latestVersion ??= await latestLaunchTemplateVersion(group);

        const instances = group.Instances ?? [];
        const activeInstances = instances.filter((instance) => !String(instance.LifecycleState).startsWith("Terminating"));
        const inService = instances.filter((instance) =>
            instance.LifecycleState === "InService" &&
            instance.HealthStatus === "Healthy"
        );
        const latestInService = inService.filter((instance) =>
            String(instance.LaunchTemplate?.Version ?? "") === String(latestVersion)
        );
        const launchTemplateReady = capacity === 0 ||
            (allowMixedLaunchTemplates ? latestInService.length > 0 : latestInService.length === inService.length);
        const targetHealth = await targetGroupHealth(group, inService.map((instance) => instance.InstanceId));
        const targetsHealthy = targetHealth === null ||
            (targetHealth.length === inService.length && targetHealth.every((target) => target.State === "healthy"));

        lastStatus = [
            `instances=${instances.length}`,
            `active=${activeInstances.length}`,
            `inService=${inService.length}`,
            `latestLt=${latestVersion}`,
            `latestInService=${latestInService.length}`,
            `launchTemplateReady=${launchTemplateReady}`,
            targetHealth === null ? "targetHealth=unknown" : `targetHealth=${targetHealth.map((target) => `${target.Id}:${target.State}`).join(",")}`,
        ].join(" ");
        console.log(lastStatus);

        if (capacity === 0 && (strictWait ? instances.length === 0 : activeInstances.length === 0)) {
            return;
        }
        if (
            capacity > 0 &&
            (strictWait ? instances.length === capacity : activeInstances.length === capacity) &&
            inService.length === capacity &&
            launchTemplateReady &&
            targetsHealthy
        ) {
            return;
        }

        await sleep(intervalMs);
    }

    throw new Error(`timed out waiting for ${name} capacity=${capacity}; last status: ${lastStatus}`);
}

async function describeAutoScalingGroup(name) {
    const result = await run("aws", [
        "autoscaling",
        "describe-auto-scaling-groups",
        "--region",
        region,
        "--auto-scaling-group-names",
        name,
        "--query",
        "AutoScalingGroups[0]",
        "--output",
        "json",
    ]);
    const group = JSON.parse(result.stdout);
    if (!group) {
        throw new Error(`ASG not found: ${name}`);
    }
    return group;
}

async function latestLaunchTemplateVersion(group) {
    const launchTemplateId = group.LaunchTemplate?.LaunchTemplateId;
    if (!launchTemplateId) {
        return null;
    }
    const result = await run("aws", [
        "ec2",
        "describe-launch-template-versions",
        "--region",
        region,
        "--launch-template-id",
        launchTemplateId,
        "--versions",
        "$Latest",
        "--query",
        "LaunchTemplateVersions[0].VersionNumber",
        "--output",
        "text",
    ]);
    return result.stdout.trim();
}

async function targetGroupHealth(group, instanceIds) {
    const [targetGroupArn] = group.TargetGroupARNs ?? [];
    if (!targetGroupArn || instanceIds.length === 0) {
        return [];
    }
    const result = await run("aws", [
        "elbv2",
        "describe-target-health",
        "--region",
        region,
        "--target-group-arn",
        targetGroupArn,
        "--output",
        "json",
    ], { allowFailure: true });
    if (result.code !== 0) {
        return null;
    }

    const parsed = JSON.parse(result.stdout);
    return (parsed.TargetHealthDescriptions ?? [])
        .filter((entry) => instanceIds.includes(entry.Target?.Id))
        .map((entry) => ({
            Id: entry.Target.Id,
            State: entry.TargetHealth.State,
        }));
}

function loadEnvFile(file) {
    if (!file) {
        return;
    }
    const resolved = path.resolve(root, file);
    let contents;
    try {
        contents = readFileSync(resolved, "utf8");
    } catch (error) {
        throw error;
    }

    for (const line of contents.split(/\r?\n/)) {
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith("#")) {
            continue;
        }
        const match = trimmed.match(/^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/);
        if (!match) {
            continue;
        }
        const [, key, rawValue] = match;
        if (process.env[key] !== undefined) {
            continue;
        }
        process.env[key] = unquote(rawValue.trim());
    }
}

function unquote(value) {
    if (
        (value.startsWith("\"") && value.endsWith("\"")) ||
        (value.startsWith("'") && value.endsWith("'"))
    ) {
        return value.slice(1, -1);
    }
    return value;
}

function requiredEnv(key) {
    const value = process.env[key];
    if (!value) {
        throw new Error(`${key} is required`);
    }
    return value;
}

function numberEnv(key, fallback) {
    const value = process.env[key];
    if (!value) {
        return fallback;
    }
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed <= 0) {
        throw new Error(`${key} must be a positive number`);
    }
    return parsed;
}

function numberOption(name, fallback) {
    const value = optionValue(name);
    if (value === undefined) {
        return fallback;
    }
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed <= 0) {
        throw new Error(`${name} must be a positive number`);
    }
    return parsed;
}

function optionValue(name) {
    const index = args.indexOf(name);
    if (index === -1) {
        return undefined;
    }
    const value = args[index + 1];
    if (!value || value.startsWith("--")) {
        throw new Error(`${name} requires a value`);
    }
    return value;
}

function run(command, commandArgs, options = {}) {
    return new Promise((resolve, reject) => {
        const child = spawn(command, commandArgs, {
            cwd: options.cwd ?? root,
            env: options.env ?? process.env,
            stdio: ["ignore", "pipe", "pipe"],
        });
        let stdout = "";
        let stderr = "";

        child.stdout.on("data", (chunk) => {
            stdout += chunk;
        });
        child.stderr.on("data", (chunk) => {
            stderr += chunk;
        });
        child.on("error", reject);
        child.on("close", (code) => {
            const result = { code, stdout, stderr };
            if (code === 0 || options.allowFailure) {
                resolve(result);
            } else {
                reject(new Error(`${command} ${commandArgs.join(" ")} failed with ${code}\n${stderr || stdout}`));
            }
        });
    });
}

function usage() {
    console.log(`Usage: node scripts/scale-pulumi-asg.mjs <capacity> [--no-wait] [--strict] [--env path/to/env-file] [--wait-ms 900000] [--poll-ms 10000]

Examples:
  npm run scale:down
  npm run scale:up
  npm run scale:set -- 2

By default, terminating/draining instances are ignored once the requested healthy
capacity is present. Use --strict to wait until AWS removes terminating instances
from the ASG instance list.
`);
}
