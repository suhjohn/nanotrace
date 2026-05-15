#!/usr/bin/env node
import path from "node:path";
import { spawn, spawnSync } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";

const root = path.resolve(new URL("..", import.meta.url).pathname);
const pulumiCwd = path.join(root, "deploy/pulumi/nanotrace");

const args = process.argv.slice(2);
if (args.includes("--help") || args.includes("-h")) {
    usage();
    process.exit(0);
}

const buildId = optionValue("--build-id") ?? `deploy-${timestamp()}`;
const skipRoll = args.includes("--no-roll");
const refreshWaitMs = numberOption("--wait-ms", numberEnv("NANOTRACE_ROLL_WAIT_MS", 20 * 60_000));
const refreshPollMs = numberOption("--poll-ms", numberEnv("NANOTRACE_ROLL_POLL_MS", 15_000));
const node24 = process.env.NANOTRACE_NODE24_BIN ?? path.join(process.env.HOME ?? "", ".nvm/versions/node/v24.12.0/bin");

const deployEnv = {
    ...process.env,
    PULUMI_CONFIG_PASSPHRASE: process.env.PULUMI_CONFIG_PASSPHRASE ?? "",
    PATH: node24 ? `${node24}:${process.env.PATH ?? ""}` : process.env.PATH,
};
const region = requiredEnv("AWS_REGION");
const deploymentId = currentPulumiStack();
const baseName = process.env.NANOTRACE_NAME ?? `nanotrace-${deploymentId}`;

console.log(`imageBuildId=${buildId}`);

await run("pulumi", ["config", "set", "imageBuildId", buildId], {
    cwd: pulumiCwd,
    env: deployEnv,
    inherit: true,
});
await run("pulumi", ["config", "set", "imageTag", buildId], {
    cwd: pulumiCwd,
    env: deployEnv,
    inherit: true,
});
await run("pulumi", ["up", "--yes"], {
    cwd: pulumiCwd,
    env: deployEnv,
    inherit: true,
});

if (!skipRoll) {
    const groups = [
        await findAutoScalingGroup(`${baseName}-asg`),
        await findAutoScalingGroup(`${baseName}-query-asg`),
    ];
    for (const group of groups) {
        await startInstanceRefresh(group);
    }
    await Promise.all(groups.map((group) => waitForInstanceRefresh(group, refreshWaitMs, refreshPollMs)));
}

console.log("deploy roll complete");

function currentPulumiStack() {
    const result = spawnSync("pulumi", ["stack", "--show-name"], {
        cwd: pulumiCwd,
        env: deployEnv,
        encoding: "utf8",
    });
    if (result.status === 0 && result.stdout.trim()) {
        return result.stdout.trim();
    }
    return "prod";
}

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

async function startInstanceRefresh(group) {
    const result = await run("aws", [
        "autoscaling",
        "start-instance-refresh",
        "--region",
        region,
        "--auto-scaling-group-name",
        group,
        "--preferences",
        "{\"MinHealthyPercentage\":100,\"InstanceWarmup\":120,\"SkipMatching\":false}",
        "--query",
        "InstanceRefreshId",
        "--output",
        "text",
    ]);
    console.log(`refresh started: ${group} id=${result.stdout.trim()}`);
}

async function waitForInstanceRefresh(group, timeoutMs, intervalMs) {
    const deadline = Date.now() + timeoutMs;
    let lastStatus = "";
    while (Date.now() < deadline) {
        const result = await run("aws", [
            "autoscaling",
            "describe-instance-refreshes",
            "--region",
            region,
            "--auto-scaling-group-name",
            group,
            "--max-records",
            "1",
            "--query",
            "InstanceRefreshes[0]",
            "--output",
            "json",
        ]);
        const refresh = JSON.parse(result.stdout);
        if (!refresh) {
            throw new Error(`no instance refresh found for ${group}`);
        }
        lastStatus = `${group} status=${refresh.Status} percent=${refresh.PercentageComplete ?? 0}`;
        console.log(lastStatus);
        if (refresh.Status === "Successful") {
            return;
        }
        if (["Failed", "Cancelled", "RollbackFailed", "RollbackSuccessful"].includes(refresh.Status)) {
            throw new Error(`instance refresh did not succeed: ${lastStatus} reason=${refresh.StatusReason ?? ""}`);
        }
        await sleep(intervalMs);
    }
    throw new Error(`timed out waiting for instance refresh; last status: ${lastStatus}`);
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

function requiredEnv(key) {
    const value = process.env[key];
    if (!value) {
        throw new Error(`${key} is required`);
    }
    return value;
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

function timestamp() {
    return new Date().toISOString().replace(/[-:TZ.]/g, "").slice(0, 14);
}

function run(command, commandArgs, options = {}) {
    return new Promise((resolve, reject) => {
        const child = spawn(command, commandArgs, {
            cwd: options.cwd ?? root,
            env: options.env ?? process.env,
            stdio: options.inherit ? "inherit" : ["ignore", "pipe", "pipe"],
        });
        let stdout = "";
        let stderr = "";

        if (!options.inherit) {
            child.stdout.on("data", (chunk) => {
                stdout += chunk;
            });
            child.stderr.on("data", (chunk) => {
                stderr += chunk;
            });
        }
        child.on("error", reject);
        child.on("close", (code) => {
            if (code === 0) {
                resolve({ stdout, stderr });
            } else {
                reject(new Error(`${command} ${commandArgs.join(" ")} failed with ${code}\n${stderr || stdout}`));
            }
        });
    });
}

function usage() {
    console.log(`Usage: node scripts/deploy-roll-pulumi.mjs [--build-id deploy-...] [--wait-ms 1200000] [--poll-ms 15000] [--no-roll]

Examples:
  npm run deploy:roll
  infisical run -- npm run deploy:roll
  op run -- npm run deploy:roll
  npm run deploy:roll -- --wait-ms 1200000
  npm run deploy:roll -- --no-roll
`);
}
