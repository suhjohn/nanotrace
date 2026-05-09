#!/usr/bin/env node
import { readFileSync } from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";

const root = path.resolve(new URL("..", import.meta.url).pathname);
const pulumiCwd = path.join(root, "deploy/pulumi/nanotrace");

const args = process.argv.slice(2);
if (args.includes("--help") || args.includes("-h")) {
    usage();
    process.exit(0);
}

const envFile = optionValue("--env") ?? process.env.NANOTRACE_ENV_FILE;
const buildId = optionValue("--build-id") ?? `deploy-${timestamp()}`;
const skipRoll = args.includes("--no-roll");
const peakCapacity = numberOption("--peak", numberEnv("NANOTRACE_ROLL_PEAK_CAPACITY", 2));
const finalCapacity = numberOption("--final", numberEnv("NANOTRACE_ROLL_FINAL_CAPACITY", 1));
const node24 = process.env.NANOTRACE_NODE24_BIN ?? path.join(process.env.HOME ?? "", ".nvm/versions/node/v24.12.0/bin");

loadEnvFile(envFile);

const deployEnv = {
    ...process.env,
    PULUMI_CONFIG_PASSPHRASE: process.env.PULUMI_CONFIG_PASSPHRASE ?? "",
    PATH: node24 ? `${node24}:${process.env.PATH ?? ""}` : process.env.PATH,
};

if (envFile) {
    console.log(`env=${envFile}`);
}
console.log(`imageBuildId=${buildId}`);

await run("pulumi", ["config", "set", "imageBuildId", buildId], {
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
    if (peakCapacity < finalCapacity) {
        throw new Error(`--peak (${peakCapacity}) must be >= --final (${finalCapacity})`);
    }
    await run(process.execPath, scaleArgs(peakCapacity, envFile), {
        cwd: root,
        env: deployEnv,
        inherit: true,
    });
    await run(process.execPath, scaleArgs(finalCapacity, envFile), {
        cwd: root,
        env: deployEnv,
        inherit: true,
    });
}

console.log("deploy roll complete");

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

function scaleArgs(capacity, envFile) {
    const args = ["scripts/scale-pulumi-asg.mjs", String(capacity)];
    if (envFile) {
        args.push("--env", envFile);
    }
    return args;
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
    console.log(`Usage: node scripts/deploy-roll-pulumi.mjs [--env path/to/env-file] [--build-id deploy-...] [--peak 2] [--final 1] [--no-roll]

Examples:
  npm run deploy:roll
  npm run deploy:roll -- --peak 2 --final 1
  npm run deploy:roll -- --no-roll
`);
}
