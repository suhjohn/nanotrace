#!/usr/bin/env node
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";

const command = process.argv[2];
if (command !== "create" && command !== "delete") {
    throw new Error("Usage: node scripts/clickhouse-cloud-service.mjs <create|delete>");
}

const orgId = requiredEnv("CLICKHOUSE_CLOUD_ORG_ID");
const apiKey = process.env.CLICKHOUSE_CLOUD_API_KEY || process.env.CLICKHOUSE_CLOUD_API_KEY_ID;
const apiSecret = process.env.CLICKHOUSE_CLOUD_API_SECRET || process.env.CLICKHOUSE_CLOUD_API_KEY_SECRET;
if (!apiKey || !apiSecret) {
    throw new Error("CLICKHOUSE_CLOUD_API_KEY/SECRET or CLICKHOUSE_CLOUD_API_KEY_ID/KEY_SECRET are required");
}

const apiUrl = trimTrailingSlash(process.env.CLICKHOUSE_CLOUD_API_URL || "https://api.clickhouse.cloud/v1");
const stateFile = process.env.CLICKHOUSE_CLOUD_STATE_FILE || path.join(".nanotrace", "clickhouse-cloud", `${orgId}.json`);

if (command === "delete") {
    await deleteService();
} else {
    await createService();
}

async function createService() {
    const name = requiredEnv("CLICKHOUSE_CLOUD_SERVICE_NAME");
    const provider = requiredEnv("CLICKHOUSE_CLOUD_PROVIDER");
    const region = requiredEnv("CLICKHOUSE_CLOUD_REGION");
    const password = requiredEnv("CLICKHOUSE_CLOUD_PASSWORD");

    const existing = await findServiceByName(name);
    if (existing) {
        const ready = await waitForReady(existing.id);
        await setPassword(ready.id, password);
        const output = normalizeOutput(await waitForQueryReady(ready, password));
        saveState(output);
        console.log(JSON.stringify(output));
        return;
    }

    const body = {
        name,
        provider,
        region,
        idleScaling: boolEnv("CLICKHOUSE_CLOUD_IDLE_SCALING", true),
        idleTimeoutMinutes: numberEnv("CLICKHOUSE_CLOUD_IDLE_TIMEOUT_MINUTES", 15),
        ipAccessList: parseIpAccess(process.env.CLICKHOUSE_CLOUD_IP_ACCESS || "0.0.0.0/0"),
    };
    if (process.env.CLICKHOUSE_CLOUD_TIER) {
        body.tier = process.env.CLICKHOUSE_CLOUD_TIER;
    }
    if (process.env.CLICKHOUSE_CLOUD_MIN_TOTAL_MEMORY_GB) {
        body.minTotalMemoryGb = numberEnv("CLICKHOUSE_CLOUD_MIN_TOTAL_MEMORY_GB");
    }
    if (process.env.CLICKHOUSE_CLOUD_MAX_TOTAL_MEMORY_GB) {
        body.maxTotalMemoryGb = numberEnv("CLICKHOUSE_CLOUD_MAX_TOTAL_MEMORY_GB");
    }
    if (process.env.CLICKHOUSE_CLOUD_NUM_REPLICAS) {
        body.numReplicas = numberEnv("CLICKHOUSE_CLOUD_NUM_REPLICAS");
    }

    const created = await request("POST", `/organizations/${orgId}/services`, body);
    const createdServiceId =
        created.result?.id ||
        created.id ||
        (await findServiceByName(name))?.id;
    const service = await waitForReady(createdServiceId);
    await setPassword(service.id, password);
    const output = normalizeOutput(await waitForQueryReady(service, password));
    saveState(output);
    console.log(JSON.stringify(output));
}

async function setPassword(serviceId, password) {
    await request("PATCH", `/organizations/${orgId}/services/${serviceId}/password`, {
        newPasswordHash: createHash("sha256").update(password).digest("hex"),
        newDoubleSha1Hash: createHash("sha1")
            .update(createHash("sha1").update(password).digest())
            .digest("hex"),
    });
}

async function deleteService() {
    const state = readState();
    if (!state?.id) {
        return;
    }
    await request("DELETE", `/organizations/${orgId}/services/${state.id}`, undefined, { allow404: true });
}

async function findServiceByName(name) {
    const response = await request("GET", `/organizations/${orgId}/services`);
    const services = response.result || response.data || [];
    return services.find((service) => service.name === name);
}

async function waitForReady(serviceId) {
    if (!serviceId) {
        throw new Error("ClickHouse Cloud service response did not include service id");
    }
    const deadline = Date.now() + numberEnv("CLICKHOUSE_CLOUD_WAIT_MS", 30 * 60_000);
    let last;
    while (Date.now() < deadline) {
        const response = await request("GET", `/organizations/${orgId}/services/${serviceId}`);
        const service = response.result || response;
        last = service.state;
        const hasHttps = (service.endpoints || []).some((endpoint) => endpoint.protocol === "https" || endpoint.port === 8443);
        if (hasHttps && !["provisioning", "creating", "starting"].includes(String(service.state || "").toLowerCase())) {
            return service;
        }
        await sleep(numberEnv("CLICKHOUSE_CLOUD_POLL_MS", 10_000));
    }
    throw new Error(`timed out waiting for ClickHouse Cloud service ${serviceId}; last state=${last}`);
}

async function waitForQueryReady(service, password) {
    const endpoint = httpsEndpoint(service);
    const deadline = Date.now() + numberEnv("CLICKHOUSE_CLOUD_QUERY_WAIT_MS", 5 * 60_000);
    let last = "not attempted";
    while (Date.now() < deadline) {
        try {
            const response = await fetch(`https://${endpoint.host}:${endpoint.port}`, {
                method: "POST",
                headers: {
                    authorization: `Basic ${Buffer.from(`default:${password}`).toString("base64")}`,
                    "content-type": "text/plain; charset=utf-8",
                },
                body: "SELECT 1",
            });
            if (response.ok) {
                return service;
            }
            last = `${response.status}: ${await response.text()}`;
        } catch (error) {
            last = error instanceof Error ? error.message : String(error);
        }
        await sleep(numberEnv("CLICKHOUSE_CLOUD_QUERY_POLL_MS", 5_000));
    }
    throw new Error(`timed out waiting for ClickHouse Cloud query auth for ${service.id}; last=${last}`);
}

function normalizeOutput(service) {
    const endpoint = httpsEndpoint(service);
    return {
        id: service.id,
        name: service.name,
        provider: service.provider,
        region: service.region,
        state: service.state,
        url: `https://${endpoint.host}:${endpoint.port}`,
    };
}

function httpsEndpoint(service) {
    const endpoint = (service.endpoints || []).find((candidate) => candidate.protocol === "https")
        || (service.endpoints || []).find((candidate) => candidate.port === 8443);
    if (!endpoint) {
        throw new Error(`ClickHouse Cloud service ${service.id} has no HTTPS endpoint`);
    }
    return endpoint;
}

function readState() {
    try {
        return JSON.parse(readFileSync(stateFile, "utf8"));
    } catch {
        return undefined;
    }
}

async function request(method, route, body, options = {}) {
    const response = await fetch(`${apiUrl}${route}`, {
        method,
        headers: {
            authorization: `Basic ${Buffer.from(`${apiKey}:${apiSecret}`).toString("base64")}`,
            "content-type": "application/json",
        },
        body: body === undefined ? undefined : JSON.stringify(body),
    });
    const text = await response.text();
    if (response.status === 404 && options.allow404) {
        return {};
    }
    if (!response.ok) {
        throw new Error(`ClickHouse Cloud API ${method} ${route} failed (${response.status}): ${text}`);
    }
    return text ? JSON.parse(text) : {};
}

function parseIpAccess(value) {
    return value
        .split(",")
        .map((entry) => entry.trim())
        .filter(Boolean)
        .map((entry) => {
            const [source, description] = entry.split(":", 2);
            return { source, description: description || `Nanotrace access ${source}` };
        });
}

function saveState(value) {
    mkdirSync(path.dirname(stateFile), { recursive: true });
    writeFileSync(stateFile, `${JSON.stringify(value, null, 2)}\n`);
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
        if (fallback === undefined) {
            throw new Error(`${key} is required`);
        }
        return fallback;
    }
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed <= 0) {
        throw new Error(`${key} must be a positive number`);
    }
    return parsed;
}

function boolEnv(key, fallback) {
    const value = process.env[key];
    if (!value) {
        return fallback;
    }
    return ["1", "true", "yes", "on"].includes(value.toLowerCase());
}

function trimTrailingSlash(value) {
    return value.replace(/\/+$/, "");
}
