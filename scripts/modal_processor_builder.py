#!/usr/bin/env python3
import base64
import hashlib
import json
import os
import sys
import time
import traceback
from datetime import datetime, timezone

import boto3
import modal


APP_NAME = os.environ.get("MODAL_APP_NAME", "nanotrace-processor-builder")
TARGET = os.environ.get("PROCESSOR_TARGET", "aarch64-unknown-linux-gnu")


def main() -> int:
    bucket = required_env("PROCESSOR_BUCKET")
    name = required_env("PROCESSOR_NAME")
    s3 = boto3.client("s3")

    try:
        manifest = get_json(s3, bucket, manifest_key(name))
        stages = sorted(set(manifest.get("stages") or []))
        if not stages:
            raise RuntimeError("processor manifest has no stages")

        mark_manifest(s3, bucket, name, manifest, status="building", error=None)
        artifacts = dict(manifest.get("artifacts") or {})
        configs = dict(manifest.get("configs") or {})

        app = modal.App.lookup(APP_NAME, create_if_missing=True)
        image = (
            modal.Image.from_registry("rust:1-bookworm", add_python="3.11")
            .apt_install("gcc-aarch64-linux-gnu", "pkg-config")
            .run_commands(f"rustup target add {TARGET}")
        )
        sandbox = modal.Sandbox.create(
            "sleep",
            "1200",
            app=app,
            image=image,
            name=f"nanotrace-processor-{name}-{int(time.time())}",
            timeout=1200,
        )

        try:
            for stage in stages:
                source = get_text(s3, bucket, f"processors/{name}/{stage}/source.rs")
                config = get_json(s3, bucket, f"processors/{name}/{stage}/config.json")
                configs[stage] = config
                artifact = build_stage(sandbox, name, stage, source)
                artifact_key = f"processors/{name}/{stage}/artifacts/{TARGET}/libprocessor.so"
                s3.put_object(Bucket=bucket, Key=artifact_key, Body=artifact)
                artifacts[stage] = {
                    "key": artifact_key,
                    "sha256": hashlib.sha256(artifact).hexdigest(),
                }
        finally:
            try:
                sandbox.terminate()
            except Exception:
                pass
            try:
                sandbox.detach()
            except Exception:
                pass

        manifest["status"] = "ready"
        manifest["configs"] = configs
        manifest["artifacts"] = artifacts
        manifest["error"] = None
        manifest["updated_at"] = now()
        put_json(s3, bucket, manifest_key(name), manifest)
        return 0
    except Exception as exc:
        error = f"{exc}\n{traceback.format_exc()}"
        try:
            manifest = get_json(s3, bucket, manifest_key(name))
            mark_manifest(s3, bucket, name, manifest, status="failed", error=error)
        except Exception:
            print(error, file=sys.stderr)
        return 1


def build_stage(sandbox, name: str, stage: str, source: str) -> bytes:
    root = f"/work/{stage}"
    mkdir = sandbox.exec("mkdir", "-p", f"{root}/.cargo", f"{root}/src", timeout=60)
    mkdir.wait()
    if mkdir.returncode != 0:
        raise RuntimeError(f"failed to create sandbox work directories: {read_stream(mkdir.stderr)}")

    sandbox.filesystem.write_text(cargo_toml(), f"{root}/Cargo.toml")
    sandbox.filesystem.write_text(cargo_config(), f"{root}/.cargo/config.toml")
    sandbox.filesystem.write_text(source, f"{root}/src/user.rs")
    sandbox.filesystem.write_text(wrapper_rs(), f"{root}/src/lib.rs")

    process = sandbox.exec(
        "bash",
        "-lc",
        f"export PATH=/usr/local/cargo/bin:$PATH && cd {root} && cargo build --release --target {TARGET}",
        timeout=900,
    )
    process.wait()
    if process.returncode != 0:
        stdout = read_stream(process.stdout)
        stderr = read_stream(process.stderr)
        raise RuntimeError(
            f"Rust build failed for processor {name}/{stage}: {process.returncode}\n{stdout}\n{stderr}"
        )

    return sandbox.filesystem.read_bytes(
        f"{root}/target/{TARGET}/release/libnanotrace_processor.so"
    )


def cargo_toml() -> str:
    return """[package]
name = "nanotrace_processor"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
anyhow = "1"
serde_json = "1"
"""


def cargo_config() -> str:
    if TARGET == "aarch64-unknown-linux-gnu":
        return """[target.aarch64-unknown-linux-gnu]
linker = "aarch64-linux-gnu-gcc"
"""
    return ""


def wrapper_rs() -> str:
    encoded = base64.b64encode(
        br'''
mod user;

use serde_json::Value;
use std::{panic::{catch_unwind, AssertUnwindSafe}, slice};

#[repr(C)]
pub struct NtBytes {
    ptr: *const u8,
    len: usize,
}

#[repr(C)]
pub struct NtOwnedBytes {
    ptr: *mut u8,
    len: usize,
    cap: usize,
}

#[no_mangle]
pub extern "C" fn nanotrace_transform_v1(
    input: NtBytes,
    config: NtBytes,
    output: *mut NtOwnedBytes,
    error: *mut NtOwnedBytes,
) -> i32 {
    let result = catch_unwind(AssertUnwindSafe(|| transform(input, config)));
    match result {
        Ok(Ok(bytes)) => unsafe {
            if !output.is_null() {
                *output = into_owned(bytes);
            }
            0
        },
        Ok(Err(err)) => unsafe {
            if !error.is_null() {
                *error = into_owned(err.to_string().into_bytes());
            }
            1
        },
        Err(_) => unsafe {
            if !error.is_null() {
                *error = into_owned(b"processor panicked".to_vec());
            }
            1
        },
    }
}

#[no_mangle]
pub extern "C" fn nanotrace_free_v1(bytes: NtOwnedBytes) {
    if bytes.ptr.is_null() {
        return;
    }
    unsafe {
        drop(Vec::from_raw_parts(bytes.ptr, bytes.len, bytes.cap));
    }
}

fn transform(input: NtBytes, config: NtBytes) -> anyhow::Result<Vec<u8>> {
    let input = unsafe { slice::from_raw_parts(input.ptr, input.len) };
    let config = unsafe { slice::from_raw_parts(config.ptr, config.len) };
    let config: Value = if config.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(config)?
    };

    let mut output = Vec::with_capacity(input.len());
    for line in input.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        let event: Value = serde_json::from_slice(line)?;
        if let Some(event) = user::transform_event(event, &config)?.into_processor_output() {
            serde_json::to_writer(&mut output, &event)?;
            output.push(b'\n');
        }
    }
    Ok(output)
}

trait IntoProcessorOutput {
    fn into_processor_output(self) -> Option<Value>;
}

impl IntoProcessorOutput for Value {
    fn into_processor_output(self) -> Option<Value> {
        Some(self)
    }
}

impl IntoProcessorOutput for Option<Value> {
    fn into_processor_output(self) -> Option<Value> {
        self
    }
}

fn into_owned(mut bytes: Vec<u8>) -> NtOwnedBytes {
    let out = NtOwnedBytes {
        ptr: bytes.as_mut_ptr(),
        len: bytes.len(),
        cap: bytes.capacity(),
    };
    std::mem::forget(bytes);
    out
}
'''
    ).decode("ascii")
    return base64_decode_rs(encoded)


def base64_decode_rs(encoded: str) -> str:
    # Keep the large Rust wrapper above readable in this Python file while
    # still writing plain Rust into the sandbox.
    return base64.b64decode(encoded).decode("utf-8")


def read_stream(stream) -> str:
    data = stream.read()
    if isinstance(data, bytes):
        return data.decode("utf-8", "replace")
    return str(data)


def manifest_key(name: str) -> str:
    return f"processors/{name}/manifest.json"


def get_text(s3, bucket: str, key: str) -> str:
    return get_bytes(s3, bucket, key).decode("utf-8")


def get_json(s3, bucket: str, key: str):
    return json.loads(get_text(s3, bucket, key))


def get_bytes(s3, bucket: str, key: str) -> bytes:
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read()


def put_json(s3, bucket: str, key: str, value) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(value, indent=2, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )


def mark_manifest(s3, bucket: str, name: str, manifest, status: str, error: str | None) -> None:
    manifest["status"] = status
    manifest["error"] = error
    manifest["updated_at"] = now()
    put_json(s3, bucket, manifest_key(name), manifest)


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def required_env(key: str) -> str:
    value = os.environ.get(key)
    if not value:
        raise RuntimeError(f"{key} is required")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
