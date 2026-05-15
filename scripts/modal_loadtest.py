#!/usr/bin/env python3
import io
import math
import os
import sys
import tarfile
import time
from pathlib import Path

import modal


APP_NAME = os.environ.get("MODAL_LOADTEST_APP_NAME", "nanotrace-loadtest")
DEFAULT_GENERATORS = 4
DEFAULT_START_RPS = 1
DEFAULT_MAX_RPS = 2_000
DEFAULT_MAX_IN_FLIGHT = 2_000


def main() -> int:
    root = repo_root()
    generators = positive_int(os.environ.get("NANOTRACE_LOADTEST_GENERATORS"), DEFAULT_GENERATORS)
    require_env("NANOTRACE_INGEST_URL")
    require_env("NANOTRACE_API_KEY")

    env = loadtest_env(generators)
    archive = repo_archive(root)

    app = modal.App.lookup(APP_NAME, create_if_missing=True)
    image = modal.Image.from_registry("rust:1-bookworm", add_python="3.11")
    sandboxes = []
    processes = []
    started_at = time.time()
    print(f"modalLoadtestApp={APP_NAME}")
    print(f"modalGenerators={generators}")
    print(f"runId={env['NANOTRACE_LOADTEST_RUN_ID']}")

    try:
        for index in range(generators):
            sandbox = modal.Sandbox.create(
                "sleep",
                "7200",
                app=app,
                image=image,
                name=f"nanotrace-loadtest-{safe_name(env['NANOTRACE_LOADTEST_RUN_ID'])}-g{index}",
                timeout=7200,
                cpu=float(os.environ.get("NANOTRACE_LOADTEST_MODAL_CPU", "2")),
                memory=int(os.environ.get("NANOTRACE_LOADTEST_MODAL_MEMORY", "4096")),
            )
            sandboxes.append(sandbox)
            prepare_sandbox(sandbox, archive)
            worker_env = dict(env)
            worker_env["NANOTRACE_LOADTEST_SEQUENCE_OFFSET"] = str(index)
            worker_env["NANOTRACE_LOADTEST_SEQUENCE_STRIDE"] = str(generators)
            worker_env["NANOTRACE_LOADTEST_WORKER_INDEX"] = str(index)
            worker_env["NANOTRACE_LOADTEST_WORKER_COUNT"] = str(generators)
            process = sandbox.exec(
                "bash",
                "-lc",
                "export PATH=/usr/local/cargo/bin:$PATH && cd /work/nanotrace && cargo run --release -p nanotrace-loadtest --",
                env=worker_env,
                timeout=7200,
            )
            processes.append((index, process))
            print(f"startedGenerator={index}")

        failed = False
        for index, process in processes:
            process.wait()
            stdout = process.stdout.read()
            stderr = process.stderr.read()
            print(f"\n=== modal generator {index} stdout ===")
            print(stdout.rstrip())
            if stderr.strip():
                print(f"\n=== modal generator {index} stderr ===", file=sys.stderr)
                print(stderr.rstrip(), file=sys.stderr)
            if process.returncode != 0:
                failed = True
                print(f"generatorFailed={index} exitCode={process.returncode}", file=sys.stderr)

        elapsed = time.time() - started_at
        print(f"modalLoadtestElapsedSeconds={elapsed:.1f}")
        return 1 if failed else 0
    finally:
        for sandbox in sandboxes:
            try:
                sandbox.terminate()
            except Exception:
                pass


def loadtest_env(generators: int) -> dict[str, str]:
    env = {
        key: value
        for key, value in os.environ.items()
        if key.startswith("NANOTRACE_LOADTEST_")
        or key in {"NANOTRACE_INGEST_URL", "NANOTRACE_API_KEY"}
        or key.startswith("CLICKHOUSE_")
    }
    env.setdefault("NANOTRACE_LOADTEST_RUN_ID", f"modal-loadtest-{int(time.time())}")
    env["NANOTRACE_LOADTEST_START_RPS"] = str(
        per_generator_int("NANOTRACE_LOADTEST_START_RPS", DEFAULT_START_RPS, generators)
    )
    env["NANOTRACE_LOADTEST_MAX_RPS"] = str(
        per_generator_int("NANOTRACE_LOADTEST_MAX_RPS", DEFAULT_MAX_RPS, generators)
    )
    env["NANOTRACE_LOADTEST_MAX_IN_FLIGHT"] = str(
        per_generator_int("NANOTRACE_LOADTEST_MAX_IN_FLIGHT", DEFAULT_MAX_IN_FLIGHT, generators)
    )
    return env


def per_generator_int(key: str, fallback: int, generators: int) -> int:
    value = positive_int(os.environ.get(key), fallback)
    return max(1, math.ceil(value / generators))


def prepare_sandbox(sandbox, archive: bytes) -> None:
    sandbox.mkdir("/work", parents=True)
    with sandbox.open("/work/nanotrace.tar.gz", "wb") as output:
        output.write(archive)
    process = sandbox.exec(
        "bash",
        "-lc",
        "tar -xzf /work/nanotrace.tar.gz -C /work",
        timeout=120,
    )
    process.wait()
    if process.returncode != 0:
        raise RuntimeError(process.stderr.read())


def repo_archive(root: Path) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for path in sorted(root.rglob("*")):
            if should_skip(path, root):
                continue
            archive.add(path, arcname=Path("nanotrace") / path.relative_to(root), recursive=False)
    return buffer.getvalue()


def should_skip(path: Path, root: Path) -> bool:
    rel = path.relative_to(root)
    parts = set(rel.parts)
    if parts & {
        ".git",
        ".pulumi-docker",
        ".nanotrace",
        "node_modules",
        "target",
        "dist",
        "__pycache__",
    }:
        return True
    if path.is_file() and path.stat().st_size > 20 * 1024 * 1024:
        return True
    return False


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def require_env(key: str) -> str:
    value = os.environ.get(key, "").strip()
    if not value:
        raise RuntimeError(f"{key} is required")
    return value


def safe_name(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "-_" else "-" for ch in value)[:80]


def positive_int(value: str | None, fallback: int) -> int:
    if value is None or value == "":
        return fallback
    parsed = int(value)
    if parsed <= 0:
        raise RuntimeError(f"expected positive integer, got {value}")
    return parsed


if __name__ == "__main__":
    raise SystemExit(main())
