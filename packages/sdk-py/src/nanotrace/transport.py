from __future__ import annotations

import asyncio
import json
import socket
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

from .types import JsonObject


class NanotraceTransportError(RuntimeError):
    pass


def _post_json(
    *,
    events_url: str,
    error_prefix: str,
    headers: dict[str, str],
    payload: JsonObject | list[JsonObject],
    timeout: float,
) -> None:
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    request = urllib.request.Request(
        events_url,
        data=body,
        method="POST",
        headers=headers,
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = response.status
    except urllib.error.HTTPError as error:
        raise NanotraceTransportError(f"{error_prefix}: HTTP {error.code}") from error
    if status < 200 or status >= 300:
        raise NanotraceTransportError(f"{error_prefix}: HTTP {status}")


@dataclass
class HttpTransport:
    url: str
    key: str
    timeout: float = 5.0

    def __post_init__(self) -> None:
        self.events_url = f"{self.url.rstrip('/')}/v1/events"
        self.auth = f"Bearer {self.key}"

    def send(self, event: JsonObject) -> None:
        _post_json(
            events_url=self.events_url,
            error_prefix="Nanotrace ingest failed",
            headers={
                "authorization": self.auth,
                "content-type": "application/json",
            },
            payload=event,
            timeout=self.timeout,
        )

    def send_batch(self, events: list[JsonObject]) -> None:
        if not events:
            return
        _post_json(
            events_url=self.events_url,
            error_prefix="Nanotrace ingest failed",
            headers={
                "authorization": self.auth,
                "content-type": "application/json",
            },
            payload=events,
            timeout=self.timeout,
        )


@dataclass
class SidecarHttpTransport:
    url: str = "http://127.0.0.1:4320"
    timeout: float = 1.0

    def __post_init__(self) -> None:
        self.events_url = f"{self.url.rstrip('/')}/events"

    def send(self, event: JsonObject) -> None:
        _post_json(
            events_url=self.events_url,
            error_prefix="Nanotrace sidecar ingest failed",
            headers={"content-type": "application/json"},
            payload=event,
            timeout=self.timeout,
        )

    def send_batch(self, events: list[JsonObject]) -> None:
        if not events:
            return
        _post_json(
            events_url=self.events_url,
            error_prefix="Nanotrace sidecar ingest failed",
            headers={"content-type": "application/json"},
            payload=events,
            timeout=self.timeout,
        )


class UdpTransport:
    def __init__(self, host: str = "127.0.0.1", port: int = 4319) -> None:
        self.address = (host, port)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def send(self, event: JsonObject) -> None:
        self.socket.sendto(json.dumps(event, separators=(",", ":")).encode("utf-8"), self.address)

    def close(self) -> None:
        self.socket.close()


class AsyncHttpTransport:
    def __init__(self, url: str, key: str, timeout: float = 5.0) -> None:
        self._sync = HttpTransport(url=url, key=key, timeout=timeout)

    async def send(self, event: JsonObject) -> None:
        await asyncio.to_thread(self._sync.send, event)

    async def send_batch(self, events: list[JsonObject]) -> None:
        await asyncio.to_thread(self._sync.send_batch, events)


class AsyncSidecarHttpTransport:
    def __init__(self, url: str = "http://127.0.0.1:4320", timeout: float = 1.0) -> None:
        self._sync = SidecarHttpTransport(url=url, timeout=timeout)

    async def send(self, event: JsonObject) -> None:
        await asyncio.to_thread(self._sync.send, event)

    async def send_batch(self, events: list[JsonObject]) -> None:
        await asyncio.to_thread(self._sync.send_batch, events)


class AsyncUdpTransport:
    def __init__(self, host: str = "127.0.0.1", port: int = 4319) -> None:
        self.address = (host, port)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setblocking(False)
        self.socket.connect(self.address)

    async def send(self, event: JsonObject) -> None:
        loop = asyncio.get_running_loop()
        body = json.dumps(event, separators=(",", ":")).encode("utf-8")
        await loop.sock_sendall(self.socket, body)

    def close(self) -> None:
        self.socket.close()


def http_transport(url: str, key: str, timeout: float = 5.0) -> HttpTransport:
    return HttpTransport(url=url, key=key, timeout=timeout)


def sidecar_http_transport(
    url: str = "http://127.0.0.1:4320",
    timeout: float = 1.0,
) -> SidecarHttpTransport:
    return SidecarHttpTransport(url=url, timeout=timeout)


def udp_transport(host: str = "127.0.0.1", port: int = 4319) -> UdpTransport:
    return UdpTransport(host=host, port=port)


def async_http_transport(url: str, key: str, timeout: float = 5.0) -> AsyncHttpTransport:
    return AsyncHttpTransport(url=url, key=key, timeout=timeout)


def async_sidecar_http_transport(
    url: str = "http://127.0.0.1:4320",
    timeout: float = 1.0,
) -> AsyncSidecarHttpTransport:
    return AsyncSidecarHttpTransport(url=url, timeout=timeout)


def async_udp_transport(host: str = "127.0.0.1", port: int = 4319) -> AsyncUdpTransport:
    return AsyncUdpTransport(host=host, port=port)
