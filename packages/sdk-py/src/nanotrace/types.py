from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol, TypeAlias

Json: TypeAlias = None | bool | int | float | str | list["Json"] | dict[str, "Json"]
JsonObject: TypeAlias = dict[str, Json]
CommonFields: TypeAlias = Mapping[str, Any]


class Transport(Protocol):
    def send(self, event: JsonObject) -> None:
        ...


class AsyncTransport(Protocol):
    async def send(self, event: JsonObject) -> None:
        ...


class Span(Protocol):
    trace_id: str
    span_id: str

    def set(self, key: str, value: Any) -> None:
        ...

    def event(self, name: str, data: CommonFields | None = None) -> None:
        ...

    def end(self, data: CommonFields | None = None) -> None:
        ...


class AsyncSpan(Protocol):
    trace_id: str
    span_id: str

    def set(self, key: str, value: Any) -> None:
        ...

    def event(self, name: str, data: CommonFields | None = None) -> None:
        ...

    def end(self, data: CommonFields | None = None) -> None:
        ...
