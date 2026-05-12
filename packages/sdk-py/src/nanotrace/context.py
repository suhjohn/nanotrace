from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Iterator

Context = dict[str, Any]

_current_context: ContextVar[Context] = ContextVar("nanotrace_context", default={})


def current_context() -> Context:
    return dict(_current_context.get())


@contextmanager
def trace_context(**values: Any) -> Iterator[None]:
    merged = {**_current_context.get(), **{k: v for k, v in values.items() if v is not None}}
    token = _current_context.set(merged)
    try:
        yield
    finally:
        _current_context.reset(token)
