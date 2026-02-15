from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Generic, Optional, TypeVar

T = TypeVar("T")


@dataclass
class _CacheEntry(Generic[T]):
    value: T
    expires_at: float


class TTLCache(Generic[T]):
    """Very small in-memory TTL cache (process-local)."""

    def __init__(self, maxsize: int = 10_000):
        self._maxsize = maxsize
        self._store: dict[str, _CacheEntry[T]] = {}

    def get(self, key: str) -> Optional[T]:
        e = self._store.get(key)
        if e is None:
            return None
        if e.expires_at <= time.time():
            self._store.pop(key, None)
            return None
        return e.value

    def set(self, key: str, value: T, ttl_seconds: int) -> None:
        if ttl_seconds <= 0:
            return
        if len(self._store) >= self._maxsize:
            # naive eviction: drop expired, otherwise drop arbitrary oldest-like key
            now = time.time()
            for k in list(self._store.keys()):
                if self._store[k].expires_at <= now:
                    self._store.pop(k, None)
            if len(self._store) >= self._maxsize:
                self._store.pop(next(iter(self._store.keys())), None)

        self._store[key] = _CacheEntry(value=value, expires_at=time.time() + ttl_seconds)
