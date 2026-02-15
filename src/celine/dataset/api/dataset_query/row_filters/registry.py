from __future__ import annotations

import importlib
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional, Protocol

from celine.dataset.core.config import settings
from celine.dataset.security.models import AuthenticatedUser
from celine.dataset.api.dataset_query.row_filters.cache import TTLCache
from celine.dataset.api.dataset_query.row_filters.models import RowFilterPlan
from celine.dataset.api.dataset_query.row_filters.utils import token_ttl_seconds

logger = logging.getLogger(__name__)


class RowFilterHandler(Protocol):
    """Handler contract.

    A handler resolves a governance spec into a RowFilterPlan for a given physical table.
    """

    name: str

    async def resolve(
        self,
        *,
        table: str,
        user: AuthenticatedUser,
        args: dict[str, Any],
        request_context: dict[str, Any] | None = None,
    ) -> RowFilterPlan: ...


@dataclass
class RowFilterRegistry:
    """Registry + shared cache for row filter handlers."""

    handlers: Dict[str, RowFilterHandler]
    cache: TTLCache[RowFilterPlan]

    def get(self, name: str) -> Optional[RowFilterHandler]:
        return self.handlers.get(name)

    def register(self, handler: RowFilterHandler) -> None:
        if handler.name in self.handlers:
            raise ValueError(f"Duplicate row filter handler name: {handler.name}")
        self.handlers[handler.name] = handler

    async def resolve_with_cache(
        self,
        *,
        handler_name: str,
        table: str,
        user: AuthenticatedUser,
        args: dict[str, Any],
        request_context: dict[str, Any] | None = None,
    ) -> RowFilterPlan:
        handler = self.get(handler_name)
        if handler is None:
            raise KeyError(handler_name)

        # cache key must include relevant identity + args
        args_key = str(sorted(args.items()))
        sub = user.sub
        key = f"{handler_name}|{table}|{sub}|{args_key}"

        cached = self.cache.get(key)
        if cached is not None:
            return cached

        plan = await handler.resolve(
            table=table,
            user=user,
            args=args,
            request_context=request_context,
        )

        # TTL: token lifetime if available, else default
        ttl = token_ttl_seconds(user)
        default_ttl = getattr(settings, "row_filters_cache_ttl", 300)
        if ttl is None:
            ttl = default_ttl
        else:
            ttl = max(0, min(ttl, default_ttl))

        self.cache.set(key, plan, ttl_seconds=int(ttl))
        return plan


_registry: RowFilterRegistry | None = None


def _load_modules() -> None:
    modules = getattr(settings, "row_filters_modules", [])
    if not modules:
        return
    if isinstance(modules, str):
        modules = [m.strip() for m in modules.split(",") if m.strip()]
    for m in modules:
        try:
            importlib.import_module(m)
            logger.info("Loaded row filter module: %s", m)
        except Exception:
            logger.exception("Failed to load row filter module: %s", m)
            raise


def get_row_filter_registry() -> RowFilterRegistry:
    global _registry
    if _registry is not None:
        return _registry

    from celine.dataset.api.dataset_query.row_filters.handlers import (
        DirectUserMatchHandler,
        HttpInListHandler,
        TablePointerHandler,
        RecRegistryHandler,
    )

    reg = RowFilterRegistry(
        handlers={},
        cache=TTLCache(maxsize=getattr(settings, "row_filters_cache_maxsize", 10_000)),
    )
    # built-ins
    reg.register(DirectUserMatchHandler())
    reg.register(HttpInListHandler())
    reg.register(TablePointerHandler())
    reg.register(RecRegistryHandler())

    # optional plugin imports
    _load_modules()

    _registry = reg
    return reg
