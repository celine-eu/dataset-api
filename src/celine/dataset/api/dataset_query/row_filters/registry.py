from __future__ import annotations

import importlib
import logging
from dataclasses import dataclass
from importlib.metadata import entry_points
from typing import Any, Awaitable, Callable, Dict, Optional, Protocol

from celine.dataset.core.config import get_settings
from celine.dataset.security.models import AuthenticatedUser
from celine.dataset.api.dataset_query.row_filters.cache import TTLCache
from celine.dataset.api.dataset_query.row_filters.models import RowFilterPlan
from celine.dataset.api.dataset_query.row_filters.utils import token_ttl_seconds

logger = logging.getLogger(__name__)


class RowFilterHandler(Protocol):
    """Handler contract.

    A handler resolves a governance spec into a RowFilterPlan for a given physical table.

    **`principals` is who the rows must belong to.** When it is ``None`` the
    handler resolves the caller's own data, which is the self-service case and
    what every handler did before delegation existed. When it carries a list,
    the rows belong to *those* people instead — a dataspace query is authorised
    for the subjects who consented, never for the caller, and the caller is a
    service identity that owns none of it.

    The two cases differ only in *whose* data; how a person maps to values in a
    column is the handler's business either way, which is why this is one
    protocol and not two.
    """

    name: str

    async def resolve(
        self,
        *,
        table: str,
        user: AuthenticatedUser,
        args: dict[str, Any],
        request_context: dict[str, Any] | None = None,
        principals: list[str] | None = None,
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
        principals: list[str] | None = None,
        ttl_override: int | None = None,
    ) -> RowFilterPlan:
        handler = self.get(handler_name)
        if handler is None:
            raise KeyError(handler_name)

        # The cache key must include **whose** data the plan is for, not just
        # who asked. In delegation the caller is one service account for every
        # agreement, so keying on `sub` alone made two different consented
        # subject sets share a plan — and the second one to ask would have been
        # served the first one's rows.
        args_key = str(sorted(args.items()))
        sub = user.sub
        principals_key = ",".join(sorted(principals)) if principals else "self"
        key = f"{handler_name}|{table}|{sub}|{principals_key}|{args_key}"

        cached = self.cache.get(key)
        if cached is not None:
            return cached

        plan = await handler.resolve(
            table=table,
            user=user,
            args=args,
            request_context=request_context,
            principals=principals,
        )

        # TTL. In delegation the control plane supplies it and it wins, because
        # the token cannot: an EDR token carries **no `exp`** (EDC 0.16 mints
        # `jti/aud/iss/sub/iat` and nothing else), so deriving a lifetime from it
        # would let a plan outlive the consent that justified it. That window is
        # how long a revoked agreement keeps yielding rows, so it belongs to
        # whoever knows about the revocation.
        default_ttl = get_settings().row_filters_cache_ttl
        if ttl_override is not None:
            ttl = max(0, min(ttl_override, default_ttl))
        else:
            ttl = token_ttl_seconds(user)
            if ttl is None:
                ttl = default_ttl
            else:
                ttl = max(0, min(ttl, default_ttl))

        self.cache.set(key, plan, ttl_seconds=int(ttl))
        return plan


_registry: RowFilterRegistry | None = None


def _load_modules() -> None:
    modules = get_settings().row_filters_modules
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
        cache=TTLCache(maxsize=get_settings().row_filters_cache_maxsize),
    )
    # built-ins
    reg.register(DirectUserMatchHandler())
    reg.register(HttpInListHandler())
    reg.register(TablePointerHandler())
    reg.register(RecRegistryHandler())

    # Assign before loading external modules so they can call
    # get_row_filter_registry() to register their own handlers.
    _registry = reg

    _load_modules()

    # Entry-point discovered handlers (external packages)
    for ep in entry_points(group="celine.dataset.row_filters"):
        try:
            handler_cls = ep.load()
            reg.register(handler_cls())
            logger.info("Loaded entry-point row filter: %s", ep.name)
        except Exception:
            logger.exception("Failed to load entry-point row filter: %s", ep.name)

    return reg
