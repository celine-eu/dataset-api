from __future__ import annotations

import hashlib
import importlib
import inspect
import logging
from dataclasses import dataclass, field
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

    ``[]`` is therefore **not** the self-service case. It says "delegated, and
    nobody" — an allow-list that narrows to nothing — and every handler must
    read it that way. Conflating it with ``None`` is how a decision naming no
    principal came to be answered with the *caller's* rows, and in
    ``rec_registry`` with every row in the table, because the caller is a
    service account and service accounts bypass that filter.

    **`keys` are the same subjects in the column's own vocabulary** — typed data
    keys (``"pod:…"``), the values the holder already stores their rows under,
    registered with the consent. A handler reads whichever list it knows:
    ``direct_user_match`` and ``rec_registry`` read the principals,
    ``subject_key_match`` reads the keys. Both are allow-lists and an empty one
    narrows to nothing.

    A handler written before typed keys keeps working: the registry passes
    ``keys`` only to a ``resolve`` that declares it.

    The cases differ only in *whose* data; how a person maps to values in a
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
        keys: list[str] | None = None,
    ) -> RowFilterPlan: ...


@dataclass
class RowFilterRegistry:
    """Registry + shared cache for row filter handlers."""

    handlers: Dict[str, RowFilterHandler]
    cache: TTLCache[RowFilterPlan]

    #: Which handlers accept `keys`, worked out once per handler. See
    #: `_accepts_keys`.
    _keys_support: Dict[str, bool] = field(default_factory=dict)

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
        keys: list[str] | None = None,
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
        # A delegated request has **no logged-in user** — the caller is a
        # service identity and the data belongs to other people entirely — so
        # the identity half of the key comes from the allow-lists instead.
        sub = user.sub if user is not None else "delegated"
        key = "|".join(
            [
                handler_name,
                table,
                sub,
                self._allow_list_key(principals, keys),
                args_key,
            ]
        )

        cached = self.cache.get(key)
        if cached is not None:
            return cached

        call: dict[str, Any] = {
            "table": table,
            "user": user,
            "args": args,
            "request_context": request_context,
            "principals": principals,
        }
        if self._accepts_keys(handler):
            call["keys"] = keys
        elif keys:
            # Not an error, and not a widening: the lists name the same people,
            # and a handler reads the one it knows. An older handler keying on
            # principals is unaffected by a list it was never going to read.
            logger.debug(
                "Row filter handler %r predates typed keys — %d key(s) not passed",
                handler_name,
                len(keys),
            )
        plan = await handler.resolve(**call)

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
            ttl = token_ttl_seconds(user) if user is not None else None
            if ttl is None:
                ttl = default_ttl
            else:
                ttl = max(0, min(ttl, default_ttl))

        self.cache.set(key, plan, ttl_seconds=int(ttl))
        return plan

    @staticmethod
    def _allow_list_key(
        principals: list[str] | None, keys: list[str] | None
    ) -> str:
        """The half of the cache key that says *whose* rows the plan is for.

        Three things it has to keep apart, because each pair of them used to
        collide into one entry:

        - **self-service** (`principals is None`) from **delegated-but-nobody**
          (`principals == []`). Both used to render `self`, so a decision
          naming no principal reused the caller's own plan;
        - one set of principals from another (already true, kept);
        - one set of **keys** from another. Without this a second consent's
          supply points were answered with the first consent's predicate, which
          is the same failure one layer down and the one that matters most here,
          because a `subject_key_match` decision often carries *no* principals
          at all — so the rest of the key is identical between two of them.

        The keys are hashed rather than embedded: they are personal data, and a
        cache key is the kind of string that ends up in a repr or a debug line.
        A digest keys just as well and says nothing.
        """
        if principals is None:
            scope = "self"
        else:
            scope = "principals:" + ",".join(sorted(principals))
        if keys:
            digest = hashlib.sha256(
                "\x00".join(sorted(keys)).encode("utf-8")
            ).hexdigest()
            scope += f"|keys:{digest}"
        return scope

    def _accepts_keys(self, handler: RowFilterHandler) -> bool:
        """Whether this handler's `resolve` declares `keys`.

        Handlers arrive from three places — this package, `ROW_FILTERS_MODULES`
        and the `celine.dataset.row_filters` entry points — and the last two are
        packaged elsewhere. Passing an argument an older one never declared
        would turn every filtered request into a `TypeError`, so the signature
        is read once and the argument is offered only where it fits.
        """
        cached = self._keys_support.get(handler.name)
        if cached is not None:
            return cached
        try:
            parameters = inspect.signature(handler.resolve).parameters
        except (TypeError, ValueError):  # a callable with no introspectable signature
            accepts = False
        else:
            accepts = "keys" in parameters or any(
                p.kind is inspect.Parameter.VAR_KEYWORD for p in parameters.values()
            )
        self._keys_support[handler.name] = accepts
        return accepts


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
        SubjectKeyMatchHandler,
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
    reg.register(SubjectKeyMatchHandler())
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
