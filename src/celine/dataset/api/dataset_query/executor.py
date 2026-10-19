# src/celine/dataset/api/dataset_query/executor.py
from __future__ import annotations

import json
import logging

import httpx
from typing import Optional, Dict, Sequence, List
from fastapi import HTTPException, Request
from sqlalchemy import RowMapping, Table, text, select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import DBAPIError

from sqlglot import exp as sqlglot_exp

from celine.dataset.schemas.dataset_query import DatasetQueryResult
from celine.dataset.db.models.dataset_entry import DatasetEntry
from celine.dataset.db.reflection import reflect_table_async
from celine.dataset.core.datasets import load_dataset_entry, physical_table
from celine.dataset.security.audit import ReadAudit, Refused
from celine.dataset.security.disclosure import AccessLevel, is_available
from celine.dataset.security.governance import (
    enforce_dataset_access,
    resolve_datasets_for_tables,
)
from celine.dataset.security.edr import (
    EDRRequestContext,
    audit_query,
    authorize_dataplane,
)
from celine.dataset.security.models import AuthenticatedUser
from celine.dataset.api.dataset_query.parser import parse_sql_query
from celine.dataset.api.dataset_query.log_safety import sql_shape
from celine.dataset.api.dataset_query.pagination import (
    paginated_sql as build_paginated_sql,
)
from celine.dataset.api.dataset_query.row_filters import (
    RowFilterSpecError,
    apply_row_filter_plans,
    get_row_filter_registry,
    get_row_filter_specs,
    split_layers,
)
from celine.dataset.api.dataset_query.row_filters.models import RowFilterPlan
from celine.dataset.api.dataset_query.row_filters.handlers import HANDLER_BINDS
from celine.dataset.api.dataset_query.row_filters.utils import is_admin_user
from celine.dataset.core.config import get_settings
from celine.governance import ALLOW_ALL, DENY_ALL

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Limits
# ---------------------------------------------------------------------------

DEFAULT_LIMIT = 100
MAX_LIMIT = 10_000
STATEMENT_TIMEOUT_MS = get_settings().query_statement_timeout_ms

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _clamp_limit(limit: int) -> int:
    if limit <= 0:
        return DEFAULT_LIMIT
    return min(limit, MAX_LIMIT)


def _check_row_policy(
    ds: DatasetEntry,
    ref_table: str,
    layers: Dict[str, List[dict]],
    audit: ReadAudit | None,
) -> None:
    """The row policy's two layers, before any handler runs (GS-10).

    A layer carrying `deny_all` refuses the dataset: `403 row_policy_denied`. A
    layer with nothing declared admits every row on an `open` dataset and is
    otherwise `ROW_FILTERS_DEFAULT`: `deny` refuses with
    `403 row_policy_undeclared`; `allow` serves it, and says so in the log and
    the audit record under the same reason — the report-only mode.
    """
    for layer, specs in layers.items():
        if any(spec["handler"] == DENY_ALL for spec in specs):
            raise Refused(
                403,
                f"Dataset {ref_table}: its {layer} row policy admits no row",
                reason="row_policy_denied",
            )

    try:
        is_open = AccessLevel.from_value(ds.access_level) is AccessLevel.OPEN
    except ValueError:
        is_open = False  # unreachable: `enforce_dataset_access` refused it
    undeclared = [layer for layer, specs in layers.items() if not specs]
    if not undeclared or is_open:
        return

    what = " and ".join(undeclared)
    if get_settings().row_filters_default == "deny":
        logger.warning(
            "Dataset %s declares no %s row policy — refused (ROW_FILTERS_DEFAULT=deny)",
            ds.dataset_id,
            what,
        )
        raise Refused(
            403,
            f"Dataset {ref_table} declares no {what} row policy",
            reason="row_policy_undeclared",
        )
    logger.warning(
        "Dataset %s declares no %s row policy — served whole "
        "(ROW_FILTERS_DEFAULT=allow, row_policy_undeclared)",
        ds.dataset_id,
        what,
    )
    if audit is not None:
        audit.reasons[ds.dataset_id] = "row_policy_undeclared"


# ---------------------------------------------------------------------------
# Main executor
# ---------------------------------------------------------------------------


async def _execute_sql_with_timeout(
    db,
    sql: str,
    params: dict | None = None,
    timeout: int = STATEMENT_TIMEOUT_MS,
):
    try:
        await db.execute(text(f"SET LOCAL statement_timeout = {timeout}"))

        return await db.execute(text(sql), params or {})

    except DBAPIError as exc:
        # The SQL stays at DEBUG (it can be long and can name private tables); the
        # driver's own reason is logged where the service can be read at INFO, because
        # without it a day of 400s left no trace anywhere — not here, not in PostgreSQL.
        original = getattr(exc, "orig", None) or exc
        logger.warning(
            "Query failed: %s: %s (connection_invalidated=%s)",
            type(original).__name__,
            original,
            exc.connection_invalidated,
        )
        # The shape, not the text, and not `exc`: SQLAlchemy's message repeats the
        # statement and its parameters, literals and all (QE-03).
        logger.debug("Query failed sql=%s", sql_shape(sql))
        if "statement timeout" in str(exc).lower():
            raise HTTPException(400, "Query exceeded time limit") from None
        if exc.connection_invalidated:
            # The pooled connection was already dead; SQLAlchemy has discarded it. Not
            # the caller's query — a retry lands on a fresh connection.
            raise HTTPException(
                503, "Database connection lost, retry the query"
            ) from None
        raise HTTPException(400, "Database query failed") from None


async def execute_rows_with_timeout(
    db,
    sql: str,
    params: dict | None = None,
) -> Sequence[RowMapping]:
    result = await _execute_sql_with_timeout(db, sql, params)
    return result.mappings().all()


async def execute_scalar_with_timeout(
    db,
    sql: str,
    params: dict | None = None,
) -> int:
    result = await _execute_sql_with_timeout(db, sql, params)
    return int(result.scalar_one())


async def execute_query(
    *,
    catalogue_db: AsyncSession,
    datasets_db: AsyncSession,
    raw_sql: Optional[str],
    limit: int,
    offset: int,
    user: Optional[AuthenticatedUser],
    edr_context: Optional[EDRRequestContext] = None,
    skip_count: bool = False,
    audit: ReadAudit | None = None,
) -> DatasetQueryResult:
    """
    Execute a validated SQL query against a dataset.

    Guarantees:
    - dataset access enforced (OPA / disclosure)
    - SQL validated (SELECT-only, table allowlist)
    - LIMIT/OFFSET enforced server-side, after the statement's ORDER BY (QE-04)
    - hard row cap applied
    - row-level filters applied (pluggable governance handlers)

    `audit`, when given, is told which datasets the statement resolved to; the
    caller writes one record per dataset (GS-01).
    """
    if raw_sql is None or raw_sql.strip() == "":
        raise HTTPException(400, "sql query not provided")

    # QE-03: a statement's literals are the caller's data (a boundary lookup's
    # point is a supply address's coordinates); only its shape is logged.
    logger.debug("Parsing raw SQL: %s", sql_shape(raw_sql))
    try:
        parsed = parse_sql_query(raw_sql)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("SQL validation failed: %s", type(exc).__name__)  # QE-03
        raise HTTPException(400, str(exc)) from exc

    if not parsed.tables:
        raise HTTPException(400, "Query references no datasets")

    datasets = await resolve_datasets_for_tables(
        db=catalogue_db, table_names=parsed.tables
    )
    if audit is not None:
        audit.datasets = [d.dataset_id for d in datasets.values()]

    tables_map: dict[str, str] = {}
    row_filter_plans = []
    # EDR-path disclosures to record with ds after the query runs: (dataset_id,
    # the consenting subjects' DIDs). Collected here, emitted once the row count
    # is known. **DIDs, never the principals** — see the append below.
    edr_disclosures: list[tuple[str, Optional[list[str]]]] = []

    registry = get_row_filter_registry()

    # One decision per request, not per dataset: the datasets are already
    # resolved, and a join must be judged as a whole — ds answers `deny` if any
    # of them is refused, so asking per table would let a partial answer through.
    edr_decision = None
    if edr_context is not None:
        if audit is not None:
            audit.agreement_id = edr_context.agreement_id
        # Is this dataset offered into the dataspace at all? Checked before ds is
        # asked, because the two answer different questions: ds decides whether
        # *this consumer* holds a valid agreement, while `dataspace_expose` says
        # whether the dataset is on offer to anyone. A dataset that was never
        # offered must be refused even if a contract somehow references it.
        #
        # Every dataset in the query, not the first refusal: a join is judged as
        # a whole, exactly as the ds call below is, so a partial answer cannot
        # leak through a join with an unoffered table.
        withheld = sorted(
            d.dataset_id for d in datasets.values() if not d.dataspace_expose
        )
        if withheld:
            logger.warning(
                "Dataspace request refused — not offered: %s", ", ".join(withheld)
            )
            # Named rather than a bare 403: the catalogue already tells this
            # consumer the dataset exists, so naming it discloses nothing, and
            # the alternative is an unactionable error for a legitimate caller.
            raise Refused(
                403,
                "Dataset not offered in the dataspace: " + ", ".join(withheld),
                reason="not_offered",
            )

        edr_decision = await authorize_dataplane(
            context=edr_context,
            dataset_ids=[d.dataset_id for d in datasets.values()],
        )
        if audit is not None:
            # The release link (`security/audit.py`): only an allow has a
            # reference, and it is the connector's, carried as received.
            audit.decision_ref = edr_decision.decision_ref if edr_decision.allowed else None
        if not edr_decision.allowed:
            # ds's reason names the gate, never who holds the agreement, so it
            # is safe to relay.
            raise Refused(
                403, f"Refused by ds: {edr_decision.reason}", reason="ds_refused"
            )

    for ref_table, ds in datasets.items():
        # Unexposed, secret, or a level nobody can read: one answer for all three.
        if not is_available(ds.expose, ds.access_level):
            logger.warning("Requested dataset %s is not available", ds.dataset_id)
            raise Refused(403, "Dataset not available", reason="not_available")

        # Every dataset is mapped, checked and filtered below — there is no path
        # that skips ahead. An entry that states no table was once left unmapped
        # *and* unchecked: its logical name reached SQL with no access decision.
        phy_table_name = physical_table(ds.dataset_id, ds.backend_type, ds.backend_config)
        if phy_table_name is None:
            raise HTTPException(
                400,
                f"Dataset {ds.dataset_id} is not queryable: its backend "
                f"'{ds.backend_type}' has no SQL table",
            )

        logger.debug(f"Mapped SQL table {ref_table} -> {phy_table_name}")
        tables_map[ref_table] = phy_table_name

        # ------------------------------------------------------------------
        # EDR path — request came through the EDC data plane
        # ------------------------------------------------------------------
        if edr_context is not None:
            # ds returns the row filter **as governance declared it** — handler
            # and args — plus the consenting subjects, named twice: as
            # `principals` (identifiers native to this system) and as `keys`
            # (typed data keys, the values this holder already stores their rows
            # under). The handler is what knows which naming its column speaks:
            # `rec_registry` resolves members to devices, `direct_user_match`
            # matches the principal directly, `subject_key_match` matches the
            # keys. A decision reduced to a column would have forced this branch
            # to assume one of them, which is how the previous version came to
            # inject nothing at all.
            row_filter = edr_decision.row_filter_for(ds.dataset_id)
            if row_filter is not None and HANDLER_BINDS.get(row_filter.handler) == "organization":
                # ds sends an organization filter naming nobody for a dataset
                # that is not consent-gated (ds X-17). The organization layer is
                # this service's own declaration, applied below to the verdict's
                # `organizations` (GS-13); ds's copy of it is not the person layer.
                row_filter = None

            # The local declaration, both layers (GS-13): ds's verdict supplies
            # the person layer (the consenting subjects) and the consumer's
            # organizations; which filters exist is this service's catalogue.
            # A dataset the verdict does not list falls through the same path
            # with neither.
            try:
                layers = split_layers(ds, get_row_filter_specs(ds))
            except RowFilterSpecError as exc:
                logger.error("Unreadable row filters: %s", exc)
                raise Refused(
                    403,
                    f"Row filters of {ref_table} cannot be read, so no rows may be served",
                    reason="row_filter_unenforceable",
                ) from exc
            person_stated = row_filter is not None or any(
                spec["handler"] != ALLOW_ALL for spec in layers["person"]
            )
            if row_filter is None and person_stated:
                # The dataset narrows per person here and ds named nobody: the
                # two ends disagree, and this end cannot apply a person filter
                # without the consenting subjects.
                raise Refused(
                    403,
                    f"Dataset {ref_table} declares a person row filter but the "
                    "dataspace decision carries none, so no rows may be served",
                    reason="row_filter_unenforceable",
                )
            if row_filter is not None and not layers["person"]:
                # ds's filter states the person layer for this end.
                layers["person"] = [{"handler": row_filter.handler, "binds": "person"}]
            _check_row_policy(ds, ref_table, layers, audit)

            organizations = edr_decision.organizations_for(ds.dataset_id)
            for spec in layers["organization"]:
                if spec["handler"] == ALLOW_ALL:
                    continue
                try:
                    plan = await registry.resolve_with_cache(
                        handler_name=spec["handler"],
                        table=phy_table_name,
                        user=user,
                        args=spec.get("args") or {},
                        request_context={"agreement_id": edr_context.agreement_id},
                        organizations=organizations or [],
                        ttl_override=edr_decision.cache_ttl,
                    )
                except (KeyError, NotImplementedError) as exc:
                    logger.error(
                        "Cannot enforce organization filter %r for %s: %s",
                        spec["handler"],
                        ds.dataset_id,
                        exc,
                    )
                    raise Refused(
                        403,
                        f"Cannot enforce row filter handler '{spec['handler']}' "
                        f"for {ds.dataset_id} on a dataspace request, so no rows "
                        "may be served",
                        reason="row_filter_unenforceable",
                    ) from exc
                row_filter_plans.append(plan)

            if row_filter is None:
                # No person behind these rows, or every row admitted on purpose;
                # the organization layer, if any, is applied above. Still a
                # disclosure — record it.
                edr_disclosures.append((ds.dataset_id, None))
                continue

            try:
                plan = await registry.resolve_with_cache(
                    handler_name=row_filter.handler,
                    table=phy_table_name,
                    user=user,
                    args=row_filter.args,
                    request_context={"agreement_id": edr_context.agreement_id},
                    principals=row_filter.principals,
                    keys=row_filter.keys,
                    # The control plane's TTL wins: an EDR token has no `exp`, so
                    # a lifetime derived from it would let a plan outlive the
                    # consent that justified it.
                    ttl_override=edr_decision.cache_ttl,
                )
            except (KeyError, NotImplementedError) as exc:
                # An *allow* carrying a filter says "these rows". A data plane
                # that cannot apply the filter has not been permitted to serve
                # unfiltered ones — it has been handed an instruction it does
                # not understand, and the only answer is to serve nothing.
                logger.error(
                    "Cannot enforce row filter handler %r for %s: %s",
                    row_filter.handler,
                    ds.dataset_id,
                    exc,
                )
                raise Refused(
                    403,
                    f"Cannot enforce row filter handler '{row_filter.handler}' "
                    f"for {ds.dataset_id}: this data plane implements no such "
                    "handler, so no rows may be served",
                    reason="row_filter_unenforceable",
                ) from exc
            row_filter_plans.append(plan)
            # **The subject DIDs, and neither allow-list.** Both of the others
            # are personal data that may reach a predicate and nothing else: the
            # keys the collector registered with the consent, and the principals
            # — which in a realm where the Keycloak username is the person's
            # email are their addresses. This line sent the principals until
            # 2026-09-20 and so wrote 22 of them into one run's `QueryExecuted`
            # provenance. ds drops every non-DID it is sent, which is a backstop;
            # sending the DIDs is the fix, and they are the only identifiers here
            # a provenance record admits.
            #
            # Empty when the decision came from a ds that predates the field.
            # That is the same thin-but-true record ds's own filtering already
            # produces, and it is why a query is not refused over it.
            edr_disclosures.append((ds.dataset_id, row_filter.subject_dids))
            continue  # skip normal auth + spec loop for this dataset

        # ------------------------------------------------------------------
        # Normal path — Keycloak / OPA authenticated request
        # ------------------------------------------------------------------
        await enforce_dataset_access(entry=ds, user=user)

        try:
            layers = split_layers(ds, get_row_filter_specs(ds))
        except RowFilterSpecError as exc:
            # GS-11: an unreadable declaration is refused, never read as none.
            logger.error("Unreadable row filters: %s", exc)
            raise Refused(
                403,
                f"Row filters of {ref_table} cannot be read, so no rows may be served",
                reason="row_filter_unenforceable",
            ) from exc

        if user is None:
            # `enforce_dataset_access` refuses an anonymous caller on every level
            # (GS-12); this is the backstop, never the gate.
            raise Refused(
                401,
                f"Dataset {ref_table} requires authentication",
                reason="auth_required",
            )

        # The platform administrator reads every row, the row policy included
        # (kept as is); placed before the default so `deny` never denies the bypass.
        if is_admin_user(user):
            continue

        _check_row_policy(ds, ref_table, layers, audit)

        for spec in (s for layer in layers.values() for s in layer):
            handler_name = spec["handler"]
            args = spec.get("args") or {}
            if handler_name == ALLOW_ALL:
                # States the layer; narrows nothing.
                continue

            try:
                plan = await registry.resolve_with_cache(
                    handler_name=handler_name,
                    table=phy_table_name,
                    user=user,
                    args=args,
                    request_context={},
                )
            except KeyError:
                # GS-11: a handler this service does not implement has not said
                # "every row" — it is an instruction nobody can carry out.
                logger.error(
                    f"Unknown row filter handler '{handler_name}' for dataset {ref_table}"
                )
                raise Refused(
                    403,
                    f"Cannot enforce row filter handler '{handler_name}' for "
                    f"{ref_table}: this service implements no such handler, so no "
                    "rows may be served",
                    reason="row_filter_unenforceable",
                )
            except httpx.HTTPError:
                logger.error(f"Row filter resolution failed for dataset {ref_table}")
                raise Refused(
                    403,
                    f"Row filter resolution failed for dataset {ref_table}",
                    reason="row_filter_unresolved",
                )
            except Exception as e:
                logger.error(f"Row filter handler failed: {e}")
                raise HTTPException(
                    500,
                    f"Row filter handler '{handler_name}' failed for dataset {ref_table}",
                )

            row_filter_plans.append(plan)

    # Logical -> physical substitution
    mapped_ast = parsed.to_ast(tables_map=tables_map)
    complete_sql = mapped_ast.sql(dialect="postgres")
    logger.debug("Complete SQL (after table mapping): %s", sql_shape(mapped_ast))

    # Apply row-level filters to the mapped AST, never to a re-parse of its text
    # (see ParsedSQL.to_ast), and render once, as postgres.
    final_ast = mapped_ast
    if row_filter_plans:
        try:
            ast = apply_row_filter_plans(mapped_ast, row_filter_plans)
            complete_sql = ast.sql(dialect="postgres")
            final_ast = ast
        except Exception as exc:
            # The type only (QE-03): a filter's error text can quote its predicate,
            # which lists the caller's sensor ids.
            logger.error("Failed to apply row filters: %s", type(exc).__name__)
            raise HTTPException(500, "Failed to apply row filters") from None
        if edr_context is not None:
            # Not the SQL. A delegated predicate is a literal list of the
            # consenting subjects — usernames, or the data keys a collector
            # registered with the consent — and those are personal data that ds
            # says must not reach a log. The query before filtering is already
            # logged above, which is what a reader of this line was after.
            logger.debug(
                "Complete SQL (after row filters): withheld — the predicate "
                "names the consenting subjects"
            )
        else:
            # The shape: a self-service predicate lists the caller's sensor ids.
            logger.debug("Complete SQL (after row filters): %s", sql_shape(ast))

    # Pagination & caps
    limit = _clamp_limit(limit)
    offset = max(offset, 0)

    # QE-04: the statement's top-level ORDER BY is carried onto the outer query,
    # naming q's columns, so the page is cut from rows in the caller's order.
    paginated_sql = build_paginated_sql(complete_sql, final_ast)

    count_sql = f"""
        SELECT COUNT(*) FROM (
            {complete_sql}
        ) AS q
    """

    # Execute count (skipped when caller does not need total — saves one full table scan)
    total: Optional[int] = None
    if not skip_count:
        try:
            total = await execute_scalar_with_timeout(datasets_db, count_sql)
        except HTTPException:
            raise
        except Exception as exc:
            # Not `logger.exception`: a non-driver SQLAlchemy error (StatementError)
            # escapes the DBAPIError handler, and its text and traceback repeat the
            # statement and its parameters, literals and all (QE-03).
            logger.error(
                "Count query failed: %s sql=%s",
                type(exc).__name__,
                sql_shape(count_sql),
            )
            raise HTTPException(500, "Query failed") from None

    # Execute data query
    try:
        rows = await execute_rows_with_timeout(
            datasets_db,
            paginated_sql,
            {"limit": limit, "offset": offset},
        )
    except HTTPException:
        raise
    except Exception as exc:
        # As for the count: the error's type and the statement's shape only (QE-03).
        logger.error(
            "Query execution failed: %s sql=%s",
            type(exc).__name__,
            sql_shape(paginated_sql),
        )
        raise HTTPException(500, "Query execution failed") from None

    # Post-process rows (geometry → GeoJSON)
    items = []
    for r in rows:
        row = dict(r)
        for col, val in list(row.items()):
            if val is None:
                continue
            if hasattr(val, "__geo_interface__"):
                row[col] = val.__geo_interface__
            elif val.__class__.__name__ == "WKBElement":
                geojson = await datasets_db.scalar(select(func.ST_AsGeoJSON(val)))
                if geojson:
                    row[col] = json.loads(geojson)
        items.append(row)

    logger.debug(f"SQL items={len(items)} total={total} offset={offset} limit={limit}")

    # Record the disclosure with ds — one QueryExecuted per disclosed dataset.
    # authorize_dataplane was the decision; this is the accountability record,
    # and ds emits the provenance event nowhere else. `row_count` is the rows
    # this response actually returned (the page), best-effort so a provenance
    # outage never fails a query the control plane already authorised.
    if edr_context is not None:
        for disclosed_id, subject_dids in edr_disclosures:
            await audit_query(
                dataset_id=disclosed_id,
                consumer_id=edr_context.consumer_id,
                agreement_id=edr_context.agreement_id,
                transfer_id=edr_context.transfer_id,
                row_count=len(items),
                authorized_subject_ids=subject_dids,
                # The connector that decided is the connector that is told.
                provider_id=edr_context.provider_id,
                # The release link: the decision this disclosure was served under.
                decision_ref=edr_decision.decision_ref if edr_decision else None,
            )

    return DatasetQueryResult(
        items=items,
        offset=offset,
        limit=limit,
        count=len(items),
        total=total,
    )
