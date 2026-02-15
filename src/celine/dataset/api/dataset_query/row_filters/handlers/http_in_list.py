from __future__ import annotations

import logging
from typing import Any, Iterable, List, Optional

import httpx
from sqlglot import exp

from celine.dataset.api.dataset_query.row_filters.models import RowFilterPlan
from celine.dataset.security.models import AuthenticatedUser

logger = logging.getLogger(__name__)


def _format_obj(obj: Any, user: AuthenticatedUser) -> Any:
    if isinstance(obj, str):
        return obj.format(
            sub=user.sub,
            username=(user.username or ""),
            email=(user.email or ""),
            token=(user.token or ""),
        )
    if isinstance(obj, dict):
        return {k: _format_obj(v, user) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_format_obj(v, user) for v in obj]
    return obj


def _extract_path(payload: Any, path: str | None) -> Any:
    if path is None or path == "" or path == "$":
        return payload
    cur = payload
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur


class HttpInListHandler:
    """Row filter: call an HTTP endpoint and filter with `column IN (items)`.

    Governance args:
      - column: str (required) target column on the dataset table
      - url: str (required)
      - method: "GET" | "POST" (default "GET")
      - headers: object (optional) values support python format with {sub}
      - params: object (optional, GET) values support {sub}
      - json: object (optional, POST) values support {sub}
      - response_path: str (optional) dot path to list in JSON response, default "$"
      - timeout_seconds: int (optional, default 5)
      - max_items: int (optional, default 2000) hard cap for IN list
      - empty_means_deny: bool (optional, default true)
    """

    name = "http_in_list"

    async def resolve(
        self,
        *,
        table: str,
        user: AuthenticatedUser,
        args: dict[str, Any],
        request_context: dict[str, Any] | None = None,
    ) -> RowFilterPlan:
        column = args.get("column")
        url = args.get("url")
        if not isinstance(column, str) or not column:
            raise ValueError("http_in_list requires args.column")
        if not isinstance(url, str) or not url:
            raise ValueError("http_in_list requires args.url")

        method = str(args.get("method") or "GET").upper()
        timeout_seconds = int(args.get("timeout_seconds") or 5)
        response_path = args.get("response_path") or "$"
        max_items = int(args.get("max_items") or 2000)
        empty_means_deny = bool(args.get("empty_means_deny", True))

        headers = args.get("headers") or {}

        if args.get("forward_token", False):
            headers["authorization"] = user.token

        params = args.get("params") or {}
        json_body = args.get("json") or None

        headers = _format_obj(headers, user)
        params = _format_obj(params, user)
        json_body = _format_obj(json_body, user) if json_body is not None else None

        async with httpx.AsyncClient(timeout=timeout_seconds) as client:
            if method == "GET":
                resp = await client.get(url, headers=headers, params=params)
            elif method == "POST":
                resp = await client.post(
                    url, headers=headers, params=params, json=json_body
                )
            else:
                raise ValueError(f"http_in_list unsupported method: {method}")

        resp.raise_for_status()
        payload = resp.json()
        items = _extract_path(payload, response_path)

        if items is None:
            logger.warning("http_in_list response_path not found: %s", response_path)
            items_list: list[Any] = []
        elif isinstance(items, list):
            items_list = items
        else:
            items_list = [items]

        # normalize & cap
        flat: list[Any] = []
        for it in items_list:
            if it is None:
                continue
            flat.append(it)

        if not flat:
            if empty_means_deny:
                return RowFilterPlan(
                    table=table, kind="deny", meta={"reason": "empty_list"}
                )
            predicate = exp.Boolean(this=True)
            return RowFilterPlan(
                table=table, kind="predicate", predicate_template=predicate
            )

        if len(flat) > max_items:
            flat = flat[:max_items]

        literals: list[exp.Expression] = []
        for v in flat:
            # keep simple: represent as string unless numeric/bool
            if isinstance(v, bool):
                literals.append(exp.Boolean(this=v))
            elif isinstance(v, (int, float)):
                literals.append(exp.Literal.number(str(v)))
            else:
                literals.append(exp.Literal.string(str(v)))

        predicate = exp.In(
            this=exp.Column(this=exp.Identifier(this=column, quoted=False)),
            expressions=literals,
        )
        return RowFilterPlan(
            table=table,
            kind="predicate",
            predicate_template=predicate,
            meta={"items": len(flat), "url": url},
        )
