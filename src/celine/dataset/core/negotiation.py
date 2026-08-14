"""Content negotiation for paths that answer both a browser and an API client.

`/catalogue/{id}` is one URL with two representations: a page for a human and a
DCAT-AP JSON-LD document for a consumer. Starlette matches routes by path alone,
so the choice cannot be made by the router — it is made here, from `Accept`.
"""
from __future__ import annotations

from fastapi import Request

HTML_MEDIA_TYPES = ("text/html", "application/xhtml+xml")
JSON_MEDIA_TYPES = ("application/ld+json", "application/json")


def accept_quality(request: Request, media_types: tuple[str, ...]) -> float:
    """Best q-value the client advertises for any of `media_types`.

    Wildcards (`*/*`, `type/*`) count, so a client that states no preference
    scores the same on every branch and the caller's default decides.
    """
    best = 0.0

    for part in request.headers.get("accept", "").split(","):
        token, _, params = part.strip().partition(";")
        token = token.strip().lower()
        if not token:
            continue

        if token == "*/*":
            candidates = set(media_types)
        elif token.endswith("/*"):
            candidates = {mt for mt in media_types if mt.startswith(token[:-1])}
        else:
            candidates = {token}

        if not candidates & set(media_types):
            continue

        quality = 1.0
        for param in params.split(";"):
            key, _, value = param.partition("=")
            if key.strip().lower() == "q":
                try:
                    quality = float(value.strip())
                except ValueError:
                    quality = 0.0

        best = max(best, quality)

    return best


def wants_html(request: Request) -> bool:
    """HTML only when the client asks for it more strongly than for JSON.

    Browsers send `text/html,…,application/xml;q=0.9,*/*;q=0.8` and get the
    page. An API client (`application/ld+json`, `*/*`, or no `Accept` at all)
    gets the document: this is an API path, so the API representation is the
    default and HTML is the negotiated exception.
    """
    html = accept_quality(request, HTML_MEDIA_TYPES)
    return html > 0.0 and html > accept_quality(request, JSON_MEDIA_TYPES)
