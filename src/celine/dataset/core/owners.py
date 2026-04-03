"""Owner identity registry.

Maps the short alias strings used in governance.yaml ``ownership`` blocks to
canonical machine-readable identifiers (DID or URL) and rich metadata.

The ontology for ``type`` follows Schema.org (https://schema.org/) which is the
most broadly understood vocabulary and aligns with DCAT-AP's use of
``foaf:Agent`` for publishers — Schema.org types are emitted alongside
``foaf:Organization`` in JSON-LD output for full compatibility.

Common type values:
  schema:Organization           — generic fallback
  schema:Corporation            — for-profit company / srl / ltd
  schema:GovernmentOrganization — public authority / ministry / agency
  schema:ResearchOrganization   — university, institute, research centre
  schema:NGO                    — non-governmental / non-profit organisation
  schema:Project                — project consortium without separate legal entity
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, ConfigDict

logger = logging.getLogger(__name__)


class OwnerEntry(BaseModel):
    """Canonical identity record for a governance owner alias.

    Fields
    ------
    id   : alias used in governance.yaml ``ownership`` blocks (e.g. ``spxl``)
    type : Schema.org type CURIE — governs the ``@type`` emitted in JSON-LD
    name : human-readable display name
    did  : ``did:web:`` URI when the owner operates a dataspace connector
    url  : canonical homepage URI — used as publisher URI when no DID is set
    """

    model_config = ConfigDict(extra="ignore")

    id: str
    type: str = "schema:Organization"
    name: Optional[str] = None
    did: Optional[str] = None
    url: Optional[str] = None


class OwnersRegistry:
    """Loaded registry supporting O(1) lookup by alias or by canonical URI."""

    def __init__(self, entries: list[OwnerEntry]) -> None:
        self._by_id: dict[str, OwnerEntry] = {e.id: e for e in entries}
        # index by DID and URL so the DCAT formatter can look up stored URIs
        self._by_uri: dict[str, OwnerEntry] = {}
        for e in entries:
            if e.did:
                self._by_uri[e.did] = e
            if e.url:
                self._by_uri[e.url] = e

    def by_id(self, alias: str) -> Optional[OwnerEntry]:
        """Look up by the short alias used in governance.yaml."""
        return self._by_id.get(alias)

    def by_uri(self, uri: str) -> Optional[OwnerEntry]:
        """Look up by canonical URI (DID or URL) — used by the DCAT formatter."""
        return self._by_uri.get(uri)

    def canonical_uri(self, alias: str) -> Optional[str]:
        """Return the canonical URI for an alias: DID takes priority over URL."""
        entry = self._by_id.get(alias)
        if entry is None:
            return None
        return entry.did or entry.url

    def __len__(self) -> int:
        return len(self._by_id)


def load_owners_yaml(path: Path) -> OwnersRegistry:
    """Load an owners.yaml file and return an OwnersRegistry.

    Raises
    ------
    FileNotFoundError
        If the path does not exist. Callers that treat the registry as optional
        should catch this and fall back to ``OwnersRegistry([])``.
    """
    with path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    entries = [OwnerEntry.model_validate(item) for item in (raw.get("owners") or [])]
    logger.debug("Loaded %d owner entries from %s", len(entries), path)
    return OwnersRegistry(entries)
