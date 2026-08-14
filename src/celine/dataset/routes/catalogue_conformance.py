"""`POST /catalogue/{dataset_id}/conformance` — do this dataset's rows satisfy
the shapes its mapping pins?

Off unless `CONFORMANCE_ENABLED=true`; when off, the route is not registered at
all rather than answering 404, because "not deployed" is what is actually true.

**Authorised exactly like `/query`, and deliberately so.** This endpoint reads
real rows and returns violation messages that quote their values. Served under
`/vocabulary`'s unauthenticated discovery rules it would be a row-level data
leak wearing a metadata endpoint's clothes. It therefore goes through
`execute_query` — the same parser, the same governance and OPA checks, the same
row filters — rather than reading the table itself. Reimplementing the read here
would mean two paths to the same rows and only one of them audited.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from celine.dataset.api.catalogue.conformance import (
    ConformanceUnavailable,
    check_conformance,
)
from celine.dataset.api.dataset_query.executor import execute_query
from celine.dataset.core.config import get_settings
from celine.dataset.core.datasets import load_catalogue_entry
from celine.dataset.db.engine import get_datasets_session, get_session
from celine.dataset.security.auth import get_optional_user
from celine.dataset.security.edr import EDRRequestContext, verify_edr_consumer
from celine.dataset.security.models import AuthenticatedUser

logger = logging.getLogger(__name__)

router = APIRouter()
tags = ["catalogue"]

#: Read by `register_routes`. Evaluated at import, so flipping the setting needs
#: a restart — which is the same lifecycle every other setting here has.
enabled = get_settings().conformance_enabled

if enabled:
    # Fail at startup, not at the first request. An operator who turned the
    # feature on should learn it is unusable while they are still looking at the
    # deploy, rather than from a consumer's 500 some hours later.
    try:  # pragma: no cover - exercised by the install, not the suite
        import pyshacl  # noqa: F401
        from celine.mapper.profiles import load_profile  # noqa: F401
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "CONFORMANCE_ENABLED=true but the `conformance` extra is not "
            "installed. Install dataset[conformance] "
            "(celine-ontologies[mapper]>=1.10.1) or turn the setting off."
        ) from exc

#: A catalogue id is a dotted identifier. Checked before it is interpolated into
#: SQL even though it comes from the catalogue rather than from the caller —
#: the value is written by an import from another repository, and "trusted
#: because it is in our database" is how the first injection gets in.
_DATASET_ID_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")


class ConformanceRequest(BaseModel):
    limit: Optional[int] = Field(
        default=None,
        description=(
            "Rows to sample. Defaults to CONFORMANCE_SAMPLE_LIMIT, capped at "
            "CONFORMANCE_MAX_SAMPLE."
        ),
    )
    profile_version: Optional[str] = Field(
        default=None,
        description=(
            "Validate against this ontology version instead of the one the "
            "mapping pins. For deciding an upgrade before making it — the "
            "report says which version actually ran."
        ),
    )
    context: Optional[dict[str, Any]] = Field(
        default=None,
        description=(
            "Values for the mapping spec's context_vars, which are not columns "
            "(e.g. community_key)."
        ),
    )


def _sample_sql(dataset_id: str) -> str:
    """`SELECT * FROM <dataset>`, in the logical namespace `/query` uses.

    No LIMIT clause: the parser rejects one in a top-level query, because the
    executor applies the bound itself from the `limit` argument. Writing it into
    the SQL would be a second, unenforced answer to how many rows come back.

    Catalogue ids are 3-part (`datasets.schema.table`) where SQL references are
    2-part; the executor resolves either, but the shorter form is what the
    parser's allowlist is written for.
    """
    ref = dataset_id
    if ref.count(".") == 2:
        ref = ref.split(".", 1)[1]
    return f"SELECT * FROM {ref}"


@router.post(
    "/catalogue/{dataset_id}/conformance",
    description="Validate a sample of this dataset's rows against its ontology shapes",
    name="Dataset conformance check",
)
async def dataset_conformance(
    dataset_id: str,
    body: ConformanceRequest | None = None,
    catalogue_db: AsyncSession = Depends(get_session),
    datasets_db: AsyncSession = Depends(get_datasets_session),
    user: Optional[AuthenticatedUser] = Depends(get_optional_user),
    authorization: Optional[str] = Header(default=None),
    edc_contract_agreement_id: Optional[str] = Header(default=None),
    edc_transfer_process_id: Optional[str] = Header(default=None),
    edc_purpose: Optional[str] = Header(default=None),
    edc_bpn: Optional[str] = Header(default=None),
):
    """Map a bounded sample of rows through the dataset's mapping and validate.

    A non-conforming dataset is a **successful check**: 200 with
    `conforms: false`. Returning 4xx would conflate "the check failed to run"
    with "the check ran and found violations", and the second is the endpoint
    working.

    What a green result asserts is structural only — that the graph the mapping
    produces satisfies the shapes. It is not a statement that the columns mean
    what the mapping says they mean; that half of `dct:conformsTo` remains the
    producer's assertion.

    404 when the dataset is not exposed or declares no mapping, matching
    `/vocabulary` — a caller who may not see the dataset may not learn it exists.
    """
    settings = get_settings()
    body = body or ConformanceRequest()

    entry = await load_catalogue_entry(db=catalogue_db, dataset_id=dataset_id)
    if not entry.ontology_mapping:
        raise HTTPException(status_code=404, detail="Dataset declares no semantic model")

    if not _DATASET_ID_RE.match(entry.dataset_id):
        logger.error("Refusing to sample malformed dataset id %r", entry.dataset_id)
        raise HTTPException(status_code=500, detail="Dataset identifier is not sampleable")

    if body.profile_version:
        # A caller-supplied version that does not exist is a bad request, not a
        # broken service — and the message has to name what *is* available, or
        # the three-version window is a policy nobody can discover.
        from celine.mapper.profiles import available_profiles  # noqa: PLC0415

        known = available_profiles()
        if not any(body.profile_version in versions for versions in known.values()):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Unknown ontology version {body.profile_version!r}. "
                    f"Available: "
                    + "; ".join(f"{n}: {', '.join(v)}" for n, v in known.items())
                ),
            )

    limit = body.limit or settings.conformance_sample_limit
    limit = max(0, min(limit, settings.conformance_max_sample))

    # Mirrors /query exactly: dataspace mode is selected by the agreement
    # header, and never falls back to the user-auth path on failure — a fallback
    # between two authorization regimes is a bypass with extra steps.
    edr_context: Optional[EDRRequestContext] = None
    if settings.edr_enabled and edc_contract_agreement_id:
        edr_context = EDRRequestContext(
            agreement_id=edc_contract_agreement_id,
            consumer_id=await verify_edr_consumer(authorization),
            transfer_id=edc_transfer_process_id,
            purpose=[p.strip() for p in (edc_purpose or "").split(",") if p.strip()],
        )

    result = await execute_query(
        catalogue_db=catalogue_db,
        datasets_db=datasets_db,
        raw_sql=_sample_sql(entry.dataset_id),
        limit=limit,
        offset=0,
        user=user,
        edr_context=edr_context,
        skip_count=True,
    )

    try:
        report = check_conformance(
            dataset_id=entry.dataset_id,
            mapping=entry.ontology_mapping,
            rows=list(result.items),
            context=body.context,
            profile_version=body.profile_version,
        )
    except ConformanceUnavailable as exc:
        # The shapes could not be loaded, or the stored mapping no longer parses.
        # Neither is a finding about the data, and reporting it as one would file
        # a broken deployment as a data-quality result.
        logger.exception("Conformance check unavailable for %s", dataset_id)
        raise HTTPException(status_code=503, detail=str(exc))

    return report.to_dict()
