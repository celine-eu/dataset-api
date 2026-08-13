"""Requests arriving through the dataspace are refused unless the dataset is offered.

`dataspace_expose` and the ds authorization answer different questions, and both
have to hold:

- ``dataspace_expose`` — is this dataset on offer *to anyone*
- ``authorize_dataplane`` — does *this consumer* hold an agreement covering it

The gate is checked first and independently. A dataset that was never offered
must be refused even if a contract somehow names it, because until now the flag
that would have granted it meant something else entirely: `dataspace.expose` was
the only way a dataset could reach the catalogue, so its `true` values are
statements about catalogue visibility, not about dataspace consent.
"""
from __future__ import annotations

import pytest
from fastapi import HTTPException

from celine.dataset.api.dataset_query import executor as executor_mod
from celine.dataset.api.dataset_query.executor import execute_query
from celine.dataset.db.models.dataset_entry import DatasetEntry
from celine.dataset.security.edr import EDRRequestContext

TABLE = "dataset_api.edr_gate_table"


@pytest.fixture()
async def offered_and_withheld(test_session):
    """Two datasets, identical but for the dataspace gate."""
    from sqlalchemy import text

    await test_session.execute(
        text(f"CREATE TABLE IF NOT EXISTS {TABLE} (id INTEGER)")
    )
    await test_session.execute(text(f"INSERT INTO {TABLE} VALUES (1)"))
    for dataset_id, offered in (("ds_offered", True), ("ds_withheld", False)):
        test_session.add(
            DatasetEntry(
                dataset_id=dataset_id,
                title=dataset_id,
                backend_type="postgres",
                backend_config={"table": TABLE},
                expose=True,          # both are in the catalogue
                dataspace_expose=offered,
                access_level="open",
            )
        )
    await test_session.commit()
    yield
    await test_session.execute(text(f"DROP TABLE IF EXISTS {TABLE}"))
    await test_session.commit()


def _edr() -> EDRRequestContext:
    return EDRRequestContext(
        agreement_id="agr-1", consumer_id="did:web:consumer", transfer_id="tr-1"
    )


async def _run(session, dataset_id: str, edr):
    return await execute_query(
        catalogue_db=session,
        datasets_db=session,
        raw_sql=f"SELECT * FROM {dataset_id}",
        limit=10,
        offset=0,
        user=None,
        edr_context=edr,
    )


@pytest.mark.asyncio
async def test_a_withheld_dataset_is_refused_before_ds_is_asked(
    test_session, offered_and_withheld, monkeypatch
) -> None:
    """Refused locally — ds is never consulted about a dataset never offered."""
    called = False

    async def _never(**kwargs):
        nonlocal called
        called = True
        raise AssertionError("ds must not be asked about an unoffered dataset")

    monkeypatch.setattr(executor_mod, "authorize_dataplane", _never)

    with pytest.raises(HTTPException) as exc:
        await _run(test_session, "ds_withheld", _edr())

    assert exc.value.status_code == 403
    assert "not offered in the dataspace" in exc.value.detail
    assert "ds_withheld" in exc.value.detail  # actionable for a legitimate caller
    assert called is False


@pytest.mark.asyncio
async def test_an_offered_dataset_reaches_the_ds_decision(
    test_session, offered_and_withheld, monkeypatch
) -> None:
    """The gate does not replace authorization — it precedes it."""
    seen: dict = {}

    async def _deny(*, context, dataset_ids):
        seen["dataset_ids"] = dataset_ids
        return type("D", (), {"allowed": False, "reason": "no agreement"})()

    monkeypatch.setattr(executor_mod, "authorize_dataplane", _deny)

    with pytest.raises(HTTPException) as exc:
        await _run(test_session, "ds_offered", _edr())

    assert exc.value.status_code == 403
    assert "Refused by ds" in exc.value.detail
    assert seen["dataset_ids"] == ["ds_offered"]


@pytest.mark.asyncio
async def test_the_gate_does_not_apply_to_api_requests(
    test_session, offered_and_withheld
) -> None:
    """No EDR context means no dataspace request; `expose` alone decides.

    This is the half that must not regress: the dataset is withheld from the
    dataspace and still served over the API, which is the whole reason the two
    gates were separated.
    """
    result = await _run(test_session, "ds_withheld", None)
    assert result is not None


@pytest.mark.asyncio
async def test_one_withheld_dataset_refuses_the_whole_join(
    test_session, offered_and_withheld, monkeypatch
) -> None:
    """A join is judged as a whole, so an unoffered table cannot ride along."""
    monkeypatch.setattr(
        executor_mod,
        "authorize_dataplane",
        lambda **kw: (_ for _ in ()).throw(AssertionError("must not be reached")),
    )

    with pytest.raises(HTTPException) as exc:
        await _run(
            test_session,
            "ds_offered a JOIN ds_withheld b ON a.id = b.id",
            _edr(),
        )

    assert exc.value.status_code == 403
    assert "ds_withheld" in exc.value.detail
    assert "ds_offered" not in exc.value.detail  # only the offending table named
