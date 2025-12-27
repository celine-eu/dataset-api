import pytest
from fastapi import HTTPException

from celine.dataset.core.config import settings
from celine.dataset.security import governance as gov
from celine.dataset.security.disclosure import DisclosureLevel, DISCLOSURE_MATRIX


@pytest.fixture(autouse=True)
def enable_opa_for_governance_tests():
    old = settings.opa_enabled
    settings.opa_enabled = True
    yield
    settings.opa_enabled = old


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


class DummyOPAClient:
    def __init__(self, result: bool):
        self._result = result

    async def evaluate(self, dataset, user):
        return self._result


@pytest.fixture(autouse=True)
def reset_opa_cache():
    """
    Ensure OPA singleton cache does not leak between tests.
    """
    gov._opa_client = None
    yield
    gov._opa_client = None


# ----------------------------------------------------------------------
# Disclosure matrix sanity check
# ----------------------------------------------------------------------


def test_disclosure_matrix_is_exhaustive():
    """
    Ensure every DisclosureLevel has a policy.
    """
    for level in DisclosureLevel:
        assert level in DISCLOSURE_MATRIX


# ----------------------------------------------------------------------
# OPEN
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_open_allows_anonymous(anon_user):
    entry = gov_entry = None
    from tests.security.conftest import make_entry

    entry = make_entry(disclosure=DisclosureLevel.OPEN)

    # should not raise
    await gov.enforce_dataset_access(entry=entry, user=anon_user)


@pytest.mark.asyncio
async def test_open_allows_authenticated(user):
    from tests.security.conftest import make_entry

    entry = make_entry(disclosure=DisclosureLevel.OPEN)
    await gov.enforce_dataset_access(entry=entry, user=user)


# ----------------------------------------------------------------------
# INTERNAL (auth + OPA)
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_internal_requires_opa_allow(monkeypatch, user):
    from tests.security.conftest import make_entry

    entry = make_entry(disclosure=DisclosureLevel.INTERNAL)

    monkeypatch.setattr(
        gov,
        "_get_opa_client",
        lambda: DummyOPAClient(result=True),
    )

    await gov.enforce_dataset_access(entry=entry, user=user)


@pytest.mark.asyncio
async def test_internal_denied_by_opa(monkeypatch, user):
    from tests.security.conftest import make_entry

    entry = make_entry(disclosure=DisclosureLevel.INTERNAL)

    monkeypatch.setattr(
        gov,
        "_get_opa_client",
        lambda: DummyOPAClient(result=False),
    )

    with pytest.raises(HTTPException) as exc:
        await gov.enforce_dataset_access(entry=entry, user=user)

    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_internal_denies_anonymous(anon_user):
    from tests.security.conftest import make_entry

    entry = make_entry(disclosure=DisclosureLevel.INTERNAL)

    with pytest.raises(HTTPException) as exc:
        await gov.enforce_dataset_access(entry=entry, user=anon_user)

    assert exc.value.status_code == 401


# ----------------------------------------------------------------------
# RESTRICTED (auth + OPA)
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_restricted_requires_opa_allow(monkeypatch, admin_user):
    from tests.security.conftest import make_entry

    entry = make_entry(
        disclosure=DisclosureLevel.RESTRICTED,
        governance={"owner": "admin-1"},
    )

    monkeypatch.setattr(
        gov,
        "_get_opa_client",
        lambda: DummyOPAClient(result=True),
    )

    await gov.enforce_dataset_access(entry=entry, user=admin_user)


@pytest.mark.asyncio
async def test_restricted_denied_by_opa(monkeypatch, admin_user):
    from tests.security.conftest import make_entry

    entry = make_entry(disclosure=DisclosureLevel.RESTRICTED)

    monkeypatch.setattr(
        gov,
        "_get_opa_client",
        lambda: DummyOPAClient(result=False),
    )

    with pytest.raises(HTTPException) as exc:
        await gov.enforce_dataset_access(entry=entry, user=admin_user)

    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_restricted_denies_anonymous(anon_user):
    from tests.security.conftest import make_entry

    entry = make_entry(disclosure=DisclosureLevel.RESTRICTED)

    with pytest.raises(HTTPException) as exc:
        await gov.enforce_dataset_access(entry=entry, user=anon_user)

    assert exc.value.status_code == 401
