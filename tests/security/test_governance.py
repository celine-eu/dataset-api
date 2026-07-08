import pytest
from fastapi import HTTPException

from celine.dataset.core.config import get_settings
from celine.dataset.security import governance as gov
from celine.dataset.security.disclosure import AccessLevel, ACCESS_LEVEL_MATRIX


@pytest.fixture(autouse=True)
def enable_opa_for_governance_tests():
    s = get_settings()
    old = s.policies_check_enabled
    s.policies_check_enabled = True
    yield
    s.policies_check_enabled = old


@pytest.fixture(autouse=True)
def reset_policy_engine():
    gov._policy_engine = None
    yield
    gov._policy_engine = None


class DummyPolicyEngine:
    """Stub that mimics CachedPolicyEngine.evaluate_decision."""

    def __init__(self, *, allowed: bool):
        self._allowed = allowed

    def evaluate_decision(self, policy_package, policy_input, **kw):
        from celine.sdk.policies.engine import Decision

        return Decision(
            allowed=self._allowed,
            reason="test" if self._allowed else "denied by test",
            policy="test_policy",
            cached=False,
        )

    @property
    def policy_count(self):
        return 1

    def get_packages(self):
        return ["celine.dataset"]

    @property
    def cache_stats(self):
        return {}


# ----------------------------------------------------------------------
# Disclosure matrix sanity check
# ----------------------------------------------------------------------


def test_disclosure_matrix_is_exhaustive():
    for level in AccessLevel:
        assert level in ACCESS_LEVEL_MATRIX


# ----------------------------------------------------------------------
# OPEN
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_open_allows_anonymous(anon_user):
    from tests.security.conftest import make_entry

    entry = make_entry(disclosure=AccessLevel.OPEN)
    await gov.enforce_dataset_access(entry=entry, user=anon_user)


@pytest.mark.asyncio
async def test_open_allows_authenticated(user):
    from tests.security.conftest import make_entry

    entry = make_entry(disclosure=AccessLevel.OPEN)
    await gov.enforce_dataset_access(entry=entry, user=user)


# ----------------------------------------------------------------------
# INTERNAL (auth + OPA)
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_internal_requires_opa_allow(monkeypatch, user):
    from tests.security.conftest import make_entry

    entry = make_entry(disclosure=AccessLevel.INTERNAL)

    monkeypatch.setattr(
        gov,
        "_get_policy_engine",
        lambda: DummyPolicyEngine(allowed=True),
    )

    await gov.enforce_dataset_access(entry=entry, user=user)


@pytest.mark.asyncio
async def test_internal_denied_by_opa(monkeypatch, user):
    from tests.security.conftest import make_entry

    entry = make_entry(disclosure=AccessLevel.INTERNAL)

    monkeypatch.setattr(
        gov,
        "_get_policy_engine",
        lambda: DummyPolicyEngine(allowed=False),
    )

    with pytest.raises(HTTPException) as exc:
        await gov.enforce_dataset_access(entry=entry, user=user)

    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_internal_denies_anonymous(anon_user):
    from tests.security.conftest import make_entry

    entry = make_entry(disclosure=AccessLevel.INTERNAL)

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
        disclosure=AccessLevel.RESTRICTED,
        governance={"owner": "admin-1"},
    )

    monkeypatch.setattr(
        gov,
        "_get_policy_engine",
        lambda: DummyPolicyEngine(allowed=True),
    )

    await gov.enforce_dataset_access(entry=entry, user=admin_user)


@pytest.mark.asyncio
async def test_restricted_denied_by_opa(monkeypatch, admin_user):
    from tests.security.conftest import make_entry

    entry = make_entry(disclosure=AccessLevel.RESTRICTED)

    monkeypatch.setattr(
        gov,
        "_get_policy_engine",
        lambda: DummyPolicyEngine(allowed=False),
    )

    with pytest.raises(HTTPException) as exc:
        await gov.enforce_dataset_access(entry=entry, user=admin_user)

    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_restricted_denies_anonymous(anon_user):
    from tests.security.conftest import make_entry

    entry = make_entry(disclosure=AccessLevel.RESTRICTED)

    with pytest.raises(HTTPException) as exc:
        await gov.enforce_dataset_access(entry=entry, user=anon_user)

    assert exc.value.status_code == 401
