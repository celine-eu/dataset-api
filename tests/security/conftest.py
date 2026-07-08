import pytest
from types import SimpleNamespace
from typing import Any, Dict, cast

from celine.dataset.security.disclosure import AccessLevel
from celine.dataset.security.models import AuthenticatedUser
from celine.dataset.db.models.dataset_entry import DatasetEntry


@pytest.fixture
def user() -> AuthenticatedUser:
    return AuthenticatedUser(
        sub="user-123",
        username="alice",
        roles=["DATA_USER"],
        claims={"sub": "user-123", "scope": "openid"},
    )


@pytest.fixture
def admin_user() -> AuthenticatedUser:
    return AuthenticatedUser(
        sub="admin-1",
        username="admin",
        roles=["ADMIN", "DATA_OWNER"],
        scopes=["dataset-api.admin"],
        claims={"sub": "admin-1", "scope": "dataset-api.admin openid"},
    )


@pytest.fixture
def anon_user():
    return None


def make_entry(
    *,
    disclosure: AccessLevel,
    governance: dict | None = None,
):
    lineage = None
    if governance:
        lineage = {"facets": {"governance": governance}}

    return cast(
        DatasetEntry,
        SimpleNamespace(
            dataset_id="test_dataset",
            access_level=disclosure.value,
            backend_type="postgres",
            lineage=lineage,
        ),
    )
