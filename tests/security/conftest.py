import pytest
from types import SimpleNamespace
from typing import Any, Dict, cast

from dataset.security.disclosure import DisclosureLevel
from dataset.db.models.dataset_entry import DatasetEntry


@pytest.fixture
def user() -> Dict[str, Any]:
    return {
        "sub": "user-123",
        "roles": ["DATA_USER"],
        "preferred_username": "alice",
    }


@pytest.fixture
def admin_user() -> Dict[str, Any]:
    return {
        "sub": "admin-1",
        "roles": ["ADMIN", "DATA_OWNER"],
    }


@pytest.fixture
def anon_user():
    return None


def make_entry(
    *,
    disclosure: DisclosureLevel,
    governance: dict | None = None,
):
    """
    Minimal DatasetEntry stub.
    Only fields used by governance are provided.
    """
    return cast(
        DatasetEntry,
        SimpleNamespace(
            dataset_id="test_dataset",
            access_level=disclosure.value,
            governance=governance or {},
        ),
    )
