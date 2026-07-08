# celine.dataset.ext — public API surface for extension authors
#
# Import from here instead of reaching into internal modules.

from celine.dataset.core.config import Settings, configure, get_settings, reset_settings
from celine.dataset.main import create_app

from celine.dataset.db.models.dataset_entry import DatasetEntry
from celine.dataset.api.catalogue.schema import BackendConfig, DatasetEntryModel

from celine.dataset.security.governance import (
    enforce_dataset_access,
    resolve_datasets_for_tables,
)
from celine.dataset.security.models import AuthenticatedUser
from celine.dataset.security.auth import get_current_user, get_optional_user

from celine.dataset.api.dataset_query.row_filters.registry import (
    RowFilterHandler,
    RowFilterRegistry,
    get_row_filter_registry,
)
from celine.dataset.api.dataset_query.row_filters.models import RowFilterPlan

from celine.dataset.schemas.dataset_query import DatasetQueryModel, DatasetQueryResult

from celine.dataset.db.engine import get_datasets_session, get_session

__all__ = [
    "Settings",
    "configure",
    "get_settings",
    "reset_settings",
    "create_app",
    "DatasetEntry",
    "DatasetEntryModel",
    "BackendConfig",
    "enforce_dataset_access",
    "resolve_datasets_for_tables",
    "AuthenticatedUser",
    "get_current_user",
    "get_optional_user",
    "RowFilterHandler",
    "RowFilterPlan",
    "RowFilterRegistry",
    "get_row_filter_registry",
    "DatasetQueryModel",
    "DatasetQueryResult",
    "get_session",
    "get_datasets_session",
]
