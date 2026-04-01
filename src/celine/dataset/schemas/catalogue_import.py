# Canonical definitions live in api/catalogue/schema.py.
# This module re-exports them so existing imports continue to work unchanged.
from celine.dataset.api.catalogue.schema import (  # noqa: F401
    BackendConfig,
    CatalogueImportModel,
    ContactPoint,
    DatasetEntryModel,
    Lineage,
    Tags,
    TemporalCoverage,
)
