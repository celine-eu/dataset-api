"""The startup check that makes the development defaults safe.

Every default in `Settings` describes the local platform: the `securepassword123`
database, the local Keycloak, policies that may be switched off. They are
deliberate, and they are acceptable only under `CELINE_ENV=dev`. Anywhere else
this refuses to start, listing every offending setting at once.
"""

from __future__ import annotations

from celine.dataset.core.config import Settings
from celine.sdk.posture import PostureGuard


def posture_guard(settings: Settings) -> PostureGuard:
    """Register this service's development defaults and switches."""
    guard = PostureGuard("dataset-api", env=settings.env)
    guard.forbid_dev_database_url("DATABASE_URL", settings.database_url)
    guard.forbid_dev_database_url("DATASETS_DATABASE_URL", settings.datasets_database_url)
    guard.forbid_false(
        "POLICIES_CHECK_ENABLED",
        settings.policies_check_enabled,
        "Leave policy evaluation on: with it off every internal and restricted "
        "dataset is readable by any authenticated caller.",
    )
    guard.require_explicit_oidc(settings.oidc)
    guard.forbid_secret_equal_to_client_id(
        "CELINE_OIDC_CLIENT_SECRET", settings.oidc.client_id, settings.oidc.client_secret
    )
    return guard


def enforce_posture(settings: Settings) -> None:
    """Warn under `CELINE_ENV=dev`; refuse to start anywhere else."""
    posture_guard(settings).enforce()
