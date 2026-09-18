"""Settings for the DPS data plane, kept apart from `core/config.py`.

Separate on purpose: the mode is off by default, and the shared settings class is
edited by other work. Every variable carries the `DPS_` prefix.
"""
from __future__ import annotations

from functools import lru_cache
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DpsSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="DPS_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    enabled: bool = Field(
        default=False,
        description="Mount the DPS signalling and pull routes (DPS-01).",
    )
    dataplane_id: str = Field(
        default="dataset-api",
        description="The id this data plane registers under.",
    )
    signaling_url: str = Field(
        default="http://localhost:8001",
        description=(
            "Base URL at which the control plane reaches this service. The "
            "registered DPS endpoint is this plus `/dps/v1/dataflows`."
        ),
    )
    public_url: str = Field(
        default="http://localhost:8001",
        description=(
            "Base URL at which a consumer reaches this service. The data "
            "address endpoint is this plus `/dps/public`."
        ),
    )
    transfer_types: list[str] = Field(
        default_factory=lambda: ["HttpData-PULL"],
        description="Transfer types accepted and registered. Pull only.",
    )
    labels: list[str] = Field(default_factory=list)
    control_plane_clients: list[str] = Field(
        default_factory=list,
        description=(
            "OIDC client ids allowed to signal this data plane. Empty admits "
            "nobody (DPS-02)."
        ),
    )
    token_signing_key: Optional[str] = Field(
        default=None,
        description=(
            "PEM EC P-256 private key that signs pull tokens. Required when "
            "the mode is enabled: every worker must share it, and it must "
            "survive a restart."
        ),
    )
    token_key_id: Optional[str] = Field(default=None)


@lru_cache(maxsize=1)
def get_dps_settings() -> DpsSettings:
    return DpsSettings()
