# dataset/routes/dps.py
"""Mounts the DPS data plane (`celine.dataset.dps`) when `DPS_ENABLED` is set.

The package lives outside `routes/` so its core stays free of the app. This file
exists only because routes are discovered by globbing this directory.
"""
from __future__ import annotations

from fastapi import APIRouter

from celine.dataset.dps.settings import get_dps_settings

enabled = get_dps_settings().enabled
tags = ["dataspace"]

router = APIRouter()
if enabled:
    from celine.dataset.dps.api import get_dataplane, public_router, signaling_router

    # Built now so a missing signing key fails the start-up, not the first
    # transfer. No database connection is opened here.
    get_dataplane()
    router.include_router(signaling_router)
    router.include_router(public_router)
