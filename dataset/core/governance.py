# dataset/core/governance.py
from typing import Optional, Dict, Any
from fastapi import HTTPException, status

from dataset.db.models.dataset_entry import DatasetEntry


def requires_auth(entry: DatasetEntry) -> bool:
    return (entry.access_level or "open").lower() not in {
        "open",
        "public",
        "green",
    }


def enforce_dataset_access(
    *,
    entry: DatasetEntry,
    user: Optional[Dict[str, Any]],
) -> None:
    if requires_auth(entry) and user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required for this dataset",
        )
