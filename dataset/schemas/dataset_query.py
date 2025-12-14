# dataset/schemas/dataset_query.py
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel
from typing import Any, List, Optional


class DatasetQueryResult(BaseModel):
    dataset_id: str
    items: List[dict[str, Any]]
    offset: int
    limit: int
    count: int
    total: Optional[int] = None


class DatasetQueryModel(BaseModel):
    filter: Optional[str] = None
    limit: int = 100
    offset: int = 0
