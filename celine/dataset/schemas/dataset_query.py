# dataset/schemas/dataset_query.py
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel
from typing import Any, List, Optional


class DatasetQueryResult(BaseModel):
    items: List[dict[str, Any]]
    offset: int
    limit: int
    count: int
    total: Optional[int] = None


class DatasetQueryModel(BaseModel):
    sql: Optional[str] = None
    limit: int = 100
    offset: int = 0
