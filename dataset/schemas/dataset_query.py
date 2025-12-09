# dataset/schemas/dataset_query.py
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class DatasetQueryModel(BaseModel):
    filter: Optional[str] = None
    limit: int = 100
    offset: int = 0
