# dataset/api/metadata/schema_builder.py
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple
import logging
from sqlalchemy import Table

logger = logging.getLogger(__name__)


def _sa_type_to_json(col_type: Any) -> Tuple[str, Optional[str]]:
    name = col_type.__class__.__name__.lower()

    if "int" in name:
        return "integer", None
    if "numeric" in name or "float" in name or "double" in name:
        return "number", None
    if "bool" in name:
        return "boolean", None
    if "timestamp" in name or "datetime" in name:
        return "string", "date-time"
    if "date" in name:
        return "string", "date"
    if "time" in name:
        return "string", "time"
    if "json" in name:
        return "object", None
    if "geometry" in name:
        return "object", "geojson"

    return "string", None


def build_json_schema(table: Optional[Table]) -> Dict[str, Any]:
    """
    Convert SQLAlchemy reflected table → JSON Schema 2020-12.
    If table is None, return empty object schema.
    """
    if table is None:
        logger.warning("JSON schema requested but table reflection returned None")
        return {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {},
            "required": [],
        }

    properties = {}
    required = []

    for col in table.columns:
        json_type, json_fmt = _sa_type_to_json(col.type)
        schema = {
            "type": json_type,
            "nullable": col.nullable,
        }
        if json_fmt:
            schema["format"] = json_fmt

        properties[col.name] = schema

        if not col.nullable:
            required.append(col.name)

    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": False,
    }
