# dataset/cli/utils.py
from __future__ import annotations

import logging
from typing import Any
import yaml
from pathlib import Path


def setup_cli_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def load_yaml_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"YAML file does not exist: {path}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def write_yaml_file(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True)


# ---------------------------------------------------------------------------
# Namespace resolution
# ---------------------------------------------------------------------------
def resolve_namespaces(all_namespaces: list[str], filters: list[str]) -> list[str]:
    includes = set()
    excludes = set()
    star = False

    for flt in filters:
        if flt == "*":
            star = True
        elif flt.startswith("+"):
            includes.add(flt[1:])
        elif flt.startswith("-"):
            excludes.add(flt[1:])
        else:
            includes.add(flt)

    if star:
        selected = set(all_namespaces)
    elif includes:
        selected = {ns for ns in all_namespaces if ns in includes}
    else:
        raise ValueError("No namespaces specified. Use --ns <name> or --ns '*'.")

    selected -= excludes
    return sorted(selected)
