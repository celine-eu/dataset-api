# dataset/cli/ontology_fetch.py
from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Dict, Any
import httpx
import typer
import yaml

from dataset.cli.utils import setup_cli_logging

logger = logging.getLogger(__name__)
ontology_app = typer.Typer(
    name="ontology", help="Download ontology definitions with keyword filters."
)


# ---------------------------------------------------------
# Keyword filter logic
# ---------------------------------------------------------
def matches_keywords(ontology: Dict[str, Any], patterns: List[str]) -> bool:
    """
    patterns supports:
      *            → include all
      +foo         → must include foo
      -bar         → must NOT include bar
    """
    if not patterns:
        return True

    kws = set(ontology.get("keywords") or [])

    includes = []
    excludes = []

    star = False
    for p in patterns:
        if p == "*":
            star = True
        elif p.startswith("+"):
            includes.append(p[1:])
        elif p.startswith("-"):
            excludes.append(p[1:])
        else:
            includes.append(p)

    if star:
        # Star disables include-logic but exclusion still applies
        for neg in excludes:
            if neg in kws:
                return False
        return True

    # Need ALL included
    for inc in includes:
        if inc not in kws:
            return False

    # Need NONE of excluded
    for neg in excludes:
        if neg in kws:
            return False

    return True


# ---------------------------------------------------------
# File download helper
# ---------------------------------------------------------
def safe_name(name: str) -> str:
    return (
        name.lower()
        .replace(" ", "_")
        .replace("/", "_")
        .replace("(", "")
        .replace(")", "")
        .replace(",", "")
    )


def download_file(url: str) -> bytes:
    with httpx.Client(timeout=20.0, follow_redirects=True) as client:
        resp = client.get(url)
        resp.raise_for_status()
        return resp.content


# ---------------------------------------------------------
# Load ontologies YAML
# ---------------------------------------------------------
def load_ontologies(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("ontologies") or []


# ---------------------------------------------------------
# Main CLI command
# ---------------------------------------------------------
def fetch_ontologies(
    ontologies_file: Path,
    keywords: List[str],
    output_dir: Path,
    verbose: bool,
):
    """
    Download ontology 'definitions' to disk with robust error handling.
    """
    setup_cli_logging(verbose)

    if not ontologies_file.exists():
        logger.error("Input YAML does not exist: %s", ontologies_file)
        raise typer.Exit(1)

    onts = load_ontologies(ontologies_file)
    if not onts:
        logger.error("No ontologies found in YAML.")
        raise typer.Exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Loaded %d ontologies", len(onts))
    logger.info("Applying keyword filters: %s", keywords)

    filtered = [o for o in onts if matches_keywords(o, keywords or [])]

    logger.info("Selected %d ontologies after filtering", len(filtered))

    for ont in filtered:
        name = ont.get("name") or "unknown"
        defs = ont.get("definitions") or []
        subdir = output_dir / safe_name(name)
        subdir.mkdir(parents=True, exist_ok=True)

        if not defs:
            logger.warning("Ontology '%s' has no definitions. Skipping.", name)
            continue

        logger.info("Downloading %d definitions for %s", len(defs), name)

        for url in defs:
            try:
                logger.debug("Downloading %s", url)
                content = download_file(url)
            except Exception as exc:
                logger.error("Failed to download %s: %s", url, exc)
                continue

            # Determine filename
            fname = url.split("/")[-1] or "definition"
            outfile = subdir / fname

            try:
                outfile.write_bytes(content)
                logger.info("→ Saved %s", outfile)
            except Exception as exc:
                logger.error("Failed to save %s: %s", outfile, exc)
                continue

    logger.info("Ontology fetch complete.")
