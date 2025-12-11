from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Any, List, Optional

import typer
from rdflib import Graph, Node, URIRef
from rdflib.namespace import RDF, RDFS, OWL, DCTERMS

from dataset.cli.utils import setup_cli_logging, write_yaml_file

logger = logging.getLogger(__name__)

SUPPORTED_SUFFIXES = {
    ".ttl",
    ".rdf",
    ".owl",
    ".xml",
    ".nt",
    ".n3",
    ".trig",
    ".jsonld",
    ".rj",
}


def scan_folders(base: Path, patterns: Optional[List[str]]) -> List[Path]:
    """Return subdirectories matching substring filters."""
    if not base.exists() or not base.is_dir():
        logger.error("Ontology base directory does not exist: %s", base)
        raise typer.Exit(1)

    dirs = [d for d in base.iterdir() if d.is_dir()]

    if not patterns:
        return dirs

    matched: List[Path] = []
    for d in dirs:
        name = d.name.lower()
        if any(p.lower() in name for p in patterns):
            matched.append(d)

    return matched


def load_graph(files: List[Path]) -> Graph:
    g = Graph()

    for f in files:
        tried = False

        # Prefer safe formats
        for fmt in ["turtle", "nt", "n3", "trig", "json-ld"]:
            try:
                g.parse(f, format=fmt)
                logger.debug("Parsed %s as %s", f, fmt)
                tried = True
                break
            except Exception:
                continue

        if tried:
            continue

        # LAST RESORT: RDF/XML (generates weird warnings)
        try:
            g.parse(f, format="xml")
            logger.debug("Parsed %s as xml", f)
        except Exception:
            logger.debug("Could not parse %s in any known format", f)

    return g


def get_label(graph: Graph, node: Node) -> Optional[str]:
    if not isinstance(node, URIRef):
        return None

    for p in (RDFS.label, DCTERMS.title):
        val = next(graph.objects(node, p), None)
        if val:
            return str(val)
    return None


def get_comment(graph: Graph, node: Node) -> Optional[str]:
    if not isinstance(node, URIRef):
        return None

    for p in (RDFS.comment, DCTERMS.description):
        val = next(graph.objects(node, p), None)
        if val:
            return str(val)
    return None


def extract_tree(graph: Graph) -> Dict[str, Any]:
    """Extract classes, properties, individuals from an RDF graph."""
    classes: Dict[str, Any] = {}
    properties: Dict[str, Any] = {}
    individuals: Dict[str, Any] = {}

    # ---------------- CLASSES ----------------
    for cls in graph.subjects(RDF.type, OWL.Class):
        uri = str(cls)
        classes[uri] = {
            "label": get_label(graph, cls),
            "comment": get_comment(graph, cls),
            "parents": [str(o) for o in graph.objects(cls, RDFS.subClassOf)],
        }

    # ---------------- PROPERTIES ----------------
    prop_types = {
        RDF.Property,
        OWL.ObjectProperty,
        OWL.DatatypeProperty,
        OWL.AnnotationProperty,
    }

    for ptype in prop_types:
        for p in graph.subjects(RDF.type, ptype):
            uri = str(p)
            properties[uri] = {
                "label": get_label(graph, p),
                "comment": get_comment(graph, p),
                "domain": [str(o) for o in graph.objects(p, RDFS.domain)],
                "range": [str(o) for o in graph.objects(p, RDFS.range)],
            }

    # ---------------- INDIVIDUALS ----------------
    for s, _, o in graph.triples((None, RDF.type, None)):
        if isinstance(s, URIRef):
            uri = str(s)
            # skip classes & properties
            if uri not in classes and uri not in properties:
                individuals[uri] = {
                    "types": [str(t) for t in graph.objects(s, RDF.type)]
                }

    return {
        "classes": classes,
        "properties": properties,
        "individuals": individuals,
    }


def generate_ontology_tree(
    ontologies_dir: Path,
    folder_filters: Optional[List[str]],
    verbose: bool,
) -> None:
    """Generate one ontology-tree.yaml per ontology folder."""
    setup_cli_logging(verbose)
    logger.debug("Starting ontology tree generation...")

    try:
        folders = scan_folders(ontologies_dir, folder_filters or [])
    except Exception:
        raise typer.Exit(1)

    if not folders:
        logger.error("No ontology folders matched filters: %s", folder_filters)
        raise typer.Exit(1)

    for folder in folders:
        logger.debug("Processing ontology folder: %s", folder)

        try:
            files = [
                f
                for f in folder.iterdir()
                if f.is_file() and f.suffix.lower() in SUPPORTED_SUFFIXES
            ]
        except Exception as exc:
            logger.error("Could not list files in %s: %s", folder, exc)
            continue

        if not files:
            logger.debug("No ontology files found in %s", folder)
            continue

        try:
            graph = load_graph(files)
        except Exception as exc:
            logger.error("Failed to load RDF from %s: %s", folder, exc)
            continue

        try:
            tree = extract_tree(graph)
        except Exception as exc:
            logger.error("Failed to extract tree for %s: %s", folder, exc)
            continue

        # Write inside the ontology folder itself
        output_file = folder / "ontology-tree.yaml"

        try:
            write_yaml_file(output_file, tree)
            logger.debug("Wrote ontology tree → %s", output_file)
        except Exception as exc:
            logger.error("Failed to write YAML to %s: %s", output_file, exc)

    logger.debug("Ontology tree generation complete.")
