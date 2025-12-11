# dataset/cli/ontology_analyze.py
from __future__ import annotations

import logging
import yaml
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import typer
from rdflib import Graph, URIRef
from rdflib.namespace import RDF, RDFS, OWL, DC, DCTERMS, split_uri
from xml.etree import ElementTree as ET

from dataset.cli.utils import setup_cli_logging, write_yaml_file

logger = logging.getLogger(__name__)

ontology_analyze_app = typer.Typer(
    name="ontology-analyze",
    help="Analyze ontology directory and build a cross-ontology dependency graph.",
)


# ---------------------------------------------------------------------------
# Helper: normalize namespaces
# ---------------------------------------------------------------------------


def normalize_ns(ns: str | None) -> Optional[str]:
    """
    Apply consistent namespace normalization:
      - strip trailing "/" and "#"
      - return None if empty
    """
    if ns is None:
        return None
    ns = ns.rstrip("/#")
    return ns or None


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class OntologyStats:
    namespace: str
    files: Set[str] = field(default_factory=set)
    triples: int = 0
    classes: int = 0
    properties: int = 0
    individuals: int = 0
    imports: Set[str] = field(default_factory=set)
    label: Optional[str] = None
    description: Optional[str] = None


class OntologyAnalyzer:
    """
    Accumulates ontology statistics and cross-ontology references
    while processing multiple RDF / XSD files.
    """

    CLASS_TYPES = {OWL.Class, RDFS.Class}
    PROPERTY_TYPES = {
        RDF.Property,
        OWL.ObjectProperty,
        OWL.DatatypeProperty,
        OWL.AnnotationProperty,
        OWL.OntologyProperty,
    }
    DEP_PREDICATES = {
        RDFS.subClassOf,
        RDFS.subPropertyOf,
        OWL.equivalentClass,
        OWL.equivalentProperty,
        OWL.sameAs,
        OWL.inverseOf,
        RDFS.seeAlso,
    }

    def __init__(self) -> None:
        self.namespaces: Dict[str, OntologyStats] = {}
        self.references: Dict[str, Dict[str, int]] = {}
        self.imports: Dict[str, Set[str]] = {}

    # ------------------------ Core helpers ------------------------ #

    def _ensure_ns(self, raw_ns: str | None) -> OntologyStats:
        if raw_ns is None:
            raw_ns = "unknown"
        ns = normalize_ns(raw_ns) or "unknown"
        if ns not in self.namespaces:
            self.namespaces[ns] = OntologyStats(namespace=ns)
        return self.namespaces[ns]

    @staticmethod
    def _get_namespace(node: URIRef) -> Optional[str]:
        if not isinstance(node, URIRef):
            return None
        uri_str = str(node)
        try:
            ns, _ = split_uri(node)
            return normalize_ns(str(ns))
        except Exception:
            if "#" in uri_str:
                return normalize_ns(uri_str.rsplit("#", 1)[0])
            if "/" in uri_str:
                return normalize_ns(uri_str.rsplit("/", 1)[0])
        return None

    def _register_reference(
        self, raw_src: str | None, raw_dst: str | None, weight: int = 1
    ) -> None:
        src = normalize_ns(raw_src)
        dst = normalize_ns(raw_dst)
        if not src or not dst or src == dst:
            return

        self.references.setdefault(src, {})
        self.references[src][dst] = self.references[src].get(dst, 0) + weight

    def _register_import(self, raw_src: str | None, raw_dst: str | None) -> None:
        src = normalize_ns(raw_src)
        dst = normalize_ns(raw_dst)
        if not src or not dst or src == dst:
            return

        self.imports.setdefault(src, set()).add(dst)
        self._register_reference(src, dst, weight=1)

    # ------------------------ RDF processing ------------------------ #

    def process_rdf_graph(self, file_path: Path, g: Graph) -> None:
        file_str = str(file_path)

        for s, p, o in g:
            s_ns = None

            if isinstance(s, URIRef):
                s_ns = self._get_namespace(s)
                if s_ns:
                    stats = self._ensure_ns(s_ns)
                    stats.files.add(file_str)
                    stats.triples += 1

            # Classification
            if p == RDF.type and isinstance(s, URIRef) and isinstance(o, URIRef):
                s_ns = self._get_namespace(s)
                o_ns = self._get_namespace(o)
                if s_ns:
                    stats = self._ensure_ns(s_ns)
                    if o in self.CLASS_TYPES:
                        stats.classes += 1
                    elif o in self.PROPERTY_TYPES:
                        stats.properties += 1
                    else:
                        stats.individuals += 1

            # Cross-namespace references
            self._process_dependencies_for_triple(s, p, o)

        # owl:imports
        for onto in g.subjects(RDF.type, OWL.Ontology):
            if not isinstance(onto, URIRef):
                continue
            onto_ns = self._get_namespace(onto)
            stats = self._ensure_ns(onto_ns)
            stats.files.add(file_str)

            for imported in g.objects(onto, OWL.imports):
                if isinstance(imported, URIRef):
                    imp_ns = self._get_namespace(imported)
                    self._ensure_ns(imp_ns)
                    self._register_import(onto_ns, imp_ns)

    def _process_dependencies_for_triple(self, s: Any, p: Any, o: Any) -> None:
        s_ns = self._get_namespace(s) if isinstance(s, URIRef) else None
        p_ns = self._get_namespace(p) if isinstance(p, URIRef) else None
        o_ns = self._get_namespace(o) if isinstance(o, URIRef) else None

        # Subject uses foreign property
        if s_ns and p_ns and s_ns != p_ns:
            self._register_reference(s_ns, p_ns)

        # rdf:type
        if p == RDF.type and s_ns and o_ns and s_ns != o_ns:
            self._register_reference(s_ns, o_ns)

        # subclass, equivalent, sameAs etc.
        if p in self.DEP_PREDICATES and s_ns and o_ns and s_ns != o_ns:
            self._register_reference(s_ns, o_ns)

    # ------------------------ XSD processing ------------------------ #

    def process_xsd(self, file_path: Path) -> None:
        try:
            tree = ET.parse(file_path)
        except Exception as exc:
            logger.warning("Failed to parse XSD %s: %s", file_path, exc)
            return

        root = tree.getroot()
        tns = root.attrib.get("targetNamespace")
        tns = normalize_ns(tns) or f"file://{file_path.name}"

        src_stats = self._ensure_ns(tns)
        src_stats.files.add(str(file_path))

        for elem in root.findall(".//{http://www.w3.org/2001/XMLSchema}import"):
            ns = elem.attrib.get("namespace")
            ns = normalize_ns(ns)
            if ns:
                self._ensure_ns(ns)
                self._register_import(tns, ns)

    # ------------------------ Descriptions ------------------------ #

    def attach_descriptions_from_graph(self, merged_graph: Graph) -> None:
        for onto in merged_graph.subjects(RDF.type, OWL.Ontology):
            if not isinstance(onto, URIRef):
                continue
            ns = self._get_namespace(onto)
            stats = self._ensure_ns(ns)

            labels = (
                list(merged_graph.objects(onto, RDFS.label))
                or list(merged_graph.objects(onto, DCTERMS.title))
                or list(merged_graph.objects(onto, DC.title))
            )
            if labels and not stats.label:
                stats.label = str(labels[0])

            descs = (
                list(merged_graph.objects(onto, DCTERMS.description))
                or list(merged_graph.objects(onto, RDFS.comment))
                or list(merged_graph.objects(onto, DC.description))
            )
            if descs and not stats.description:
                stats.description = str(descs[0])

    # ------------------------ Finalization / export ------------------------ #

    @staticmethod
    def _make_short_id(ns: str, fallback_index: int) -> str:
        s = ns.rstrip("/#")
        if not s:
            return f"ns{fallback_index}"
        last = s.split("/")[-1].split("#")[-1]
        return last.lower().replace("-", "_").replace(".", "_") or f"ns{fallback_index}"

    def _build_id_map(self, merged_graph: Graph) -> Dict[str, str]:
        id_map: Dict[str, str] = {}
        ns_list = sorted(self.namespaces.keys())

        # 1) Use RDF prefixes
        for prefix, ns in merged_graph.namespace_manager.namespaces():
            ns_str = normalize_ns(str(ns))
            if ns_str and ns_str in self.namespaces and ns_str not in id_map:
                id_map[ns_str] = prefix

        # 2) Fallback for remaining
        counter = 1
        for ns in ns_list:
            if ns not in id_map:
                id_map[ns] = self._make_short_id(ns, counter)
                counter += 1

        return id_map

    def build_result_document(self, merged_graph: Graph) -> Dict[str, Any]:
        self.attach_descriptions_from_graph(merged_graph)

        id_map = self._build_id_map(merged_graph)

        # incoming
        incoming = {ns: 0 for ns in self.namespaces}
        for src, targets in self.references.items():
            for dst, count in targets.items():
                if dst in incoming:
                    incoming[dst] += count

        # outgoing
        outgoing = {
            ns: sum(self.references.get(ns, {}).values()) for ns in self.namespaces
        }

        # Nodes
        nodes: List[Dict[str, Any]] = []
        for ns, stats in sorted(self.namespaces.items()):
            node_id = id_map[ns]
            nodes.append(
                {
                    "id": node_id,
                    "namespace": ns,
                    "label": stats.label or node_id,
                    "description": stats.description,
                    "triples": stats.triples,
                    "classes": stats.classes,
                    "properties": stats.properties,
                    "individuals": stats.individuals,
                    "files": sorted(stats.files),
                    "imports": sorted(self.imports.get(ns, set())),
                    "incoming_references": incoming.get(ns, 0),
                    "outgoing_references": outgoing.get(ns, 0),
                }
            )

        # Edges
        edge_map: Dict[Tuple[str, str], Dict[str, Any]] = {}

        for src_ns, targets in self.references.items():
            src_id = id_map.get(src_ns)
            if not src_id:
                continue
            for dst_ns, ref_count in targets.items():
                dst_id = id_map.get(dst_ns)
                if not dst_id:
                    continue
                key = (src_id, dst_id)
                edge = edge_map.setdefault(
                    key,
                    {
                        "source": src_id,
                        "target": dst_id,
                        "reference_count": 0,
                        "import": False,
                    },
                )
                edge["reference_count"] += ref_count

        for src_ns, dst_set in self.imports.items():
            src_id = id_map.get(src_ns)
            if not src_id:
                continue
            for dst_ns in dst_set:
                dst_id = id_map.get(dst_ns)
                if not dst_id:
                    continue
                key = (src_id, dst_id)
                edge = edge_map.setdefault(
                    key,
                    {
                        "source": src_id,
                        "target": dst_id,
                        "reference_count": 0,
                        "import": False,
                    },
                )
                edge["import"] = True

        edges = list(edge_map.values())

        # Rankings
        most_referenced = sorted(
            nodes, key=lambda n: n["incoming_references"], reverse=True
        )
        largest_consumers = sorted(
            nodes, key=lambda n: n["outgoing_references"], reverse=True
        )

        return {
            "ontologies": nodes,
            "graph": {"nodes": nodes, "edges": edges},
            "ranking": {
                "most_referenced": [
                    {
                        "id": n["id"],
                        "namespace": n["namespace"],
                        "label": n["label"],
                        "incoming_references": n["incoming_references"],
                    }
                    for n in most_referenced
                ],
                "largest_consumers": [
                    {
                        "id": n["id"],
                        "namespace": n["namespace"],
                        "label": n["label"],
                        "outgoing_references": n["outgoing_references"],
                    }
                    for n in largest_consumers
                ],
            },
        }


# ---------------------------------------------------------------------------
# File discovery / parsing
# ---------------------------------------------------------------------------


def matches_keywords(metadata: Dict[str, Any], patterns: List[str]) -> bool:
    if not patterns:
        return True

    kws = set(metadata.get("keywords") or [])
    includes, excludes = [], []
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
        return not any(e in kws for e in excludes)

    return all(i in kws for i in includes) and not any(e in excludes for e in kws)


def _load_metadata_for_file(file_path: Path) -> Optional[Dict[str, Any]]:
    meta_path = file_path.parent / "metadata.yaml"
    if not meta_path.exists():
        return None
    try:
        with meta_path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception as exc:
        logger.warning("Failed to load metadata for %s: %s", file_path, exc)
        return None


def _iter_files(input_dir: Path) -> List[Path]:
    files: List[Path] = []
    for path in input_dir.rglob("*"):
        if not path.is_file():
            continue

        if path.suffix.lower() in {
            ".ttl",
            ".rdf",
            ".owl",
            ".xml",
            ".xsd",
            ".nt",
            ".n3",
            ".trig",
            ".jsonld",
            ".rj",
        }:
            files.append(path)
            continue

        if path.suffix == "":
            files.append(path)
            continue

    return sorted(files)


def _load_rdf_file(path: Path, merged_graph: Graph) -> Optional[Graph]:
    g = Graph()
    try:
        g.parse(path)
        merged_graph += g
        logger.info("Parsed %s via auto-detection", path)
        return g
    except Exception:
        pass

    for fmt in ["turtle", "xml", "n3", "nt", "trig"]:
        try:
            g = Graph()
            g.parse(path, format=fmt)
            merged_graph += g
            logger.info("Parsed %s as %s", path, fmt)
            return g
        except Exception:
            continue

    logger.warning("Could not parse %s in any RDF format", path)
    return None


def _looks_like_xsd(path: Path) -> bool:
    if path.suffix.lower() == ".xsd":
        return True

    if path.suffix == "":
        try:
            with path.open("r", encoding="utf-8", errors="ignore") as f:
                first_kb = f.read(1024)
            return (
                "schema" in first_kb and "http://www.w3.org/2001/XMLSchema" in first_kb
            )
        except Exception:
            return False

    return False


def write_graphviz_dot(
    path: Path, nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]]
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        f.write("digraph ontologies {\n")

        for node in nodes:
            node_id = node["id"]
            label = node.get("label") or node_id
            ns = node.get("namespace", "")
            full_label = f"{label}\\n{ns}".replace('"', '\\"')
            f.write(f'  "{node_id}" [label="{full_label}"];\n')

        for edge in edges:
            src = edge["source"]
            dst = edge["target"]
            parts = []
            if edge.get("import"):
                parts.append("import")
            if edge.get("reference_count", 0):
                parts.append(f"refs={edge['reference_count']}")
            label = " / ".join(parts)
            if label:
                f.write(f'  "{src}" -> "{dst}" [label="{label}"];\n')
            else:
                f.write(f'  "{src}" -> "{dst}";\n')

        f.write("}\n")

    logger.info("Wrote Graphviz DOT to %s", path)


# ---------------------------------------------------------------------------
# CLI command
# ---------------------------------------------------------------------------


def analyze_ontologies(
    input_dir: Path,
    output_file: Path,
    graphviz_out: Optional[Path],
    keywords: List[str],
    verbose: bool,
):
    setup_cli_logging(verbose)

    if not input_dir.exists() or not input_dir.is_dir():
        logger.error(
            "Input directory does not exist or is not a directory: %s", input_dir
        )
        raise typer.Exit(1)

    files = _iter_files(input_dir)
    if not files:
        logger.error("No ontology-like files found under %s", input_dir)
        raise typer.Exit(1)

    logger.info("Found %d candidate ontology files in %s", len(files), input_dir)

    merged_graph = Graph()
    analyzer = OntologyAnalyzer()

    metadata_by_file: Dict[Path, Dict[str, Any]] = {}
    metadata_by_ns: Dict[str, Dict[str, Any]] = {}

    for f in files:
        logger.info("Processing %s", f)

        meta = _load_metadata_for_file(f)
        if meta:
            metadata_by_file[f] = meta

        if _looks_like_xsd(f):
            analyzer.process_xsd(f)
            continue

        rdf_graph = _load_rdf_file(f, merged_graph)
        if rdf_graph:
            analyzer.process_rdf_graph(f, rdf_graph)

    # Map metadata.yaml to namespaces
    for ns, stats in analyzer.namespaces.items():
        for file_path in stats.files:
            fp = Path(file_path)
            if fp in metadata_by_file:
                metadata_by_ns[ns] = metadata_by_file[fp]
                break

    result_doc = analyzer.build_result_document(merged_graph)

    # Keyword filtering
    if keywords:
        logger.info("Applying keyword filters: %s", keywords)

        def ok(node):
            ns = node["namespace"]
            meta = metadata_by_ns.get(ns) or {}
            return matches_keywords(meta, keywords)

        filtered_nodes = [n for n in result_doc["graph"]["nodes"] if ok(n)]
        keep_ids = {n["id"] for n in filtered_nodes}

        filtered_edges = [
            e
            for e in result_doc["graph"]["edges"]
            if e["source"] in keep_ids and e["target"] in keep_ids
        ]

        result_doc["graph"]["nodes"] = filtered_nodes
        result_doc["graph"]["edges"] = filtered_edges
        result_doc["ontologies"] = filtered_nodes

    write_yaml_file(output_file, result_doc)
    logger.info("Ontology analysis written to %s", output_file)

    if graphviz_out:
        g = result_doc.get("graph") or {}
        write_graphviz_dot(graphviz_out, g.get("nodes", []), g.get("edges", []))
