# dataset/cli/ontology_analyze.py
from __future__ import annotations

import logging
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
        # namespace IRI -> OntologyStats
        self.namespaces: Dict[str, OntologyStats] = {}
        # ns_from -> (ns_to -> count)
        self.references: Dict[str, Dict[str, int]] = {}
        # ns_from -> set(ns_to) (explicit owl:imports or XSD imports)
        self.imports: Dict[str, Set[str]] = {}

    # ------------------------ Core helpers ------------------------ #

    def _ensure_ns(self, ns: str) -> OntologyStats:
        if ns not in self.namespaces:
            self.namespaces[ns] = OntologyStats(namespace=ns)
        return self.namespaces[ns]

    @staticmethod
    def _get_namespace(node: URIRef) -> Optional[str]:
        """
        Extract namespace from a URIRef using rdflib's split_uri.
        Returns None if no sensible split can be found.
        """
        if not isinstance(node, URIRef):
            return None
        uri_str = str(node)
        try:
            ns, _ = split_uri(node)
            return str(ns)
        except Exception:
            # Fallback: crude split on last '#' or '/'
            if "#" in uri_str:
                return uri_str.rsplit("#", 1)[0] + "#"
            if "/" in uri_str:
                return uri_str.rsplit("/", 1)[0] + "/"
        return None

    def _register_reference(self, src_ns: str, dst_ns: str, weight: int = 1) -> None:
        if src_ns == dst_ns:
            return
        if src_ns not in self.references:
            self.references[src_ns] = {}
        self.references[src_ns][dst_ns] = (
            self.references[src_ns].get(dst_ns, 0) + weight
        )

    def _register_import(self, src_ns: str, dst_ns: str) -> None:
        if src_ns == dst_ns:
            return
        self.imports.setdefault(src_ns, set()).add(dst_ns)
        # Also treat import as a semantic reference
        self._register_reference(src_ns, dst_ns, weight=1)

    # ------------------------ RDF processing ------------------------ #

    def process_rdf_graph(self, file_path: Path, g: Graph) -> None:
        """
        Process triples from a single RDF graph and aggregate statistics.
        """
        file_str = str(file_path)

        for s, p, o in g:
            # Count total triples per namespace of the subject
            if isinstance(s, URIRef):
                s_ns = self._get_namespace(s)
                if s_ns:
                    stats = self._ensure_ns(s_ns)
                    stats.files.add(file_str)
                    stats.triples += 1

            # Class / property / individual classification
            if p == RDF.type and isinstance(s, URIRef) and isinstance(o, URIRef):
                s_ns = self._get_namespace(s)
                if not s_ns:
                    continue
                stats = self._ensure_ns(s_ns)
                if o in self.CLASS_TYPES:
                    stats.classes += 1
                elif o in self.PROPERTY_TYPES:
                    stats.properties += 1
                else:
                    stats.individuals += 1

            # Cross-namespace references
            self._process_dependencies_for_triple(s, p, o)

        # Handle owl:Ontology + owl:imports
        for onto in g.subjects(RDF.type, OWL.Ontology):
            if not isinstance(onto, URIRef):
                continue
            onto_ns = self._get_namespace(onto) or str(onto)
            self._ensure_ns(onto_ns).files.add(file_str)

            for imported in g.objects(onto, OWL.imports):
                if not isinstance(imported, URIRef):
                    continue
                imp_ns = self._get_namespace(imported) or str(imported)
                self._ensure_ns(imp_ns)
                self._register_import(onto_ns, imp_ns)

    def _process_dependencies_for_triple(self, s: Any, p: Any, o: Any) -> None:
        """
        Detect cross-namespace dependencies based on triple patterns:
        - rdf:type (A uses class B)
        - subclass / equivalent / sameAs / seeAlso
        - foreign property usage: subject in nsA, predicate in nsB, nsA != nsB
        """
        s_ns = self._get_namespace(s) if isinstance(s, URIRef) else None
        p_ns = self._get_namespace(p) if isinstance(p, URIRef) else None
        o_ns = self._get_namespace(o) if isinstance(o, URIRef) else None

        # Foreing property usage: subject ns uses predicate ns
        if s_ns and p_ns and s_ns != p_ns:
            self._register_reference(s_ns, p_ns)

        if isinstance(o, URIRef):
            # rdf:type: subject ns depends on object ns
            if p == RDF.type and s_ns and o_ns and s_ns != o_ns:
                self._register_reference(s_ns, o_ns)

            # subclass/equivalence/etc: subject ns depends on object ns
            if p in self.DEP_PREDICATES and s_ns and o_ns and s_ns != o_ns:
                self._register_reference(s_ns, o_ns)

    # ------------------------ XSD processing ------------------------ #

    def process_xsd(self, file_path: Path) -> None:
        """
        Very lightweight XSD handling: read targetNamespace and xs:import namespaces
        and register them as dependencies.
        """
        try:
            tree = ET.parse(file_path)
        except Exception as exc:
            logger.warning("Failed to parse XSD %s: %s", file_path, exc)
            return

        root = tree.getroot()
        tns = root.attrib.get("targetNamespace")
        if not tns:
            # If no targetNamespace, treat file as its own pseudo-namespace
            tns = f"file://{file_path.name}"
        src_stats = self._ensure_ns(tns)
        src_stats.files.add(str(file_path))

        xs_ns = "http://www.w3.org/2001/XMLSchema"
        imports = set()
        for elem in root.findall(".//{http://www.w3.org/2001/XMLSchema}import"):
            ns = elem.attrib.get("namespace")
            if ns:
                imports.add(ns)

        for ns in imports:
            self._ensure_ns(ns)
            self._register_import(tns, ns)

    # ------------------------ Descriptions ------------------------ #

    def attach_descriptions_from_graph(self, merged_graph: Graph) -> None:
        """
        Use owl:Ontology nodes (and their labels/comments) to enrich OntologyStats.
        """
        for onto in merged_graph.subjects(RDF.type, OWL.Ontology):
            if not isinstance(onto, URIRef):
                continue
            ns = self._get_namespace(onto) or str(onto)
            stats = self._ensure_ns(ns)

            # Label candidates
            labels = (
                list(merged_graph.objects(onto, RDFS.label))
                or list(merged_graph.objects(onto, DCTERMS.title))
                or list(merged_graph.objects(onto, DC.title))
            )
            if labels and not stats.label:
                stats.label = str(labels[0])

            # Description candidates
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
        """
        Derive a compact identifier for an ontology namespace, used as node id.

        Priority:
        - last non-empty path segment
        - 'ns{index}' fallback
        """
        s = ns.rstrip("/#")
        if not s:
            return f"ns{fallback_index}"
        last = s.split("/")[-1].split("#")[-1]
        if not last:
            return f"ns{fallback_index}"
        return last.lower().replace("-", "_").replace(".", "_")

    def _build_id_map(self, merged_graph: Graph) -> Dict[str, str]:
        """
        Build mapping: namespace IRI -> short id, using rdflib prefixes if possible.
        """
        id_map: Dict[str, str] = {}
        ns_list = sorted(self.namespaces.keys())

        # 1) Use existing prefixes if they match
        for prefix, ns in merged_graph.namespace_manager.namespaces():
            ns_str = str(ns)
            if ns_str in self.namespaces and ns_str not in id_map:
                id_map[ns_str] = prefix

        # 2) Fallback for remaining
        counter = 1
        for ns in ns_list:
            if ns in id_map:
                continue
            id_map[ns] = self._make_short_id(ns, counter)
            counter += 1

        return id_map

    def build_result_document(self, merged_graph: Graph) -> Dict[str, Any]:
        """
        Build a comprehensive structure suitable for YAML/JSON and downstream
        visualization / ontology selection for dataset mappers.
        """
        # First, attach descriptions & labels
        self.attach_descriptions_from_graph(merged_graph)

        # id map for graph nodes
        id_map = self._build_id_map(merged_graph)

        # Compute incoming reference counts
        incoming: Dict[str, int] = {ns: 0 for ns in self.namespaces}
        for src_ns, targets in self.references.items():
            for dst_ns, count in targets.items():
                if dst_ns in incoming:
                    incoming[dst_ns] += count

        # Compute outgoing reference counts
        outgoing: Dict[str, int] = {
            ns: sum(self.references.get(ns, {}).values()) for ns in self.namespaces
        }

        # Build node list
        nodes: List[Dict[str, Any]] = []
        for ns, stats in sorted(self.namespaces.items(), key=lambda x: x[0]):
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

        # Build edge list
        edge_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for src_ns, targets in self.references.items():
            for dst_ns, ref_count in targets.items():
                src_id = id_map.get(src_ns)
                dst_id = id_map.get(dst_ns)
                if not src_id or not dst_id:
                    continue
                key = (src_id, dst_id)
                edge_entry = edge_map.setdefault(
                    key,
                    {
                        "source": src_id,
                        "target": dst_id,
                        "reference_count": 0,
                        "import": False,
                    },
                )
                edge_entry["reference_count"] += ref_count

        for src_ns, imported_set in self.imports.items():
            for dst_ns in imported_set:
                src_id = id_map.get(src_ns)
                dst_id = id_map.get(dst_ns)
                if not src_id or not dst_id:
                    continue
                key = (src_id, dst_id)
                edge_entry = edge_map.setdefault(
                    key,
                    {
                        "source": src_id,
                        "target": dst_id,
                        "reference_count": 0,
                        "import": False,
                    },
                )
                edge_entry["import"] = True

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
            "graph": {
                "nodes": nodes,
                "edges": edges,
            },
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
# File discovery & parsing
# ---------------------------------------------------------------------------


def _iter_files(input_dir: Path) -> List[Path]:
    """Return a list of files under input_dir (recursively) that look like ontology-ish files."""
    exts = {".ttl", ".rdf", ".owl", ".xml", ".xsd", ".nt", ".n3", ".trig"}
    files: List[Path] = []
    for path in input_dir.rglob("*"):
        if path.is_file():
            if path.suffix.lower() in exts or path.suffix == "":
                files.append(path)
    return sorted(files)


def _load_rdf_file(path: Path, merged_graph: Graph) -> Optional[Graph]:
    """
    Try multiple RDF formats until one succeeds. On success, returns a
    per-file Graph which has also been merged into merged_graph.
    """
    formats = ["turtle", "xml", "application/rdf+xml", "n3", "trig", None]
    last_error: Optional[Exception] = None

    for fmt in formats:
        g = Graph()
        try:
            g.parse(path, format=fmt)
            logger.info("Parsed %s as %s", path, fmt or "auto")
            merged_graph += g
            return g
        except Exception as exc:  # pragma: no cover - defensive
            last_error = exc
            continue

    logger.warning("Failed to parse RDF from %s: %s", path, last_error)
    return None


def _looks_like_xsd(path: Path) -> bool:
    if path.suffix.lower() == ".xsd":
        return True
    # crude heuristic for extension-less files
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
    """
    Export a simple directed graph as Graphviz DOT for visualization.
    - Nodes: id, label
    - Edges: source -> target with label (type + count)
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        f.write("digraph ontologies {\n")

        # Nodes
        for node in nodes:
            node_id = node["id"]
            label = node.get("label") or node_id
            ns = node.get("namespace", "")
            # show namespace in tooltip-ish way
            full_label = f"{label}\\n{ns}"
            safe_label = full_label.replace('"', '\\"')
            f.write(f'  "{node_id}" [label="{safe_label}"];\n')

        # Edges
        for edge in edges:
            src = edge["source"]
            dst = edge["target"]
            ref_count = edge.get("reference_count", 0)
            is_import = edge.get("import", False)

            label_parts = []
            if is_import:
                label_parts.append("import")
            if ref_count:
                label_parts.append(f"refs={ref_count}")
            label_str = " / ".join(label_parts)

            if label_str:
                f.write(f'  "{src}" -> "{dst}" [label="{label_str}"];\n')
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
    verbose: bool,
):
    """
    Analyze ontology directory and build a comprehensive dependency graph.

    The output includes, for each ontology/namespace:
    - files, triples, classes, properties, individuals
    - imports (owl:imports, XSD imports)
    - incoming/outgoing reference counts
    - label/description (where available)

    This can be used to:
    - visualize the ontology ecosystem
    - decide which ontology to use for a dataset mapper
    - inspect which ontology is most reused or most dependent on others
    """
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

    for f in files:
        logger.info("Processing %s", f)
        # XSD handling
        if _looks_like_xsd(f):
            analyzer.process_xsd(f)
            continue

        # RDF/OWL handling
        rdf_graph = _load_rdf_file(f, merged_graph)
        if rdf_graph is None:
            continue
        analyzer.process_rdf_graph(f, rdf_graph)

    # Build final result document
    result_doc = analyzer.build_result_document(merged_graph)

    # Write YAML output
    write_yaml_file(output_file, result_doc)
    logger.info("Ontology analysis written to %s", output_file)

    # Optionally write Graphviz DOT
    if graphviz_out is not None:
        graph = result_doc.get("graph") or {}
        nodes = graph.get("nodes") or []
        edges = graph.get("edges") or []
        write_graphviz_dot(graphviz_out, nodes, edges)
