# dataset/cli/ontology/main.py
from __future__ import annotations
import typer

from dataset.cli.ontology.fetch import fetch_ontologies
from dataset.cli.ontology.analyze import analyze_ontologies

from pathlib import Path
from typing import List, Optional
import typer

ontology_app = typer.Typer(
    name="ontology",
    help="Ontology utilities: download, analyze, inspect.",
)


@ontology_app.command("fetch")
def cmd_fetch_ontologies(
    ontologies_file: Path = typer.Option(
        Path(__file__).resolve().parent.parent.parent
        / "ontologies"
        / "open-repository.yaml",
        "--input",
        "-i",
        help="YAML list of ontologies.",
    ),
    keywords: List[str] = typer.Option(
        None,
        "--keywords",
        "-k",
        help="Keyword filters (+foo -bar *). Use multiple times.",
    ),
    output_dir: Path = typer.Option(
        Path("./data/ontologies"),
        "--output",
        "-o",
        help="Base directory for downloaded definitions.",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
):
    fetch_ontologies(
        ontologies_file=ontologies_file,
        keywords=keywords,
        output_dir=output_dir,
        verbose=verbose,
    )


@ontology_app.command("analyze")
def cmd_analyze_ontologies(
    input_dir: Path = typer.Option(
        Path("./data/ontologies"),
        "--input",
        "-i",
        help="Directory containing downloaded ontology files (TTL/OWL/RDF/XML/XSD).",
    ),
    output_file: Path = typer.Option(
        Path("./data/ontologies/ontology-graph.yaml"),
        "--output",
        "-o",
        help="YAML file where the analysis result will be written.",
    ),
    graphviz_out: Optional[Path] = typer.Option(
        None,
        "--graphviz",
        help="Optional Graphviz DOT output for visualization.",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
):
    analyze_ontologies(
        input_dir=input_dir,
        output_file=output_file,
        graphviz_out=graphviz_out,
        verbose=verbose,
    )
