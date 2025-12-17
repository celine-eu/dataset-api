from __future__ import annotations
import typer
from pathlib import Path
from typing import List, Optional

from celine.dataset.cli.ontology.fetch import fetch_ontologies
from celine.dataset.cli.ontology.analyze import analyze_ontologies
from celine.dataset.cli.ontology.tree import generate_ontology_tree

ontology_app = typer.Typer(
    name="ontology", help="Ontology utilities: download, analyze, tree view."
)


@ontology_app.command("fetch")
def cmd_fetch_ontologies(
    ontologies_file: Path = typer.Option(
        Path(__file__).resolve().parent.parent.parent.parent
        / "ontologies"
        / "open-repository.yaml",
        "--input",
        "-i",
    ),
    keywords: List[str] = typer.Option(None, "--keywords", "-k"),
    output_dir: Path = typer.Option(Path("./data/ontologies"), "--output", "-o"),
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
    input_dir: Path = typer.Option(Path("./data/ontologies"), "--input", "-i"),
    output_file: Path = typer.Option(
        Path("./data/ontologies/ontology-graph.yaml"), "--output", "-o"
    ),
    graphviz_out: Optional[Path] = typer.Option(
        Path("./data/ontologies/ontology-graph.dot"), "--graphviz"
    ),
    keywords: List[str] = typer.Option(None, "--keywords", "-k"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
):
    analyze_ontologies(
        input_dir=input_dir,
        output_file=output_file,
        graphviz_out=graphviz_out,
        keywords=keywords,
        verbose=verbose,
    )


@ontology_app.command("tree")
def cmd_ontology_tree(
    ontologies_dir: Path = typer.Option(Path("./data/ontologies"), "--input", "-i"),
    folder: Optional[List[str]] = typer.Option(
        None, "--folder", "-f", help="Fuzzy folder-name filter (multiple allowed)"
    ),
    output: Path = typer.Option(
        Path("./data/ontologies/ontology-tree.yaml"), "--output", "-o"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
):
    generate_ontology_tree(
        ontologies_dir=ontologies_dir,
        folder_filters=folder,
        verbose=verbose,
    )
