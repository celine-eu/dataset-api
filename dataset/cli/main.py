# dataset/cli/main.py
from __future__ import annotations
import typer

from dataset.cli.export_openlineage import export_app
from dataset.cli.import_catalogue import import_app
from dataset.cli.ontology.main import ontology_app

app = typer.Typer(help="Dataset command-line utilities", no_args_is_help=True)

app.add_typer(export_app, name="export")
app.add_typer(import_app, name="import")
app.add_typer(ontology_app, name="ontology")


def run():
    app()


if __name__ == "__main__":
    run()
