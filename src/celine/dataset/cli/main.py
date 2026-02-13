# dataset/cli/main.py
from __future__ import annotations
import typer

from celine.dataset.cli.export import export_app
from celine.dataset.cli.import_catalogue import import_app

app = typer.Typer(help="Dataset command-line utilities", no_args_is_help=True)

app.add_typer(export_app, name="export")
app.add_typer(import_app, name="import")


def run():
    app()


if __name__ == "__main__":
    run()
