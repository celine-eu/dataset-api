import typer
from celine.dataset.cli.export_postgres import export_postgres_cmd
from celine.dataset.cli.export_openlineage import export_openlineage_cmd


export_app = typer.Typer(
    name="export", help="Export datasets into YAML for catalogue import"
)

export_app.command("openlineage")(export_openlineage_cmd)
export_app.command("postgres")(export_postgres_cmd)
