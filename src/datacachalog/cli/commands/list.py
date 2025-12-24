"""List command for CLI."""

from __future__ import annotations

import typer
from rich.console import Console
from rich.table import Table

from datacachalog.cli.formatting import (
    _format_status_with_color,
    _get_cache_state,
    _load_catalog_datasets,
)
from datacachalog.cli.main import app, load_catalog_context
from datacachalog.core.exceptions import CatalogLoadError


@app.command(name="list")
def list_datasets(
    catalog: str | None = typer.Option(
        None,
        "--catalog",
        "-c",
        help="Show datasets from a specific catalog only.",
    ),
    status: bool = typer.Option(
        False,
        "--status",
        help="Show cache state (fresh/stale/missing).",
    ),
) -> None:
    """List all datasets in the catalog."""
    cat, _root, _catalogs = load_catalog_context(catalog_name=catalog)

    # Load datasets using helper
    try:
        all_datasets = _load_catalog_datasets(catalog_name=catalog)
    except CatalogLoadError as e:
        typer.echo(f"Error: {e}", err=True)
        if e.recovery_hint:
            typer.echo(f"Hint: {e.recovery_hint}", err=True)
        raise typer.Exit(1) from None

    if not all_datasets:
        typer.echo("No datasets found. Run 'catalog init' to get started.")
        return

    # Build Rich table
    table = Table()
    table.add_column("Name")
    if status:
        table.add_column("Source")
        table.add_column("Status")
    else:
        table.add_column("Source")

    # Add rows for each dataset
    for display_name, ds_name, source in all_datasets:
        # Check cache state if --status flag is set
        if status:
            state = _get_cache_state(cat, ds_name)
            colored_status = _format_status_with_color(state)
            table.add_row(display_name, source, colored_status)
        else:
            table.add_row(display_name, source)

    # Print table using Rich Console
    # Force terminal output to ensure tables render correctly in all environments
    console = Console(force_terminal=True)
    console.print(table)
