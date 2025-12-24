"""Status command for CLI."""

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


@app.command()
def status(
    catalog: str | None = typer.Option(
        None,
        "--catalog",
        "-c",
        help="Show status for a specific catalog only.",
    ),
) -> None:
    """Show cache state (cached/stale/missing) per dataset."""
    cat, _root, _catalogs = load_catalog_context(catalog_name=catalog)

    # Load datasets using helper
    try:
        catalog_datasets = _load_catalog_datasets(catalog_name=catalog)
    except CatalogLoadError as e:
        typer.echo(f"Error: {e}", err=True)
        if e.recovery_hint:
            typer.echo(f"Hint: {e.recovery_hint}", err=True)
        raise typer.Exit(1) from None

    if not catalog_datasets:
        typer.echo("No datasets found. Run 'catalog init' to get started.")
        return

    # Build Rich table
    table = Table()
    table.add_column("Name")
    table.add_column("Status")

    # Add rows for each dataset
    for display_name, ds_name, _source in catalog_datasets:
        state = _get_cache_state(cat, ds_name)

        # Apply color coding to status
        colored_status = _format_status_with_color(state)
        table.add_row(display_name, colored_status)

    # Print table using Rich Console
    console = Console(force_terminal=True)
    console.print(table)
