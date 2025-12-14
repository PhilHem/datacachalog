"""CLI commands for datacachalog."""

from pathlib import Path

import typer


app = typer.Typer(
    name="catalog",
    help="Data catalog with file-based caching for remote storage.",
    no_args_is_help=True,
)

# Default data directory structure
DEFAULT_DATA_DIRS = ["raw", "intermediate", "processed", "output"]


def _create_numbered_dirs(base: Path, names: list[str]) -> list[Path]:
    """Create numbered directories like 01_raw, 02_intermediate."""
    created = []
    for i, name in enumerate(names, 1):
        dir_path = base / f"{i:02d}_{name}"
        dir_path.mkdir(parents=True, exist_ok=True)
        created.append(dir_path)
    return created


def _create_dirs(base: Path, names: list[str]) -> list[Path]:
    """Create directories without numbering."""
    created = []
    for name in names:
        dir_path = base / name
        dir_path.mkdir(parents=True, exist_ok=True)
        created.append(dir_path)
    return created


DEFAULT_CATALOG_TEMPLATE = '''\
"""Default dataset catalog.

Define your datasets here. Each dataset specifies:
- name: Unique identifier for fetching
- source: Remote location (s3://... or local path)
- description: Optional human-readable description
"""

from datacachalog import Dataset

datasets = [
    # Example dataset - replace with your own
    # Dataset(
    #     name="customers",
    #     source="s3://my-bucket/data/customers.parquet",
    #     description="Customer master data",
    # ),
]
'''


@app.command()
def init(
    directory: str | None = typer.Argument(
        None,
        help="Directory to initialize. Defaults to current directory.",
    ),
    dirs: str | None = typer.Option(
        None,
        "--dirs",
        "-d",
        help="Comma-separated list of data subdirectories to create.",
    ),
    numbered: bool = typer.Option(
        False,
        "--numbered",
        "-n",
        help="Add numeric prefixes to directories (01_, 02_, etc.).",
    ),
    flat: bool = typer.Option(
        False,
        "--flat",
        "-f",
        help="Create only data/ with no subdirectories.",
    ),
) -> None:
    """Initialize a new datacachalog project structure."""
    target = Path(directory) if directory else Path.cwd()
    target = target.resolve()

    # Create .datacachalog/catalogs/
    catalogs_dir = target / ".datacachalog" / "catalogs"
    if not catalogs_dir.exists():
        catalogs_dir.mkdir(parents=True)
        typer.echo(f"Created {catalogs_dir.relative_to(target)}/")

    # Create default.py if it doesn't exist
    default_py = catalogs_dir / "default.py"
    if not default_py.exists():
        default_py.write_text(DEFAULT_CATALOG_TEMPLATE)
        typer.echo(f"Created {default_py.relative_to(target)}")

    # Create data directory structure
    data_dir = target / "data"
    if not data_dir.exists():
        data_dir.mkdir(parents=True)
        typer.echo(f"Created {data_dir.relative_to(target)}/")

    # Determine which subdirectories to create
    if flat:
        # No subdirectories
        pass
    elif dirs:
        # Custom directories
        dir_names = [d.strip() for d in dirs.split(",") if d.strip()]
        if numbered:
            created = _create_numbered_dirs(data_dir, dir_names)
        else:
            created = _create_dirs(data_dir, dir_names)
        for d in created:
            typer.echo(f"Created {d.relative_to(target)}/")
    else:
        # Default numbered directories
        created = _create_numbered_dirs(data_dir, DEFAULT_DATA_DIRS)
        for d in created:
            if not d.exists():
                d.mkdir(parents=True)
            typer.echo(f"Created {d.relative_to(target)}/")


@app.command()
def fetch(
    name: str = typer.Argument(..., help="Name of the dataset to fetch."),
    catalog: str | None = typer.Option(
        None,
        "--catalog",
        "-c",
        help="Fetch from a specific catalog only.",
    ),
) -> None:
    """Fetch a dataset, downloading if stale."""
    from datacachalog import Catalog, DatasetNotFoundError, RichProgressReporter
    from datacachalog.config import find_project_root
    from datacachalog.discovery import discover_catalogs, load_catalog

    root = find_project_root()
    catalogs = discover_catalogs(root)

    # Filter to specific catalog if requested
    if catalog:
        if catalog not in catalogs:
            typer.echo(f"Catalog '{catalog}' not found.")
            typer.echo(f"Available catalogs: {', '.join(sorted(catalogs.keys()))}")
            raise typer.Exit(1)
        catalogs = {catalog: catalogs[catalog]}

    # Load all datasets
    all_datasets = []
    cache_dir = "data"
    for _catalog_name, catalog_path in catalogs.items():
        datasets, cat_cache_dir = load_catalog(catalog_path)
        all_datasets.extend(datasets)
        if cat_cache_dir:
            cache_dir = cat_cache_dir

    # Create catalog and fetch
    cat = Catalog.from_directory(all_datasets, directory=root, cache_dir=cache_dir)

    try:
        with RichProgressReporter() as progress:
            path = cat.fetch(name, progress=progress)
        typer.echo(str(path))
    except DatasetNotFoundError as e:
        typer.echo(f"Dataset '{name}' not found.")
        if e.recovery_hint:
            typer.echo(f"Hint: {e.recovery_hint}")
        raise typer.Exit(1) from None


@app.command(name="list")
def list_datasets(
    catalog: str | None = typer.Option(
        None,
        "--catalog",
        "-c",
        help="Show datasets from a specific catalog only.",
    ),
) -> None:
    """List all datasets in the catalog."""
    from datacachalog.config import find_project_root
    from datacachalog.discovery import discover_catalogs, load_catalog

    root = find_project_root()
    catalogs = discover_catalogs(root)

    if not catalogs:
        typer.echo("No datasets found. Run 'catalog init' to get started.")
        return

    # Filter to specific catalog if requested
    if catalog:
        if catalog not in catalogs:
            typer.echo(f"Catalog '{catalog}' not found.")
            typer.echo(f"Available catalogs: {', '.join(sorted(catalogs.keys()))}")
            raise typer.Exit(1)
        catalogs = {catalog: catalogs[catalog]}

    # Load and display datasets
    all_datasets: list[tuple[str, str, str]] = []  # (catalog, name, source)

    for catalog_name, catalog_path in sorted(catalogs.items()):
        datasets, _ = load_catalog(catalog_path)
        for ds in datasets:
            all_datasets.append((catalog_name, ds.name, ds.source))

    if not all_datasets:
        typer.echo("No datasets found. Run 'catalog init' to get started.")
        return

    # Display datasets with catalog prefix
    for catalog_name, name, source in all_datasets:
        if len(catalogs) == 1 and catalog:
            # Single catalog mode - don't show prefix
            typer.echo(f"{name}: {source}")
        else:
            typer.echo(f"{catalog_name}/{name}: {source}")


def main() -> None:
    """Entry point for the CLI."""
    app()
