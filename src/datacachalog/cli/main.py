"""CLI commands for datacachalog."""

from datetime import datetime
from pathlib import Path

import typer

from datacachalog.core.exceptions import CatalogLoadError


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
    name: str | None = typer.Argument(None, help="Name of the dataset to fetch."),
    all_datasets: bool = typer.Option(
        False,
        "--all",
        "-a",
        help="Fetch all datasets.",
    ),
    catalog: str | None = typer.Option(
        None,
        "--catalog",
        "-c",
        help="Fetch from a specific catalog only.",
    ),
    as_of: datetime | None = typer.Option(
        None,
        "--as-of",
        help="Fetch version at this time (ISO format: 2024-12-10 or 2024-12-10T09:30:00).",
        formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"],
    ),
    version_id: str | None = typer.Option(
        None,
        "--version-id",
        help="Fetch specific version by ID (from 'catalog versions').",
    ),
) -> None:
    """Fetch a dataset, downloading if stale."""
    from datacachalog import Catalog, DatasetNotFoundError, RichProgressReporter
    from datacachalog.config import find_project_root
    from datacachalog.core.exceptions import VersionNotFoundError
    from datacachalog.discovery import discover_catalogs, load_catalog

    # Validate arguments
    if not name and not all_datasets:
        typer.echo("Error: Either provide a dataset name or use --all.")
        raise typer.Exit(1)

    if name and all_datasets:
        typer.echo("Error: Cannot use both a dataset name and --all.")
        raise typer.Exit(1)

    if all_datasets and (as_of or version_id):
        typer.echo("Error: --as-of and --version-id cannot be used with --all.")
        raise typer.Exit(1)

    if as_of and version_id:
        typer.echo("Error: --as-of and --version-id are mutually exclusive.")
        raise typer.Exit(1)

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
    all_ds = []
    cache_dir = "data"
    for _catalog_name, catalog_path in catalogs.items():
        try:
            datasets, cat_cache_dir = load_catalog(catalog_path)
        except CatalogLoadError as e:
            typer.echo(f"Error: {e}", err=True)
            if e.recovery_hint:
                typer.echo(f"Hint: {e.recovery_hint}", err=True)
            raise typer.Exit(1) from None
        all_ds.extend(datasets)
        if cat_cache_dir:
            cache_dir = cat_cache_dir

    # Create catalog and fetch
    cat = Catalog.from_directory(all_ds, directory=root, cache_dir=cache_dir)

    # Make as_of timezone-aware (UTC) if provided
    # Typer parses date strings as naive datetimes, but S3 versions are timezone-aware
    if as_of is not None and as_of.tzinfo is None:
        from datetime import UTC

        as_of = as_of.replace(tzinfo=UTC)

    try:
        with RichProgressReporter() as progress:
            if all_datasets:
                paths = cat.fetch_all(progress=progress)
                for ds_name, result in paths.items():
                    if isinstance(result, list):
                        # Glob dataset - list all paths
                        for p in result:
                            typer.echo(f"{ds_name}: {p}")
                    else:
                        typer.echo(f"{ds_name}: {result}")
            else:
                assert name is not None  # Validated above
                result = cat.fetch(
                    name, progress=progress, as_of=as_of, version_id=version_id
                )
                if isinstance(result, list):
                    # Glob dataset - print each path on its own line
                    for path in result:
                        typer.echo(str(path))
                else:
                    typer.echo(str(result))
    except DatasetNotFoundError as e:
        typer.echo(f"Dataset '{name}' not found.")
        if e.recovery_hint:
            typer.echo(f"Hint: {e.recovery_hint}")
        raise typer.Exit(1) from None
    except VersionNotFoundError as e:
        typer.echo(f"Error: {e}")
        if e.recovery_hint:
            typer.echo(f"Hint: {e.recovery_hint}")
        raise typer.Exit(1) from None
    except ValueError as e:
        typer.echo(f"Error: {e}")
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
        try:
            datasets, _ = load_catalog(catalog_path)
        except CatalogLoadError as e:
            typer.echo(f"Error: {e}", err=True)
            if e.recovery_hint:
                typer.echo(f"Hint: {e.recovery_hint}", err=True)
            raise typer.Exit(1) from None
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
    from datacachalog import Catalog
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

    # Load datasets per catalog
    catalog_datasets: list[tuple[str, str, str]] = []  # (catalog_name, ds_name, source)
    all_ds = []
    cache_dir = "data"

    for catalog_name, catalog_path in sorted(catalogs.items()):
        try:
            datasets, cat_cache_dir = load_catalog(catalog_path)
        except CatalogLoadError as e:
            typer.echo(f"Error: {e}", err=True)
            if e.recovery_hint:
                typer.echo(f"Hint: {e.recovery_hint}", err=True)
            raise typer.Exit(1) from None
        for ds in datasets:
            catalog_datasets.append((catalog_name, ds.name, ds.source))
            all_ds.append(ds)
        if cat_cache_dir:
            cache_dir = cat_cache_dir

    if not catalog_datasets:
        typer.echo("No datasets found. Run 'catalog init' to get started.")
        return

    # Create catalog to check status
    cat = Catalog.from_directory(all_ds, directory=root, cache_dir=cache_dir)

    # Check status for each dataset
    for catalog_name, ds_name, _source in catalog_datasets:
        # Check if cached
        cached = cat._cache.get(ds_name)
        if cached is None:
            state = "missing"
        elif cat.is_stale(ds_name):
            state = "stale"
        else:
            state = "fresh"

        # Format output
        if len(catalogs) == 1 and catalog:
            typer.echo(f"{ds_name}: {state}")
        else:
            typer.echo(f"{catalog_name}/{ds_name}: {state}")


@app.command()
def invalidate(
    name: str = typer.Argument(help="Name of the dataset to invalidate."),
) -> None:
    """Remove dataset from cache, forcing re-download on next fetch."""
    from datacachalog import Catalog, DatasetNotFoundError
    from datacachalog.config import find_project_root
    from datacachalog.discovery import discover_catalogs, load_catalog

    root = find_project_root()
    catalogs = discover_catalogs(root)

    if not catalogs:
        typer.echo("No catalogs found. Run 'catalog init' to get started.")
        raise typer.Exit(1)

    # Load all datasets
    all_ds = []
    cache_dir = "data"
    for _catalog_name, catalog_path in catalogs.items():
        try:
            datasets, cat_cache_dir = load_catalog(catalog_path)
        except CatalogLoadError as e:
            typer.echo(f"Error: {e}", err=True)
            if e.recovery_hint:
                typer.echo(f"Hint: {e.recovery_hint}", err=True)
            raise typer.Exit(1) from None
        all_ds.extend(datasets)
        if cat_cache_dir:
            cache_dir = cat_cache_dir

    cat = Catalog.from_directory(all_ds, directory=root, cache_dir=cache_dir)

    try:
        cat.get_dataset(name)  # Validate exists
        cat.invalidate(name)
        typer.echo(f"Invalidated '{name}'. Next fetch will re-download.")
    except DatasetNotFoundError as e:
        typer.echo(f"Dataset '{name}' not found.")
        if e.recovery_hint:
            typer.echo(f"Hint: {e.recovery_hint}")
        raise typer.Exit(1) from None


@app.command("invalidate-glob")
def invalidate_glob(
    name: str = typer.Argument(help="Name of the glob dataset to invalidate."),
) -> None:
    """Remove all cached files for a glob pattern dataset."""
    from datacachalog import Catalog, DatasetNotFoundError
    from datacachalog.config import find_project_root
    from datacachalog.discovery import discover_catalogs, load_catalog

    root = find_project_root()
    catalogs = discover_catalogs(root)

    if not catalogs:
        typer.echo("No catalogs found. Run 'catalog init' to get started.")
        raise typer.Exit(1)

    # Load all datasets
    all_ds = []
    cache_dir = "data"
    for _catalog_name, catalog_path in catalogs.items():
        try:
            datasets, cat_cache_dir = load_catalog(catalog_path)
        except CatalogLoadError as e:
            typer.echo(f"Error: {e}", err=True)
            if e.recovery_hint:
                typer.echo(f"Hint: {e.recovery_hint}", err=True)
            raise typer.Exit(1) from None
        all_ds.extend(datasets)
        if cat_cache_dir:
            cache_dir = cat_cache_dir

    cat = Catalog.from_directory(all_ds, directory=root, cache_dir=cache_dir)

    try:
        count = cat.invalidate_glob(name)
        typer.echo(f"Invalidated '{name}': {count} cached file(s) removed.")
    except DatasetNotFoundError as e:
        typer.echo(f"Dataset '{name}' not found.")
        if e.recovery_hint:
            typer.echo(f"Hint: {e.recovery_hint}")
        raise typer.Exit(1) from None
    except ValueError as e:
        typer.echo(f"Error: {e}")
        raise typer.Exit(1) from None


@app.command()
def versions(
    name: str = typer.Argument(help="Name of the dataset to list versions for."),
    limit: int = typer.Option(
        10,
        "--limit",
        "-n",
        help="Maximum number of versions to show.",
    ),
) -> None:
    """List available versions for a dataset.

    Shows version history with timestamps. Requires S3 with versioning enabled.
    """
    from datacachalog import Catalog, DatasetNotFoundError
    from datacachalog.config import find_project_root
    from datacachalog.core.exceptions import VersioningNotSupportedError
    from datacachalog.discovery import discover_catalogs, load_catalog

    root = find_project_root()
    catalogs = discover_catalogs(root)

    if not catalogs:
        typer.echo("No catalogs found. Run 'catalog init' to get started.")
        raise typer.Exit(1)

    # Load all datasets
    all_ds = []
    cache_dir = "data"
    for _catalog_name, catalog_path in catalogs.items():
        try:
            datasets, cat_cache_dir = load_catalog(catalog_path)
        except CatalogLoadError as e:
            typer.echo(f"Error: {e}", err=True)
            if e.recovery_hint:
                typer.echo(f"Hint: {e.recovery_hint}", err=True)
            raise typer.Exit(1) from None
        all_ds.extend(datasets)
        if cat_cache_dir:
            cache_dir = cat_cache_dir

    cat = Catalog.from_directory(all_ds, directory=root, cache_dir=cache_dir)

    try:
        version_list = cat.versions(name, limit=limit)

        if not version_list:
            typer.echo(f"No versions found for '{name}'.")
            return

        typer.echo(f"Versions for '{name}' (newest first):\n")
        for v in version_list:
            # Format: timestamp | size | flags (version_id hidden)
            flags = []
            if v.is_latest:
                flags.append("latest")
            if v.is_delete_marker:
                flags.append("deleted")
            flag_str = f" [{', '.join(flags)}]" if flags else ""

            size_str = f"{v.size:,} bytes" if v.size else "unknown size"
            ts_str = v.last_modified.strftime("%Y-%m-%d %H:%M:%S")

            typer.echo(f"  {ts_str}  {size_str}{flag_str}")

    except DatasetNotFoundError as e:
        typer.echo(f"Dataset '{name}' not found.")
        if e.recovery_hint:
            typer.echo(f"Hint: {e.recovery_hint}")
        raise typer.Exit(1) from None
    except VersioningNotSupportedError as e:
        typer.echo(f"Error: {e}")
        if e.recovery_hint:
            typer.echo(f"Hint: {e.recovery_hint}")
        raise typer.Exit(1) from None


def main() -> None:
    """Entry point for the CLI."""
    app()
