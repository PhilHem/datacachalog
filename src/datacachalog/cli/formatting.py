"""Shared formatting helpers for CLI commands."""

from __future__ import annotations

from typing import TYPE_CHECKING

from rich.text import Text

from datacachalog.core.formatting import status_to_color


if TYPE_CHECKING:
    from datacachalog import Catalog


def _format_status_with_color(status: str) -> Text:
    """Format status string with color coding.

    Args:
        status: Status string ("fresh", "stale", or "missing")

    Returns:
        Rich Text object with appropriate color:
        - "fresh" -> green
        - "stale" -> yellow
        - "missing" -> red
    """
    color = status_to_color(status)
    return Text(status, style=color) if color else Text(status)


def _load_catalog_datasets(
    catalog_name: str | None = None,
) -> list[tuple[str, str, str]]:
    """Load datasets from catalogs and return formatted list.

    Args:
        catalog_name: Optional catalog name to filter by.

    Returns:
        List of tuples (display_name, ds_name, source) for each dataset.

    Raises:
        CatalogLoadError: If catalog file cannot be loaded.
    """
    from datacachalog.config import find_project_root
    from datacachalog.discovery import discover_catalogs, load_catalog

    root = find_project_root()
    catalogs = discover_catalogs(root)

    if not catalogs:
        return []

    # Filter to specific catalog if requested
    if catalog_name:
        if catalog_name not in catalogs:
            return []
        catalogs = {catalog_name: catalogs[catalog_name]}

    # Load datasets per catalog to track catalog names
    catalog_datasets: list[tuple[str, str, str]] = []  # (catalog_name, ds_name, source)
    catalog_root = root / ".datacachalog" / "catalogs"

    for catalog_name_item, catalog_path in sorted(catalogs.items()):
        datasets, _ = load_catalog(catalog_path, catalog_root=catalog_root)
        # CatalogLoadError will propagate up if load_catalog fails
        for ds in datasets:
            catalog_datasets.append((catalog_name_item, ds.name, ds.source))

    # Format display names based on catalog context
    result: list[tuple[str, str, str]] = []
    for catalog_name_item, ds_name, source in catalog_datasets:
        # Format name with catalog prefix if needed
        # When there's only one catalog and a filter was specified, don't show prefix
        if len(catalogs) == 1 and catalog_name:
            # Single catalog mode with filter - don't show prefix
            display_name = ds_name
        elif len(catalogs) == 1:
            # Single catalog without filter - also don't show prefix (simpler UX)
            display_name = ds_name
        else:
            # Multiple catalogs - show prefix
            display_name = f"{catalog_name_item}/{ds_name}"
        result.append((display_name, ds_name, source))

    return result


def _get_cache_state(catalog: Catalog, dataset_name: str) -> str:
    """Get cache state for a dataset (fresh/stale/missing)."""
    cached = catalog._cache.get(dataset_name)
    if cached is None:
        return "missing"
    if catalog.is_stale(dataset_name):
        return "stale"
    return "fresh"
