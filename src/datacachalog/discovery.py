"""Catalog discovery utilities.

Discovers and loads catalog definition files from .datacachalog/catalogs/.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from datacachalog.core.models import Dataset


def discover_catalogs(root: Path) -> dict[str, Path]:
    """Find all catalog files under .datacachalog/catalogs/.

    Args:
        root: Project root directory to search from.

    Returns:
        Dict mapping catalog names to their file paths.
        Names are derived from filenames (e.g., 'core.py' -> 'core').
    """
    catalog_dir = root / ".datacachalog" / "catalogs"
    if not catalog_dir.exists():
        return {}

    return {p.stem: p for p in catalog_dir.glob("*.py") if not p.name.startswith("_")}


def load_catalog(path: Path) -> tuple[list[Dataset], str | None]:
    """Load a catalog file and extract datasets and cache_dir.

    Args:
        path: Path to the catalog Python file.

    Returns:
        Tuple of (datasets list, cache_dir or None).
    """
    # Generate a unique module name to avoid conflicts
    module_name = f"_datacachalog_catalog_{path.stem}_{id(path)}"

    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        msg = f"Could not load catalog from {path}"
        raise ImportError(msg)

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module

    try:
        spec.loader.exec_module(module)
    finally:
        # Clean up to avoid polluting sys.modules
        sys.modules.pop(module_name, None)

    datasets: list[Dataset] = getattr(module, "datasets", [])
    cache_dir: str | None = getattr(module, "cache_dir", None)

    return datasets, cache_dir
