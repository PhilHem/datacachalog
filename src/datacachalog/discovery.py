"""Catalog discovery utilities.

Discovers and loads catalog definition files from .datacachalog/catalogs/.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from datacachalog.core.exceptions import CatalogLoadError, UnsafeCatalogPathError


if TYPE_CHECKING:
    from datacachalog.core.models import Dataset


def _validate_path_containment(path: Path, catalog_root: Path) -> None:
    """Validate that path is contained within catalog_root.

    Args:
        path: The catalog file path to validate.
        catalog_root: The allowed catalog root directory.

    Raises:
        UnsafeCatalogPathError: If path is outside catalog_root.
    """
    try:
        path.resolve().relative_to(catalog_root.resolve())
    except ValueError as e:
        raise UnsafeCatalogPathError(path=path, catalog_root=catalog_root) from e


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


def load_catalog(
    path: Path, catalog_root: Path | None = None
) -> tuple[list[Dataset], str | None]:
    """Load a catalog file and extract datasets and cache_dir.

    Args:
        path: Path to the catalog Python file.
        catalog_root: Optional root directory to validate path containment.
                      If provided, raises UnsafeCatalogPathError if path is outside this directory.

    Returns:
        Tuple of (datasets list, cache_dir or None).

    Raises:
        UnsafeCatalogPathError: If catalog_root is provided and path is outside it.
    """
    if catalog_root is not None:
        _validate_path_containment(path, catalog_root)

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
    except SyntaxError as e:
        raise CatalogLoadError(
            f"Syntax error in catalog '{path.name}': {e.msg}",
            catalog_path=path,
            line=e.lineno,
            cause=e,
        ) from e
    except ImportError as e:
        raise CatalogLoadError(
            f"Import error in catalog '{path.name}': {e}",
            catalog_path=path,
            cause=e,
        ) from e
    except NameError as e:
        raise CatalogLoadError(
            f"Name error in catalog '{path.name}': {e}",
            catalog_path=path,
            cause=e,
        ) from e
    except Exception as e:
        # Catch-all for other execution errors (ValueError, TypeError, etc.)
        raise CatalogLoadError(
            f"Error loading catalog '{path.name}': {e}",
            catalog_path=path,
            cause=e,
        ) from e
    finally:
        # Clean up to avoid polluting sys.modules
        sys.modules.pop(module_name, None)

    datasets: list[Dataset] = getattr(module, "datasets", [])
    cache_dir: str | None = getattr(module, "cache_dir", None)

    return datasets, cache_dir
