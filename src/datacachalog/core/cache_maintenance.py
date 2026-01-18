"""Cache maintenance operations for Catalog.

This module contains cache maintenance operations like clean_orphaned
and cache_size that can be delegated from the Catalog class.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from datacachalog.core.glob_utils import is_glob_pattern


if TYPE_CHECKING:
    from datacachalog.core.models import Dataset
    from datacachalog.core.ports import CachePort


def clean_orphaned_keys(
    cache: CachePort,
    datasets: dict[str, Dataset],
) -> int:
    """Remove orphaned cache entries not belonging to any dataset.

    Args:
        cache: The cache port to clean.
        datasets: Dict of dataset name to Dataset.

    Returns:
        Number of orphaned cache entries removed.
    """
    all_keys = cache.list_all_keys()
    if not all_keys:
        return 0

    # Pattern for date-based versioned keys: YYYY-MM-DDTHHMMSS.ext
    versioned_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{6}\.[^.]+$")

    # Build set of valid dataset names and glob prefixes
    valid_dataset_names: set[str] = set()
    glob_prefixes: set[str] = set()

    for dataset in datasets.values():
        if is_glob_pattern(dataset.source):
            glob_prefixes.add(f"{dataset.name}/")
        else:
            valid_dataset_names.add(dataset.name)

    # Find orphaned keys
    orphaned: list[str] = []
    for key in all_keys:
        if key in valid_dataset_names:
            continue
        if any(key.startswith(prefix) for prefix in glob_prefixes):
            continue
        if versioned_pattern.match(key):
            continue
        orphaned.append(key)

    # Remove orphaned keys
    for key in orphaned:
        cache.invalidate(key)

    return len(orphaned)


def calculate_cache_size(
    name: str,
    datasets: dict[str, Dataset],
    cache: CachePort,
) -> int:
    """Calculate total cached size for a dataset in bytes.

    Args:
        name: The dataset name.
        datasets: Dict of dataset name to Dataset.
        cache: The cache port.

    Returns:
        Size in bytes. 0 if not cached.

    Raises:
        DatasetNotFoundError: If no dataset with that name exists.
    """
    from datacachalog.core.exceptions import DatasetNotFoundError

    dataset = datasets.get(name)
    if dataset is None:
        available = list(datasets.keys())
        raise DatasetNotFoundError(name, available)

    if is_glob_pattern(dataset.source):
        # Glob dataset - sum all cached files
        prefix = f"{name}/"
        keys = cache.list_all_keys()
        total = 0
        for key in keys:
            if key.startswith(prefix):
                cached = cache.get(key)
                if cached:
                    path, _ = cached
                    if path.exists():
                        total += path.stat().st_size
        return total
    # Single file
    cached = cache.get(name)
    if cached is None:
        return 0
    path, _ = cached
    if not path.exists():
        return 0
    return path.stat().st_size
