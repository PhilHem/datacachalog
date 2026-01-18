"""Path resolution utilities for Catalog.

This module contains path resolution helpers that can be delegated
from the Catalog class.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path


def resolve_version_cache_key(source: str, last_modified: datetime) -> str:
    """Generate a date-based cache key for a versioned file.

    Format: YYYY-MM-DDTHHMMSS.ext where ext comes from the original filename.

    Args:
        source: The dataset source URI.
        last_modified: The version's last_modified timestamp.

    Returns:
        Cache key in format YYYY-MM-DDTHHMMSS.ext
    """
    # Extract extension from source filename
    if "://" in source:
        path_part = source.split("://", 1)[1]
        filename = path_part.split("/", 1)[1] if "/" in path_part else path_part
    else:
        filename = Path(source).name

    # Get extension (including the dot)
    ext = Path(filename).suffix

    # Format datetime as YYYY-MM-DDTHHMMSS (no colons, no timezone)
    # Ensure UTC timezone for consistent formatting
    if last_modified.tzinfo is None:
        dt = last_modified.replace(tzinfo=UTC)
    else:
        dt = last_modified.astimezone(UTC)

    date_str = dt.strftime("%Y-%m-%dT%H%M%S")

    return f"{date_str}{ext}"
