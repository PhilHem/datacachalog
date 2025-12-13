"""Configuration utilities for datacachalog.

This module provides utilities for project configuration and path resolution.
"""

from __future__ import annotations

from pathlib import Path


def find_project_root(start: Path | None = None) -> Path:
    """Find the project root directory by walking up from start directory.

    Searches for marker files in the following priority order:
    1. .datacachalog - Explicit project marker
    2. pyproject.toml - Python project root
    3. .git - Version control root

    Args:
        start: Directory to start searching from. If None, uses current directory.

    Returns:
        Path to project root directory. Returns start directory if no markers found.

    Example:
        >>> from datacachalog.config import find_project_root
        >>> root = find_project_root()
        >>> cache_dir = root / "data"
    """
    if start is None:
        start = Path.cwd()

    markers = [".datacachalog", "pyproject.toml", ".git"]
    current = start.resolve()

    for parent in [current, *current.parents]:
        for marker in markers:
            if (parent / marker).exists():
                return parent

    return start.resolve()
