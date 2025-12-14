"""Pure utility functions for glob pattern handling.

These functions contain no I/O and are safe to use in the core domain.
"""

from __future__ import annotations


# Characters that indicate a glob pattern
_GLOB_METACHARACTERS = frozenset("*?[")


def is_glob_pattern(source: str) -> bool:
    """Check if a source string contains glob metacharacters.

    Args:
        source: A URI or path that may contain glob patterns.

    Returns:
        True if source contains *, ?, or [ characters.
    """
    return any(char in source for char in _GLOB_METACHARACTERS)


def split_glob_pattern(source: str) -> tuple[str, str]:
    """Split a glob source into prefix and pattern.

    Splits at the last / before the first glob metacharacter.

    Args:
        source: A URI or path containing glob pattern (e.g., "s3://bucket/data/*.parquet").

    Returns:
        Tuple of (prefix, pattern) where:
        - prefix: The path up to and including the last / before glob chars
        - pattern: The glob pattern portion

    Raises:
        ValueError: If source doesn't contain glob metacharacters.

    Examples:
        >>> split_glob_pattern("s3://bucket/data/*.parquet")
        ("s3://bucket/data/", "*.parquet")
        >>> split_glob_pattern("s3://bucket/data/**/*.parquet")
        ("s3://bucket/data/", "**/*.parquet")
    """
    if not is_glob_pattern(source):
        raise ValueError(f"Source is not a glob pattern: {source}")

    # Find the first glob metacharacter
    first_glob_pos = len(source)
    for char in _GLOB_METACHARACTERS:
        pos = source.find(char)
        if pos != -1 and pos < first_glob_pos:
            first_glob_pos = pos

    # Find the last / before the glob metacharacter
    last_slash = source.rfind("/", 0, first_glob_pos)

    if last_slash == -1:
        # No slash before glob - entire thing is pattern
        return "", source

    # Split at the last slash
    prefix = source[: last_slash + 1]  # Include the trailing /
    pattern = source[last_slash + 1 :]

    return prefix, pattern


def derive_cache_key(dataset_name: str, prefix: str, matched_uri: str) -> str:
    """Derive a cache key from dataset name and matched file URI.

    Creates a hierarchical cache key that preserves the directory structure
    relative to the prefix.

    Args:
        dataset_name: The name of the dataset (used as top-level key).
        prefix: The prefix that was used to list files.
        matched_uri: A full URI that matched the glob pattern.

    Returns:
        Cache key in format "{dataset_name}/{relative_path}".

    Examples:
        >>> derive_cache_key("logs", "s3://bucket/logs/", "s3://bucket/logs/2024/jan.parquet")
        "logs/2024/jan.parquet"
    """
    # Normalize prefix to have trailing slash
    normalized_prefix = prefix.rstrip("/") + "/" if prefix else ""

    # Extract relative path by removing prefix
    if matched_uri.startswith(normalized_prefix):
        relative_path = matched_uri[len(normalized_prefix) :]
    elif matched_uri.startswith(prefix):
        # Handle case where prefix doesn't have trailing slash
        relative_path = matched_uri[len(prefix) :].lstrip("/")
    else:
        # Fallback: use just the filename
        relative_path = matched_uri.rsplit("/", 1)[-1]

    return f"{dataset_name}/{relative_path}"
