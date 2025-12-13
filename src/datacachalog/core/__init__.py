"""Core domain module for datacachalog.

This module contains pure Python domain models and port definitions.
It has no I/O dependencies and can be tested in isolation.
"""

from datacachalog.core.models import CacheMetadata, Dataset, FileMetadata


__all__ = [
    "CacheMetadata",
    "Dataset",
    "FileMetadata",
]
