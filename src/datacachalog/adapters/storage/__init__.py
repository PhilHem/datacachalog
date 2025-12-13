"""Storage backend adapters."""

from datacachalog.adapters.storage.filesystem import FilesystemStorage
from datacachalog.adapters.storage.s3 import S3Storage


__all__ = ["FilesystemStorage", "S3Storage"]
