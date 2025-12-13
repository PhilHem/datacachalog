"""Storage backend adapters."""

from datacachalog.adapters.storage.filesystem import FilesystemStorage
from datacachalog.adapters.storage.router import RouterStorage, create_router
from datacachalog.adapters.storage.s3 import S3Storage


__all__ = ["FilesystemStorage", "RouterStorage", "S3Storage", "create_router"]
