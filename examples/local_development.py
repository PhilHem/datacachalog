"""Local development example using FilesystemStorage.

This example shows how to develop and test your data pipelines
locally without requiring S3 access. Use a local directory
structure that mirrors your S3 bucket layout.
"""

from pathlib import Path

from datacachalog import Catalog, Dataset, FileCache, FilesystemStorage


# Create a local directory structure mirroring S3
# In practice, this might be a shared dev fixtures directory
MOCK_S3_ROOT = Path("./test_fixtures/mock_s3")


def setup_mock_data() -> None:
    """Create mock data files for local development."""
    bucket = MOCK_S3_ROOT / "analytics-bucket"
    bucket.mkdir(parents=True, exist_ok=True)

    # Create mock data files
    (bucket / "users.json").write_text('{"users": [{"id": 1, "name": "Alice"}]}')
    (bucket / "events.csv").write_text(
        "event_id,user_id,action\n1,1,login\n2,1,purchase"
    )


def create_dev_catalog() -> Catalog:
    """Create catalog pointing to local mock S3 directory."""
    bucket = MOCK_S3_ROOT / "analytics-bucket"

    datasets = [
        Dataset(
            name="users",
            source=str(bucket / "users.json"),
            description="User profiles (mock)",
        ),
        Dataset(
            name="events",
            source=str(bucket / "events.csv"),
            description="User events (mock)",
        ),
    ]

    return Catalog(
        datasets=datasets,
        storage=FilesystemStorage(),
        cache=FileCache(Path("./data/dev_cache")),
        cache_dir=Path("./data/dev_cache"),
    )


def create_prod_catalog() -> Catalog:
    """Create catalog pointing to real S3.

    In production, swap FilesystemStorage for create_router()
    which auto-routes s3:// URIs to S3Storage.
    """
    from datacachalog import create_router

    datasets = [
        Dataset(
            name="users",
            source="s3://analytics-bucket/users.json",
            description="User profiles",
        ),
        Dataset(
            name="events",
            source="s3://analytics-bucket/events.csv",
            description="User events",
        ),
    ]

    return Catalog(
        datasets=datasets,
        storage=create_router(),  # Routes s3:// to S3Storage
        cache=FileCache(Path("./data/cache")),
        cache_dir=Path("./data/cache"),
    )


if __name__ == "__main__":
    # Setup and use mock data locally
    setup_mock_data()
    catalog = create_dev_catalog()

    # Your code works the same way in dev and prod
    users_result = catalog.fetch("users")
    assert isinstance(users_result, Path)  # Type narrowing: single-file dataset
    users_path = users_result
    events_result = catalog.fetch("events")
    assert isinstance(events_result, Path)  # Type narrowing: single-file dataset
    events_path = events_result

    print(f"Users: {users_path.read_text()}")
    print(f"Events: {events_path.read_text()}")
