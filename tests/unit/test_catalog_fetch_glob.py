"""Unit tests for Catalog glob pattern fetch operations."""

from pathlib import Path

import pytest

from datacachalog import Dataset


@pytest.mark.tra("UseCase.Fetch")
@pytest.mark.tier(1)
class TestFetchGlob:
    """Tests for glob pattern support in fetch()."""

    @pytest.fixture(autouse=True)
    def _isolate_test(self, tmp_path: Path) -> None:
        """Ensure test isolation - each test gets fresh tmp_path."""
        # tmp_path provides per-test isolation via pytest
        # This fixture explicitly signals isolation intent
        pass

    def test_fetch_glob_returns_list_of_paths(self, tmp_path: Path) -> None:
        """fetch() with glob pattern should return list[Path]."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup: create multiple files
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "data_2024_01.parquet").write_text("jan")
        (storage_dir / "data_2024_02.parquet").write_text("feb")
        (storage_dir / "data_2024_03.parquet").write_text("mar")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        # Dataset with glob pattern
        dataset = Dataset(
            name="monthly_data",
            source=str(storage_dir / "*.parquet"),
        )
        catalog = Catalog(
            datasets=[dataset],
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        # Act
        result = catalog.fetch("monthly_data")

        # Assert
        assert isinstance(result, list)
        assert len(result) == 3
        assert all(isinstance(p, Path) for p in result)

    def test_fetch_glob_downloads_all_matching_files(self, tmp_path: Path) -> None:
        """fetch() should download all files matching the glob pattern."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "a.parquet").write_text("content a")
        (storage_dir / "b.parquet").write_text("content b")
        (storage_dir / "c.csv").write_text("not matched")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(name="data", source=str(storage_dir / "*.parquet"))
        catalog = Catalog(
            datasets=[dataset],
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        # Act
        result = catalog.fetch("data")
        assert isinstance(result, list)  # Type narrowing for glob
        paths = result

        # Assert: only .parquet files matched
        assert len(paths) == 2
        contents = {p.read_text() for p in paths}
        assert contents == {"content a", "content b"}

    def test_fetch_glob_caches_each_file_separately(self, tmp_path: Path) -> None:
        """Each file matched by glob should have its own cache entry."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "file1.txt").write_text("one")
        (storage_dir / "file2.txt").write_text("two")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(name="files", source=str(storage_dir / "*.txt"))
        catalog = Catalog(
            datasets=[dataset],
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        # Act
        catalog.fetch("files")

        # Assert: each file has its own cache entry
        assert cache.get("files/file1.txt") is not None
        assert cache.get("files/file2.txt") is not None

    def test_fetch_glob_empty_match_raises_error(self, tmp_path: Path) -> None:
        """fetch() should raise EmptyGlobMatchError when pattern matches nothing."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.exceptions import EmptyGlobMatchError
        from datacachalog.core.services import Catalog

        # Setup: empty directory
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(name="data", source=str(storage_dir / "*.parquet"))
        catalog = Catalog(
            datasets=[dataset],
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        # Act & Assert
        with pytest.raises(EmptyGlobMatchError) as exc_info:
            catalog.fetch("data")

        assert "*.parquet" in str(exc_info.value)
        assert exc_info.value.recovery_hint is not None

    def test_fetch_non_glob_still_returns_single_path(self, tmp_path: Path) -> None:
        """fetch() without glob should return single Path (backward compatible)."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "single.csv").write_text("content")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(
            name="single",
            source=str(storage_dir / "single.csv"),
            cache_path=cache_dir / "single.csv",
        )
        catalog = Catalog(
            datasets=[dataset],
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        # Act
        result = catalog.fetch("single")

        # Assert: single Path, not list
        assert isinstance(result, Path)
        assert result.read_text() == "content"

    def test_fetch_glob_checks_staleness_per_file(self, tmp_path: Path) -> None:
        """Each file in glob should have independent staleness checking."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        file1 = storage_dir / "file1.txt"
        file2 = storage_dir / "file2.txt"
        file1.write_text("original 1")
        file2.write_text("original 2")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(name="files", source=str(storage_dir / "*.txt"))
        catalog = Catalog(
            datasets=[dataset],
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        # First fetch
        catalog.fetch("files")

        # Modify only file2
        file2.write_text("updated 2")

        # Second fetch
        result = catalog.fetch("files")
        assert isinstance(result, list)  # Type narrowing for glob
        paths = result

        # Assert: file2 was re-downloaded with new content
        contents = {p.read_text() for p in paths}
        assert contents == {"original 1", "updated 2"}

    def test_fetch_with_dry_run_returns_cached_path_if_fresh(
        self, tmp_path: Path
    ) -> None:
        """fetch(dry_run=True) should return cached path when cache is fresh."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup: create "remote" file
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "data.csv"
        remote_file.write_text("id,name\n1,Alice\n")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(
            name="customers",
            source=str(remote_file),
            cache_path=cache_dir / "customers.csv",
        )
        catalog = Catalog(datasets=[dataset], storage=storage, cache=cache)

        # First fetch populates cache
        result1 = catalog.fetch("customers")
        assert isinstance(result1, Path)
        path1 = result1

        # Dry-run fetch should return cached path (fresh)
        result2 = catalog.fetch("customers", dry_run=True)
        assert isinstance(result2, Path)
        path2 = result2

        assert path2 == path1, "Dry-run should return cached path when fresh"

    def test_fetch_with_dry_run_checks_staleness_but_does_not_download(
        self, tmp_path: Path
    ) -> None:
        """fetch(dry_run=True) should check staleness but skip download and cache update."""

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup: create "remote" file
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "data.csv"
        remote_file.write_text("id,name\n1,Alice\n")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(
            name="customers",
            source=str(remote_file),
            cache_path=cache_dir / "customers.csv",
        )
        catalog = Catalog(datasets=[dataset], storage=storage, cache=cache)

        # First fetch populates cache
        result1 = catalog.fetch("customers")
        assert isinstance(result1, Path)
        path1 = result1
        original_content = path1.read_text()

        # Get cache metadata before modification
        cached_before = cache.get("customers")
        assert cached_before is not None

        # Modify remote file (changes ETag via content hash)
        remote_file.write_text("id,name\n1,Alice\n2,Bob\n")

        # Dry-run fetch should check staleness but not download
        result2 = catalog.fetch("customers", dry_run=True)
        assert isinstance(result2, Path)
        path2 = result2

        # Cache should be unchanged (still has old content)
        assert path2.read_text() == original_content, (
            "Cache should not be updated in dry-run"
        )
        cached_after = cache.get("customers")
        assert cached_before == cached_after, (
            "Cache metadata should not change in dry-run"
        )

    def test_fetch_all_with_dry_run(self, tmp_path: Path) -> None:
        """fetch_all(dry_run=True) should check all datasets without downloading."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup: create "remote" files
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "customers.csv").write_text("id,name\n1,Alice\n")
        (storage_dir / "orders.csv").write_text("id,amount\n1,100\n")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        customers = Dataset(
            name="customers",
            source=str(storage_dir / "customers.csv"),
            cache_path=cache_dir / "customers.csv",
        )
        orders = Dataset(
            name="orders",
            source=str(storage_dir / "orders.csv"),
            cache_path=cache_dir / "orders.csv",
        )
        catalog = Catalog(datasets=[customers, orders], storage=storage, cache=cache)

        # First fetch_all populates cache
        catalog.fetch_all()

        # Get cache state before dry-run
        cached_customers_before = cache.get("customers")
        cached_orders_before = cache.get("orders")

        # Dry-run fetch_all
        results = catalog.fetch_all(dry_run=True)

        assert isinstance(results, dict)
        assert "customers" in results
        assert "orders" in results

        # Cache should be unchanged
        cached_customers_after = cache.get("customers")
        cached_orders_after = cache.get("orders")
        assert cached_customers_before == cached_customers_after
        assert cached_orders_before == cached_orders_after

    def test_fetch_glob_with_dry_run(self, tmp_path: Path) -> None:
        """fetch(dry_run=True) for glob dataset should check staleness without downloading."""
        import os
        import time

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        file1 = storage_dir / "file1.txt"
        file2 = storage_dir / "file2.txt"
        file1.write_text("original 1")
        file2.write_text("original 2")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(name="files", source=str(storage_dir / "*.txt"))
        catalog = Catalog(
            datasets=[dataset],
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        # First fetch populates cache
        catalog.fetch("files")

        # Get cache state before modification (check that entries exist)
        cache_entry1_before = cache.get("files/file1.txt")
        cache_entry2_before = cache.get("files/file2.txt")
        assert cache_entry1_before is not None
        assert cache_entry2_before is not None

        # Modify one file (use os.utime instead of sleep for determinism)
        file2.write_text("updated 2")
        future_time = time.time() + 10
        os.utime(file2, (future_time, future_time))

        # Dry-run fetch
        result = catalog.fetch("files", dry_run=True)
        assert isinstance(result, list)

        # Cache should be unchanged (same metadata)
        cache_entry1_after = cache.get("files/file1.txt")
        cache_entry2_after = cache.get("files/file2.txt")
        assert cache_entry1_before == cache_entry1_after, (
            "Cache should not be modified in dry-run"
        )
        assert cache_entry2_before == cache_entry2_after, (
            "Cache should not be modified in dry-run"
        )

    @pytest.mark.property
    @pytest.mark.timeout(10.0)  # Property-based tests may take longer
    def test_fetch_dry_run_cache_immutability_property(self, tmp_path: Path) -> None:
        """Property: Multiple fetch(dry_run=True) calls never modify cache state."""
        import os
        import time
        import uuid

        from hypothesis import given, settings
        from hypothesis.strategies import binary, integers

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        @settings(database=None)  # Disable example persistence for isolation
        @given(
            content=binary(),
            num_calls=integers(min_value=1, max_value=10),
            has_cache=integers(min_value=0, max_value=2),  # 0=missing, 1=fresh, 2=stale
        )
        def _test_cache_immutability(
            content: bytes, num_calls: int, has_cache: int
        ) -> None:
            # Setup: create fresh directories for each hypothesis run
            run_id = uuid.uuid4().hex[:8]
            storage_dir = tmp_path / f"storage_{run_id}"
            storage_dir.mkdir()
            remote_file = storage_dir / "data.bin"
            remote_file.write_bytes(content)

            cache_dir = tmp_path / f"cache_{run_id}"
            storage = FilesystemStorage()
            cache = FileCache(cache_dir=cache_dir)

            dataset = Dataset(
                name="test_data",
                source=str(remote_file),
                cache_path=cache_dir / "test_data.bin",
            )
            catalog = Catalog(datasets=[dataset], storage=storage, cache=cache)

            # Setup cache state based on has_cache parameter
            if has_cache > 0:  # 1=fresh, 2=stale
                # Populate cache with initial fetch
                catalog.fetch("test_data")

                if has_cache == 2:  # stale cache
                    # Modify remote file to make cache stale (use os.utime for determinism)
                    remote_file.write_bytes(content + b"_modified")
                    future_time = time.time() + 10
                    os.utime(remote_file, (future_time, future_time))

            # Capture initial cache state (may be None if no cache)
            cached_initial = cache.get("test_data")
            if cached_initial is not None:
                cached_path_initial, cached_meta_initial = cached_initial
                file_content_initial = cached_path_initial.read_bytes()
            else:
                cached_path_initial = None
                cached_meta_initial = None
                file_content_initial = None

            # Perform multiple dry-run calls
            for _ in range(num_calls):
                result = catalog.fetch("test_data", dry_run=True)
                assert isinstance(result, Path)

            # Verify cache state unchanged after all dry-run calls
            cached_after = cache.get("test_data")

            if cached_initial is None:
                # No cache before - should still be no cache after
                assert cached_after is None, "Cache should not be created in dry-run"
            else:
                # Cache existed before - verify unchanged
                assert cached_after is not None, (
                    "Cache should not be removed in dry-run"
                )
                cached_path_after, cached_meta_after = cached_after

                # Verify metadata unchanged
                assert cached_meta_initial == cached_meta_after, (
                    "Cache metadata should not change in dry-run"
                )

                # Verify file contents unchanged
                file_content_after = cached_path_after.read_bytes()
                assert file_content_initial == file_content_after, (
                    "Cache file contents should not change in dry-run"
                )

                # Verify path unchanged
                assert cached_path_initial == cached_path_after, (
                    "Cache path should not change in dry-run"
                )

        _test_cache_immutability()

    @pytest.mark.property
    @pytest.mark.tier(1)
    def test_fetch_dry_run_never_modifies_cache_property(self, tmp_path: Path) -> None:
        """Property: Multiple fetch(dry_run=True) calls never modify cache state (metadata, file contents, file count)."""
        import uuid

        from hypothesis import given, settings
        from hypothesis.strategies import binary, integers, text

        from datacachalog import Dataset
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        @settings(database=None)  # Disable example persistence for isolation
        @given(
            content=binary(),
            num_calls=integers(min_value=1, max_value=20),
            dataset_name=text(
                min_size=1,
                max_size=20,
                alphabet="abcdefghijklmnopqrstuvwxyz0123456789_",
            ),
        )
        def _test_dry_run_never_modifies_cache(
            content: bytes, num_calls: int, dataset_name: str
        ) -> None:
            # Setup: create fresh directories for each hypothesis run
            run_id = uuid.uuid4().hex[:8]
            storage_dir = tmp_path / f"storage_{run_id}"
            storage_dir.mkdir()
            remote_file = storage_dir / "data.bin"
            remote_file.write_bytes(content)

            cache_dir = tmp_path / f"cache_{run_id}"
            storage = FilesystemStorage()
            cache = FileCache(cache_dir=cache_dir)

            dataset = Dataset(
                name=dataset_name,
                source=str(remote_file),
                cache_path=cache_dir / f"{dataset_name}.bin",
            )
            catalog = Catalog(datasets=[dataset], storage=storage, cache=cache)

            # Populate cache first
            catalog.fetch(dataset_name)

            # Capture initial cache state
            cached_before = cache.get(dataset_name)
            assert cached_before is not None
            cached_path_before, cached_meta_before = cached_before
            file_content_before = cached_path_before.read_bytes()

            # Count cache files before (data + metadata)
            cache_files_before = len(list(cache_dir.rglob("*")))
            cache_data_files_before = len(
                [
                    f
                    for f in cache_dir.rglob("*")
                    if f.is_file() and not f.name.endswith(".meta.json")
                ]
            )
            cache_meta_files_before = len(list(cache_dir.rglob("*.meta.json")))

            # Perform multiple dry-run calls
            for _ in range(num_calls):
                result = catalog.fetch(dataset_name, dry_run=True)
                assert isinstance(result, Path)

            # Verify cache state unchanged after all dry-run calls
            cached_after = cache.get(dataset_name)
            assert cached_after is not None, "Cache should not be removed in dry-run"

            cached_path_after, cached_meta_after = cached_after

            # Verify metadata unchanged
            assert cached_meta_before == cached_meta_after, (
                "Cache metadata should not change in dry-run"
            )

            # Verify file contents unchanged
            file_content_after = cached_path_after.read_bytes()
            assert file_content_before == file_content_after, (
                "Cache file contents should not change in dry-run"
            )

            # Verify path unchanged
            assert cached_path_before == cached_path_after, (
                "Cache path should not change in dry-run"
            )

            # Verify file count unchanged
            cache_files_after = len(list(cache_dir.rglob("*")))
            cache_data_files_after = len(
                [
                    f
                    for f in cache_dir.rglob("*")
                    if f.is_file() and not f.name.endswith(".meta.json")
                ]
            )
            cache_meta_files_after = len(list(cache_dir.rglob("*.meta.json")))

            assert cache_files_before == cache_files_after, (
                f"Cache file count should not change in dry-run (before: {cache_files_before}, after: {cache_files_after})"
            )
            assert cache_data_files_before == cache_data_files_after, (
                f"Cache data file count should not change in dry-run (before: {cache_data_files_before}, after: {cache_data_files_after})"
            )
            assert cache_meta_files_before == cache_meta_files_after, (
                f"Cache metadata file count should not change in dry-run (before: {cache_meta_files_before}, after: {cache_meta_files_after})"
            )

        _test_dry_run_never_modifies_cache()
