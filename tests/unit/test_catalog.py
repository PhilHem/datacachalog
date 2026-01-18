"""Unit tests for Catalog service."""

from pathlib import Path

import pytest

from datacachalog import Dataset


@pytest.mark.core
@pytest.mark.tra("UseCase.CatalogInit")
@pytest.mark.tier(1)
class TestCatalogInit:
    """Tests for Catalog instantiation."""

    def test_catalog_accepts_datasets_storage_cache(self, tmp_path: Path) -> None:
        """Catalog should accept datasets, storage, and cache adapters."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        storage = FilesystemStorage()
        cache = FileCache(cache_dir=tmp_path / "cache")
        dataset = Dataset(name="test", source=str(tmp_path / "file.txt"))

        catalog = Catalog(datasets=[dataset], storage=storage, cache=cache)

        assert catalog is not None


@pytest.mark.core
@pytest.mark.tra("UseCase.GetDataset")
@pytest.mark.tier(1)
class TestGetDataset:
    """Tests for dataset lookup."""

    def test_get_dataset_returns_dataset_by_name(self, tmp_path: Path) -> None:
        """get_dataset() should return the dataset with matching name."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        storage = FilesystemStorage()
        cache = FileCache(cache_dir=tmp_path / "cache")
        customers = Dataset(name="customers", source="/data/customers.csv")

        catalog = Catalog(datasets=[customers], storage=storage, cache=cache)

        assert catalog.get_dataset("customers") == customers

    def test_get_dataset_raises_dataset_not_found_error(self, tmp_path: Path) -> None:
        """get_dataset() should raise DatasetNotFoundError for unknown names."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.exceptions import DatasetNotFoundError
        from datacachalog.core.services import Catalog

        storage = FilesystemStorage()
        cache = FileCache(cache_dir=tmp_path / "cache")

        catalog = Catalog(datasets=[], storage=storage, cache=cache)

        with pytest.raises(DatasetNotFoundError, match="unknown"):
            catalog.get_dataset("unknown")

    def test_get_dataset_error_includes_available_datasets(
        self, tmp_path: Path
    ) -> None:
        """DatasetNotFoundError should list available dataset names."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.exceptions import DatasetNotFoundError
        from datacachalog.core.services import Catalog

        storage = FilesystemStorage()
        cache = FileCache(cache_dir=tmp_path / "cache")
        ds1 = Dataset(name="alpha", source="/a.csv")
        ds2 = Dataset(name="beta", source="/b.csv")

        catalog = Catalog(datasets=[ds1, ds2], storage=storage, cache=cache)

        with pytest.raises(DatasetNotFoundError) as exc_info:
            catalog.get_dataset("missing")

        assert "alpha" in exc_info.value.recovery_hint
        assert "beta" in exc_info.value.recovery_hint


@pytest.mark.core
@pytest.mark.tra("UseCase.Fetch")
@pytest.mark.tier(1)
class TestFetchMissingCacheDir:
    """Tests for fetch() when cache configuration is missing."""

    def test_fetch_raises_configuration_error_without_cache_path_or_dir(
        self, tmp_path: Path
    ) -> None:
        """fetch() should raise ConfigurationError if neither cache_path nor cache_dir."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.exceptions import ConfigurationError
        from datacachalog.core.services import Catalog

        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "data.csv"
        remote_file.write_text("data")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        # Dataset without explicit cache_path, catalog without cache_dir
        dataset = Dataset(name="customers", source=str(remote_file))
        catalog = Catalog(datasets=[dataset], storage=storage, cache=cache)
        # Note: no cache_dir passed to Catalog

        with pytest.raises(ConfigurationError, match="cache"):
            catalog.fetch("customers")


@pytest.mark.core
@pytest.mark.tra("UseCase.Fetch")
@pytest.mark.tier(1)
class TestFetch:
    """Tests for fetch() method."""

    def test_fetch_downloads_on_cache_miss(self, tmp_path: Path) -> None:
        """fetch() should download file when cache is empty."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup: create a "remote" file
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

        # Act
        result = catalog.fetch("customers")
        assert isinstance(result, Path)  # Type narrowing
        path = result

        # Assert
        assert path.exists()
        assert path.read_text() == "id,name\n1,Alice\n"

    def test_fetch_returns_cached_when_fresh(self, tmp_path: Path) -> None:
        """fetch() should return cached path when not stale."""
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
        assert isinstance(result1, Path)  # Type narrowing
        path1 = result1

        # Modify cached file to detect if re-downloaded
        path1.write_text("MODIFIED")

        # Second fetch (remote unchanged) should return cached
        result2 = catalog.fetch("customers")
        assert isinstance(result2, Path)  # Type narrowing
        path2 = result2

        assert path2 == path1
        assert path2.read_text() == "MODIFIED"  # Proves no re-download

    def test_fetch_redownloads_when_stale(self, tmp_path: Path) -> None:
        """fetch() should re-download when remote has changed."""
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
        assert isinstance(result1, Path)  # Type narrowing
        path1 = result1
        original_content = path1.read_text()

        # Modify remote file (changes ETag)
        remote_file.write_text("id,name\n1,Alice\n2,Bob\n")

        # Second fetch should detect stale and re-download
        result2 = catalog.fetch("customers")
        assert isinstance(result2, Path)  # Type narrowing
        path2 = result2

        assert path2.read_text() == "id,name\n1,Alice\n2,Bob\n"
        assert path2.read_text() != original_content

    def test_fetch_derives_cache_path_from_source(self, tmp_path: Path) -> None:
        """fetch() should derive cache path from source when not explicit."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup: create "remote" file
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "data.parquet"
        remote_file.write_text("parquet data")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        # Dataset without explicit cache_path
        dataset = Dataset(
            name="data",
            source=str(remote_file),
        )
        catalog = Catalog(
            datasets=[dataset],
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        result = catalog.fetch("data")
        assert isinstance(result, Path)  # Type narrowing
        path = result

        # Should derive path from source filename
        assert path.exists()
        assert path.read_text() == "parquet data"

    def test_fetch_uses_explicit_cache_path(self, tmp_path: Path) -> None:
        """fetch() should use explicit cache_path when provided."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "source.csv"
        remote_file.write_text("data")

        cache_dir = tmp_path / "cache"
        custom_path = cache_dir / "custom" / "location.csv"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(
            name="test",
            source=str(remote_file),
            cache_path=custom_path,
        )
        catalog = Catalog(
            datasets=[dataset],
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        catalog.fetch("test")

        # The download should go to the explicit cache_path location
        assert custom_path.exists()
        assert custom_path.read_text() == "data"


@pytest.mark.core
@pytest.mark.tra("Catalog.DatasetsProperty")
@pytest.mark.tier(1)
class TestDatasetsProperty:
    """Tests for the catalog.datasets property."""

    def test_datasets_returns_all_registered_datasets(self, tmp_path: Path) -> None:
        """datasets property should return all registered datasets."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Arrange
        ds1 = Dataset(name="alpha", source=str(tmp_path / "a.csv"))
        ds2 = Dataset(name="beta", source=str(tmp_path / "b.csv"))
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=tmp_path / "cache")
        catalog = Catalog(datasets=[ds1, ds2], storage=storage, cache=cache)

        # Act
        result = catalog.datasets

        # Assert
        assert len(result) == 2
        assert {d.name for d in result} == {"alpha", "beta"}

    def test_datasets_returns_empty_list_when_no_datasets(self, tmp_path: Path) -> None:
        """datasets property should return empty list when catalog has no datasets."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Arrange
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=tmp_path / "cache")
        catalog = Catalog(datasets=[], storage=storage, cache=cache)

        # Act
        result = catalog.datasets

        # Assert
        assert result == []


@pytest.mark.core
@pytest.mark.tra("UseCase.CatalogInit")
@pytest.mark.tier(1)
class TestCatalogFromDirectory:
    """Tests for Catalog.from_directory() factory method."""

    def test_from_directory_discovers_project_root(self, tmp_path: Path) -> None:
        """from_directory() should discover project root from marker files."""
        from datacachalog.core.services import Catalog

        # Create project structure with .git marker
        (tmp_path / ".git").mkdir()
        source_file = tmp_path / "source.txt"
        source_file.write_text("content")

        dataset = Dataset(name="test", source=str(source_file))
        catalog = Catalog.from_directory([dataset], directory=tmp_path)

        # Default cache_dir should be {root}/data
        assert catalog._cache_dir == tmp_path / "data"

    def test_from_directory_resolves_relative_cache_paths(self, tmp_path: Path) -> None:
        """from_directory() should resolve relative cache_path against root."""
        from datacachalog.core.services import Catalog

        (tmp_path / ".git").mkdir()
        source_file = tmp_path / "source.txt"
        source_file.write_text("content")

        dataset = Dataset(
            name="test",
            source=str(source_file),
            cache_path=Path("custom/cache.txt"),  # relative path
        )
        catalog = Catalog.from_directory([dataset], directory=tmp_path)

        resolved = catalog.get_dataset("test")
        assert resolved.cache_path == tmp_path / "custom/cache.txt"

    def test_from_directory_accepts_custom_cache_dir(self, tmp_path: Path) -> None:
        """from_directory() should accept custom cache directory."""
        from datacachalog.core.services import Catalog

        (tmp_path / ".git").mkdir()
        source_file = tmp_path / "source.txt"
        source_file.write_text("content")

        dataset = Dataset(name="test", source=str(source_file))
        catalog = Catalog.from_directory(
            [dataset],
            directory=tmp_path,
            cache_dir="custom_cache",
        )

        assert catalog._cache_dir == tmp_path / "custom_cache"

    def test_from_directory_accepts_absolute_cache_dir(self, tmp_path: Path) -> None:
        """from_directory() should accept absolute cache directory path."""
        from datacachalog.core.services import Catalog

        (tmp_path / ".git").mkdir()
        source_file = tmp_path / "source.txt"
        source_file.write_text("content")
        absolute_cache = tmp_path / "absolute_cache"

        dataset = Dataset(name="test", source=str(source_file))
        catalog = Catalog.from_directory(
            [dataset],
            directory=tmp_path,
            cache_dir=absolute_cache,
        )

        assert catalog._cache_dir == absolute_cache

    def test_from_directory_creates_working_catalog(self, tmp_path: Path) -> None:
        """from_directory() should create a fully functional catalog."""
        from datacachalog.core.services import Catalog

        # Create project structure
        (tmp_path / ".git").mkdir()
        source_file = tmp_path / "source.txt"
        source_file.write_text("test content")

        dataset = Dataset(
            name="test",
            source=str(source_file),
            cache_path=Path("data/test.txt"),
        )
        catalog = Catalog.from_directory([dataset], directory=tmp_path)

        # Should be able to fetch
        result = catalog.fetch("test")
        assert isinstance(result, Path)  # Type narrowing
        path = result
        assert path.exists()
        assert path.read_text() == "test content"

    def test_from_directory_uses_cwd_when_no_directory(self, tmp_path: Path) -> None:
        """from_directory() should use current directory when not specified."""
        import os

        from datacachalog.core.services import Catalog

        # Create project structure in tmp_path
        (tmp_path / ".git").mkdir()
        source_file = tmp_path / "source.txt"
        source_file.write_text("content")

        original_cwd = Path.cwd()
        try:
            os.chdir(tmp_path)
            dataset = Dataset(name="test", source=str(source_file))
            catalog = Catalog.from_directory([dataset])

            assert catalog._cache_dir == tmp_path / "data"
        finally:
            os.chdir(original_cwd)


@pytest.mark.core
@pytest.mark.tra("Domain.ConcurrencyBoundary")
@pytest.mark.tier(1)
class TestConcurrencyBoundary:
    """Tests to verify concurrency boundary compliance in core domain."""

    def test_core_services_no_concurrency_imports(self) -> None:
        """core/services.py should not import ThreadPoolExecutorAdapter or other concurrency primitives."""
        import ast
        from pathlib import Path

        # Read the source file
        core_services_path = (
            Path(__file__).parent.parent.parent
            / "src"
            / "datacachalog"
            / "core"
            / "services.py"
        )
        source_code = core_services_path.read_text()

        # Parse AST to check imports
        tree = ast.parse(source_code, filename=str(core_services_path))

        # Check for forbidden imports
        forbidden_imports = [
            "ThreadPoolExecutorAdapter",
            "ThreadPoolExecutor",
            "ProcessPoolExecutor",
            "threading.Lock",
            "asyncio.Lock",
        ]

        violations: list[str] = []

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if any(forbidden in alias.name for forbidden in forbidden_imports):
                        violations.append(f"Import: {alias.name}")
            elif isinstance(node, ast.ImportFrom):
                if node.module and any(
                    forbidden in node.module for forbidden in forbidden_imports
                ):
                    violations.append(f"ImportFrom: {node.module}")
                for alias in node.names:
                    if any(forbidden in alias.name for forbidden in forbidden_imports):
                        violations.append(f"ImportFrom: {node.module}.{alias.name}")

        # Also check source code directly for string patterns (AST might miss some)
        if "ThreadPoolExecutorAdapter" in source_code:
            # Check if it's in a comment or string literal vs actual import
            lines = source_code.split("\n")
            for i, line in enumerate(lines, 1):
                stripped = line.strip()
                # Skip comments and docstrings, check if it's an import statement
                if (
                    "ThreadPoolExecutorAdapter" in stripped
                    and not stripped.startswith("#")
                    and not stripped.startswith('"""')
                    and "import" in stripped
                ):
                    violations.append(f"Line {i}: {stripped}")

        assert len(violations) == 0, (
            f"Concurrency boundary violations found in core/services.py: {violations}"
        )
