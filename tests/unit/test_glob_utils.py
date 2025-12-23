"""Unit tests for glob utility functions."""

import pytest


@pytest.mark.core
@pytest.mark.tra("Domain.GlobUtils")
class TestIsGlobPattern:
    """Tests for is_glob_pattern()."""

    def test_detects_asterisk(self) -> None:
        """Should detect * as glob pattern."""
        from datacachalog.core.glob_utils import is_glob_pattern

        assert is_glob_pattern("s3://bucket/data/*.parquet") is True
        assert is_glob_pattern("/local/path/*.csv") is True

    def test_detects_double_asterisk(self) -> None:
        """Should detect ** as glob pattern."""
        from datacachalog.core.glob_utils import is_glob_pattern

        assert is_glob_pattern("s3://bucket/**/*.parquet") is True

    def test_detects_question_mark(self) -> None:
        """Should detect ? as glob pattern."""
        from datacachalog.core.glob_utils import is_glob_pattern

        assert is_glob_pattern("s3://bucket/data/file?.csv") is True

    def test_detects_brackets(self) -> None:
        """Should detect [] as glob pattern."""
        from datacachalog.core.glob_utils import is_glob_pattern

        assert is_glob_pattern("s3://bucket/data/file[0-9].csv") is True

    def test_returns_false_for_plain_path(self) -> None:
        """Should return False for paths without glob characters."""
        from datacachalog.core.glob_utils import is_glob_pattern

        assert is_glob_pattern("s3://bucket/data/file.parquet") is False
        assert is_glob_pattern("/local/path/file.csv") is False
        assert is_glob_pattern("relative/path/file.txt") is False


@pytest.mark.core
@pytest.mark.tra("Domain.GlobUtils")
class TestSplitGlobPattern:
    """Tests for split_glob_pattern()."""

    def test_splits_s3_uri_with_asterisk(self) -> None:
        """Should split s3://bucket/path/*.parquet correctly."""
        from datacachalog.core.glob_utils import split_glob_pattern

        prefix, pattern = split_glob_pattern("s3://bucket/data/*.parquet")

        assert prefix == "s3://bucket/data/"
        assert pattern == "*.parquet"

    def test_splits_s3_uri_with_double_asterisk(self) -> None:
        """Should split s3://bucket/path/**/*.parquet correctly."""
        from datacachalog.core.glob_utils import split_glob_pattern

        prefix, pattern = split_glob_pattern("s3://bucket/data/**/*.parquet")

        assert prefix == "s3://bucket/data/"
        assert pattern == "**/*.parquet"

    def test_splits_local_path(self) -> None:
        """Should split /local/path/*.csv correctly."""
        from datacachalog.core.glob_utils import split_glob_pattern

        prefix, pattern = split_glob_pattern("/local/path/*.csv")

        assert prefix == "/local/path/"
        assert pattern == "*.csv"

    def test_splits_pattern_with_question_mark(self) -> None:
        """Should split path with ? pattern."""
        from datacachalog.core.glob_utils import split_glob_pattern

        prefix, pattern = split_glob_pattern("s3://bucket/logs/2024-0?-01.log")

        assert prefix == "s3://bucket/logs/"
        assert pattern == "2024-0?-01.log"

    def test_raises_for_non_glob_path(self) -> None:
        """Should raise ValueError for paths without glob characters."""
        from datacachalog.core.glob_utils import split_glob_pattern

        with pytest.raises(ValueError, match="not a glob pattern"):
            split_glob_pattern("s3://bucket/data/file.parquet")


@pytest.mark.core
@pytest.mark.tra("Domain.GlobUtils")
class TestDeriveCacheKey:
    """Tests for derive_cache_key()."""

    def test_derives_key_for_s3_uri(self) -> None:
        """Should derive cache key from S3 URI."""
        from datacachalog.core.glob_utils import derive_cache_key

        key = derive_cache_key(
            dataset_name="logs",
            prefix="s3://bucket/logs/",
            matched_uri="s3://bucket/logs/2024-01.parquet",
        )

        assert key == "logs/2024-01.parquet"

    def test_preserves_nested_structure(self) -> None:
        """Should preserve nested directory structure in cache key."""
        from datacachalog.core.glob_utils import derive_cache_key

        key = derive_cache_key(
            dataset_name="logs",
            prefix="s3://bucket/logs/",
            matched_uri="s3://bucket/logs/2024/01/data.parquet",
        )

        assert key == "logs/2024/01/data.parquet"

    def test_derives_key_for_local_path(self) -> None:
        """Should derive cache key from local path."""
        from datacachalog.core.glob_utils import derive_cache_key

        key = derive_cache_key(
            dataset_name="data",
            prefix="/storage/data/",
            matched_uri="/storage/data/file.csv",
        )

        assert key == "data/file.csv"

    def test_handles_trailing_slash_in_prefix(self) -> None:
        """Should handle prefix with or without trailing slash."""
        from datacachalog.core.glob_utils import derive_cache_key

        key1 = derive_cache_key(
            dataset_name="logs",
            prefix="s3://bucket/logs/",
            matched_uri="s3://bucket/logs/file.parquet",
        )
        key2 = derive_cache_key(
            dataset_name="logs",
            prefix="s3://bucket/logs",
            matched_uri="s3://bucket/logs/file.parquet",
        )

        assert key1 == "logs/file.parquet"
        assert key2 == "logs/file.parquet"
