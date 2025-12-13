"""Unit tests for configuration utilities.

These tests verify the behavior of find_project_root for discovering
the project root directory from marker files.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest


if TYPE_CHECKING:
    from pathlib import Path

from datacachalog.config import find_project_root


@pytest.mark.core
class TestFindProjectRoot:
    """Tests for find_project_root utility."""

    def test_find_project_root_with_datacachalog_marker(self, tmp_path: Path) -> None:
        """Should find directory containing .datacachalog marker."""
        # Arrange
        project_root = tmp_path
        (project_root / ".datacachalog").touch()
        subdir = project_root / "subdir" / "deeper"
        subdir.mkdir(parents=True)

        # Act
        result = find_project_root(start=subdir)

        # Assert
        assert result == project_root

    def test_find_project_root_with_pyproject_toml(self, tmp_path: Path) -> None:
        """Should find directory containing pyproject.toml marker."""
        # Arrange
        project_root = tmp_path
        (project_root / "pyproject.toml").touch()
        subdir = project_root / "src" / "myapp"
        subdir.mkdir(parents=True)

        # Act
        result = find_project_root(start=subdir)

        # Assert
        assert result == project_root

    def test_find_project_root_with_git(self, tmp_path: Path) -> None:
        """Should find directory containing .git marker."""
        # Arrange
        project_root = tmp_path
        (project_root / ".git").mkdir()
        subdir = project_root / "src"
        subdir.mkdir()

        # Act
        result = find_project_root(start=subdir)

        # Assert
        assert result == project_root

    def test_find_project_root_returns_start_when_no_marker(
        self, tmp_path: Path
    ) -> None:
        """Should return start directory when no markers found."""
        # Arrange
        subdir = tmp_path / "subdir"
        subdir.mkdir()

        # Act
        result = find_project_root(start=subdir)

        # Assert
        assert result == subdir

    def test_datacachalog_wins_over_pyproject(self, tmp_path: Path) -> None:
        """Should prefer .datacachalog over pyproject.toml."""
        # Arrange: nested structure with both markers
        outer_root = tmp_path
        (outer_root / "pyproject.toml").touch()

        inner_root = outer_root / "project"
        inner_root.mkdir()
        (inner_root / ".datacachalog").touch()

        code_dir = inner_root / "code"
        code_dir.mkdir()

        # Act
        result = find_project_root(start=code_dir)

        # Assert: should find .datacachalog, not pyproject.toml
        assert result == inner_root

    def test_pyproject_wins_over_git(self, tmp_path: Path) -> None:
        """Should prefer pyproject.toml over .git."""
        # Arrange
        git_root = tmp_path
        (git_root / ".git").mkdir()

        python_root = git_root / "python_project"
        python_root.mkdir()
        (python_root / "pyproject.toml").touch()

        src_dir = python_root / "src"
        src_dir.mkdir()

        # Act
        result = find_project_root(start=src_dir)

        # Assert
        assert result == python_root

    def test_uses_cwd_when_start_none(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Should use current working directory when start is None."""
        # Arrange
        (tmp_path / ".datacachalog").touch()
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        monkeypatch.chdir(subdir)

        # Act
        result = find_project_root()

        # Assert
        assert result == tmp_path

    def test_returns_absolute_path(self, tmp_path: Path) -> None:
        """Should always return absolute resolved path."""
        # Arrange
        (tmp_path / ".datacachalog").touch()

        # Act
        result = find_project_root(start=tmp_path)

        # Assert
        assert result.is_absolute()

    def test_find_project_root_exported_from_package(self) -> None:
        """find_project_root should be exported from datacachalog package."""
        from datacachalog import find_project_root as exported_func

        assert callable(exported_func)
