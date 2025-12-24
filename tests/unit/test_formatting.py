"""Unit tests for core formatting utilities."""

from __future__ import annotations

import pytest

from datacachalog.core.formatting import status_to_color


@pytest.mark.core
@pytest.mark.tra("Domain.Format.StatusColor")
@pytest.mark.tier(0)
def test_status_to_color_fresh_returns_green() -> None:
    """Test that 'fresh' status maps to 'green'."""
    assert status_to_color("fresh") == "green"


@pytest.mark.core
@pytest.mark.tra("Domain.Format.StatusColor")
@pytest.mark.tier(0)
def test_status_to_color_stale_returns_yellow() -> None:
    """Test that 'stale' status maps to 'yellow'."""
    assert status_to_color("stale") == "yellow"


@pytest.mark.core
@pytest.mark.tra("Domain.Format.StatusColor")
@pytest.mark.tier(0)
def test_status_to_color_missing_returns_red() -> None:
    """Test that 'missing' status maps to 'red'."""
    assert status_to_color("missing") == "red"


@pytest.mark.core
@pytest.mark.tra("Domain.Format.StatusColor")
@pytest.mark.tier(0)
def test_status_to_color_invalid_returns_empty_string() -> None:
    """Test that invalid status returns empty string."""
    assert status_to_color("invalid") == ""
