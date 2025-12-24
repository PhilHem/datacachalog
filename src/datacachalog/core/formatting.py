"""Formatting utilities for domain logic."""


def status_to_color(status: str) -> str:
    """Map status string to color name.

    Args:
        status: Status string ("fresh", "stale", or "missing")

    Returns:
        Color name string:
        - "fresh" -> "green"
        - "stale" -> "yellow"
        - "missing" -> "red"
        - invalid -> empty string
    """
    color_map = {
        "fresh": "green",
        "stale": "yellow",
        "missing": "red",
    }
    return color_map.get(status, "")
