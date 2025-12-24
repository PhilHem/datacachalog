"""CLI for datacachalog."""

# Import commands to register them with the app
# These imports have side effects (registering commands with @app.command())
from datacachalog.cli.commands import list as _list_module  # noqa: F401
from datacachalog.cli.commands import status as _status_module  # noqa: F401
from datacachalog.cli.main import app, main


__all__ = ["app", "main"]
