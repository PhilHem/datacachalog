"""Rich-based progress reporter for terminal output."""

from __future__ import annotations

from typing import TYPE_CHECKING

from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TaskID,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)


if TYPE_CHECKING:
    from types import TracebackType

    from datacachalog.core.ports import ProgressCallback


class RichProgressReporter:
    """Progress reporter using Rich for terminal display.

    Displays download progress bars with speed and ETA.
    Supports multiple concurrent downloads.

    Example:
        with RichProgressReporter() as reporter:
            path = catalog.fetch("customers", progress=reporter)
    """

    def __init__(self) -> None:
        """Initialize the progress display."""
        self._progress = Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
        )
        self._tasks: dict[str, TaskID] = {}
        self._started = False

    def __enter__(self) -> RichProgressReporter:
        """Start the progress display."""
        self._progress.start()
        self._started = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Stop the progress display."""
        self._progress.stop()
        self._started = False

    def start_task(self, name: str, total: int) -> ProgressCallback:
        """Start tracking a download task.

        Args:
            name: Human-readable name for the task.
            total: Total bytes to download.

        Returns:
            A callback to update progress.
        """
        # Auto-start if not in context manager
        if not self._started:
            self._progress.start()
            self._started = True

        task_id = self._progress.add_task(name, total=total)
        self._tasks[name] = task_id

        def callback(downloaded: int, _total: int) -> None:
            self._progress.update(task_id, completed=downloaded)

        return callback

    def finish_task(self, name: str) -> None:
        """Mark a task as complete.

        Args:
            name: The task name.
        """
        if name in self._tasks:
            task_id = self._tasks[name]
            task = self._progress.tasks[task_id]
            self._progress.update(task_id, completed=task.total)
