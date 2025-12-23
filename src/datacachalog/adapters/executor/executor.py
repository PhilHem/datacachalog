"""Executor adapters implementing ExecutorPort."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from collections.abc import Callable
    from concurrent.futures import Future


class SynchronousExecutor:
    """Synchronous executor that runs tasks immediately in the current thread.

    Used for sequential execution when max_workers=1 or when testing.
    Implements ExecutorPort but executes tasks synchronously.
    """

    def submit(
        self,
        fn: Callable[..., object],
        *args: object,
        **kwargs: object,
    ) -> Future[object]:
        """Execute function immediately and return completed future.

        Args:
            fn: Function to execute.
            *args: Positional arguments to pass to fn.
            **kwargs: Keyword arguments to pass to fn.

        Returns:
            Future with result already available.
        """
        from concurrent.futures import Future

        future: Future[object] = Future()
        try:
            result = fn(*args, **kwargs)
            future.set_result(result)
        except Exception as e:
            future.set_exception(e)
        return future

    def __enter__(self) -> SynchronousExecutor:
        """Enter context manager."""
        return self

    def __exit__(
        self, exc_type: object, exc_val: object, exc_tb: object
    ) -> object | None:
        """Exit context manager (no-op for synchronous executor)."""
        return None


class ThreadPoolExecutorAdapter:
    """Adapter wrapping ThreadPoolExecutor to implement ExecutorPort.

    Moves concurrency out of the core domain into the adapter layer,
    maintaining "concurrency at the edges" architecture.
    """

    def __init__(self, max_workers: int | None = None) -> None:
        """Initialize thread pool executor adapter.

        Args:
            max_workers: Maximum number of worker threads. None uses default.
        """
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

    def submit(
        self,
        fn: Callable[..., object],
        *args: object,
        **kwargs: object,
    ) -> Future[object]:
        """Submit function to thread pool.

        Args:
            fn: Function to execute.
            *args: Positional arguments to pass to fn.
            **kwargs: Keyword arguments to pass to fn.

        Returns:
            Future representing the pending result.
        """
        return self._executor.submit(fn, *args, **kwargs)

    def __enter__(self) -> ThreadPoolExecutorAdapter:
        """Enter context manager."""
        self._executor.__enter__()
        return self

    def __exit__(
        self, exc_type: object, exc_val: object, exc_tb: object
    ) -> object | None:
        """Exit context manager."""
        # Type ignore needed because ThreadPoolExecutor.__exit__ expects specific types
        # but Protocol requires object for compatibility
        return self._executor.__exit__(exc_type, exc_val, exc_tb)  # type: ignore[arg-type]
