"""Executor adapters for parallel execution."""

from datacachalog.adapters.executor.executor import (
    SynchronousExecutor,
    ThreadPoolExecutorAdapter,
)


__all__ = ["SynchronousExecutor", "ThreadPoolExecutorAdapter"]
