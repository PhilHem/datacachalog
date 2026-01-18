.PHONY: test lint format typecheck

test:
	uv run pytest

lint: typecheck
	uv run ruff check src/datacachalog/
	uv run ruff format --check src/datacachalog/

format:
	uv run ruff format src/datacachalog/

typecheck:
	uv run mypy src/datacachalog/
