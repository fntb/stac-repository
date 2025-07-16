
default:
	@just --list

uv:
	@uv -V || echo 'Please install uv: https://docs.astral.sh/uv/getting-started/installation/'

# Install the dependencies
install: uv
	uv sync --frozen --all-extras --all-groups

# Print version number
version: uv
	#!/usr/bin/bash
	version=`echo "from stac_repository.__about__ import __version__; print(__version__)" | uv run --no-project -`
	echo stac-repository ${version}

stac-repository *args: uv
	uv run stac_repository_cli {{args}}

stac-processor *args: uv
	uv run stac_processor_cli {{args}}

# Build the package
build: uv
	uv build

# Publish to pypi
publish: uv
	uv publish

clean:
	@rm -rf `find . -name __pycache__`
	@rm -f `find . -type f -name '*.py[co]'`
	@rm -f `find . -type f -name '*~'`
	@rm -f `find . -type f -name '.*~'`
	@rm -rf .cache
	@rm -rf .pytest_cache
	@rm -rf .ruff_cache
	@rm -rf htmlcov
	@rm -rf *.egg-info
	@rm -f .coverage
	@rm -f .coverage.*
	@rm -rf build
	@rm -rf dist
	@rm -rf coverage.xml