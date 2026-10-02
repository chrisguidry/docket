Docket is a distributed background task system with a focus on the scheduling of
future work as seamlessly and efficiently as immediate work.

[![PyPI - Version](https://img.shields.io/pypi/v/pydocket)](https://pypi.org/project/pydocket/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pydocket)](https://pypi.org/project/pydocket/)
[![GitHub main checks](https://img.shields.io/github/check-runs/chrisguidry/docket/main)](https://github.com/chrisguidry/docket/actions/workflows/ci.yml)
[![Codecov](https://img.shields.io/codecov/c/github/chrisguidry/docket)](https://app.codecov.io/gh/chrisguidry/docket)
[![PyPI - License](https://img.shields.io/pypi/l/pydocket)](https://github.com/chrisguidry/docket/blob/main/LICENSE)
[![Documentation](https://img.shields.io/badge/docs-latest-blue.svg)](https://docket.lol/)

## Packages

| Language | Directory | Package |
|---|---|---|
| Python 3.10+ | [`python/`](python/) | [`pydocket`](https://pypi.org/project/pydocket/) on PyPI |

Docket requires [Redis](https://redis.io/) 6.2 or later, or
[Valkey](https://valkey.io/) 8.0 or later.  Each package also has an in-memory
backend for tests.  The [documentation](https://docket.lol/) covers the
concepts and the API.

## Hacking on `docket`

The repository is a [`uv`](https://docs.astral.sh/uv/) workspace.  From a clone:

```bash
uv sync                       # every package and every repository tool
uv run prek run --all-files   # formatting, linting, types, and file sizes
uv run zensical serve         # a local preview of the documentation
```

Each package's README says how to run its tests.  We aim to maintain 100% test
coverage, which is required for all PRs to `docket`.
