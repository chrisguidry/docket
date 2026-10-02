# Monorepo setup

This plan turns the repository into the layout in [README.md](README.md)
before any Rust, Go, or TypeScript code exists.  Each step is one pull
request.  Steps 1 through 6 change no behavior of pydocket.  When they are
done, every port starts on the final layout, and it can prove its behavior
against Python from its first pull request.

File and line references are to `main` at e5834ee.

## Step 1: Fix the Python bugs

Done.  The bugs that the ports would copy were fixed one per pull request,
and the fixes shipped in pydocket 0.26.0.

## Step 2: Fix the CI coverage gate

`.coveragerc-core` and `.coveragerc-cli` are INI files.  In them, coverage
reads `exclude_also = ["\\.\\.\\.$"]` as one regular expression, and the
brackets make it a character class.  So CI excludes every line that
contains a `.`, `"`, `\`, or `$` from its 100% gate.  The TOML copy of the
same setting in `pyproject.toml` is correct, and with it the local suite
measures 99.86%.

This step writes the pattern in INI form (`\.\.\.$` on its own line), then
covers or excludes the lines that the corrected gate reports.  It comes
before the move, so that the move runs under a gate that measures the real
code.

## Step 3: Move Python into `python/`

This step moves files and changes paths.  The diff has no behavior change.

**Files that move.**  `src/`, `tests/`, and `pyproject.toml` move to
`python/`.  hatchling rejects `../` paths for `readme` and `license`
(`pyproject.toml:9,11`), and a symlink breaks the build from the sdist.  So
`python/` gets its own `README.md`, which is the PyPI description, and a
copy of `LICENSE`.  The root `README.md` becomes the overview of all
languages.

**The uv workspace.**  The root `pyproject.toml` becomes a virtual
workspace root with `python/`, `docs/`, and `conformance/` as members.
`uv.lock` stays at the root, and its editable path for pydocket changes
from `.` to `python`.  Every member keeps `requires-python = ">=3.10"`,
because the workspace uses the intersection of all members.  Members that
use pydocket declare `[tool.uv.sources] pydocket = { workspace = true }`.
The root `[tool.ruff]` table stays at the root, and members inherit it.

**Tool paths.**
- Coverage: `--cov` and the `omit` lists (`pyproject.toml:112-113,141-154`,
  both `.coveragerc` files, `.github/codecov.yml:2-8`) resolve from the
  working directory.  CI runs pytest with `uv run --directory python`.
- `tests/cli/run.py:32-36` sets `COVERAGE_PROCESS_START="pyproject.toml"`
  and assumes that the project root is the working directory.
- pyright: `venvPath` becomes `".."`, and `include` drops `chaos`.
- loq: the 8 paths in `loq.toml` gain `python/`.  The root keeps the only
  `loq.toml`, because loq ignores a nested one when it runs from the root.
- `Dockerfile:9-13` and `docker-compose.yml:4-8` switch to
  `WORKDIR /app/python` and `./python`, and `.dockerignore` gains the new
  directories.

**prek.**  prek 0.5.3 supports a nested `.pre-commit-config.yaml` in each
directory, and a nested hook runs only on its own directory.  The root
config keeps the hooks for every file: whitespace, end of file, YAML, TOML,
large files, codespell, and loq.  `python/.pre-commit-config.yaml` gets
ruff and pyright.  Each port adds its own config later.

**CI.**
- The `main` ruleset requires 45 checks by name, and it requires branches to
  be up to date.  Every job keeps its name, because a renamed job is a new
  check.
- A workflow-level `paths:` filter would leave required checks pending.  So
  a first job computes which directories changed, and each job has an
  `if:` on that result.  A skipped job satisfies a required check.
- The Python jobs run when `python/**` or `protocol/**` changes.
- `cache-dependency-glob` changes from `pyproject.toml` to `uv.lock` in all
  9 places.

**Tags and publishing.**
- Push the bridge tag `python/v0.25.2` on 98f6cfb, where `0.25.2` is.
- hatch-vcs gets `tag-pattern = "^python/v(?P<version>.+)$"`,
  `raw-options.root = ".."`, and a `describe_command` with
  `--match python/v*`.  A test build made `0.25.4.dev2+g...` from a
  `python/v0.25.3` tag.  Without the `describe_command`, a nearer
  `rust/v0.1.0` tag breaks the build.
- `publish.yml` runs only for `python/v*` releases and builds with
  `uv build --package pydocket`.  The file keeps its name, because PyPI's
  trusted publisher names the workflow file.

**chaos/.**  The chaos driver breaks in three places, and this step fixes
them, so that its two required checks keep passing:
- Its `git describe` (`driver.py:141-150`) returns a prefixed tag, and the
  PyPI check then exits 1.
- Its editable install `-e .` (`driver.py:114-115`) no longer points at the
  package.
- Its worker processes import `chaos.*` only because the repository root is
  the working directory.

**Docs and agent files.**  `AGENTS.md` (with its `CLAUDE.md` symlinks),
`README.md:105-132`, and `.claude/skills/audit-docs/SKILL.md` name the old
paths.  The skill already has stale paths at lines 18 and 36-37.

## Step 4: Extract the Lua into `protocol/`

The 13 scripts are docstrings of `@redis_script` stubs, 784 lines in all:

| File | Scripts |
|---|---|
| `_execution_scripts.py` | `_schedule`, `_claim`, `_terminal`, `_cancel_task` |
| `_execution_progress.py` | `_progress_write` |
| `_redelivery.py` | `_refresh_lease` |
| `worker.py` | `_stream_due_tasks` |
| `dependencies/_concurrency.py` | `_acquire_or_park`, `_release_and_wake`, `_scavenge_and_wake`, `_cancel_cleanup` |
| `dependencies/_debounce.py` | the debounce script |
| `dependencies/_ratelimit.py` | the rate limit script |

**Each `.lua` file is a complete script.**  `_lua.py` builds a preamble from
the stub's signature that binds `KEYS` and `ARGV` to local names
(`_lua.py:199-222`).  In `protocol/`, each file starts with those bindings
written out, such as `local stream_key = KEYS[1]`.  That header is the
contract that every language follows when it calls the script.  Python's
decorator loads the file, and a test checks that the stub's signature lists
the same keys and arguments in the same order as the header.  The
alternative is a body without the header, and each language would then
rebuild the bindings on its own.

**The extraction.**  Extract the string that Python sends at run time, not
the source text.  Three docstrings are not raw strings and contain `\\`
escapes (`_concurrency.py:176-186` and `:295-299`, `worker.py:164-174`), so
their source differs from what Python sends.  During this step, a test
asserts that each new file is exactly equal to the old preamble plus body
for all 14 scripts.  The test is removed after the step.

**Loading.**  The decorator reads the file with
`importlib.resources.files("docket")`.  The wheel already includes
non-Python files under `src/docket`, so the copy at
`python/src/docket/lua/` ships without new packaging settings.  EVALSHA,
the NOSCRIPT retry, and the pipelined and cluster paths stay the same.

**The sync hook.**  A root prek hook copies `protocol/*.lua` and `LICENSE`
into each package, and it fails when a copy was out of date.  CI runs
`prek run --all-files`, so a stale copy fails CI.  `tests/test_lua_decorator.py`
and `tests/test_task_cycle_round_trips.py` change to match.

## Step 5: Build the conformance driver

`conformance/` is a workspace member that grows from `chaos/`.

- **The agent contract.**  Each language ships an agent program:
  `<agent> produce|worker --scenario NAME --url URL --docket NAME`.
- **The events.**  Scenario tasks write to the stream
  `conformance:{scenario}:events` with the task, key, attempt, worker, and
  time.
- **The driver.**  It starts and kills agent processes, restarts Redis when
  a scenario needs it, and asserts on the events.  It takes the language as
  a parameter, so every scenario runs against every language.
- **The first scenarios.**  Exponential backoff, an automatic perpetual
  task, and a cancel before start.  The current chaos run, which mixes the
  released version with the working tree while it kills workers, becomes a
  Python scenario.
- **CI.**  A `conformance` job runs each scenario against each language.
  The two required chaos checks keep their names until the ruleset changes.

## Step 6: Turn on docs tabs

- `mkdocs.yml` lists `markdown_extensions`, so zensical's defaults do not
  apply.  Add `pymdownx.tabbed` with `alternate_style: true`, and add the
  `content.tabs.link` theme feature.  Neither is enabled today.
- Zensical publishes every non-Markdown file in `docs_dir`.  So the pages
  move to `docs/content/`, `docs/pyproject.toml` holds the docs
  dependencies, and `mkdocs.yml` stays at the root with
  `docs_dir: docs/content`.  With the config inside `docs/`, a test showed
  that incremental rebuilds miss docstring edits.
- mkdocstrings reads `python/src`.  `.readthedocs.yaml` runs uv for the docs
  member.
- The pages are rewritten concept first, with a Python tab only.  Page slugs
  stay the same, because `docket.lol/en/latest/<page>/` links appear in
  `README.md` and `pyproject.toml`.

## Step 7: Reorganize the examples

The 10 scripts in `examples/` have no CI, and they import in two ways
(`from .common` and `from common`).  They move to
`examples/<concept>/python/`, one concept per directory, and a CI job runs
each one as a smoke test.  Each port adds its own directory under each
concept.

## Steps 8 through 10: One scaffold per language

In order: Rust, Go, TypeScript.  Each scaffold pull request brings:

- the package: `rust/` as a Cargo workspace for `docket-rs`, `go/` as the
  module `github.com/chrisguidry/docket/go`, `typescript/` for
  `@chrisguidry/docket`;
- its synced copies of the Lua scripts and `LICENSE`;
- its private `memory://` engine;
- its nested prek config: rustfmt and clippy, gofmt and go vet, or the
  TypeScript formatter and linter;
- CI jobs for the Redis matrix, behind the changed-directory `if:`;
- its coverage gate at 100% of what its toolchain measures;
- its conformance agent, with the first scenarios passing;
- its publish workflow for its own tag prefix, and a Dependabot entry for
  cargo, gomod, or npm.

crates.io and npm need one first publish with a token before trusted
publishing works.  Feature pull requests follow each scaffold, one behavior
at a time, each with its conformance scenario.

## Risks

- **Required checks.**  Any renamed job fails the ruleset until the ruleset
  changes.  Each step lists the check names it adds or renames.
- **Not verified yet:** PyPI's trusted publisher after the move, Codecov
  paths for a coverage report made in `python/`, Dependabot on a uv
  workspace, and Read the Docs with prefixed tags.
- **Git installs** of pydocket need `#subdirectory=python` after step 3.
- **Open pull requests** that touch `src/` or `tests/` need a rebase after
  step 3.  Step 3 should land when few are open.
