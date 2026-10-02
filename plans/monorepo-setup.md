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

**Files that move.**  `src/`, `tests/`, `pyproject.toml`, and the two
`.coveragerc` files move to `python/`.  hatchling rejects `../` paths for `readme` and `license`
(`pyproject.toml:9,11`), and a symlink breaks the build from the sdist.  So
`python/` gets its own `README.md`, which is the PyPI description, and a
copy of `LICENSE`.  The root `README.md` becomes the overview of all
languages.

**The uv workspace.**  The root `pyproject.toml` becomes a virtual
workspace root.  Step 3 adds `python/` as its first member, and steps 5 and
6 add `conformance/` and `docs/`.  Until then, the root's `dev` group holds
the repository tools (prek, loq, codespell, the docs, and chaos), and
`python/`'s `dev` group holds the Python test and type tools.  `uv sync` at
the root installs both.  `uv sync` inside `python/` syncs only that member
and removes the root tools from the shared `.venv`, but `uv run` is safe
from any directory.
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
- The `main` ruleset requires 31 checks by name, and it requires branches to
  be up to date.  Every job keeps its name, because a renamed job is a new
  check.
- The Python test jobs run with `working-directory: python`.
- `cache-dependency-glob` changes from `pyproject.toml` to `uv.lock` in all
  9 places.
- The changed-directory filter waits for the first port's scaffold.  With
  one language, every pull request runs the Python jobs anyway.  A
  workflow-level `paths:` filter would leave required checks pending, so
  the filter is a first job that computes which directories changed, with
  an `if:` on each job.  A skipped job satisfies a required check.

**Tags and publishing.**
- New pydocket releases are tagged `python/v0.27.0`.  The old bare tags
  stay, so no bridge tag is needed.
- hatch-vcs gets `raw-options.root = ".."`, a `tag-pattern` that accepts
  `python/v0.27.0` and `0.26.0`, and a `describe_command` with
  `--match python/v* --match [0-9]*`.  A test with local tags showed that a
  `rust/v0.1.0` tag leaves the version alone and a `python/v0.27.0` tag
  gives `0.27.0`.
- `publish.yml` runs only for a `python/v*` or bare release tag and builds
  with `uv build --package pydocket`.  The file keeps its name, because
  PyPI's trusted publisher names the workflow file.

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

The 13 scripts were docstrings of `@redis_script` stubs, 925 lines in all
with their generated headers:

| File | Scripts |
|---|---|
| `_execution_scripts.py` | `_schedule`, `_claim`, `_terminal`, `_cancel_task` |
| `_execution_progress.py` | `_progress_write` |
| `_redelivery.py` | `_refresh_lease` |
| `worker.py` | `_stream_due_tasks` |
| `dependencies/_concurrency.py` | `_acquire_or_park`, `_release_and_wake`, `_scavenge_and_wake`, `_cancel_cleanup` |
| `dependencies/_debounce.py` | `_debounce` |
| `dependencies/_ratelimit.py` | `_ratelimit` |

**Each `.lua` file is a complete script.**  It starts with a header that
binds `KEYS` and `ARGV` to local names, such as `local stream_key =
KEYS[1]`, then a blank line, then the body.  That header is the contract
that every language follows when it calls the script, and
`protocol/README.md` describes it.

**The typed stubs stay.**  A stub keeps its typed signature, so pyright
checks every call site, and its name picks its file: `_claim` runs
`lua/claim.lua`.  When the module loads, the decorator builds the header
that the signature implies and checks that the file starts with exactly
that header and a blank line.  A header that drifts from its stub fails
at import, on every run.  The decorator is a `ScriptDirectory`, so the
decorator tests keep their own scripts in `tests/lua/`.

**The extraction.**  The files hold the string that Python sent at run
time, not the source text, because three docstrings were not raw strings
and contained `\\` escapes.  A one-time check compared each loaded file
with the old runtime string: all 13 matched, apart from the final newline
that each file now ends with.

**Loading.**  The decorator reads the file with
`importlib.resources.files("docket")`.  The wheel includes non-Python files
under `src/docket`, so the copy at `python/src/docket/lua/` ships without
new packaging settings.  EVALSHA, the NOSCRIPT retry, and the pipelined and
cluster paths stay the same.

**The sync hook.**  A root prek hook copies `protocol/*.lua` into each
package and removes copies that have no source, and a second hook copies
`LICENSE`.  A hook that changes a file fails, and CI runs
`prek run --all-files`, so a stale copy fails CI.

## Step 5: Build the conformance driver

`conformance/` is a workspace member that replaces `chaos/`.  Its README
holds the contract.

- **The agent contract.**  Each language ships an agent program in its own
  tree, such as `python/conformance-agent/` and later
  `rust/conformance-agent/`:
  `<agent> produce|worker --scenario NAME --url URL --docket NAME`.  The
  Python agent runs `Worker.run`, which is what `docket worker` runs, so
  signals stop it the way they stop a deployed worker.
- **The events.**  Scenario tasks write to the stream
  `conformance:{scenario}:events` with the event, task, key, attempt,
  worker, and time.
- **The driver.**  It starts and kills agent processes, restarts Redis when
  a scenario needs it, and makes assertions on the events and on the run
  state that the Lua scripts keep.  It never imports docket.
- **Implementations.**  The driver takes `--implementation language@version`.
  Until a second language exists, `python@main` (the working tree) and
  `python@release` (the newest tag, from PyPI) are the two implementations.
  Both run the agent from the working tree, so the agent uses only the
  public interface.  A port replaces `python@release` in the CI matrix.
- **The scenarios.**  `backoff`, `perpetual`, `cancel-before-start`, and
  `graceful-drain`, which was `chaos/signals.py`, run on each
  implementation.  `chaos` mixes both implementations while it kills
  workers and restarts Redis.
- **CI.**  `Conformance, python@main` and `Conformance, python@release` run
  the four scenarios.  `Chaos tests` keeps its name.  The ruleset swaps
  `Signal handling tests` for the two conformance checks.

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
