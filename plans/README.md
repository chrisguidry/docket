# Plans: docket in more languages

This repository will build and publish docket in Python, Rust, Go, and
TypeScript.  Each implementation gives a language-native way to run a
function on another machine, backed by Redis.  Each one has the same
concepts, the same task behaviors, and the same operational profile, in the
idioms of its language.

- [cross-language-developer-experience.md](cross-language-developer-experience.md) proposes the API in
  each language, side by side with Python.  It is the document for review.
- This file records the decisions made so far and the questions that are
  still open.

## Goals

- One familiar interface in every language.
- Every task behavior in every language, with the same hooks for users to
  add their own.
- Examples that read side by side, and tests that show the languages behave
  the same.
- The same Redis versions and flavors in every language.
- 100% test coverage, measured and gated, in every language.
- Docs organized by concept, with a tab for each language.

## Not goals

- Interoperation between languages.  A Go worker never reads a Python
  docket's tasks, and no docket routes tasks between languages.
- Python's parameter-default dependency injection in the other languages.
- Any change to the Python interface.  Python is the spec.

## Decisions

### Repository layout

```
python/        pydocket, moved from the root
rust/          docket-rs
go/            the Go module
typescript/    @chrisguidry/docket
protocol/      the canonical Lua scripts
conformance/   the cross-language test driver, grown from chaos/
examples/      by concept: examples/perpetual/{python,rust,go,typescript}
docs/          concept pages with a tab for each language
```

The root is a uv workspace whose members are `python/`, `conformance/`, and
`docs/`, because all three are Python.  Git installs of pydocket then need
`#subdirectory=python`.

### One set of Lua scripts

About 760 lines of Lua in 13 scripts hold every task state change and every
admission gate.  All four languages run the same scripts.

- The canonical scripts are `protocol/*.lua`.  Python's scripts move there
  from docstrings, which is an internal change.
- Each package keeps a checked-in copy of the scripts, because Cargo, the Go
  module zip, and npm each package only their own directory.  Go has no
  packaging step at all: the Go proxy serves the git tree at the tag.
- A prek hook copies the canonical scripts into each package.  CI runs
  `prek run --all-files`, so a copy that differs fails CI.

A fix to a script lands once for all four languages, and burner-redis
already runs these scripts.

### In-process Redis for `memory://`

Each language keeps `memory://`, because users run their own test suites on
it.  The engine is a private module in each package, never a separate
public package.

| Language | Engine |
|---|---|
| Python | burner-redis from PyPI, unchanged. |
| Rust | A vendored copy of the burner-redis engine (about 9,000 lines with tests), behind a `memory` cargo feature that is off by default. It is never published on its own. |
| Go | miniredis, in an internal package that the main package imports. Docket's whole Python suite passed against miniredis `master`. Release v2.39.0 fails 4 expiry tests, so we need a newer release or a pinned pseudo-version. |
| TypeScript | A lean private engine in TypeScript, with an embedded Lua VM. |

For the TypeScript Lua VM, a prototype ran docket's real scripts with a
synchronous `redis.call` on three VMs:

- **lua-redis-wasm 2.1.0** compiles Valkey 8.0's Lua 5.1 and its scripting
  code to WebAssembly, so the Redis rules come built in.  It has one
  author, and it is eight months old.
- **wasmoon 1.16.0** runs Lua 5.4 in WebAssembly and is the fastest: 24 µs
  for each run of the claim script.  The engine must add Lua 5.1 shims and
  copy Redis's number conversion rules.  Its last stable release was in
  December 2023.
- **fengari 0.1.5** runs Lua 5.3 in plain JavaScript.  Its 32-bit integers
  corrupt a docket value, and it is the slowest.

The prototype is not part of the repo.

### Cross-language conformance

The conformance driver is Python, one copy for all languages.  Each language
ships a small agent program with `produce` and `worker` subcommands for a
named scenario.  The scenario's tasks write events to a Redis stream,
`conformance:{scenario}:events`, with the task, key, attempt, worker, and
time.  The driver starts and kills the processes, then asserts on the events,
for example "three attempts, about 1 s and then 2 s apart."  The scenario
tasks exist once in each language, and the assertions exist once.

The agent command line and the event stream are a contract of the test
harness.  docket itself stays without interop.

### Names

| Registry | Name | Users write |
|---|---|---|
| PyPI | `pydocket` | `import docket` |
| crates.io | `docket-rs` | `use docket::...` |
| Go | module `github.com/chrisguidry/docket/go` | `import "github.com/chrisguidry/docket/go/docket"` |
| npm | `@chrisguidry/docket` | `import { Docket } from "@chrisguidry/docket"` |

`docket` is taken on crates.io and npm.

### Versions and tags

Each language has its own version, with a tag prefix:

```
python/v0.26.0    rust/v0.1.0    go/v0.1.0    typescript/v0.1.0
```

The Go prefix is required: a Go tag must start with the module's directory.
Python's tags are bare today, such as `0.25.2`.  The switch needs a bridge
tag `python/v0.25.2`, a hatch-vcs `tag-pattern`, a `describe_command` that
matches only `python/v*`, and `root = ".."`.  Each language gets its own
publish workflow that runs only for its prefix.  PyPI, crates.io, and npm
all support trusted publishing, but crates.io and npm need one first
publish with a token.

### Coverage

100% means 100% of what each toolchain measures well:

| Language | Measure | Tool |
|---|---|---|
| Python | branches, unchanged | pytest-cov |
| Rust | lines and regions | cargo-llvm-cov on a nightly coverage job, so `coverage(off)` works |
| Go | statements | `go test -coverprofile` gated by go-test-coverage, with `// coverage-ignore` |
| TypeScript | lines, branches, and functions | Vitest with the v8 provider |

Go has no branch coverage, and Rust's branch coverage is still unstable.

### Redis versions and flavors

Every language tests the same backends: `memory://`, Redis 6.2 (the oldest
release docket supports), Redis 8.10 (the newest), Redis 8.10 in cluster
mode, Redis 8.10 with ACL, Valkey 8.0 (the oldest), Valkey 9.1 (the
newest), and Valkey 9.1 with ACL.  Sentinel is supported but not tested
live.

Python does not run every backend on every Python version.  A bug that
depends on the Python version shows up on any backend, and a bug that
depends on the server shows up on any Python.  So every Python runs
`memory://` and Redis 8.10.  Redis 6.2 and cluster mode run on the oldest
and newest Python, and Valkey and the ACL backends run on the newest only.
A separate job runs each supported redis-py major.  Rust and Go test only the newest toolchain and the newest
dependencies, so each runs one job per backend.

### Command line

Python keeps its CLI unchanged.  The other languages are libraries first,
because docket usually runs inside a host application.  A CLI helper for
Rust and Go workers is an open question in the developer experience
document.

### Python fixes first

A separate pull request fixes four Python bugs before the ports copy the
behavior: cancel does not stop a task that the scheduler already moved to
the stream; cancel does not wake `get_result()`; per-argument `Cooldown`,
`Debounce`, and `RateLimit` read positional arguments as `None`; and
`subscribe()` can lose a terminal event.  The same pull request fixes doc
errors found along the way.

### Docs

The docs site stays on zensical.  Content tabs with `content.tabs.link`
keep the selected language the same across the whole site.  API references
link out to docs.rs, pkg.go.dev, and a TypeDoc build, because mkdocstrings
covers only Python under zensical.

## Open questions

- **Build order.**  Rust, then Go, then TypeScript, or all three at once
  after the protocol and conformance work.
- **The TypeScript Lua VM.**  lua-redis-wasm, wasmoon, or fengari.
- **Redis clients.**  redis-rs for Rust is likely, because its
  `aio::ConnectionLike` trait lets the vendored engine plug in.  Go
  (go-redis or rueidis) and TypeScript (ioredis or node-redis) are not
  researched yet.
- **Telemetry.**  Whether every language emits the same span and metric
  names, so one dashboard works for any language.
- **The conformance scenarios.**  Which behaviors get a scenario first.
- **The developer experience questions** listed in
  [cross-language-developer-experience.md](cross-language-developer-experience.md#open-questions).
