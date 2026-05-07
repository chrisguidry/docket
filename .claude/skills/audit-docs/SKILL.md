---
name: audit-docs
description: As you edit pydocket — change a function body, modify a signature, alter a Lua script, refactor a public method, rewrite a docs section — verify that the docstrings and narrative docs touching that change are still accurate, and fix them in the same edit. This is a habit, not a deliverable. Apply it continuously while working on `src/docket/`, `docs/*.md`, or `README.md`. Surface findings inline as you make changes; do not produce a separate "audit report" unless the user explicitly asks for one. Trigger phrases that mean the user wants the explicit audit-as-deliverable mode include "audit the docs", "check docstring drift", "produce an audit report", "are the docs still accurate".
---

# Keep docs honest as you work

The default mode of this skill is integrated, not batch. While you're editing pydocket, watch for changes that could invalidate a docstring or a documentation section, and fix them in the same edit — same commit, same PR. The goal is to prevent drift from accumulating, not to clean it up later.

If the user asks explicitly for an audit report (phrases like "audit the docs", "check docstring drift", "find stale documentation"), shift into the explicit report mode at the bottom of this file.

## What counts as "touching" docs

Any of these edits should make you re-read the affected docstring and any narrative-doc section that mentions the API:

- Editing the body of a function/method/class that has a docstring.
- Changing a function or method signature: added, removed, or renamed parameters; changed defaults; changed return type or shape.
- Changing a Lua script in `src/docket/execution.py` or `src/docket/_redis.py` — atomicity, ordering, and consistency claims in docstrings are usually pinned to the Lua, not the Python.
- Renaming or removing a public class, function, method, exception, or enum value.
- Changing a public exception type, error message, or condition under which something is raised.
- Changing a side effect on shared state (Redis keys, channels, sorted sets, streams).
- Adding or removing a behavior callers might rely on (e.g., idempotency on duplicate inputs, retry semantics, cancellation behavior).
- Editing a `## section` of a `docs/*.md` file or `README.md` — verify the surrounding code still does what the prose now claims.

## What to check, in order

When you've made one of the changes above:

1. **The item's own docstring.** Read it. Does it still match what the code does? Watch specifically for:
   - "raises X" claims when the code never raises X (or vice versa).
   - "returns Y" descriptions when the code returns Z.
   - Listed parameters or enum values that no longer exist.
   - Behavior described in present tense that has actually changed.
   - Silence on a load-bearing contract a caller would be surprised by — idempotency on duplicate keys is the canonical example: `Docket.add()` is idempotent on duplicate keys (no-ops), but the docstring used to be silent on that, leading callers to either avoid the pattern or learn it the hard way.
2. **Tests that pin the contract.** If a test exists for the changed behavior, the test is the contract. When the docstring and the test disagree, the test wins (or both are wrong, in which case raise that with the user — don't paper over it). Useful test directories:
   - `tests/fundamentals/` — core lifecycle, scheduling, retries, idempotency.
   - `tests/concurrency_limits/`, `tests/cron/`, `tests/perpetual/`, `tests/debounce/`, `tests/cooldown/`, `tests/rate_limit/`, `tests/strike/` — corresponding dependency docstrings.
3. **Call sites.** If a method's docstring says it raises an exception but the only caller never inspects the return value, the "raises" claim is suspect.
4. **Narrative docs.** Grep `docs/*.md` and `README.md` for the symbol you changed. For each hit, read the surrounding section and verify the prose still matches the code.

## How to fix

Make the smallest edit that restores accuracy:

- If a docstring asserts something the code doesn't do, replace just that claim. Don't rewrite the surrounding documentation.
- If a docstring is silent on a load-bearing contract, add one terse sentence. Don't expand into a manual.
- If a narrative docs section is now wrong, fix the wrong sentence(s). Don't restructure the section.
- Never modify code to match a docstring — if the code is right and the docstring is wrong, fix the docstring; if the code is wrong, that's a separate decision to surface to the user.

## What not to flag

- **Stylistic differences.** If a docstring is accurate but you'd phrase it differently, leave it alone.
- **Brevity.** A two-sentence docstring that's accurate is better than a paragraph that drifts. Don't expand a docstring just to mention every parameter.
- **Imprecise but not misleading.** "Schedules a task" is fine even if the implementation is more nuanced — only flag claims that are wrong, not claims that are incomplete.
- **`pydocket` vs `docket`.** The package is `pydocket` on PyPI; the import is `import docket`. Both terms appear deliberately throughout the docs. Don't "fix" one to the other.

## Where this habit applies

- **`src/docket/`** — every public class, function, method, and module with a docstring. Skip names with a leading underscore in any segment of the qualified name (except dunders), and skip the vendored modules: `_prometheus_exporter.py`, `_telemetry.py`, `_uuid7.py`.
- **`docs/*.md`** — every section, except `docs/api-reference.md` (auto-rendered from source docstrings via mkdocstrings; auditing it separately would double-count).
- **`README.md`** — top-level overview claims.

## Quality gate

After applying any doc edits, run:

```bash
uv run prek run --all-files
```

This covers ruff, ruff-format, codespell, loq (file-size limit), and pyright. Pure docstring edits should pass cleanly. If `loq` flags a file as over its limit because a docstring grew it past the threshold, prefer to *trim* the docstring before bumping the loq baseline.

For a cheap behavior-preservation sanity check after touching a lot of docstrings, run:

```bash
REDIS_VERSION=memory uv run pytest -x --no-cov -q
```

Docstring edits shouldn't move the test suite — but the check is fast and catches accidents.

## Explicit report mode

If the user asks for a standalone audit ("audit the docs", "produce an audit report", "find every stale docstring"), switch styles:

1. **Discover the surface.** `Glob` `src/docket/**/*.py` (skipping vendored modules) and walk each file for public docstrings. `Glob` `docs/*.md` and `README.md`, splitting markdown by `## ` headings.
2. **Verify each item** using the rubric below — read the implementation, the embedded Lua where relevant, and at least one test before classifying as `stale` or `silent`.
3. **Classify** each item:
   - `accurate` — consistent with the code (default when in doubt).
   - `stale` — asserts something the code does not do.
   - `silent` — silent on a load-bearing contract callers measurably depend on, where the omission is non-obvious *and* surprising.
   - `uncertain` — cannot determine after reading the implementation, the Lua, the tests, and the call sites.
4. **Group findings by file** and present file:line + verdict + one-sentence explanation + suggested fix.
5. **Offer to apply** only after the user has seen the report.

The seed example for the report mode is `src/docket/execution.py` around `Execution.schedule`: the docstring says `replace=False` raises an error if the task already exists, but the Lua script returns `'EXISTS'`, the Python caller never inspects it, and `Docket.add` silently no-ops on duplicate keys (pinned by `tests/fundamentals/test_idempotency.py`). If a "find every stale docstring" run does *not* surface that item, the audit missed something.
