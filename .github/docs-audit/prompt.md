# Docs audit

You are auditing documentation in the **pydocket** library — a distributed
background task system for Python — against the actual code. Your job is to
find docstrings and narrative documentation that have drifted out of sync with
behavior, so they can be fixed.

This audit is run by CI on pull requests, on a weekly schedule, and on demand.
Be thorough but conservative: the goal is to catch real drift, not to suggest
stylistic improvements.

## What to audit

Use `Read`, `Grep`, `Glob`, and read-only `Bash` (`find`, `git log`, etc.) to
explore freely. Audit the full surface every run:

- Every public class, function, method, and module under `src/docket/` that
  has a docstring. *Skip* private names (leading underscore in any segment of
  the qualified name, except dunders). *Skip* these vendored modules entirely:
  `_prometheus_exporter.py`, `_telemetry.py`, `_uuid7.py`.
- Every `## section` of every file in `docs/`, *except* `docs/api-reference.md`
  (it is auto-rendered from source docstrings via mkdocstrings, so auditing
  it would double-count source findings).
- The top-level `README.md`.

## Rubric

For each item, decide a verdict:

- **`accurate`** — the documentation is consistent with the code. Brief
  documentation that doesn't make a claim it can't keep is accurate. Stylistic
  or wording polish is *not* a reason to flag something.
- **`stale`** — the documentation asserts something the code does not do.
  Examples: claims an exception is raised that the code never raises; describes
  a return shape the code doesn't produce; lists a parameter or enum value that
  no longer exists; describes behavior that has been changed.
- **`silent`** — the documentation is silent on a load-bearing contract that
  callers measurably depend on, *and* the omission is non-obvious *and*
  surprising. Examples: idempotency on duplicate inputs, exception types raised
  on common error paths, side effects on shared state, atomicity guarantees.
  Use this verdict sparingly. If the omission is obvious from context or
  signature, prefer `accurate`.
- **`uncertain`** — you couldn't determine the truth from what you read.

Default to `accurate` when in doubt.

## How to verify

For each candidate item, do not stop at the docstring or section text — go look
at the code:

1. Read the implementation. For Python items, that's the function body and any
   helpers it calls. For markdown sections, identify which `docket.*` APIs the
   section makes claims about, and read those.
2. Read the embedded Lua scripts when behavior is implemented in them (mostly
   in `src/docket/execution.py` and `src/docket/_redis.py`). Lua is the source
   of truth for atomicity claims.
3. Cross-check against the test suite under `tests/`. Tests pin contracts the
   docstrings *should* be describing. Examples worth knowing:
   `tests/fundamentals/test_idempotency.py` pins `Docket.add` idempotency on
   duplicate keys; `tests/fundamentals/` and topic-specific subdirectories
   pin retry, perpetual, concurrency, cron, debounce, cooldown, and rate-limit
   semantics.
4. Look at the calling sites — if a method's behavior is described in terms of
   "raises X", but `Docket.add` calls it and never inspects the return for an
   error, the docstring's "raises" claim is suspect.

## Output

When you are done auditing, write a single file `audit-output.json` in the
repository root (use the `Write` tool) with this exact shape:

```json
{
  "summary": {
    "total_audited": 0,
    "accurate": 0,
    "stale": 0,
    "silent": 0,
    "uncertain": 0
  },
  "findings": [
    {
      "file": "src/docket/execution.py",
      "line": 304,
      "title": "Execution.schedule",
      "kind": "python:method",
      "verdict": "stale",
      "summary": "One short sentence explaining the drift.",
      "evidence": "Verbatim excerpt from the code that contradicts (or omits) the contract.",
      "suggested_fix": "One-line suggested change to the docstring or section."
    }
  ]
}
```

Rules for the JSON:

- Include **every** item you audited, with its verdict — including `accurate`
  ones. The post-processor uses this to compute totals and to seed a baseline.
- `kind` is one of: `python:module`, `python:class`, `python:function`,
  `python:method`, `markdown:section`, `markdown:file`.
- For `markdown:section`, set `title` to `"<relative path> ## <heading>"` and
  `line` to the line number of the `##` heading.
- For `python:*`, set `title` to the qualified name (e.g.,
  `Docket.add`, `Worker.run_until_finished`) and `line` to the start of the
  `def`/`class`.
- `evidence` and `suggested_fix` may be empty strings for `accurate` verdicts.
- Output **only** the JSON file. Do not post a comment, write a report, or
  modify any other file in the repository.

## Calibration: a known-stale item to confirm you're catching real drift

Before submitting `audit-output.json`, ensure your audit includes a finding
for `src/docket/execution.py` around the `Execution.schedule` method. Its
docstring says `replace=False` raises an error if the task already exists,
but the code returns the string `"EXISTS"` from the Lua script and the caller
never inspects it (see `Docket.add` and `tests/fundamentals/test_idempotency.py`).
Verdict for that item should be `stale`. If you don't see it, you missed it.

## Be conservative

When you're not sure whether something is `stale` vs `silent` vs `accurate`,
prefer `accurate`. The post-processor only fails CI on `stale` and `silent`
findings, so false positives there are expensive. False negatives (missed
drift) are cheap to fix in a follow-up audit run, but missed drift is cheaper
to surface than a flapping CI gate.
