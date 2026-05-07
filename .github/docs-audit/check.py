"""Post-process the docs-audit JSON written by Claude Code.

Reads ``audit-output.json``, prints a human-readable report, writes a markdown
summary to ``$GITHUB_STEP_SUMMARY`` if set, writes a sticky-comment body to
``audit-comment.md`` in the working directory, and exits non-zero when any
finding has a ``stale`` or ``silent`` verdict.

The non-zero exit makes the workflow show as failed on the PR — a red X on
the docs-audit check. The maintainer chooses whether to add it as a required
check in branch protection.

Usage:
    python .github/docs-audit/check.py audit-output.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

Verdict = Literal["accurate", "stale", "silent", "uncertain"]
FAILING: frozenset[Verdict] = frozenset({"stale", "silent"})


@dataclass(slots=True)
class Finding:
    item_id: str
    file: str
    line: int
    title: str
    kind: str
    verdict: Verdict
    summary: str
    evidence: str
    suggested_fix: str


def make_item_id(file: str, line: int, title: str) -> str:
    return hashlib.sha1(f"{file}|{line}|{title}".encode()).hexdigest()[:16]


def parse_findings(payload: Any) -> list[Finding]:
    findings: list[Finding] = []
    if not isinstance(payload, dict):
        return findings
    payload_dict = cast(dict[str, Any], payload)
    raw_list = payload_dict.get("findings", [])
    if not isinstance(raw_list, list):
        return findings
    typed_list = cast(list[Any], raw_list)
    for raw in typed_list:
        if not isinstance(raw, dict):
            continue
        finding = _to_finding(cast(dict[str, Any], raw))
        if finding is not None:
            findings.append(finding)
    findings.sort(key=lambda f: (f.file, f.line, f.title))
    return findings


def _to_finding(raw: dict[str, Any]) -> Finding | None:
    file = str(raw.get("file", ""))
    title = str(raw.get("title", ""))
    if not file or not title:
        return None
    verdict_str = str(raw.get("verdict", "uncertain"))
    valid: tuple[Verdict, ...] = ("accurate", "stale", "silent", "uncertain")
    verdict: Verdict = "uncertain"
    for v in valid:
        if verdict_str == v:
            verdict = v
            break
    line = int(raw.get("line", 0) or 0)
    return Finding(
        item_id=make_item_id(file, line, title),
        file=file,
        line=line,
        title=title,
        kind=str(raw.get("kind", "")),
        verdict=verdict,
        summary=str(raw.get("summary", "")).strip(),
        evidence=str(raw.get("evidence", "")).strip(),
        suggested_fix=str(raw.get("suggested_fix", "")).strip(),
    )


def render_human(findings: list[Finding]) -> str:
    flagged = [f for f in findings if f.verdict in FAILING]
    counts = _counts(findings)
    lines = [
        f"Docs audit: {len(findings)} item(s) audited "
        f"(accurate={counts['accurate']}, stale={counts['stale']}, "
        f"silent={counts['silent']}, uncertain={counts['uncertain']}).",
    ]
    if not flagged:
        lines.append("\nNo drift found.")
        return "\n".join(lines)
    lines.append(f"\nFlagged: {len(flagged)} item(s).\n")
    for f in flagged:
        lines.append(f"  {f.file}:{f.line}  [{f.verdict}]  {f.title}")
        if f.summary:
            lines.append(f"    {f.summary}")
        if f.evidence:
            lines.append(f"    evidence: {_truncate(f.evidence, 300)}")
        if f.suggested_fix:
            lines.append(f"    fix: {f.suggested_fix}")
        lines.append("")
    return "\n".join(lines)


def render_markdown(findings: list[Finding]) -> str:
    flagged = [f for f in findings if f.verdict in FAILING]
    counts = _counts(findings)
    out: list[str] = []
    out.append("## Docs audit\n")
    out.append(
        f"Audited **{len(findings)}** item(s) — "
        f"accurate {counts['accurate']}, stale {counts['stale']}, "
        f"silent {counts['silent']}, uncertain {counts['uncertain']}."
    )
    if not flagged:
        out.append("\n✅ No drift found.\n")
        return "\n".join(out)
    out.append(f"\n❌ **{len(flagged)} flagged item(s).**\n")
    out.append("| File | Line | Verdict | Title | Summary |")
    out.append("|---|---|---|---|---|")
    for f in flagged:
        title = _md_escape(f.title)
        summary = _md_escape(f.summary).replace("\n", " ")
        out.append(
            f"| `{f.file}` | {f.line} | **{f.verdict}** | `{title}` | {summary} |"
        )
    out.append("")
    out.append("<details><summary>Suggested fixes</summary>\n")
    for f in flagged:
        if not f.suggested_fix:
            continue
        out.append(f"- **`{_md_escape(f.title)}`** — {_md_escape(f.suggested_fix)}")
    out.append("\n</details>")
    return "\n".join(out)


def _counts(findings: list[Finding]) -> dict[str, int]:
    return {
        verdict: sum(1 for f in findings if f.verdict == verdict)
        for verdict in ("accurate", "stale", "silent", "uncertain")
    }


def _md_escape(text: str) -> str:
    return text.replace("|", "\\|").replace("`", "\\`")


def _truncate(text: str, n: int) -> str:
    return text if len(text) <= n else text[: n - 1] + "…"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "input", type=Path, help="Path to audit-output.json from the action."
    )
    parser.add_argument(
        "--comment-out",
        type=Path,
        default=Path("audit-comment.md"),
        help="Path to write the sticky-comment body. Defaults to audit-comment.md.",
    )
    args = parser.parse_args(argv)

    if not args.input.exists():
        print(f"audit output {args.input} not found.", file=sys.stderr)
        return 0

    payload = json.loads(args.input.read_text())
    findings = parse_findings(payload)

    print(render_human(findings))

    markdown = render_markdown(findings)

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a") as fh:
            fh.write(markdown)

    args.comment_out.write_text(markdown)

    flagged = [f for f in findings if f.verdict in FAILING]
    if flagged:
        print(f"\nFAIL: {len(flagged)} flagged finding(s).")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
