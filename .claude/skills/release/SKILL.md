---
name: release
description: How docket tags and publishes a release of pydocket or docket-rs - the tag formats, the docket-rs version bump that must merge first, the release-name style, the gh commands, and what to do when a publish fails. Use when I ask to cut, prepare, name, or plan a release, to bump a version, or to publish to PyPI or crates.io. Never cut a release unless I ask for one.
---

# Releasing docket

Each language publishes from its own GitHub release.  A release is
permanent: PyPI and crates.io never accept the same version twice.  So
confirm the tag, the target commit, and the title with me before you run
`gh release create`.

## Tags

| Package | Tag | Publish workflow | Registry |
|---|---|---|---|
| pydocket | `python/vX.Y.Z` | `.github/workflows/publish.yml` | PyPI |
| docket-rs and docket-rs-macros | `rust/vX.Y.Z` | `.github/workflows/publish-rust.yml` | crates.io |

Releases before the monorepo have bare tags, like `0.26.0`.  Do not make
a new bare tag: `publish.yml` treats one as a pydocket release.

Both publish workflows start on every new release.  Each one checks the
tag prefix and skips the other language's release.  Only an admin of the
repository can create a tag, so the release must come from my account.

## Versions

I pick the bump.  A release with only fixes takes a patch version.  A
release that adds features or breaks an API takes a minor version, because
both packages are still 0.x.

**pydocket** takes its version from the tag.  hatch-vcs runs
`git describe --match python/v* --match [0-9]*`, so a `rust/v` tag on the
same commit does not change it.  Nothing needs a bump before the release.

**docket-rs** takes its version only from `rust/Cargo.toml`.  Cargo cannot
read a version from a tag, and `publish-rust.yml` refuses a tag that does
not match the manifest.  So a docket-rs release needs a bump PR, merged
first, that changes:

- `version` in `[workspace.package]` in `rust/Cargo.toml`
- the exact pin `docket-rs-macros = { version = "=X.Y.Z", ... }` in
  `rust/docket/Cargo.toml`
- `rust/Cargo.lock`: run `cargo metadata --format-version 1 >/dev/null`
  in `rust/` to update it
- the install lines (`docket-rs = "X.Y"`) in `rust/docket/README.md`

Before you open the bump PR, run these in `rust/`:

```bash
cargo check --locked --workspace --all-features
cargo publish --dry-run --locked --package docket-rs-macros
```

The dry run of `docket-rs` itself fails until the new macros version is on
crates.io, so do not run it.

## Release names

The title is the tag, a hyphen, and a tagline:
`python/v0.26.1 - Papers, please`, `rust/v0.1.0 - Crabtastic`.

A tagline is a pun or a pop-culture reference about what the release does.
"Papers, please" released Redis credential providers, and "Crabtastic" was
the first docket-rs.  Others: "Survive and thrive", "Half a million
reasons", "Burn After Redising", "cron is a flat circcle", "Perpetual: The
Next Generation".

A plain summary like "Fixes for reused task keys" is the wrong style.
Read the titles of the last 20 releases (`gh release list --limit 20`),
then offer me about five taglines for each release.  I pick.

## Notes

The body is GitHub's generated notes, with a line or two from me above
them: a thank-you to a first-time contributor, an emoji, or a joke.  Ask
me for that line, and suggest a thank-you when the release has a new
contributor.

GitHub starts the generated notes at the previous release of any
language.  Always pass `--notes-start-tag` with the previous tag of the
same language, or the notes miss PRs or list the wrong ones.

## Cut the release

When both languages release from one commit, make two releases on that
commit.  GitHub marks the newer one as "Latest", so make the other one
first with `--latest=false`.

```bash
gh release create python/vX.Y.Z --target <full sha> \
  --title "python/vX.Y.Z - <tagline>" \
  --notes "<my line>" --generate-notes --notes-start-tag python/v<previous>

gh release create rust/vX.Y.Z --target <full sha> \
  --title "rust/vX.Y.Z - <tagline>" \
  --notes "<my line>" --generate-notes --notes-start-tag rust/v<previous>
```

With both `--notes` and `--generate-notes`, gh puts my line above the
generated notes.

Each publish workflow runs the full CI for its language and the Prek
workflow, then publishes with trusted publishing.  Watch the runs with a
background agent, as the `monitor-pr` skill describes, not by polling from
the main session.  When they finish, check the registry:

```bash
curl -s https://pypi.org/pypi/pydocket/json | jq -r .info.version
curl -s https://crates.io/api/v1/crates/docket-rs | jq -r .crate.max_version
```

## When a publish fails

A release runs the workflow file from its tagged commit, so a rerun uses
the same file.

- **The failure is in the code or the workflow.**  Fix it on `main`.  Then
  delete the release and its tag, and make them again on the fixed commit.
  Nothing reached the registry, so the version is still free.
- **The failure is flaky.**  Rerun the failed jobs once with
  `gh run rerun <run id> --failed`.
- **docket-rs-macros published, then docket-rs failed.**  A rerun fails at
  once, because crates.io refuses the macros version that already exists.
  Tell me.  The choices are a manual `cargo publish --locked --package
  docket-rs` from the tagged commit, or a new version of both crates.
