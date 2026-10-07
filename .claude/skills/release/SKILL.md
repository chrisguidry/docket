---
name: release
description: How docket tags and publishes a release of pydocket or docket-rs - the tag formats, the docket-rs version bump that must merge first, the release-name format, the gh commands, and what to do when a publish fails. Use when the developer asks to cut, prepare, name, or plan a release, to bump a version, or to publish to PyPI or crates.io. Never cut a release unless the developer asks for one, and never write a release's tagline: only a human writes it.
---

# Releasing docket

Each language publishes from its own GitHub release.  A release is
permanent: PyPI and crates.io never accept the same version twice.  So
confirm the tag, the target commit, and the title with the developer before you run
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
repository can create a tag, so the release must come from an admin's account.

## Versions

The developer picks the bump.  A release with only fixes takes a patch version.  A
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

**Only a human writes the tagline.  Never write one, suggest one, or
offer a list to pick from.  Ask the developer for it, and use their words
exactly.**  To help them, tell them what the release contains.

If the developer has not given a tagline, stop and ask.  Do not create
the release with a placeholder or with a tagline from an earlier
conversation that they did not confirm for this release.

## Notes

The body is GitHub's generated notes, with a line or two from the
developer above them, such as a thank-you to a first-time contributor, an
emoji, or a joke.  That line is theirs too: ask for it, and do not write
it.  When the release has a first-time contributor, tell the developer
who it is, so they can thank them.

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
  --notes "<their line>" --generate-notes --notes-start-tag python/v<previous>

gh release create rust/vX.Y.Z --target <full sha> \
  --title "rust/vX.Y.Z - <tagline>" \
  --notes "<their line>" --generate-notes --notes-start-tag rust/v<previous>
```

With both `--notes` and `--generate-notes`, gh puts their line above the
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
  Tell the developer.  The choices are a manual `cargo publish --locked --package
  docket-rs` from the tagged commit, or a new version of both crates.
