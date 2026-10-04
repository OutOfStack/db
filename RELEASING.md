# Releasing

Create releases through GitHub with a `vX.Y.Z` tag targeting `main`. Clicking **Publish release** runs
[the release workflow](.github/workflows/release.yaml), which validates the matching changelog section, repeats the
full CI suite on the tagged commit, and then publishes:

- `db_<tag>_<os>_<arch>.tar.gz` for every platform below, each holding `db`, `db-cli`, the license, the example
  configurations and CLI scripts, and the documentation (README, CHANGELOG, COMPATIBILITY, SECURITY, this file and
  `docs/operations.md`) at its repository paths, so links between documents resolve;
- `SHA256SUMS`, covering every archive;
- the container image `ghcr.io/outofstack/db:<X.Y.Z>` for `linux/amd64` and `linux/arm64`, also tagged `<X.Y>` and
  `latest` unless the version is a pre-release (`v1.0.0-rc.1`).

The archive build checks the host platform's binaries report the tag from `-version`; the container smoke test
checks the image's version too.

Saving a draft or pushing a tag alone does not run the release workflow. Publishing a stable release or prerelease
starts it. The GitHub release is public while CI and packaging run, so assets appear only after those jobs succeed.
The workflow preserves the release title and notes and does not edit repository files.

## Changelog during development

Choose the planned next version in the feature PR, before merging. For example:

```markdown
## [0.14.0] - Unreleased

### Fixed
- A panicking command now closes its connection and fences storage.
```

Other feature PRs intended for the same release add entries to that section. Its comparison link points from the
previous release tag to `HEAD`. The GitHub tag must match the heading: `v0.14.0` for `[0.14.0]`.

No separate release-preparation PR or date-only commit is required. The `Unreleased` date marker records that the
release was pending when the entry was written; GitHub records the actual publication date. When the next feature
PR starts a new version, replace the previous entry's marker with that date and change its comparison link from
`HEAD` to its published tag. Do not change the contents of an already-published tag.

## Platforms

| Platform | Status |
|----------|--------|
| Linux amd64, arm64 | Supported, including durable (WAL or tiered) deployments. The container image is Linux only |
| macOS amd64, arm64 | Built for development and evaluation. Durable deployments are not supported: crash consistency is untested there |
| Windows amd64 | Built for development and evaluation. Durable deployments are not supported: the server does not fsync directories there, and crash consistency is untested |

## Checklist

1. **The release commit is green.** CI on `main` passed at that commit: build, `golangci-lint`, `go test -race ./...`
   (which also reads the frozen golden fixtures in `internal/compat`), the backup and restore drill, and the
   container smoke test. Never regenerate the fixtures of an already-frozen version to make a build pass.
2. **Compatibility.** For a v1.x release, nothing in [COMPATIBILITY.md](COMPATIBILITY.md) changed incompatibly; a
   change that would needs a new major version.
3. **Claims match behavior.** Read the README's Features, Support Boundary and platform statements, and
   [SECURITY.md](SECURITY.md), against the release: no automatic write failover (a pool reroutes writes only after a
   manual `PROMOTE`), what each sync policy guarantees after a crash, what is preview, the trusted-network boundary,
   and the platform table above.
4. **Changelog.** Confirm the feature PR already included a section for the version you will publish. Its date
   may still say `Unreleased`. Copy the section's entries into the GitHub release notes, or write your own notes.
5. **Dry run** (optional): `make dist VERSION=vX.Y.Z` builds the same archives locally into `dist/` and checks the
   host platform's binaries report the version.
6. **Publish through GitHub.** Open **Releases → Draft a new release**, choose **Create new tag** with `vX.Y.Z`,
   and select **main** as the target. Set the title and notes; for a prerelease, use a tag such as `vX.Y.Z-rc.1`
   and select the prerelease option. Save a draft if needed, then click **Publish release** when ready.
   Watch the Release workflow in Actions. It attaches the archives and checksums to this release and publishes the
   image after CI passes. If a job fails, the release remains published; inspect the failure and rerun failed jobs
   when appropriate. Do not move the tag to another commit to repair a source problem.
7. **Verify the published artifacts**, from a clean directory:
   ```bash
   gh release download vX.Y.Z --repo OutOfStack/db
   sha256sum -c SHA256SUMS
   tar -xzf db_vX.Y.Z_linux_amd64.tar.gz
   ./db_vX.Y.Z_linux_amd64/db -version
   ```
   and against the image, with a CLI from the same release on `PATH` as `bin/db-cli` or pointed to by `BIN`:
   ```bash
   VERSION=vX.Y.Z BIN=db_vX.Y.Z_linux_amd64 ./scripts/container-smoke.sh ghcr.io/outofstack/db:X.Y.Z
   ```

A GHCR package is private when first created: after the first release, make `ghcr.io/outofstack/db` public in the
package settings once.

A broken release is never re-tagged. Fix it on `main` and release the next patch version.
