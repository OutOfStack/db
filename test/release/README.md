# Release verification

Black-box checks that run against an extracted release archive rather than the working tree. They are Go tests behind
the `release` build tag, so `go test ./...` skips them (except the resource monitor's own unit tests). They start the
archive's `db` and `db-cli`, talk to them only through the CLI and raw TCP, and import nothing from the implementation;
their RESP codec is written independently of `internal/protocol`.

They need Linux, Go, bash, GNU coreutils and tar, run from a checkout:

```bash
make release-verify                     # build a host archive in dist/verify/ and check it, with a 30-second soak
make release-verify SOAK_SECONDS=10800  # the three-hour v1 RC soak
make release-check                      # lint, go test, vet, race, the image and release-verify
```

To check a published archive, download it with its checksums and run the checks from a checkout of the same tag:

```bash
gh release download vX.Y.Z --repo OutOfStack/db --pattern '*linux_amd64.tar.gz' --pattern SHA256SUMS
test/release/run.sh /absolute/path/db_vX.Y.Z_linux_amd64.tar.gz [soak-seconds]
```

[`run.sh`](run.sh) verifies the archive against `SHA256SUMS`, extracts it, checks both binaries' `-version` against
`VERSION` when set, runs the checks with `RELEASE_ROOT` pointing at the extracted archive, runs the archive's restore
drill, then the soak, and with `IMAGE` set the container smoke test using the archive's CLI. Example files and scripts
come from the archive; the frozen v1 fixtures come from the checkout.

| Check | Risk it covers |
|-------|----------------|
| `TestCLIContract` | `smoke.txt` exits 0; each failing command in `errors.txt` reports its own line and code and changes nothing |
| `TestMutationDelivery` | A lost or partial reply, or a truncated request, never makes a direct or pooled client run `INCR` twice |
| `TestLifecycle` | Missing config, lock contention and a wrong engine are refused without touching data; SIGTERM is bounded |
| `TestAbruptDeath` | After SIGKILL under each sync policy, recovery keeps the reported sync watermark and applies nothing twice |
| `TestFrozenFixtures` | The release recovers the frozen v1 data directory exactly and answers the frozen wire corpora |
| `TestCorruptionPreservesEvidence` | Damaged WAL segments and snapshots stop startup and stay byte-identical |
| `TestSoak` | Seeded churn of every mutation type against a shadow map, across WAL rotation, snapshots, restart and restore, connection and message limits, with RSS, descriptors and disk held to absolute and growth budgets |

Limits:

- The partial request is cut at the proxy's upstream side. Short writes from the client's own socket are covered by
  the `internal/network` tests.
- SIGKILL is a process crash. It cannot simulate power loss or the kernel dropping dirty pages.
- The resource budgets detect sustained growth above them, not arbitrarily small leaks, and are not a performance
  target. The 30-second soak checks workload coverage, not long-term stability.
- A failing check blocks the release's archives and image tags, not the GitHub tag or release page already created.
