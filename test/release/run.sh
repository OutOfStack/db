#!/usr/bin/env bash
# Verify a release archive against SHA256SUMS beside it, extract it, and run this checkout's black-box checks against
# its binaries, example files and scripts. Usage: test/release/run.sh <archive.tar.gz> [soak-seconds, default 30].
# VERSION, when set, is the release both binaries must report; IMAGE adds the container smoke test for that image.
set -euo pipefail
ARCHIVE=$(realpath "${1:?usage: test/release/run.sh archive.tar.gz [soak-seconds]}")
SOAK_SECONDS=${2:-30}
[[ $SOAK_SECONDS =~ ^[0-9]+$ ]] && ((SOAK_SECONDS >= 1 && SOAK_SECONDS <= 14400)) || {
	echo 'soak-seconds must be between 1 and 14400' >&2
	exit 1
}
REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

name=$(basename "$ARCHIVE")
expected=$(awk -v name="$name" '$2 == name || $2 == "*" name { print $1 }' "$(dirname "$ARCHIVE")/SHA256SUMS")
actual=$(sha256sum "$ARCHIVE" | awk '{ print $1 }')
[[ -n $expected && $expected != *$'\n'* && $actual == "$expected" ]] || {
	echo "FAIL: $name checksum is missing, duplicated or incorrect in SHA256SUMS" >&2
	exit 1
}
tar -xzf "$ARCHIVE" -C "$WORK" --no-same-owner
roots=("$WORK"/*)
[[ ${#roots[@]} -eq 1 && -d ${roots[0]} ]] || { echo 'FAIL: archive must contain one root directory' >&2; exit 1; }
ROOT=${roots[0]}

# A caller's DB_* configuration must not change the subject of a release check.
for variable in ${!DB_@}; do unset "$variable"; done
for command in db db-cli; do
	identity=$("$ROOT/$command" -version)
	printf '%s\n' "$identity"
	if [[ -n ${VERSION:-} ]]; then
		release=$(awk '$1 == "release:" { print $2 }' <<<"$identity")
		[[ $release == "$VERSION" ]] || { echo "FAIL: $command release is $release, expected $VERSION" >&2; exit 1; }
	fi
done

cd "$REPO"
export RELEASE_ROOT=$ROOT
go test -tags release -count=1 -v -timeout 15m -skip '^TestSoak$' ./test/release
(cd "$ROOT" && timeout --kill-after=10s 2m env BIN="$ROOT" bash scripts/restore-drill.sh)
RELEASE_SOAK=${SOAK_SECONDS}s go test -tags release -count=1 -v -timeout "$((SOAK_SECONDS + 300))s" \
	-run '^TestSoak$' ./test/release
if [[ -n ${IMAGE:-} ]]; then
	(cd "$ROOT" && timeout --kill-after=10s 3m env BIN="$ROOT" bash scripts/container-smoke.sh "$IMAGE")
fi
echo 'PASS: release archive verification'
