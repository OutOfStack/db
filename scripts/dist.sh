#!/usr/bin/env bash
# Build release archives and checksums. VERSION and COMMIT identify the release; DIST and PLATFORMS override the
# output directory and space-separated os/arch list. The host platform's binaries are checked after unpacking.
set -euo pipefail

: "${VERSION:?VERSION must name the release, e.g. v0.14.0}"
: "${COMMIT:?COMMIT must name the source commit}"
DIST=${DIST:-dist}
PLATFORMS=${PLATFORMS:-linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64}
VERSION_PKG=github.com/OutOfStack/db/internal/version
LDFLAGS="-s -w -X $VERSION_PKG.release=$VERSION -X $VERSION_PKG.commit=$COMMIT"
# Preserve repository paths so links between the included documents resolve.
FILES=(
	LICENSE README.md CHANGELOG.md COMPATIBILITY.md SECURITY.md RELEASING.md docs/operations.md
	examples/smoke.txt examples/errors.txt config.server.example.yaml config.client.example.yaml
)

mkdir -p "$DIST"
STAGE=$(mktemp -d)
trap 'rm -rf "$STAGE"' EXIT
ARCHIVES=()

for platform in $PLATFORMS; do
	goos=${platform%/*}
	goarch=${platform#*/}
	ext=
	[[ $goos == windows ]] && ext=.exe
	name=db_${VERSION}_${goos}_${goarch}
	dir=$STAGE/$name
	mkdir -p "$dir"
	echo "==> $name"
	for cmd in db db-cli; do
		CGO_ENABLED=0 GOOS=$goos GOARCH=$goarch \
			go build -trimpath -ldflags "$LDFLAGS" -o "$dir/$cmd$ext" "./cmd/$cmd"
	done
	for file in "${FILES[@]}"; do
		mkdir -p "$dir/$(dirname "$file")"
		cp "$file" "$dir/$file"
	done
	tar -C "$STAGE" -czf "$DIST/$name.tar.gz" "$name"
	ARCHIVES+=("$name.tar.gz")
done

(
	cd "$DIST"
	if command -v sha256sum >/dev/null; then
		sha256sum -- "${ARCHIVES[@]}" >SHA256SUMS
	else
		shasum -a 256 -- "${ARCHIVES[@]}" >SHA256SUMS
	fi
)

host=db_${VERSION}_$(go env GOOS)_$(go env GOARCH)
if [[ -d $STAGE/$host ]]; then
	check=$STAGE/check
	mkdir -p "$check"
	tar -C "$check" -xzf "$DIST/$host.tar.gz"
	for cmd in db db-cli; do
		release=$("$check/$host/$cmd" -version | awk '$1 == "release:" { print $2 }')
		if [[ $release != "$VERSION" ]]; then
			echo "dist FAILED: $host/$cmd reports '$release', want '$VERSION'" >&2
			exit 1
		fi
	done
	echo "==> $host binaries report $VERSION"
fi

echo "==> artifacts in $DIST:"
cat "$DIST/SHA256SUMS"
