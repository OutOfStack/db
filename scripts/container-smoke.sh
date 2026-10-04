#!/usr/bin/env bash
# Container smoke test. It runs the image the way the README documents — durable WAL on a named volume, a mounted
# config, all container interfaces opted in and the port published on host loopback only — and fails unless:
#
#   1. the server reports the expected release from `-version` (when VERSION is set);
#   2. it starts, answers PING and STATUS, and accepts a write;
#   3. SIGTERM (docker stop) shuts it down with exit status 0;
#   4. it restarts over the same volume and still returns the write.
#
# Usage: scripts/container-smoke.sh <image> (after `make build-cli`). BIN overrides the CLI directory, SMOKE_PORT the
# host port, and VERSION the release the image must report.
set -euo pipefail

IMAGE=${1:?usage: container-smoke.sh <image>}
BIN=${BIN:-bin}
PORT=${SMOKE_PORT:-37224}
ADDR=127.0.0.1:$PORT
NAME=db-smoke-$$
VOLUME=db-smoke-data-$$
WORK=$(mktemp -d)

cleanup() {
	docker rm -f "$NAME" >/dev/null 2>&1 || true
	docker volume rm "$VOLUME" >/dev/null 2>&1 || true
	rm -rf "$WORK"
}
trap cleanup EXIT

fail() {
	echo "container smoke FAILED: $*" >&2
	echo "--- container log ---" >&2
	docker logs "$NAME" >&2 2>&1 || true
	exit 1
}

step() { echo "==> $*"; }

cli() { "$BIN/db-cli" -address "$ADDR" "$@"; }

wait_ready() {
	for _ in $(seq 1 50); do
		if echo PING | cli -q >/dev/null 2>&1; then
			return 0
		fi
		sleep 0.2
	done
	fail "server did not answer PING on $ADDR"
}

stop_cleanly() {
	docker stop --time 20 "$NAME" >/dev/null
	local code
	code=$(docker inspect --format '{{.State.ExitCode}}' "$NAME")
	[[ $code == 0 ]] || fail "SIGTERM shutdown exited with status $code, want 0"
}

if [[ -n ${VERSION:-} ]]; then
	step "check the image reports $VERSION"
	release=$(docker run --rm "$IMAGE" -version | awk '$1 == "release:" { print $2 }')
	[[ $release == "$VERSION" ]] || fail "image reports release '$release', want '$VERSION'"
fi

# The server runs as the image's nonroot user, so the mounted config has to be world-readable.
cat >"$WORK/db.yaml" <<'EOF'
wal:
  enabled: true
  data_dir: data
  sync: always
EOF
chmod 0644 "$WORK/db.yaml"

step "start $IMAGE"
docker volume create "$VOLUME" >/dev/null
docker run -d --name "$NAME" -p "$ADDR:3223" \
	-e DB_ADDRESS=0.0.0.0:3223 -e DB_ALLOW_REMOTE=true \
	-v "$WORK/db.yaml:/home/nonroot/db.yaml:ro" -v "$VOLUME:/home/nonroot/data" \
	"$IMAGE" -config db.yaml >/dev/null
wait_ready

step "query"
echo 'SET smoke greeting hello' | cli -q || fail "SET failed"
[[ $(echo 'GET smoke greeting' | cli) == hello ]] || fail "GET did not return the value just written"
echo STATUS | cli | tr '\n' ' ' | grep -q 'ready true' || fail "STATUS does not report ready"

step "stop with SIGTERM"
stop_cleanly

step "restart and query again"
docker start "$NAME" >/dev/null
wait_ready
[[ $(echo 'GET smoke greeting' | cli) == hello ]] || fail "the write did not survive the restart"
stop_cleanly

step "container smoke passed"
