#!/usr/bin/env bash
# Offline backup and restore drill. It runs the procedure docs/operations.md documents against the real binaries and
# fails unless every step behaves as documented:
#
#   1. populate a durable server so its data directory holds a snapshot and a WAL tail after it;
#   2. stop it cleanly (SIGTERM, exit status 0) and copy the complete data directory;
#   3. wipe the original, and restore the copy into a new, empty directory;
#   4. check that a server configured for the other engine refuses the restored directory and leaves it untouched;
#   5. start the right server over it and check the logical contents match what was there before the backup.
#
# Usage: scripts/restore-drill.sh (after `make build`). BIN overrides the binary directory, DRILL_PORT the port.
set -euo pipefail

BIN=${BIN:-bin}
PORT=${DRILL_PORT:-37223}
ADDR=127.0.0.1:$PORT
WORK=$(mktemp -d)
SERVER_PID=

cleanup() {
	if [[ -n $SERVER_PID ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
		kill -KILL "$SERVER_PID" 2>/dev/null || true
	fi
	rm -rf "$WORK"
}
trap cleanup EXIT

fail() {
	echo "restore drill FAILED: $*" >&2
	if [[ -f $WORK/server.log ]]; then
		echo "--- server log ---" >&2
		cat "$WORK/server.log" >&2
	fi
	exit 1
}

step() { echo "==> $*"; }

cli() { "$BIN/db-cli" -address "$ADDR" "$@"; }

# write_config <file> <engine> <data dir>
write_config() {
	local file=$1 engine=$2 dir=$3
	if [[ $engine == in_memory ]]; then
		cat >"$file" <<EOF
engine:
  type: in_memory
wal:
  enabled: true
  data_dir: "$dir"
  sync: always
  segment_size: 1
  snapshot_interval: 1s
network:
  address: "$ADDR"
  max_message_size: 64
logging:
  level: warn
  output: "$WORK/server.log"
EOF
	else
		cat >"$file" <<EOF
engine:
  type: tiered
  data_dir: "$dir"
network:
  address: "$ADDR"
logging:
  level: warn
  output: "$WORK/server.log"
EOF
	fi
}

start_server() {
	"$BIN/db" -config "$1" &
	SERVER_PID=$!
	for _ in $(seq 100); do
		if echo PING | cli -q 2>/dev/null; then
			return 0
		fi
		kill -0 "$SERVER_PID" 2>/dev/null || fail "server exited during startup"
		sleep 0.1
	done
	fail "server did not answer PING"
}

stop_server() {
	kill -TERM "$SERVER_PID"
	local status=0
	wait "$SERVER_PID" || status=$?
	SERVER_PID=
	[[ $status -eq 0 ]] || fail "server exited with status $status on SIGTERM; the directory is not a clean backup"
}

# status_field <name> prints one field of the STATUS reply, which the CLI renders one element per line.
status_field() {
	echo STATUS | cli | awk -v want="$1" 'previous == want { print; exit } { previous = $0 }'
}

[[ -x $BIN/db && -x $BIN/db-cli ]] || fail "build the binaries first (make build)"

DATA=$WORK/data
RESTORED=$WORK/restored
write_config "$WORK/server.yaml" in_memory "$DATA"

step "populating a durable server"
start_server "$WORK/server.yaml"
{
	echo "SET users name 'Ada Lovelace'"
	echo "SET users age 36"
	echo "SET users ratio 0.5"
	echo "SET users active true"
	echo "SET users tags '[\"math\",\"engines\"]'"
	echo "SET users zip '\"01234\"'"
	echo "HSET users profile city London"
	echo "HSET users profile born 1815"
	for i in $(seq 1 300); do
		echo "SET bulk key-$i value-$i"
	done
	for i in $(seq 1 20); do
		echo "INCR stats hits $i"
		echo "APPEND stats log $i"
	done
} | cli -q || fail "populating the server"

# Wait for a snapshot, then write more, so the backup holds a snapshot and a WAL tail that recovery replays over it.
for _ in $(seq 50); do
	[[ -n $(status_field last_snapshot) ]] && break
	sleep 0.1
done
[[ -n $(status_field last_snapshot) ]] || fail "no snapshot was written"
printf '%s\n' "DEL bulk key-1" "INCR stats hits 1000" "SET after snapshot yes" | cli -q || fail "writing the WAL tail"

# Every read the verification compares, generated from what was written.
{
	echo TABLES
	for table in users bulk stats after; do
		echo "KEYS $table"
	done
	for key in name age ratio active tags zip profile; do
		echo "TYPE users $key"
		echo "GET users $key"
	done
	for i in $(seq 1 300); do
		echo "GET bulk key-$i"
	done
	echo "GET stats hits"
	echo "GET stats log"
	echo "GET after snapshot"
} >"$WORK/verify.txt"
cli <"$WORK/verify.txt" >"$WORK/before.out" || fail "reading the dataset before the backup"
grep -qx 'not found' "$WORK/before.out" || fail "the deleted key should read as not found"

step "stopping cleanly and copying the data directory"
stop_server
ls "$DATA"/snapshot-*.db >/dev/null || fail "the data directory holds no snapshot"
ls "$DATA"/wal-*.log >/dev/null || fail "the data directory holds no WAL segment"
grep -q '"engine": "in_memory"' "$DATA/MANIFEST" || fail "the manifest does not name the engine"
cp -a "$DATA" "$WORK/backup"

step "wiping the original and restoring into an empty directory"
rm -rf "$DATA"
mkdir "$RESTORED"
cp -a "$WORK/backup/." "$RESTORED/"

step "checking a server for the other engine refuses the restored directory"
write_config "$WORK/tiered.yaml" tiered "$RESTORED"
if timeout 10 "$BIN/db" -config "$WORK/tiered.yaml"; then
	fail "the tiered engine started over an in_memory data directory"
fi
diff -r "$WORK/backup" "$RESTORED" >/dev/null || fail "the refused start modified the restored directory"

step "starting over the restored directory and verifying its contents"
write_config "$WORK/restored.yaml" in_memory "$RESTORED"
start_server "$WORK/restored.yaml"
cli <"$WORK/verify.txt" >"$WORK/after.out" || fail "reading the restored dataset"
diff -u "$WORK/before.out" "$WORK/after.out" || fail "the restored dataset differs from the backed-up one"
[[ $(status_field ready) == true ]] || fail "STATUS does not report the restored server ready"
[[ $(status_field state) == ok ]] || fail "STATUS does not report the restored server healthy"
[[ $(status_field snapshot_lsn) != 0 ]] || fail "STATUS reports no recovered snapshot"
cli -q <examples/smoke.txt || fail "the smoke file failed against the restored server"
stop_server

echo "restore drill passed: $(wc -l <"$WORK/verify.txt") reads matched after backup, wipe and restore"
