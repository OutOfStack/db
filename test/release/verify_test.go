//go:build release && linux

package release_test

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"maps"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

var failureLine = regexp.MustCompile(`(?m)^line (\d+): ([A-Z]+) .+$`)

// TestCLIContract runs the shipped example files: smoke.txt exits 0, and every failing command in errors.txt reports
// its own line number and error code without changing the database.
func TestCLIContract(t *testing.T) {
	s := newServer(t, serverConfig{})
	s.start()
	if res := s.cli(readFile(t, filepath.Join(releaseRoot(t), "examples", "smoke.txt")), ""); res.code != 0 {
		t.Fatalf("smoke.txt exit %d: %s", res.code, res.stderr)
	}

	ex := loadErrorsExample(t)
	if res := s.cli(strings.Join(ex.setup, "\n")+"\n", ""); res.code != 0 {
		t.Fatalf("errors.txt setup exit %d: %s", res.code, res.stderr)
	}
	before := logicalState(t, s.addr)
	// Blank the setup lines so the reported line numbers still match the file while only failing commands run.
	failing := make([]string, len(ex.lines))
	for i, line := range ex.lines {
		if !slices.Contains(ex.setup, line) {
			failing[i] = line
		}
	}
	assertFailures(t, s.cli(strings.Join(failing, "\n")+"\n", ""), ex.want)
	if !reflect.DeepEqual(logicalState(t, s.addr), before) {
		t.Fatal("rejected commands changed the database")
	}
	// The complete shipped file, setup included, reports the same diagnostics.
	assertFailures(t, s.cli(strings.Join(ex.lines, "\n")+"\n", ""), ex.want)
}

// errorsExample is examples/errors.txt split into its setup commands and its failing commands' expected diagnostics.
type errorsExample struct {
	lines []string
	setup []string
	want  []string // "<line number> <code>" for every failing command, in file order
}

func loadErrorsExample(t *testing.T) errorsExample {
	t.Helper()
	codes := map[string]string{
		"SET users name": "ARITY", "GET users name extra": "ARITY", "UNKNOWN foo": "UNKNOWNCMD",
		`GET users ""`: "ARGUMENT", `SET types broken '{"a":'`: "ARGUMENT", "INCR stats hits abc": "ARGUMENT",
		"INCR errors list": "WRONGTYPE", "HGET errors text field": "WRONGTYPE",
		"REPLICATION STATUS": "UNAVAILABLE", "PROMOTE": "UNAVAILABLE",
	}
	setup := []string{"SET errors text hello", "APPEND errors list 1"}
	text := readFile(t, filepath.Join(releaseRoot(t), "examples", "errors.txt"))
	ex := errorsExample{lines: strings.Split(strings.TrimRight(text, "\n"), "\n")}
	seen := map[string]int{}
	for i, line := range ex.lines {
		switch {
		case strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#"):
			continue
		case slices.Contains(setup, line):
			ex.setup = append(ex.setup, line)
		case codes[line] != "":
			ex.want = append(ex.want, fmt.Sprintf("%d %s", i+1, codes[line]))
		default:
			t.Fatalf("errors.txt line %d has no expected outcome: %q", i+1, line)
		}
		seen[line]++
	}
	// Each case must appear exactly once, so a duplicate cannot stand in for a case that was dropped.
	for _, line := range slices.Concat(setup, slices.Collect(maps.Keys(codes))) {
		if seen[line] != 1 {
			t.Fatalf("errors.txt holds %q %d times, want exactly once", line, seen[line])
		}
	}
	return ex
}

func assertFailures(t *testing.T, res result, want []string) {
	t.Helper()
	matches := failureLine.FindAllStringSubmatch(res.stderr, -1)
	got := make([]string, 0, len(matches))
	for _, match := range matches {
		got = append(got, match[1]+" "+match[2])
	}
	if res.code != 1 || !slices.Equal(got, want) || strings.Count(res.stderr, "\n") != len(want) {
		t.Fatalf("exit %d, diagnostics %q, want exit 1 and %q; stderr:\n%s", res.code, got, want, res.stderr)
	}
}

// logicalState is every key's type and rendered value.
func logicalState(t *testing.T, addr string) map[string][2]any {
	t.Helper()
	c := dial(t, addr)
	defer c.Close()
	state := map[string][2]any{}
	for _, table := range strs(t, c.must(t, "TABLES")) {
		for _, key := range strs(t, c.must(t, "KEYS", table)) {
			state[table+"/"+key] = [2]any{c.must(t, "TYPE", table, key), c.must(t, "GET", table, key)}
		}
	}
	return state
}

// TestMutationDelivery proves a client never transparently executes one INCR twice, directly or through a pool, when
// the reply is lost or cut short, or when the request reaches the server incomplete.
func TestMutationDelivery(t *testing.T) {
	for _, pooled := range []bool{false, true} {
		for _, mode := range []string{lostReply, partialReply, partialRequest} {
			t.Run(fmt.Sprintf("pooled=%t/%s", pooled, mode), func(t *testing.T) {
				checkMutationDelivery(t, pooled, mode)
			})
		}
	}
}

func checkMutationDelivery(t *testing.T, pooled bool, mode string) {
	t.Helper()
	s := newServer(t, serverConfig{})
	s.start()
	if reply := s.call("SET", "t", "counter", "0"); reply != "OK" {
		t.Fatalf("SET: %#v", reply)
	}
	proxy := startProxy(t, s.addr, mode)
	config := filepath.Join(t.TempDir(), "client.yaml")
	writeFile(t, config, fmt.Sprintf(`network:
  address: %[1]q
  idle_timeout: 1s
  max_message_size: 64
pool:
  enabled: %[2]t
  selection_strategy: master_first
  max_retries: 3
  retry_delay: 10ms
  failure_timeout: 1ms
  servers:
    - address: %[1]q
      role: master
`, proxy.addr, pooled))

	res := s.cli("INCR t counter\n", config)
	if commands := proxy.close(t); commands != 1 {
		t.Fatalf("client transparently retried a mutation: %d commands reached the proxy", commands)
	}
	if res.code != 1 || !strings.Contains(res.stderr, "outcome unknown") {
		t.Fatalf("exit %d, stderr %q; want exit 1 and an unknown outcome", res.code, res.stderr)
	}
	want := "1"
	if mode == partialRequest {
		want = "0"
	}
	if got := s.call("GET", "t", "counter"); got != want {
		t.Fatalf("counter = %#v, want %q", got, want)
	}
}

// TestLifecycle covers missing configuration, directory-lock contention, the SIGTERM bound with an incomplete client
// frame, and an engine that does not match the data directory. No refused start may change the data.
func TestLifecycle(t *testing.T) {
	work := t.TempDir()
	refused(t, filepath.Join(work, "missing.yaml"), work)

	s := newServer(t, serverConfig{})
	s.start()
	s.call("SET", "t", "key", "kept")
	before := hashes(t, s.data)
	contender := newServer(t, serverConfig{Data: s.data})
	if out := refused(t, contender.config, contender.work); !strings.Contains(strings.ToLower(out), "lock") {
		t.Fatalf("contending server output does not mention the lock: %s", out)
	}
	if !maps.Equal(hashes(t, s.data), before) {
		t.Fatal("a refused contender changed the data directory")
	}
	// An idle client holding an incomplete frame must not hold shutdown hostage.
	idle := dial(t, s.addr)
	_, _ = idle.Write([]byte("*3\r\n$3\r\nSET\r\n"))
	s.stop(false)
	_ = idle.Close()

	before = hashes(t, s.data)
	wrong := newServer(t, serverConfig{Data: s.data, Engine: "tiered"})
	if out := refused(t, wrong.config, wrong.work); !strings.Contains(strings.ToLower(out), "tiered") {
		t.Fatalf("wrong-engine output does not name the engine: %s", out)
	}
	if !maps.Equal(hashes(t, s.data), before) {
		t.Fatal("a refused wrong-engine start changed the data directory")
	}
	s.start()
	if got := s.call("GET", "t", "key"); got != "kept" {
		t.Fatalf("GET after restart = %#v", got)
	}
}

// TestAbruptDeath kills the server under every sync policy. Recovery keeps at least the reported sync watermark and
// never applies a counter increment twice. SIGKILL is a process crash: it cannot drop the kernel's page cache.
func TestAbruptDeath(t *testing.T) {
	for _, policy := range []string{"always", "everysec", "no"} {
		t.Run(policy, func(t *testing.T) {
			s := newServer(t, serverConfig{Policy: policy, Snapshot: "1s"})
			s.start()
			incr := func(want int64) {
				if got := atoi(t, s.call("INCR", "crash", "counter")); got != want {
					t.Fatalf("INCR = %d, want %d", got, want)
				}
			}
			for i := range int64(40) {
				incr(i + 1)
			}
			// Every policy must establish a nonzero watermark, 'no' through a snapshot.
			eventually(t, func() bool { return atoi(t, s.status()["synced_lsn"]) >= 40 })
			for i := range int64(40) {
				incr(i + 41)
			}
			watermark := atoi(t, s.status()["synced_lsn"])
			if policy == "always" && watermark != 80 {
				t.Fatalf("synced_lsn = %d under always, want 80", watermark)
			}
			s.stop(true)
			s.start()
			value := atoi(t, s.call("GET", "crash", "counter"))
			if value < watermark || value > 80 {
				t.Fatalf("recovered counter %d outside [%d, 80]", value, watermark)
			}
			if ready := s.status()["ready"]; ready != "true" {
				t.Fatalf("ready = %q after recovery", ready)
			}
			incr(value + 1)
		})
	}
}

// TestFrozenFixtures recovers a copy of the frozen v1 data directory and replays the frozen wire corpora.
func TestFrozenFixtures(t *testing.T) {
	t.Run("data", checkFrozenData)
	t.Run("wire", checkFrozenWire)
}

func checkFrozenData(t *testing.T) {
	golden := goldenDir(t)
	data := filepath.Join(t.TempDir(), "data")
	copyDir(t, filepath.Join(golden, "data"), data)
	s := newServer(t, serverConfig{Data: data, Policy: "everysec"})
	s.start()

	state := map[[2]string]string{
		{"counters", "hits"}: "12", {"users", "active"}: "true", {"users", "beta"}: "false",
		{"users", "mixed"}: "[true,2.0]", {"users", "name"}: "vlad",
		{"users", "profile"}: `{"city":"berlin","verified":false}`, {"users", "ratio"}: "-0.5",
		{"users", "score"}: "41.75", {"users", "tags"}: "[1,2,3]",
	}
	if got := strs(t, s.call("TABLES")); !slices.Equal(got, []string{"counters", "users"}) {
		t.Fatalf("TABLES = %q", got)
	}
	for _, table := range []string{"counters", "users"} {
		var keys []string
		for id := range state {
			if id[0] == table {
				keys = append(keys, id[1])
			}
		}
		slices.Sort(keys)
		if got := strs(t, s.call("KEYS", table)); !slices.Equal(got, keys) {
			t.Fatalf("KEYS %s = %q, want %q", table, got, keys)
		}
	}
	for id, want := range state {
		if got := s.call("GET", id[0], id[1]); got != want {
			t.Fatalf("GET %s %s = %#v, want %q", id[0], id[1], got, want)
		}
	}
	if got := s.call("GET", "users", "age"); got != (null{}) {
		t.Fatalf("GET users age = %#v, want null", got)
	}
	if lsn := s.status()["applied_lsn"]; lsn != "16" {
		t.Fatalf("applied_lsn = %q, want 16", lsn)
	}

	c := dial(t, s.addr)
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(ioTimeout))
	if _, err := c.Write([]byte(readFile(t, filepath.Join(golden, "wire", "operations-requests.resp")))); err != nil {
		t.Fatal(err)
	}
	frozen := bufio.NewReader(strings.NewReader(readFile(t, filepath.Join(golden, "wire", "operations-replies.resp"))))
	if got, want := mustDecode(t, c.r), mustDecode(t, frozen); !reflect.DeepEqual(got, want) {
		t.Fatalf("operations reply = %#v, want %#v", got, want)
	}
	// STATUS values change from run to run; its field names and their order are frozen.
	if got, want := strs(t, mustDecode(t, c.r)), strs(t, mustDecode(t, frozen)); !slices.Equal(evens(got), evens(want)) {
		t.Fatalf("STATUS fields = %q, want %q", evens(got), evens(want))
	}
}

func evens(items []string) []string {
	out := make([]string, 0, (len(items)+1)/2)
	for i := 0; i < len(items); i += 2 {
		out = append(out, items[i])
	}
	return out
}

// checkFrozenWire sends every request of the frozen request corpus. It and the reply corpus are independent shape
// corpora, not paired command results, so each reply is checked only for success or the documented refusal.
func checkFrozenWire(t *testing.T) {
	s := newServer(t, serverConfig{})
	s.start()
	c := dial(t, s.addr)
	defer c.Close()
	requests := bufio.NewReader(strings.NewReader(readFile(t, filepath.Join(goldenDir(t), "wire", "requests.resp"))))
	for {
		if _, err := requests.Peek(1); errors.Is(err, io.EOF) {
			break
		}
		command := strs(t, mustDecode(t, requests))
		reply := c.must(t, command...)
		refusal, isErr := reply.(respError)
		switch command[0] {
		case "PROMOTE", "REPLICATION":
			if !isErr || !strings.HasPrefix(string(refusal), "UNAVAILABLE ") {
				t.Fatalf("%q = %#v, want UNAVAILABLE", command, reply)
			}
		default:
			if isErr {
				t.Fatalf("%q failed: %s", command, refusal)
			}
		}
	}
	if got := s.call("GET", "users", "raw"); got != "a b\r\nc\x00d" {
		t.Fatalf("GET users raw = %#v", got)
	}
	if got := s.call("GET", "users", "empty"); got != "" {
		t.Fatalf("GET users empty = %#v", got)
	}
}

// TestCorruptionPreservesEvidence damages WAL segments and snapshots. Startup must fail and leave every file as it was;
// only an incomplete record at the end of the newest segment is repaired.
func TestCorruptionPreservesEvidence(t *testing.T) {
	source := newServer(t, serverConfig{})
	source.start()
	for i := range 3 {
		source.call("SET", "t", strconv.Itoa(i), fmt.Sprintf("payload-%d", i))
	}
	source.stop(false)
	segment := filepath.Base(onlyFile(t, filepath.Join(source.data, "wal-*.log")))
	original := []byte(readFile(t, filepath.Join(source.data, segment)))

	walCases := []struct {
		name   string
		damage func(t *testing.T, wal []byte, dir string) []byte
	}{
		{"checksum-middle", func(_ *testing.T, wal []byte, _ string) []byte { return flip(wal, "payload-1") }},
		{"checksum-tail", func(_ *testing.T, wal []byte, _ string) []byte { return flip(wal, "payload-2") }},
		{"header", func(_ *testing.T, wal []byte, _ string) []byte { wal[6] = 255; return wal }},
		{"oversize", func(_ *testing.T, wal []byte, _ string) []byte {
			return bytes.Replace(wal, []byte("*4\r\n"), []byte("*999999999999\r\n"), 1)
		}},
		{"lsn-gap", func(t *testing.T, wal []byte, _ string) []byte {
			t.Helper()
			return lsnGap(t, wal)
		}},
		{"sealed-tail", func(t *testing.T, wal []byte, dir string) []byte {
			t.Helper()
			// A later segment seals this one, so its torn last record is corruption, not a crash tail.
			writeFile(t, filepath.Join(dir, "wal-00000000000000000004.log"), string(wal[:walHeader]))
			return wal[:len(wal)-1]
		}},
	}
	for _, tc := range walCases {
		t.Run("wal/"+tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "data")
			copyDir(t, source.data, dir)
			writeFile(t, filepath.Join(dir, segment), string(tc.damage(t, slices.Clone(original), dir)))
			assertRefusedUnchanged(t, dir)
		})
	}

	t.Run("wal/torn-tail", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "data")
		copyDir(t, source.data, dir)
		path := filepath.Join(dir, segment)
		writeFile(t, path, string(original[:len(original)-1]))
		s := newServer(t, serverConfig{Data: dir})
		s.start()
		if got := s.call("GET", "t", "1"); got != "payload-1" {
			t.Fatalf("GET t 1 = %#v", got)
		}
		if got := s.call("GET", "t", "2"); got != (null{}) {
			t.Fatalf("GET t 2 = %#v, want the torn record dropped", got)
		}
		s.stop(false)
		if !bytes.HasPrefix(original, []byte(readFile(t, path))) {
			t.Fatal("repair rewrote more than the torn tail")
		}
	})

	// A damaged snapshot without the WAL prefix it replaced cannot reconstruct verified state.
	snapshotCases := []struct {
		name   string
		damage func([]byte) []byte
	}{
		{"body", func(b []byte) []byte { b[len(b)/2] ^= 1; return b }},
		{"truncated", func(b []byte) []byte { return b[:len(b)-1] }},
		{"empty", func([]byte) []byte { return nil }},
		{"header", func(b []byte) []byte { b[6] = 255; return b }},
	}
	for _, tc := range snapshotCases {
		t.Run("snapshot/"+tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "data")
			copyDir(t, filepath.Join(goldenDir(t), "data"), dir)
			path := onlyFile(t, filepath.Join(dir, "snapshot-*.db"))
			writeFile(t, path, string(tc.damage([]byte(readFile(t, path)))))
			assertRefusedUnchanged(t, dir)
		})
	}
}

// walHeader is the segment format header: a magic string and a version byte. Each record is an 8-byte big-endian
// LSN, a RESP command and a CRC-32 (IEEE) of both.
const (
	walHeader = 7
	walLSN    = 8
)

func flip(wal []byte, marker string) []byte {
	wal[bytes.Index(wal, []byte(marker))] ^= 1
	return wal
}

// lsnGap renumbers the first record to LSN 99 and recomputes its checksum, so only LSN continuity is broken.
func lsnGap(t *testing.T, wal []byte) []byte {
	t.Helper()
	rest := bytes.NewReader(wal[walHeader+walLSN:])
	r := bufio.NewReader(rest)
	mustDecode(t, r)
	end := len(wal) - rest.Len() - r.Buffered()
	binary.BigEndian.PutUint64(wal[walHeader:], 99)
	binary.BigEndian.PutUint32(wal[end:], crc32.ChecksumIEEE(wal[walHeader:end]))
	return wal
}

func assertRefusedUnchanged(t *testing.T, dir string) {
	t.Helper()
	before := hashes(t, dir)
	s := newServer(t, serverConfig{Data: dir})
	refused(t, s.config, s.work)
	if !maps.Equal(hashes(t, dir), before) {
		t.Fatal("recovery modified corrupt evidence")
	}
}
