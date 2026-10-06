//go:build release && linux

package release_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	soakBatch       = 64  // mutations per connection
	soakKeys        = 128 // a stable key space, so growth is not mistaken for live data
	soakArrayLimit  = 16  // arrays are reset at this length for the same reason
	checkpointEvery = 8   // batches between shadow-map comparisons
	soakPause       = 100 * time.Millisecond
	reportInterval  = time.Minute
	warmup          = time.Minute
	window          = time.Minute
)

// TestSoak runs seeded churn of every mutation type against one supported-mode server for RELEASE_SOAK and compares a
// client-side shadow map at checkpoints. The first and last checkpoints restart and restore the data directory; one
// process stays alive between them so repeated restarts cannot conceal a leak. Resource use is sampled after every
// batch against the budgets in newSoak.
func TestSoak(t *testing.T) {
	duration, seed := soakSettings(t)
	st := newSoak(t, seed)
	st.s.start()
	st.limits()
	deadline := st.started.Add(duration)
	for time.Now().Before(deadline) || st.batches < checkpointEvery {
		st.step()
		time.Sleep(soakPause)
	}
	st.verify()
	if st.checkpoints == 0 || len(st.segments) < 2 {
		t.Fatalf("soak did not cross a restore and WAL rotation: %d checkpoints, %d segments", st.checkpoints,
			len(st.segments))
	}
	for _, op := range []string{"SET", "DEL", "INCR", "APPEND", "HSET"} {
		if st.counts[op] == 0 {
			t.Fatalf("soak never ran %s", op)
		}
	}
	st.restoreCheckpoint()
	st.report(map[string]any{"result": "PASS"})
}

func soakSettings(t *testing.T) (time.Duration, uint64) {
	t.Helper()
	value := os.Getenv("RELEASE_SOAK")
	if value == "" {
		t.Skip("set RELEASE_SOAK to a duration to run the soak")
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		t.Fatalf("RELEASE_SOAK=%q is not a positive duration", value)
	}
	seed := uint64(17)
	if value = os.Getenv("RELEASE_SOAK_SEED"); value != "" {
		if seed, err = strconv.ParseUint(value, 10, 64); err != nil {
			t.Fatalf("RELEASE_SOAK_SEED: %v", err)
		}
	}
	if _, err = os.Stat("/proc/self/status"); err != nil {
		t.Fatal("the soak reads process resources from Linux /proc")
	}
	return duration, seed
}

type soak struct {
	t                    *testing.T
	s                    *server
	rng                  *rand.Rand
	seed                 uint64
	monitor              *resourceMonitor
	shadow               map[string]any // string, int64, []int64 or map[string]int64, as the server should render it
	counts               map[string]int
	segments             map[string]bool
	batches, checkpoints int
	started, nextReport  time.Time
}

func newSoak(t *testing.T, seed uint64) *soak {
	t.Helper()
	const mib = 1 << 20
	now := time.Now()
	return &soak{
		t: t, s: newServer(t, serverConfig{Policy: "everysec", Snapshot: "2s"}), seed: seed,
		rng: rand.New(rand.NewPCG(seed, seed)),
		monitor: newResourceMonitor(
			usage{"rss_bytes": 64 * mib, "disk_bytes": 32 * mib, "descriptors": 64},
			usage{"rss_bytes": 8 * mib, "disk_bytes": 8 * mib, "descriptors": 8},
			warmup, window, now),
		shadow: map[string]any{}, counts: map[string]int{}, segments: map[string]bool{},
		started: now, nextReport: now.Add(reportInterval),
	}
}

func (st *soak) step() {
	st.t.Helper()
	st.mutate()
	st.batches++
	segments, _ := filepath.Glob(filepath.Join(st.s.data, "wal-*.log"))
	for _, path := range segments {
		st.segments[filepath.Base(path)] = true
	}
	current := resourceUsage(st.t, st.s.pid(), st.s.data)
	if err := st.monitor.sample(current, time.Now()); err != nil {
		st.t.Fatal(err)
	}
	if st.batches%checkpointEvery != 0 {
		return
	}
	st.verify()
	if st.checkpoints == 0 {
		st.restoreCheckpoint()
		st.monitor.reset(time.Now())
	}
	st.checkpoints++
	if now := time.Now(); st.checkpoints == 1 || now.After(st.nextReport) {
		st.report(map[string]any{"usage": current})
		st.nextReport = now.Add(reportInterval)
	}
}

type sender func(args ...string) any

func (st *soak) mutate() {
	st.t.Helper()
	c := dial(st.t, st.s.addr)
	defer c.Close()
	send := func(args ...string) any {
		reply := c.must(st.t, args...)
		if refusal, isErr := reply.(respError); isErr {
			st.t.Fatalf("%s failed: %s", args[0], refusal)
		}
		st.counts[args[0]]++
		return reply
	}
	for index := range soakBatch {
		st.churn(send)
		slot := strconv.Itoa(st.rng.IntN(8))
		switch index % 3 {
		case 0:
			st.incr(send, "counter-"+slot)
		case 1:
			st.appendItem(send, "array-"+slot)
		default:
			st.hset(send, "map-"+slot)
		}
	}
}

func (st *soak) churn(send sender) {
	key := fmt.Sprintf("key-%03d", st.rng.IntN(soakKeys))
	if st.rng.IntN(5) == 0 {
		send("DEL", "soak", key)
		delete(st.shadow, key)
		return
	}
	value := fmt.Sprintf("%d-%d-%s", st.batches, st.rng.IntN(1_000_000), strings.Repeat("x", 8192))
	if reply := send("SET", "soak", key, value); reply != "OK" {
		st.t.Fatalf("SET = %#v", reply)
	}
	st.shadow[key] = value
}

func (st *soak) incr(send sender, key string) {
	delta := int64(st.rng.IntN(11) - 5)
	n, _ := st.shadow[key].(int64)
	n += delta
	st.shadow[key] = n
	if got := atoi(st.t, send("INCR", "soak", key, strconv.FormatInt(delta, 10))); got != n {
		st.t.Fatalf("INCR %s = %d, want %d", key, got, n)
	}
}

func (st *soak) appendItem(send sender, key string) {
	items, _ := st.shadow[key].([]int64)
	if len(items) >= soakArrayLimit {
		send("DEL", "soak", key)
		items = nil
	}
	item := int64(st.rng.IntN(1000))
	items = append(items, item)
	st.shadow[key] = items
	if got := send("APPEND", "soak", key, strconv.FormatInt(item, 10)); got != int64(len(items)) {
		st.t.Fatalf("APPEND %s = %#v, want %d", key, got, len(items))
	}
}

func (st *soak) hset(send sender, key string) {
	field, value := "field-"+strconv.Itoa(st.rng.IntN(4)), int64(st.rng.IntN(1000))
	send("HSET", "soak", key, field, strconv.FormatInt(value, 10))
	fields, ok := st.shadow[key].(map[string]int64)
	if !ok {
		fields = map[string]int64{}
		st.shadow[key] = fields
	}
	fields[field] = value
}

// verify compares the server's soak table with the shadow map over one connection.
func (st *soak) verify() {
	st.t.Helper()
	c := dial(st.t, st.s.addr)
	defer c.Close()
	keys := slices.Sorted(maps.Keys(st.shadow))
	if got := strs(st.t, c.must(st.t, "KEYS", "soak")); !slices.Equal(got, keys) {
		st.t.Fatalf("soak key set differs: got %d keys, want %d", len(got), len(keys))
	}
	for _, key := range keys {
		if got, want := c.must(st.t, "GET", "soak", key), st.render(st.shadow[key]); got != want {
			st.t.Fatalf("state differs for %s: got %#v, want %q", key, got, want)
		}
	}
	if status := st.s.status(); status["ready"] != "true" || status["state"] != "ok" {
		st.t.Fatalf("unhealthy status: %v", status)
	}
}

// render is the literal GET returns: strings as stored, other values as compact JSON with sorted map keys.
func (st *soak) render(value any) string {
	if s, ok := value.(string); ok {
		return s
	}
	data, err := json.Marshal(value)
	if err != nil {
		st.t.Fatal(err)
	}
	return string(data)
}

// limits fills the connection limit, requires one more connection to be refused, and requires an oversized message
// to be rejected without being applied.
func (st *soak) limits() {
	t := st.t
	t.Helper()
	conns := make([]*conn, 0, maxConnections)
	for range maxConnections {
		c := dial(t, st.s.addr)
		conns = append(conns, c)
		if reply := c.must(t, "PING"); reply != "PONG" {
			t.Fatalf("connection below the limit got %#v", reply)
		}
	}
	extra := dial(t, st.s.addr)
	_ = extra.SetDeadline(time.Now().Add(ioTimeout))
	_, _ = extra.Write(encode("PING"))
	buf := make([]byte, 64)
	n, err := extra.Read(buf)
	_ = extra.Close()
	for _, c := range conns {
		_ = c.Close()
	}
	if netErr, ok := errors.AsType[net.Error](err); (ok && netErr.Timeout()) || (n > 0 && buf[0] != '-') {
		t.Fatalf("connection limit not enforced: read %q, %v", buf[:n], err)
	}
	eventually(t, st.s.answers)

	reply := st.s.call("SET", "soak", "too-large", strings.Repeat("x", (maxMessageKB+6)*1024))
	if refusal, isErr := reply.(respError); !isErr || !strings.HasPrefix(string(refusal), "TOOLARGE ") {
		t.Fatalf("oversized SET = %#v, want TOOLARGE", reply)
	}
	if got := st.s.call("GET", "soak", "too-large"); got != (null{}) {
		t.Fatal("oversized mutation was applied")
	}
}

// restoreCheckpoint restarts cleanly, then restores a full copy of the stopped server's data directory.
func (st *soak) restoreCheckpoint() {
	t := st.t
	t.Helper()
	eventually(t, func() bool { return atoi(t, st.s.status()["snapshot_lsn"]) > 0 })
	st.s.stop(false)
	st.s.start()
	st.verify()
	st.s.stop(false)
	backup := st.s.data + ".backup"
	copyDir(t, st.s.data, backup)
	if err := os.RemoveAll(st.s.data); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(backup, st.s.data); err != nil {
		t.Fatal(err)
	}
	st.s.start()
	st.verify()
	st.limits()
}

func (st *soak) report(fields map[string]any) {
	fields["elapsed_s"] = time.Since(st.started).Round(100 * time.Millisecond).Seconds()
	fields["seed"], fields["batches"], fields["checkpoints"] = st.seed, st.batches, st.checkpoints
	fields["peaks"], fields["baseline"], fields["mutations"] = st.monitor.peak, st.monitor.baseline, st.counts
	fields["wal_segments_seen"] = len(st.segments)
	line, err := json.Marshal(fields)
	if err != nil {
		st.t.Fatal(err)
	}
	st.t.Log(string(line))
}

func resourceUsage(t *testing.T, pid int, data string) usage {
	t.Helper()
	var rss int64
	for line := range strings.Lines(readFile(t, fmt.Sprintf("/proc/%d/status", pid))) {
		if rest, ok := strings.CutPrefix(line, "VmRSS:"); ok {
			kb, err := strconv.ParseInt(strings.Fields(rest)[0], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			rss = kb * 1024
		}
	}
	var disk int64
	// Snapshot pruning can remove a sealed WAL segment while the directory is walked; a vanished file counts as zero.
	err := filepath.WalkDir(data, func(_ string, d fs.DirEntry, walkErr error) error {
		if errors.Is(walkErr, fs.ErrNotExist) {
			return nil
		}
		if walkErr != nil || d.IsDir() {
			return walkErr
		}
		info, err := d.Info()
		switch {
		case errors.Is(err, fs.ErrNotExist):
			return nil
		case err != nil:
			return err
		}
		disk += info.Size()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	fds, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
	if err != nil {
		t.Fatal(err)
	}
	return usage{"rss_bytes": rss, "disk_bytes": disk, "descriptors": int64(len(fds))}
}
