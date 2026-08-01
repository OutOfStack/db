package storage_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/storage"
	"github.com/OutOfStack/db/internal/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func exec(t *testing.T, store *storage.Storage, cmd string, args ...string) protocol.Reply {
	t.Helper()
	reply, err := store.Execute(context.Background(), cmd, args)
	require.NoError(t, err, "%s %v", cmd, args)
	return reply
}

func execErr(t *testing.T, store *storage.Storage, cmd string, args ...string) error {
	t.Helper()
	_, err := store.Execute(context.Background(), cmd, args)
	require.Error(t, err, "%s %v", cmd, args)
	return err
}

func TestTypedSetGetType(t *testing.T) {
	t.Parallel()
	store := storage.New(engine.New())

	tests := []struct {
		literal  string
		wantType string
		wantGet  string
	}{
		{"vlad", "string", "vlad"},
		{`"42"`, "string", "42"},
		{"hello world", "string", "hello world"},
		{"42", "int", "42"},
		{"-7", "int", "-7"},
		{"42.5", "float", "42.5"},
		{"1", "int", "1"},
		{"true", "bool", "true"},
		{"[1,2,3]", "array", "[1,2,3]"},
		{`{"a":1,"b":"x"}`, "map", `{"a":1,"b":"x"}`},
	}

	for i, tt := range tests {
		key := fmt.Sprintf("k%d", i)
		exec(t, store, "SET", "t", key, tt.literal)
		assert.Equal(t, protocol.SimpleString(tt.wantType), exec(t, store, "TYPE", "t", key), "TYPE of %s", tt.literal)
		assert.Equal(t, protocol.BulkString(tt.wantGet), exec(t, store, "GET", "t", key), "GET of %s", tt.literal)
	}
}

func TestSetRejectsMalformedLiteral(t *testing.T) {
	t.Parallel()
	store := storage.New(engine.New())

	err := execErr(t, store, "SET", "t", "k", `{"a":`)
	assert.Contains(t, err.Error(), "invalid value literal")
	_, getErr := store.Execute(context.Background(), "GET", []string{"t", "k"})
	require.ErrorIs(t, getErr, storage.ErrNotFound)
}

func TestIncr(t *testing.T) {
	t.Parallel()
	store := storage.New(engine.New())

	assert.Equal(t, protocol.BulkString("1"), exec(t, store, "INCR", "t", "hits"))
	assert.Equal(t, protocol.BulkString("11"), exec(t, store, "INCR", "t", "hits", "10"))
	assert.Equal(t, protocol.BulkString("9"), exec(t, store, "INCR", "t", "hits", "-2"))
	assert.Equal(t, protocol.SimpleString("int"), exec(t, store, "TYPE", "t", "hits"))

	assert.Equal(t, protocol.BulkString("9.5"), exec(t, store, "INCR", "t", "hits", "0.5"))
	assert.Equal(t, protocol.SimpleString("float"), exec(t, store, "TYPE", "t", "hits"))

	assert.Equal(t, protocol.BulkString("10.5"), exec(t, store, "INCR", "t", "hits", "1"))

	exec(t, store, "SET", "t", "big", "9223372036854775807")
	require.ErrorContains(t, execErr(t, store, "INCR", "t", "big"), "overflow")
	assert.Equal(t, protocol.BulkString("9223372036854775807"), exec(t, store, "GET", "t", "big"),
		"a rejected increment must not change the value")

	assert.ErrorContains(t, execErr(t, store, "INCR", "t", "hits", "abc"), "must be int or float")
}

func TestAppend(t *testing.T) {
	t.Parallel()
	store := storage.New(engine.New())

	assert.Equal(t, protocol.Integer(1), exec(t, store, "APPEND", "t", "list", "1"))
	assert.Equal(t, protocol.Integer(2), exec(t, store, "APPEND", "t", "list", "two"))
	assert.Equal(t, protocol.Integer(3), exec(t, store, "APPEND", "t", "list", `{"a":true}`))

	assert.Equal(t, protocol.SimpleString("array"), exec(t, store, "TYPE", "t", "list"))
	assert.Equal(t, protocol.BulkString(`[1,"two",{"a":true}]`), exec(t, store, "GET", "t", "list"))
}

func TestHSetHGet(t *testing.T) {
	t.Parallel()
	store := storage.New(engine.New())

	exec(t, store, "HSET", "t", "user", "name", "vlad")
	exec(t, store, "HSET", "t", "user", "age", "42")
	exec(t, store, "HSET", "t", "user", "age", "43") // overwrite

	assert.Equal(t, protocol.SimpleString("map"), exec(t, store, "TYPE", "t", "user"))
	assert.Equal(t, protocol.BulkString("vlad"), exec(t, store, "HGET", "t", "user", "name"))
	assert.Equal(t, protocol.BulkString("43"), exec(t, store, "HGET", "t", "user", "age"))
	assert.Equal(t, protocol.BulkString(`{"age":43,"name":"vlad"}`), exec(t, store, "GET", "t", "user"))

	_, err := store.Execute(context.Background(), "HGET", []string{"t", "user", "nope"})
	require.ErrorIs(t, err, storage.ErrNotFound)
}

func TestWrongTypeMatrix(t *testing.T) {
	t.Parallel()

	values := map[string]string{
		"string": "vlad",
		"int":    "42",
		"float":  "42.5",
		"bool":   "true",
		"array":  "[1]",
		"map":    `{"a":1}`,
	}
	commands := []struct {
		cmd    string
		args   []string
		accept []string
	}{
		{"INCR", nil, []string{"int", "float"}},
		{"APPEND", []string{"x"}, []string{"array"}},
		{"HSET", []string{"f", "x"}, []string{"map"}},
		{"HGET", []string{"f"}, []string{"map"}},
	}

	for _, command := range commands {
		for kind, literal := range values {
			t.Run(command.cmd+"/"+kind, func(t *testing.T) {
				t.Parallel()
				store := storage.New(engine.New())
				exec(t, store, "SET", "t", "k", literal)

				args := append([]string{"t", "k"}, command.args...)
				_, err := store.Execute(context.Background(), command.cmd, args)
				if slices.Contains(command.accept, kind) {
					if err != nil {
						require.ErrorIs(t, err, storage.ErrNotFound)
					}
					return
				}
				require.ErrorIs(t, err, storage.ErrWrongType)
				assert.Contains(t, err.Error(), "key holds "+kind)
				assert.Contains(t, err.Error(), command.cmd+" requires")
			})
		}
	}
}

func TestConcurrentIncrIsAtomic(t *testing.T) {
	t.Parallel()
	store := storage.New(engine.New())

	const n = 200
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			_, err := store.Execute(context.Background(), "INCR", []string{"t", "hits"})
			assert.NoError(t, err)
		})
	}
	wg.Wait()

	assert.Equal(t, protocol.BulkString(strconv.Itoa(n)), exec(t, store, "GET", "t", "hits"))
}

func TestReplayReproducesState(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var lsn uint64
	var records []wal.Record
	log := &fakeWAL{append: func(_ context.Context, command string, args []string) (uint64, error) {
		mu.Lock()
		defer mu.Unlock()
		lsn++
		records = append(records, wal.Record{LSN: lsn, Command: command, Args: args})
		return lsn, nil
	}}

	live := engine.New()
	store := storage.New(live, storage.WithWAL(log))

	exec(t, store, "SET", "t", "name", "vlad")
	exec(t, store, "SET", "t", "cfg", `{"a":[1,2]}`)
	exec(t, store, "SET", "t", "gone", "1")
	exec(t, store, "DEL", "t", "gone")
	exec(t, store, "INCR", "t", "hits", "5")
	exec(t, store, "INCR", "t", "hits", "0.5")
	exec(t, store, "APPEND", "t", "list", "x")
	exec(t, store, "APPEND", "t", "list", "2")
	exec(t, store, "HSET", "t", "user", "name", "vlad")
	exec(t, store, "HSET", "t", "user", "n", "1")
	exec(t, store, "SET", "t", "big", "9223372036854775807")
	// Every mutation is logged before it is applied, so each of these failures is already durable when it fails. Replay
	// has to reproduce the failure, not abort on it.
	execErr(t, store, "INCR", "t", "name")              // wrong type
	execErr(t, store, "DEL", "t", "missing")            // missing key
	execErr(t, store, "INCR", "t", "big")               // int64 overflow
	execErr(t, store, "APPEND", "t", "deep", tooDeep()) // nesting past what decodes
	require.Len(t, records, 15, "every mutation is logged, including the failing ones")

	replayed := engine.New()
	for _, record := range records {
		encoded, err := wal.EncodeRecord(record)
		require.NoError(t, err, "record %d", record.LSN)
		decoded, err := wal.ReadRecord(bufio.NewReader(bytes.NewReader(encoded)))
		require.NoError(t, err, "record %d", record.LSN)
		require.NoError(t, storage.ApplyReplay(context.Background(), replayed, decoded.Command, decoded.Args))
	}

	assert.Equal(t, snapshot(live), snapshot(replayed))
}

func snapshot(source storage.SnapshotSource) map[string]string {
	state := make(map[string]string)
	source.Range(func(table, key, value string) bool {
		state[table+"/"+key] = value
		return true
	})
	return state
}

// tooDeep returns a literal nested as deeply as the codec accepts: storing it is fine, but wrapping it in one more
// array or map is not.
func tooDeep() string {
	const depth = 64
	return strings.Repeat("[", depth) + "1" + strings.Repeat("]", depth)
}

// TestAppendAndHSetRejectUnreadableNesting pins the rule that nothing is stored that cannot be read back: APPEND and
// HSET add a level of nesting, and the value that would exceed the codec's depth is refused instead of silently turning
// the key into an opaque string.
func TestAppendAndHSetRejectUnreadableNesting(t *testing.T) {
	t.Parallel()

	t.Run("append", func(t *testing.T) {
		t.Parallel()
		store := storage.New(engine.New())

		exec(t, store, "APPEND", "t", "list", "keeper")
		err := execErr(t, store, "APPEND", "t", "list", tooDeep())
		require.ErrorIs(t, err, storage.ErrTooDeep)
		// The refused append left the array exactly as it was.
		assert.Equal(t, protocol.SimpleString("array"), exec(t, store, "TYPE", "t", "list"))
		assert.Equal(t, protocol.BulkString(`["keeper"]`), exec(t, store, "GET", "t", "list"))
	})

	t.Run("hset", func(t *testing.T) {
		t.Parallel()
		store := storage.New(engine.New())

		exec(t, store, "HSET", "t", "user", "name", "vlad")
		err := execErr(t, store, "HSET", "t", "user", "deep", tooDeep())
		require.ErrorIs(t, err, storage.ErrTooDeep)
		assert.Equal(t, protocol.SimpleString("map"), exec(t, store, "TYPE", "t", "user"))
		assert.Equal(t, protocol.BulkString(`{"name":"vlad"}`), exec(t, store, "GET", "t", "user"))
	})

	t.Run("a value at the limit still stores", func(t *testing.T) {
		t.Parallel()
		store := storage.New(engine.New())

		exec(t, store, "SET", "t", "k", tooDeep())
		assert.Equal(t, protocol.SimpleString("array"), exec(t, store, "TYPE", "t", "k"))
	})
}

// TestIncrOverflowReplaysAsNoOp is the recovery half of the overflow guard: the failing INCR is already in the log, so
// replaying it must leave the counter at its ceiling rather than abort recovery.
func TestIncrOverflowReplaysAsNoOp(t *testing.T) {
	t.Parallel()

	recovered := engine.New()
	records := []wal.Record{
		{LSN: 1, Command: wal.CommandSet, Args: []string{"t", "big", protocol.Encode(protocol.IntValue(math.MaxInt64))}},
		{LSN: 2, Command: wal.CommandIncr, Args: []string{"t", "big", protocol.Encode(protocol.IntValue(1))}},
	}
	for _, record := range records {
		require.NoError(t, storage.ApplyReplay(context.Background(), recovered, record.Command, record.Args))
	}

	store := storage.New(recovered)
	assert.Equal(t, protocol.BulkString("9223372036854775807"), exec(t, store, "GET", "t", "big"))
}
