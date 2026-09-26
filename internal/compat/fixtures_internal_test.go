package compat

import (
	"path/filepath"

	"github.com/OutOfStack/db/internal/datadir"
	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/wal"
)

// Fixture layout. Paths are part of the contract too: the reader tests below and any future generator address the same
// files, so a v1.x build cannot quietly read a different set.
const (
	goldenDir              = "testdata/golden/v1"
	requestsFile           = "wire/requests.resp"
	repliesFile            = "wire/replies.resp"
	operationsRequestsFile = "wire/operations-requests.resp"
	operationsRepliesFile  = "wire/operations-replies.resp"
	dataDir                = "data"
)

func goldenPath(name string) string {
	return filepath.Join(filepath.FromSlash(goldenDir), filepath.FromSlash(name))
}

// goldenRequest is one recorded request frame.
type goldenRequest struct {
	cmd  string
	args []string
}

// wireSet is one pair of recorded request and reply streams. Each file is its list's frames concatenated, so the reader
// tests both decode it and re-encode it byte for byte.
type wireSet struct {
	name         string
	requestsFile string
	repliesFile  string
	requests     []goldenRequest
	replies      []protocol.Reply
}

// goldenWireSets lists every frozen wire fixture. A set is only ever added, never regenerated: the operations set froze
// the health commands in addition to the core set, rather than by rewriting it.
var goldenWireSets = []wireSet{ //nolint:gochecknoglobals // the fixture expectations are package-level data by nature
	{name: "core", requestsFile: requestsFile, repliesFile: repliesFile, requests: goldenRequests, replies: goldenReplies},
	{
		name: "operations", requestsFile: operationsRequestsFile, repliesFile: operationsRepliesFile,
		requests: goldenOperationsRequests, replies: goldenOperationsReplies,
	},
}

// goldenRequests is every data and replication command the v1 registry accepts, in the wire form a v1 client emits;
// goldenOperationsRequests covers the health commands.
var goldenRequests = []goldenRequest{ //nolint:gochecknoglobals // the fixture expectations are package-level data by nature
	{cmd: "SET", args: []string{"users", "name", "vlad"}},
	{cmd: "SET", args: []string{"users", "age", "41"}},
	{cmd: "SET", args: []string{"users", "score", "41.5"}},
	{cmd: "SET", args: []string{"users", "active", "true"}},
	{cmd: "SET", args: []string{"users", "tags", "[1,2]"}},
	{cmd: "SET", args: []string{"users", "profile", `{"city":"berlin"}`}},
	{cmd: "SET", args: []string{"users", "quoted", `"41"`}},
	{cmd: "GET", args: []string{"users", "name"}},
	{cmd: "DEL", args: []string{"users", "name"}},
	{cmd: "TYPE", args: []string{"users", "age"}},
	{cmd: "INCR", args: []string{"counters", "hits"}},
	{cmd: "INCR", args: []string{"counters", "hits", "-2"}},
	{cmd: "APPEND", args: []string{"users", "tags", "3"}},
	{cmd: "HSET", args: []string{"users", "profile", "city", "berlin"}},
	{cmd: "HGET", args: []string{"users", "profile", "city"}},
	{cmd: "TABLES", args: nil},
	{cmd: "EXISTS", args: []string{"users"}},
	{cmd: "KEYS", args: []string{"users"}},
	{cmd: "PROMOTE", args: nil},
	{cmd: "REPLICATION", args: []string{"STATUS"}},
	// An argument may carry any bytes: RESP framing is length-prefixed, so whitespace, CRLF and NUL survive it.
	{cmd: "SET", args: []string{"users", "raw", "a b\r\nc\x00d"}},
	{cmd: "SET", args: []string{"users", "empty", ""}},
}

// goldenReplies is every reply shape the server emits, including one error per wire code. The stream in replies.resp is
// these frames concatenated.
var goldenReplies = []protocol.Reply{ //nolint:gochecknoglobals // the fixture expectations are package-level data by nature
	protocol.SimpleString("OK"),
	protocol.SimpleString("int"),
	protocol.BulkString("vlad"),
	protocol.BulkString(""),
	protocol.BulkString("a b\r\nc\x00d"),
	protocol.NullBulkString(),
	protocol.Integer(0),
	protocol.Integer(-1),
	protocol.Integer(9223372036854775807),
	protocol.BulkStringArray(nil),
	protocol.BulkStringArray([]string{"counters", "users"}),
	protocol.Array([]protocol.Reply{protocol.BulkString("users"), protocol.Integer(2), protocol.NullBulkString()}),
	protocol.CodedError(protocol.CodeErr, "unspecified failure"),
	protocol.CodedError(protocol.CodeProtocol, "expected RESP array command"),
	protocol.CodedError(protocol.CodeUnknownCommand, "unknown command: NOPE"),
	protocol.CodedError(protocol.CodeArity, "GET requires 2 arguments: GET <table> <key>"),
	protocol.CodedError(protocol.CodeArgument, "table cannot be empty"),
	protocol.CodedError(protocol.CodeTooLarge, "message size exceeds limit"),
	protocol.CodedError(protocol.CodeWrongType, "wrong type: key holds string, INCR requires int or float"),
	protocol.CodedError(protocol.CodeReadOnly, "readonly"),
	protocol.CodedError(protocol.CodeUnavailable, "storage is in a terminal state"),
}

var goldenOperationsRequests = []goldenRequest{ //nolint:gochecknoglobals // the fixture expectations are package-level data by nature
	{cmd: "PING", args: nil},
	{cmd: "STATUS", args: nil},
}

// goldenOperationsReplies holds PONG, a complete STATUS reply — whose field names, in order, are the frozen STATUS
// contract that TestGoldenStatusFields holds the live reporter to — and the refusal of an over-limit listing.
var goldenOperationsReplies = []protocol.Reply{ //nolint:gochecknoglobals // the fixture expectations are package-level data by nature
	protocol.SimpleString("PONG"),
	protocol.BulkStringArray([]string{
		"ready", "true",
		"state", "ok",
		"error", "",
		"role", "standalone",
		"engine", "in_memory",
		"durability", "everysec",
		"applied_lsn", "16",
		"synced_lsn", "16",
		"snapshot_lsn", "5",
		"last_sync", "2026-09-25T10:00:00Z",
		"last_snapshot", "",
		"release", "v1.0.0",
		"protocol", "RESP2",
		"wal_format", "2",
		"snapshot_format", "3",
		"segment_format", "2",
	}),
	protocol.CodedError(protocol.CodeTooLarge,
		"listing exceeds the 4096-byte reply limit (network.max_message_size); nothing was truncated"),
}

// goldenManifest is the MANIFEST the fixture data directory carries. Its format numbers are written out rather than
// taken from the wal package, because they record what v1.0 wrote, not what this build writes.
var goldenManifest = datadir.Manifest{ //nolint:gochecknoglobals // the fixture expectations are package-level data by nature
	Version: 1,
	Engine:  "in_memory",
	Formats: map[string]int{"wal": 2, "snapshot": 3},
	Sync:    "always",
	Release: "v1.0.0",
}

// goldenSnapshotLSN is the LSN the fixture snapshot carries; the fixture segment continues from it.
const goldenSnapshotLSN = 5

// goldenSnapshotEntries is the state the fixture snapshot holds, in the order it was written. Values are stored in
// their encoded form, which is what the snapshot and the WAL carry.
//
// Between this list and goldenRecords below, every value kind — string, int, float, bool, array, map — is persisted in
// both files. A kind that appears in neither is a kind whose storage encoding these fixtures do not freeze: a
// self-consistent change to it (flipping float byte order, say) would still round-trip in the new build and pass every
// other test in the repository, which is exactly the regression this package exists to catch.
var goldenSnapshotEntries = []struct { //nolint:gochecknoglobals // the fixture expectations are package-level data by nature
	table, key string
	value      protocol.Value
}{
	{table: "counters", key: "hits", value: protocol.IntValue(7)},
	{table: "users", key: "active", value: protocol.BoolValue(true)},
	{table: "users", key: "age", value: protocol.IntValue(41)},
	{table: "users", key: "name", value: protocol.StringValue("vlad")},
	{table: "users", key: "score", value: protocol.FloatValue(41.5)},
}

// goldenRecords is the WAL tail the fixture segment holds, one record per mutating command. Their arguments are already
// encoded, exactly as the storage layer logs them.
var goldenRecords = []wal.Record{ //nolint:gochecknoglobals // the fixture expectations are package-level data by nature
	{LSN: 6, Command: wal.CommandSet, Args: []string{
		"users", "tags", protocol.Encode(protocol.ArrayValue([]protocol.Value{protocol.IntValue(1), protocol.IntValue(2)})),
	}},
	{LSN: 7, Command: wal.CommandDel, Args: []string{"users", "age"}},
	{LSN: 8, Command: wal.CommandIncr, Args: []string{"counters", "hits", protocol.Encode(protocol.IntValue(5))}},
	{LSN: 9, Command: wal.CommandAppend, Args: []string{"users", "tags", protocol.Encode(protocol.IntValue(3))}},
	{LSN: 10, Command: wal.CommandHSet, Args: []string{
		"users", "profile", "city", protocol.Encode(protocol.StringValue("berlin")),
	}},
	// A float and a bool through the WAL as well as the snapshot, plus float arithmetic and a bool nested in a map, so
	// replay exercises the encodings rather than only the loader.
	{LSN: 11, Command: wal.CommandSet, Args: []string{"users", "ratio", protocol.Encode(protocol.FloatValue(-0.5))}},
	{LSN: 12, Command: wal.CommandSet, Args: []string{"users", "beta", protocol.Encode(protocol.BoolValue(false))}},
	{LSN: 13, Command: wal.CommandIncr, Args: []string{"users", "score", protocol.Encode(protocol.FloatValue(0.25))}},
	{LSN: 14, Command: wal.CommandHSet, Args: []string{
		"users", "profile", "verified", protocol.Encode(protocol.BoolValue(false)),
	}},
	{LSN: 15, Command: wal.CommandAppend, Args: []string{"users", "mixed", protocol.Encode(protocol.BoolValue(true))}},
	{LSN: 16, Command: wal.CommandAppend, Args: []string{"users", "mixed", protocol.Encode(protocol.FloatValue(2))}},
}

// goldenState is the state a build must reach after loading the fixture snapshot and replaying the fixture segment,
// sorted by table then key. Each entry is checked twice: the bytes the fixture stored must equal what this build's
// codec encodes for the value (freezing the storage format), and the value must render to the frozen literal (freezing
// the client-facing syntax).
var goldenState = []struct { //nolint:gochecknoglobals // the fixture expectations are package-level data by nature
	table, key string
	value      protocol.Value
	rendered   string
}{
	{table: "counters", key: "hits", value: protocol.IntValue(12), rendered: "12"},
	{table: "users", key: "active", value: protocol.BoolValue(true), rendered: "true"},
	{table: "users", key: "beta", value: protocol.BoolValue(false), rendered: "false"},
	{
		table: "users", key: "mixed",
		value:    protocol.ArrayValue([]protocol.Value{protocol.BoolValue(true), protocol.FloatValue(2)}),
		rendered: "[true,2.0]",
	},
	{table: "users", key: "name", value: protocol.StringValue("vlad"), rendered: "vlad"},
	{
		table: "users", key: "profile",
		value: protocol.MapValue(map[string]protocol.Value{
			"city":     protocol.StringValue("berlin"),
			"verified": protocol.BoolValue(false),
		}),
		rendered: `{"city":"berlin","verified":false}`,
	},
	{table: "users", key: "ratio", value: protocol.FloatValue(-0.5), rendered: "-0.5"},
	// 41.5 from the snapshot plus a 0.25 float INCR on replay: the arithmetic, not just the encoding, is frozen.
	{table: "users", key: "score", value: protocol.FloatValue(41.75), rendered: "41.75"},
	{
		table: "users", key: "tags",
		value: protocol.ArrayValue([]protocol.Value{
			protocol.IntValue(1), protocol.IntValue(2), protocol.IntValue(3),
		}),
		rendered: "[1,2,3]",
	},
}
