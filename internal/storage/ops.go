package storage

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/wal"
)

var (
	// ErrWrongType is returned when a typed operation meets a value of another type. It reaches the client as "ERR wrong
	// type: ...".
	ErrWrongType = errors.New("wrong type")
	// ErrOverflow ErrNotFinite and ErrTooDeep are the other ways the command semantics refuse a value: arithmetic that
	// leaves the int64 range or the finite floats, and nesting the codec could not read back.
	ErrOverflow  = errors.New("increment would overflow int64")
	ErrNotFinite = errors.New("increment result is not a finite number")
	ErrTooDeep   = errors.New("value nests too deep to be stored")
)

const replyOK = "OK"

// Apply mutates eng for a WAL command. Live writes, recovery, and replication share this path; value arguments are
// already encoded.
func Apply(ctx context.Context, eng Engine, cmd string, args []string) (protocol.Reply, error) {
	switch cmd {
	case wal.CommandSet:
		if err := eng.Set(ctx, args[0], args[1], args[2]); err != nil {
			return protocol.Reply{}, err
		}
		return protocol.SimpleString(replyOK), nil
	case wal.CommandDel:
		if err := eng.Del(ctx, args[0], args[1]); err != nil {
			return protocol.Reply{}, err
		}
		return protocol.SimpleString(replyOK), nil
	case wal.CommandIncr:
		return applyIncr(ctx, eng, args)
	case wal.CommandAppend:
		return applyAppend(ctx, eng, args)
	case wal.CommandHSet:
		return applyHSet(ctx, eng, args)
	default:
		return protocol.Reply{}, fmt.Errorf("unsupported command %q", cmd)
	}
}

// ApplyReplay treats deterministic no-ops as success so they do not abort recovery or replication.
func ApplyReplay(ctx context.Context, eng Engine, cmd string, args []string) error {
	_, err := Apply(ctx, eng, cmd, args)
	if rejected(err) {
		return nil
	}
	return err
}

// rejected reports whether err is the command semantics refusing a value rather than storage failing. A refusal is
// deterministic — the record was logged before it failed, and replaying it fails identically — so recovery and
// replication have to treat it as the no-op it already was. A storage failure must still stop them.
func rejected(err error) bool {
	return errors.Is(err, engine.ErrNotFound) ||
		errors.Is(err, ErrWrongType) ||
		errors.Is(err, ErrOverflow) ||
		errors.Is(err, ErrNotFinite) ||
		errors.Is(err, ErrTooDeep)
}

func applyIncr(ctx context.Context, eng Engine, args []string) (protocol.Reply, error) {
	delta := protocol.Decode(args[2])
	var result protocol.Value
	err := eng.Update(ctx, args[0], args[1], func(old string, exists bool) (string, error) {
		current := protocol.IntValue(0)
		if exists {
			current = protocol.Decode(old)
		}
		sum, aErr := add(current, delta)
		if aErr != nil {
			return "", aErr
		}
		result = sum
		return protocol.Encode(sum), nil
	})
	if err != nil {
		return protocol.Reply{}, err
	}
	return protocol.BulkString(protocol.Render(result)), nil
}

func applyAppend(ctx context.Context, eng Engine, args []string) (protocol.Reply, error) {
	element := protocol.Decode(args[2])
	var length int
	err := eng.Update(ctx, args[0], args[1], func(old string, exists bool) (string, error) {
		var items []protocol.Value
		if exists {
			current := protocol.Decode(old)
			if current.Kind != protocol.KindArray {
				return "", wrongType(wal.CommandAppend, current.Kind, protocol.KindArray)
			}
			items = current.Array
		}
		items = append(items, element)
		length = len(items)
		return encodeStorable(protocol.ArrayValue(items))
	})
	if err != nil {
		return protocol.Reply{}, err
	}
	return protocol.Integer(int64(length)), nil
}

func applyHSet(ctx context.Context, eng Engine, args []string) (protocol.Reply, error) {
	value := protocol.Decode(args[3])
	err := eng.Update(ctx, args[0], args[1], func(old string, exists bool) (string, error) {
		fields := make(map[string]protocol.Value, 1)
		if exists {
			current := protocol.Decode(old)
			if current.Kind != protocol.KindMap {
				return "", wrongType(wal.CommandHSet, current.Kind, protocol.KindMap)
			}
			fields = current.Map
		}
		fields[args[2]] = value
		return encodeStorable(protocol.MapValue(fields))
	})
	if err != nil {
		return protocol.Reply{}, err
	}
	return protocol.SimpleString(replyOK), nil
}

// encodeStorable encodes a value only if it reads back as the value it was. APPEND and HSET wrap their argument in one
// more level of nesting, which can push the result past the depth the codec will decode; storing it would turn the key
// into an opaque string on the very next read.
func encodeStorable(v protocol.Value) (string, error) {
	encoded := protocol.Encode(v)
	if protocol.Decode(encoded).Kind != v.Kind {
		return "", ErrTooDeep
	}
	return encoded, nil
}

// add sums two numbers, keeping int arithmetic exact: int + int stays an int unless it would overflow, and any float
// operand makes the result a float.
func add(current, delta protocol.Value) (protocol.Value, error) {
	if !numeric(current) {
		return protocol.Value{}, wrongType(wal.CommandIncr, current.Kind, protocol.KindInt)
	}
	if !numeric(delta) {
		return protocol.Value{}, wrongType(wal.CommandIncr, delta.Kind, protocol.KindInt)
	}
	if current.Kind == protocol.KindInt && delta.Kind == protocol.KindInt {
		sum := current.Int + delta.Int
		// Signed overflow wraps silently in Go: a counter that would pass math.MaxInt64 must fail loudly instead of turning
		// negative.
		if (delta.Int > 0 && sum < current.Int) || (delta.Int < 0 && sum > current.Int) {
			return protocol.Value{}, ErrOverflow
		}
		return protocol.IntValue(sum), nil
	}
	sum := asFloat(current) + asFloat(delta)
	if math.IsNaN(sum) || math.IsInf(sum, 0) {
		return protocol.Value{}, ErrNotFinite
	}
	return protocol.FloatValue(sum), nil
}

func numeric(v protocol.Value) bool {
	return v.Kind == protocol.KindInt || v.Kind == protocol.KindFloat
}

func asFloat(v protocol.Value) float64 {
	if v.Kind == protocol.KindInt {
		return float64(v.Int)
	}
	return v.Float
}

func wrongType(cmd string, got, want protocol.Kind) error {
	if cmd == wal.CommandIncr {
		return fmt.Errorf("%w: key holds %s, %s requires int or float", ErrWrongType, got, cmd)
	}
	return fmt.Errorf("%w: key holds %s, %s requires %s", ErrWrongType, got, cmd, want)
}
