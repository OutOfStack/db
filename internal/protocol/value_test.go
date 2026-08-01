package protocol_test

import (
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/OutOfStack/db/internal/protocol"
)

func TestValueCodecRoundTrip(t *testing.T) {
	t.Parallel()

	nested := protocol.ArrayValue([]protocol.Value{
		protocol.MapValue(map[string]protocol.Value{
			"id":    protocol.IntValue(-1),
			"score": protocol.FloatValue(0.5),
			"tags":  protocol.ArrayValue([]protocol.Value{protocol.StringValue("a"), protocol.BoolValue(true)}),
		}),
		protocol.ArrayValue(nil),
	})

	tests := []struct {
		name  string
		value protocol.Value
	}{
		{"string", protocol.StringValue("vlad has\nbytes\x00")},
		{"empty string", protocol.StringValue("")},
		{"int", protocol.IntValue(42)},
		{"int min", protocol.IntValue(math.MinInt64)},
		{"int max", protocol.IntValue(math.MaxInt64)},
		{"float", protocol.FloatValue(42.5)},
		{"float smallest", protocol.FloatValue(math.SmallestNonzeroFloat64)},
		{"float max", protocol.FloatValue(math.MaxFloat64)},
		{"bool true", protocol.BoolValue(true)},
		{"bool false", protocol.BoolValue(false)},
		{"empty array", protocol.ArrayValue(nil)},
		{"empty map", protocol.MapValue(nil)},
		{"nested", nested},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := protocol.Decode(protocol.Encode(tt.value))
			if !equalValues(got, tt.value) {
				t.Errorf("Decode(Encode(%v)) = %v", tt.value, got)
			}
		})
	}
}

func TestEncodeIsDeterministic(t *testing.T) {
	t.Parallel()

	first := protocol.MapValue(map[string]protocol.Value{
		"a": protocol.IntValue(1), "b": protocol.IntValue(2), "c": protocol.IntValue(3),
	})
	second := protocol.MapValue(map[string]protocol.Value{
		"c": protocol.IntValue(3), "b": protocol.IntValue(2), "a": protocol.IntValue(1),
	})
	if protocol.Encode(first) != protocol.Encode(second) {
		t.Error("map encoding depends on insertion order")
	}
}

// TestDecodeUntaggedIsString pins Decode's defensive default. It is total, so bytes that are not a well-formed encoding
// come back as the string they are rather than as an error every caller would have to handle.
func TestDecodeUntaggedIsString(t *testing.T) {
	t.Parallel()

	for _, untagged := range []string{"", "vlad", "42", "{not json", "\x07\x00\x00", strings.Repeat("x", 1000)} {
		got := protocol.Decode(untagged)
		if got.Kind != protocol.KindString || got.Str != untagged {
			t.Errorf("Decode(%q) = %v, want string %q", untagged, got, untagged)
		}
	}
}

func TestDecodeRejectsCorruptEncoding(t *testing.T) {
	t.Parallel()

	encoded := protocol.Encode(protocol.ArrayValue([]protocol.Value{protocol.IntValue(1), protocol.IntValue(2)}))
	corrupt := []string{
		encoded[:len(encoded)-1],   // truncated
		encoded + "x",              // trailing garbage
		"\x04\xff\xff\xff\xff\x0f", // array tag with a huge element count
		"\x01",                     // int tag with no payload
		"\x02\x00",                 // float tag, short payload
		"\x03\x09",                 // bool tag, invalid payload
	}
	for _, s := range corrupt {
		got := protocol.Decode(s)
		if got.Kind != protocol.KindString || got.Str != s {
			t.Errorf("Decode(%q) = %v, want the raw string back", s, got)
		}
	}
}

func TestParseLiteral(t *testing.T) {
	t.Parallel()

	tests := []struct {
		literal string
		want    protocol.Value
	}{
		{"42", protocol.IntValue(42)},
		{"-7", protocol.IntValue(-7)},
		{"42.5", protocol.FloatValue(42.5)},
		{"1e3", protocol.FloatValue(1000)},
		{"true", protocol.BoolValue(true)},
		{"false", protocol.BoolValue(false)},
		{`"42"`, protocol.StringValue("42")},
		{`"quoted"`, protocol.StringValue("quoted")},
		{"hello", protocol.StringValue("hello")},
		{"hello world", protocol.StringValue("hello world")},
		{"", protocol.StringValue("")},
		{" 42 ", protocol.StringValue(" 42 ")}, // surrounding space is data, not syntax
		{"007", protocol.StringValue("007")},   // not valid JSON, so plain text
		{"null", protocol.StringValue("null")},
		{"1 2", protocol.StringValue("1 2")},
		{"9223372036854775808", protocol.FloatValue(9223372036854775808)}, // past int64
		{"[1,2,3]", protocol.ArrayValue([]protocol.Value{
			protocol.IntValue(1), protocol.IntValue(2), protocol.IntValue(3),
		})},
		{"[]", protocol.ArrayValue(nil)},
		{`{"a":1}`, protocol.MapValue(map[string]protocol.Value{"a": protocol.IntValue(1)})},
		{"{}", protocol.MapValue(nil)},
		{`[{"a":[true,"x"]}]`, protocol.ArrayValue([]protocol.Value{
			protocol.MapValue(map[string]protocol.Value{
				"a": protocol.ArrayValue([]protocol.Value{protocol.BoolValue(true), protocol.StringValue("x")}),
			}),
		})},
	}

	for _, tt := range tests {
		t.Run(tt.literal, func(t *testing.T) {
			t.Parallel()
			got, err := protocol.ParseLiteral(tt.literal)
			if err != nil {
				t.Fatalf("ParseLiteral(%q) error = %v", tt.literal, err)
			}
			if !equalValues(got, tt.want) {
				t.Errorf("ParseLiteral(%q) = %v, want %v", tt.literal, got, tt.want)
			}
		})
	}
}

func TestParseLiteralRejectsMalformedStructure(t *testing.T) {
	t.Parallel()

	deeplyNested := strings.Repeat("[", 200) + "1" + strings.Repeat("]", 200) // valid JSON, past the depth cap
	for _, literal := range []string{"[1,2", `{"a":`, `"unterminated`, deeplyNested} {
		if _, err := protocol.ParseLiteral(literal); err == nil {
			t.Errorf("ParseLiteral(%q) = nil error, want a parse error", literal)
		}
	}
}

func TestRender(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value protocol.Value
		want  string
	}{
		{"bare string", protocol.StringValue("hello world"), "hello world"},
		{"string that looks numeric", protocol.StringValue("42"), "42"},
		{"int", protocol.IntValue(-7), "-7"},
		{"whole float keeps its type", protocol.FloatValue(1), "1.0"},
		{"float", protocol.FloatValue(0.25), "0.25"},
		{"bool", protocol.BoolValue(false), "false"},
		{"array quotes nested strings", protocol.ArrayValue([]protocol.Value{
			protocol.IntValue(1), protocol.StringValue("a b"),
		}), `[1,"a b"]`},
		{"map is sorted", protocol.MapValue(map[string]protocol.Value{
			"b": protocol.IntValue(2), "a": protocol.StringValue("x"),
		}), `{"a":"x","b":2}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := protocol.Render(tt.value); got != tt.want {
				t.Errorf("Render() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestRenderParsesBack is the round-trip that matters to clients: what GET prints must parse back to the same value.
// Only a top-level string is exempt, deliberately, so plain text keeps rendering bare.
func TestRenderParsesBack(t *testing.T) {
	t.Parallel()

	values := []protocol.Value{
		protocol.IntValue(math.MinInt64),
		protocol.IntValue(math.MaxInt64),
		protocol.FloatValue(1),
		protocol.FloatValue(-0.125),
		protocol.FloatValue(math.MaxFloat64),
		protocol.BoolValue(true),
		protocol.ArrayValue([]protocol.Value{protocol.StringValue(`quote " and \ backslash`), protocol.FloatValue(2)}),
		protocol.MapValue(map[string]protocol.Value{"k\n": protocol.ArrayValue([]protocol.Value{protocol.IntValue(0)})}),
	}

	for _, value := range values {
		rendered := protocol.Render(value)
		got, err := protocol.ParseLiteral(rendered)
		if err != nil {
			t.Fatalf("ParseLiteral(%q) error = %v", rendered, err)
		}
		if !equalValues(got, value) {
			t.Errorf("ParseLiteral(Render(%v)) = %v", value, got)
		}
	}
}

func FuzzValueCodec(f *testing.F) {
	seeds := []string{"", "42", "-0.0", "true", `"x"`, "[1,[2,[3]]]", `{"a":{"b":[1,2]}}`, "\x00\x01\x02", "hello",
		// Found by this fuzzer while strings were stored untagged: these bytes are also a valid encoding of int 24, so the
		// string read back as that int.
		"\x010",
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, literal string) {
		value, err := protocol.ParseLiteral(literal)
		if err != nil {
			return // malformed structure, rejected before it can be stored
		}
		got := protocol.Decode(protocol.Encode(value))
		if !equalValues(got, value) {
			t.Fatalf("Decode(Encode(%v)) = %v, from literal %q", value, got, literal)
		}
	})
}

// equalValues compares decoded values; reflect.DeepEqual is not enough because a nil and an empty array or map are the
// same value here.
func equalValues(a, b protocol.Value) bool {
	if a.Kind != b.Kind {
		return false
	}
	switch a.Kind {
	case protocol.KindArray:
		if len(a.Array) != len(b.Array) {
			return false
		}
		for i := range a.Array {
			if !equalValues(a.Array[i], b.Array[i]) {
				return false
			}
		}
		return true
	case protocol.KindMap:
		if len(a.Map) != len(b.Map) {
			return false
		}
		for key, value := range a.Map {
			other, ok := b.Map[key]
			if !ok || !equalValues(value, other) {
				return false
			}
		}
		return true
	case protocol.KindFloat:
		return math.Float64bits(a.Float) == math.Float64bits(b.Float)
	case protocol.KindString, protocol.KindInt, protocol.KindBool:
		return reflect.DeepEqual(a, b)
	default:
		return reflect.DeepEqual(a, b)
	}
}
