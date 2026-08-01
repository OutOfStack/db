package protocol

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
)

// Kind is the type tag of a stored value.
type Kind byte

// Value kinds. The numeric values are part of the on-disk encoding.
const (
	KindString Kind = iota
	KindInt
	KindFloat
	KindBool
	KindArray
	KindMap
)

// maxDepth bounds nesting on both the parse and the decode side, so a corrupt or hostile value cannot drive Decode into
// unbounded recursion. Operations that add a level of nesting (APPEND, HSET) check that their result still decodes
// before storing it, so a value that reads back as an opaque string cannot be written in the first place.
const maxDepth = 64

const kindStringName = "string"

func (k Kind) String() string {
	switch k {
	case KindInt:
		return "int"
	case KindFloat:
		return "float"
	case KindBool:
		return "bool"
	case KindArray:
		return "array"
	case KindMap:
		return "map"
	case KindString:
		return kindStringName
	default:
		return kindStringName // an unknown tag never decodes, so it can only be a string
	}
}

// Value is a typed value. Only the field matching Kind carries data.
type Value struct {
	Kind  Kind
	Str   string
	Int   int64
	Float float64
	Bool  bool
	Array []Value
	Map   map[string]Value
}

// StringValue returns a string Value.
func StringValue(s string) Value { return Value{Kind: KindString, Str: s} }

// IntValue returns an int Value.
func IntValue(n int64) Value { return Value{Kind: KindInt, Int: n} }

// FloatValue returns a float Value.
func FloatValue(f float64) Value { return Value{Kind: KindFloat, Float: f} }

// BoolValue returns a bool Value.
func BoolValue(b bool) Value { return Value{Kind: KindBool, Bool: b} }

// ArrayValue returns an array Value.
func ArrayValue(items []Value) Value { return Value{Kind: KindArray, Array: items} }

// MapValue returns a map Value.
func MapValue(fields map[string]Value) Value { return Value{Kind: KindMap, Map: fields} }

// Encode serializes v for storage, the WAL, and replication: a type tag byte followed by the payload (varint int,
// IEEE-754 float, length-prefixed string, count plus recursive elements for array and map). Map keys are encoded in
// sorted order, so a value always encodes to the same bytes.
//
// Strings are tagged too; otherwise arbitrary string bytes could be mistaken for another encoded type.
func Encode(v Value) string {
	return string(appendValue(nil, v))
}

func appendValue(buf []byte, v Value) []byte {
	buf = append(buf, byte(v.Kind))
	switch v.Kind {
	case KindString:
		buf = binary.AppendUvarint(buf, uint64(len(v.Str)))
		buf = append(buf, v.Str...)
	case KindInt:
		buf = binary.AppendVarint(buf, v.Int)
	case KindFloat:
		buf = binary.BigEndian.AppendUint64(buf, math.Float64bits(v.Float))
	case KindBool:
		var b byte
		if v.Bool {
			b = 1
		}
		buf = append(buf, b)
	case KindArray:
		buf = binary.AppendUvarint(buf, uint64(len(v.Array)))
		for _, item := range v.Array {
			buf = appendValue(buf, item)
		}
	case KindMap:
		buf = binary.AppendUvarint(buf, uint64(len(v.Map)))
		for _, key := range sortedKeys(v.Map) {
			buf = binary.AppendUvarint(buf, uint64(len(key)))
			buf = append(buf, key...)
			buf = appendValue(buf, v.Map[key])
		}
	}
	return buf
}

// Decode returns the value stored in s. It is total: bytes that are not a well-formed encoding come back as the string
// they are, rather than as an error every caller would have to handle. Persistence readers reject a file whose format
// header is missing or unknown, so malformed bytes never reach here from a file this build wrote.
func Decode(s string) Value {
	v, rest, ok := decodeValue([]byte(s), 0)
	if !ok || len(rest) != 0 {
		return StringValue(s)
	}
	return v
}

func decodeValue(buf []byte, depth int) (Value, []byte, bool) {
	if len(buf) == 0 || depth > maxDepth {
		return Value{}, nil, false
	}
	kind, rest := Kind(buf[0]), buf[1:]
	switch kind {
	case KindString:
		s, tail, ok := decodeString(rest)
		if !ok {
			return Value{}, nil, false
		}
		return StringValue(s), tail, true
	case KindInt:
		n, used := binary.Varint(rest)
		if used <= 0 {
			return Value{}, nil, false
		}
		return IntValue(n), rest[used:], true
	case KindFloat:
		if len(rest) < 8 {
			return Value{}, nil, false
		}
		return FloatValue(math.Float64frombits(binary.BigEndian.Uint64(rest[:8]))), rest[8:], true
	case KindBool:
		if len(rest) == 0 || rest[0] > 1 {
			return Value{}, nil, false
		}
		return BoolValue(rest[0] == 1), rest[1:], true
	case KindArray:
		return decodeArray(rest, depth)
	case KindMap:
		return decodeMap(rest, depth)
	default:
		return Value{}, nil, false
	}
}

func decodeArray(buf []byte, depth int) (Value, []byte, bool) {
	count, rest, ok := decodeCount(buf)
	if !ok {
		return Value{}, nil, false
	}
	items := make([]Value, 0, count)
	for range count {
		item, tail, itemOK := decodeValue(rest, depth+1)
		if !itemOK {
			return Value{}, nil, false
		}
		items = append(items, item)
		rest = tail
	}
	return ArrayValue(items), rest, true
}

func decodeMap(buf []byte, depth int) (Value, []byte, bool) {
	count, rest, ok := decodeCount(buf)
	if !ok {
		return Value{}, nil, false
	}
	fields := make(map[string]Value, count)
	for range count {
		key, tail, keyOK := decodeString(rest)
		if !keyOK {
			return Value{}, nil, false
		}
		value, next, valueOK := decodeValue(tail, depth+1)
		if !valueOK {
			return Value{}, nil, false
		}
		fields[key] = value
		rest = next
	}
	return MapValue(fields), rest, true
}

// decodeCount reads a uvarint length or element count and rejects one larger than the bytes that remain, so a corrupt
// length cannot force a huge allocation. Every element costs at least one byte.
func decodeCount(buf []byte) (int, []byte, bool) {
	count, used := binary.Uvarint(buf)
	if used <= 0 {
		return 0, nil, false
	}
	rest := buf[used:]
	if count > uint64(len(rest)) { // #nosec G115 -- a length is never negative
		return 0, nil, false
	}
	return int(count), rest, true // #nosec G115 -- bounded by len(rest) above
}

func decodeString(buf []byte) (string, []byte, bool) {
	length, rest, ok := decodeCount(buf)
	if !ok {
		return "", nil, false
	}
	return string(rest[:length]), rest[length:], true
}

// ParseLiteral parses the client-facing literal syntax shared by the CLI, the client library, and any RESP client: 42
// (int), 42.5 (float), true and false (bool), "quoted" (string), [1,2,3] (array), {"a":1} (map). Anything that is not a
// single JSON literal is a plain string.
func ParseLiteral(s string) (Value, error) {
	// Leading or trailing space is never syntax here: a value that carries it is text the caller wants stored verbatim,
	// not a literal to trim and reinterpret.
	if s == "" || s != strings.TrimSpace(s) {
		return StringValue(s), nil
	}

	decoder := json.NewDecoder(strings.NewReader(s))
	decoder.UseNumber()
	var raw any
	if err := decoder.Decode(&raw); err != nil {
		return bareString(s)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return bareString(s) // trailing content: not one literal
	}
	value, ok := fromJSON(raw, 0)
	if !ok {
		return bareString(s)
	}
	return value, nil
}

// bareString treats input as plain text, unless it opens with a structural character: there, falling back silently
// would store a typo'd array, map, or quoted string as a string and hide the mistake.
func bareString(s string) (Value, error) {
	switch s[0] {
	case '[', '{', '"':
		return Value{}, fmt.Errorf("invalid value literal: %s", s)
	default:
		return StringValue(s), nil
	}
}

func fromJSON(raw any, depth int) (Value, bool) {
	if depth > maxDepth {
		return Value{}, false
	}
	switch typed := raw.(type) {
	case json.Number:
		return fromNumber(typed)
	case string:
		return StringValue(typed), true
	case bool:
		return BoolValue(typed), true
	case []any:
		items := make([]Value, 0, len(typed))
		for _, item := range typed {
			value, ok := fromJSON(item, depth+1)
			if !ok {
				return Value{}, false
			}
			items = append(items, value)
		}
		return ArrayValue(items), true
	case map[string]any:
		fields := make(map[string]Value, len(typed))
		for key, item := range typed {
			value, ok := fromJSON(item, depth+1)
			if !ok {
				return Value{}, false
			}
			fields[key] = value
		}
		return MapValue(fields), true
	default:
		return Value{}, false // JSON null has no counterpart; it stays plain text
	}
}

// fromNumber keeps the int/float distinction JSON itself does not make: a number that fits an int64 without a
// fractional part is an int, everything else a float.
func fromNumber(number json.Number) (Value, bool) {
	if n, err := strconv.ParseInt(number.String(), 10, 64); err == nil {
		return IntValue(n), true
	}
	f, err := strconv.ParseFloat(number.String(), 64)
	if err != nil {
		return Value{}, false
	}
	return FloatValue(f), true
}

// Render renders v in the literal syntax ParseLiteral accepts. A top-level string renders bare, so a plain SET/GET
// round-trips to exactly the text that was stored; strings nested in an array or map are quoted, where bare text would
// be ambiguous.
func Render(v Value) string {
	if v.Kind == KindString {
		return v.Str
	}
	var out strings.Builder
	render(&out, v)
	return out.String()
}

func render(out *strings.Builder, v Value) {
	switch v.Kind {
	case KindString:
		out.WriteString(quote(v.Str))
	case KindInt:
		out.WriteString(strconv.FormatInt(v.Int, 10))
	case KindFloat:
		out.WriteString(formatFloat(v.Float))
	case KindBool:
		out.WriteString(strconv.FormatBool(v.Bool))
	case KindArray:
		out.WriteByte('[')
		for i, item := range v.Array {
			if i > 0 {
				out.WriteByte(',')
			}
			render(out, item)
		}
		out.WriteByte(']')
	case KindMap:
		out.WriteByte('{')
		for i, key := range sortedKeys(v.Map) {
			if i > 0 {
				out.WriteByte(',')
			}
			out.WriteString(quote(key))
			out.WriteByte(':')
			render(out, v.Map[key])
		}
		out.WriteByte('}')
	}
}

// quote renders a string as a JSON string literal. json.Marshal is used rather than strconv.Quote because only its
// escaping is guaranteed to parse back.
func quote(s string) string {
	encoded, err := json.Marshal(s)
	if err != nil {
		return strconv.Quote(s) // unreachable: a string always marshals
	}
	return string(encoded)
}

// formatFloat keeps a float distinguishable from an int on the way back in: a whole float renders as 1.0, which
// ParseLiteral reads as a float again. Text that is not int-shaped (exponents, NaN, ±Inf) is already unambiguous.
func formatFloat(f float64) string {
	text := strconv.FormatFloat(f, 'g', -1, 64)
	if _, err := strconv.ParseInt(text, 10, 64); err == nil {
		return text + ".0"
	}
	return text
}

func sortedKeys(fields map[string]Value) []string {
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
