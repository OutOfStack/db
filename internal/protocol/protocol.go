package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Version identifies the wire protocol this build speaks: RESP2 framing over the documented command subset. It is not
// Redis compatibility — the subset described in COMPATIBILITY.md is the contract.
const Version = "RESP2"

type ReplyKind int

const (
	ReplySimpleString ReplyKind = iota
	ReplyBulkString
	ReplyNull
	ReplyError
	ReplyInteger
	ReplyArray
)

type Reply struct {
	Kind    ReplyKind
	Value   string
	Integer int64
	Array   []Reply
	// Code is the wire error code of a ReplyError (see the Code* constants); it is empty on every other kind.
	Code string
}

func SimpleString(value string) Reply {
	return Reply{Kind: ReplySimpleString, Value: value}
}

func BulkString(value string) Reply {
	return Reply{Kind: ReplyBulkString, Value: value}
}

func NullBulkString() Reply {
	return Reply{Kind: ReplyNull}
}

// Error builds an unclassified error reply. Use CodedError where the failure has a documented code.
func Error(value string) Reply {
	return CodedError(CodeErr, value)
}

// CodedError builds an error reply carrying a stable wire error code.
func CodedError(code, value string) Reply {
	return Reply{Kind: ReplyError, Value: value, Code: code}
}

// ErrorFor builds the error reply for err, taking its code from the error chain.
func ErrorFor(err error) Reply {
	return CodedError(CodeOf(err), err.Error())
}

func Integer(value int64) Reply {
	return Reply{Kind: ReplyInteger, Integer: value}
}

func Array(values []Reply) Reply {
	return Reply{Kind: ReplyArray, Array: values}
}

func BulkStringArray(values []string) Reply {
	replies := make([]Reply, 0, len(values))
	for _, value := range values {
		replies = append(replies, BulkString(value))
	}
	return Array(replies)
}

// CommandSize returns the exact number of bytes WriteCommand emits for cmd/args. It lets callers reject an over-limit
// command before writing it, matching the cumulative-byte limit ReadCommand enforces on the way back in.
func CommandSize(cmd string, args []string) int {
	size := ArrayHeaderSize(len(args) + 1)
	size += BulkStringSize(cmd)
	for _, arg := range args {
		size += BulkStringSize(arg)
	}
	return size
}

// BulkStringSize returns the exact number of bytes a bulk string holding value occupies on the wire, in a request or a
// reply alike.
func BulkStringSize(value string) int {
	return 1 + intWidth(len(value)) + 2 + len(value) + 2 // $<len>\r\n<value>\r\n
}

// ArrayHeaderSize returns the exact number of bytes the header of an n-element array occupies on the wire.
func ArrayHeaderSize(n int) int {
	return 1 + intWidth(n) + 2 // *<count>\r\n
}

func intWidth(n int) int {
	return len(strconv.Itoa(n))
}

func WriteCommand(w io.Writer, cmd string, args []string) error {
	if _, err := fmt.Fprintf(w, "*%d\r\n", len(args)+1); err != nil {
		return err
	}
	if err := writeBulkString(w, cmd); err != nil {
		return err
	}
	for _, arg := range args {
		if err := writeBulkString(w, arg); err != nil {
			return err
		}
	}
	return nil
}

func ReadCommand(r *bufio.Reader, maxMessageSize int) (string, []string, error) {
	var read int
	line, err := readLine(r, maxMessageSize, &read)
	if err != nil {
		return "", nil, err
	}
	if len(line) == 0 || line[0] != '*' {
		return "", nil, NewError(CodeProtocol, "expected RESP array command")
	}

	count, err := parseLen(line[1:])
	if err != nil {
		return "", nil, NewError(CodeProtocol, "invalid command array length: %s", err)
	}
	if count <= 0 {
		return "", nil, NewError(CodeProtocol, "command array cannot be empty")
	}
	if err = checkArrayLen(count, maxMessageSize); err != nil {
		return "", nil, err
	}

	parts := make([]string, 0, min(count, maxPreallocElems))
	for range count {
		value, rErr := readCommandBulkString(r, maxMessageSize, &read)
		if rErr != nil {
			return "", nil, rErr
		}
		parts = append(parts, value)
	}

	return parts[0], parts[1:], nil
}

func WriteReply(w io.Writer, reply Reply) error {
	switch reply.Kind {
	case ReplySimpleString:
		_, err := fmt.Fprintf(w, "+%s\r\n", sanitizeLine(reply.Value))
		return err
	case ReplyBulkString:
		return writeBulkString(w, reply.Value)
	case ReplyNull:
		_, err := io.WriteString(w, "$-1\r\n")
		return err
	case ReplyError:
		code := reply.Code
		if !isErrorCode(code) {
			code = CodeErr
		}
		value := sanitizeLine(reply.Value)
		if value == "" {
			_, err := fmt.Fprintf(w, "-%s\r\n", code)
			return err
		}
		_, err := fmt.Fprintf(w, "-%s %s\r\n", code, value)
		return err
	case ReplyInteger:
		_, err := fmt.Fprintf(w, ":%d\r\n", reply.Integer)
		return err
	case ReplyArray:
		if _, err := fmt.Fprintf(w, "*%d\r\n", len(reply.Array)); err != nil {
			return err
		}
		for _, item := range reply.Array {
			if err := WriteReply(w, item); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown reply kind: %d", reply.Kind)
	}
}

func ReadReply(r *bufio.Reader, maxMessageSize int) (Reply, error) {
	var read int
	return readReply(r, maxMessageSize, &read)
}

func readReply(r *bufio.Reader, maxMessageSize int, read *int) (Reply, error) {
	line, err := readLine(r, maxMessageSize, read)
	if err != nil {
		return Reply{}, err
	}
	if len(line) == 0 {
		return Reply{}, NewError(CodeProtocol, "empty RESP reply")
	}

	switch line[0] {
	case '+':
		return SimpleString(line[1:]), nil
	case '-':
		code, msg := splitErrorCode(line[1:])
		return CodedError(code, msg), nil
	case ':':
		value, pErr := strconv.ParseInt(line[1:], 10, 64)
		if pErr != nil {
			return Reply{}, NewError(CodeProtocol, "invalid integer reply: %s", pErr)
		}
		return Integer(value), nil
	case '$':
		value, null, rErr := readBulkStringBody(r, line[1:], maxMessageSize, read)
		if rErr != nil {
			return Reply{}, rErr
		}
		if null {
			return NullBulkString(), nil
		}
		return BulkString(value), nil
	case '*':
		return readArrayReply(r, line[1:], maxMessageSize, read)
	default:
		return Reply{}, NewError(CodeProtocol, "unknown RESP reply prefix %q", line[0])
	}
}

func readArrayReply(r *bufio.Reader, lenText string, maxMessageSize int, read *int) (Reply, error) {
	count, err := parseLen(lenText)
	if err != nil {
		return Reply{}, NewError(CodeProtocol, "invalid array reply length: %s", err)
	}
	if count < 0 {
		return NullBulkString(), nil
	}
	if err = checkArrayLen(count, maxMessageSize); err != nil {
		return Reply{}, err
	}
	values := make([]Reply, 0, min(count, maxPreallocElems))
	for range count {
		value, rErr := readReply(r, maxMessageSize, read)
		if rErr != nil {
			return Reply{}, rErr
		}
		values = append(values, value)
	}
	return Array(values), nil
}

func writeBulkString(w io.Writer, value string) error {
	if _, err := fmt.Fprintf(w, "$%d\r\n", len(value)); err != nil {
		return err
	}
	if _, err := io.WriteString(w, value); err != nil {
		return err
	}
	_, err := io.WriteString(w, "\r\n")
	return err
}

func readCommandBulkString(r *bufio.Reader, maxMessageSize int, read *int) (string, error) {
	line, err := readLine(r, maxMessageSize, read)
	if err != nil {
		return "", err
	}
	if len(line) == 0 || line[0] != '$' {
		return "", NewError(CodeProtocol, "command arguments must be bulk strings")
	}

	value, null, err := readBulkStringBody(r, line[1:], maxMessageSize, read)
	if err != nil {
		return "", err
	}
	if null {
		return "", NewError(CodeProtocol, "command arguments cannot be null")
	}
	return value, nil
}

func readBulkStringBody(r *bufio.Reader, lenText string, maxMessageSize int, read *int) (string, bool, error) {
	n, err := parseLen(lenText)
	if err != nil {
		return "", false, NewError(CodeProtocol, "invalid bulk string length: %s", err)
	}
	if n == -1 {
		return "", true, nil
	}
	if n < -1 {
		return "", false, NewError(CodeProtocol, "invalid negative bulk string length")
	}
	// overflow-safe size check: n is non-negative here, so compare against the remaining budget instead of computing
	// *read+n+2, which can overflow for lengths near math.MaxInt and bypass the limit.
	if maxMessageSize > 0 && n > maxMessageSize-*read-2 {
		return "", false, NewError(CodeTooLarge, "message size exceeds limit")
	}

	buf := make([]byte, n+2)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", false, err
	}
	*read += len(buf)
	if len(buf) < 2 || buf[len(buf)-2] != '\r' || buf[len(buf)-1] != '\n' {
		return "", false, NewError(CodeProtocol, "bulk string missing CRLF terminator")
	}

	return string(buf[:n]), false, nil
}

func readLine(r *bufio.Reader, maxMessageSize int, read *int) (string, error) {
	var out []byte
	for {
		part, err := r.ReadSlice('\n')
		out = append(out, part...)
		*read += len(part)
		if maxMessageSize > 0 && *read > maxMessageSize {
			return "", NewError(CodeTooLarge, "message size exceeds limit")
		}
		if err == nil {
			break
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return "", err
	}

	if len(out) < 2 || out[len(out)-2] != '\r' {
		return "", NewError(CodeProtocol, "RESP line missing CRLF terminator")
	}
	return string(out[:len(out)-2]), nil
}

// minArrayElemSize is the smallest possible wire encoding of one array element (e.g. "+\r\n"), used to bound declared
// array lengths. maxPreallocElems caps slice preallocation so a declared length cannot force a huge allocation before
// any element is read.
const (
	minArrayElemSize = 3
	maxPreallocElems = 1024
)

// checkArrayLen rejects array lengths that could not possibly be encoded within maxMessageSize, before any per-element
// allocation happens
func checkArrayLen(count, maxMessageSize int) error {
	if maxMessageSize > 0 && count > maxMessageSize/minArrayElemSize {
		return NewError(CodeTooLarge, "message size exceeds limit")
	}
	return nil
}

// sanitizeLine makes a value safe for line-based RESP types (simple strings and errors), which must not contain CR or
// LF
func sanitizeLine(value string) string {
	if !strings.ContainsAny(value, "\r\n") {
		return value
	}
	return strings.NewReplacer("\r", " ", "\n", " ").Replace(value)
}

func parseLen(value string) (int, error) {
	if value == "" {
		return 0, errors.New("empty length")
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return 0, err
	}
	return n, nil
}
