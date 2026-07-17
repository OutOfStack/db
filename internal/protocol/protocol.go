package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

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

func Error(value string) Reply {
	return Reply{Kind: ReplyError, Value: value}
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

// CommandSize returns the exact number of bytes WriteCommand emits for cmd/args.
// It lets callers reject an over-limit command before writing it, matching the
// cumulative-byte limit ReadCommand enforces on the way back in.
func CommandSize(cmd string, args []string) int {
	size := 1 + intWidth(len(args)+1) + 2 // *<count>\r\n
	size += bulkStringSize(cmd)
	for _, arg := range args {
		size += bulkStringSize(arg)
	}
	return size
}

func bulkStringSize(value string) int {
	return 1 + intWidth(len(value)) + 2 + len(value) + 2 // $<len>\r\n<value>\r\n
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
		return "", nil, errors.New("expected RESP array command")
	}

	count, err := parseLen(line[1:])
	if err != nil {
		return "", nil, fmt.Errorf("invalid command array length: %w", err)
	}
	if count <= 0 {
		return "", nil, errors.New("command array cannot be empty")
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
		value := sanitizeLine(reply.Value)
		if value == "" {
			value = "ERR"
		} else if !strings.HasPrefix(value, "ERR ") {
			value = "ERR " + value
		}
		_, err := fmt.Fprintf(w, "-%s\r\n", value)
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
		return Reply{}, errors.New("empty RESP reply")
	}

	switch line[0] {
	case '+':
		return SimpleString(line[1:]), nil
	case '-':
		value := line[1:]
		value = strings.TrimPrefix(value, "ERR ")
		return Error(value), nil
	case ':':
		value, pErr := strconv.ParseInt(line[1:], 10, 64)
		if pErr != nil {
			return Reply{}, fmt.Errorf("invalid integer reply: %w", pErr)
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
		return Reply{}, fmt.Errorf("unknown RESP reply prefix %q", line[0])
	}
}

func readArrayReply(r *bufio.Reader, lenText string, maxMessageSize int, read *int) (Reply, error) {
	count, err := parseLen(lenText)
	if err != nil {
		return Reply{}, fmt.Errorf("invalid array reply length: %w", err)
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
		return "", errors.New("command arguments must be bulk strings")
	}

	value, null, err := readBulkStringBody(r, line[1:], maxMessageSize, read)
	if err != nil {
		return "", err
	}
	if null {
		return "", errors.New("command arguments cannot be null")
	}
	return value, nil
}

func readBulkStringBody(r *bufio.Reader, lenText string, maxMessageSize int, read *int) (string, bool, error) {
	n, err := parseLen(lenText)
	if err != nil {
		return "", false, fmt.Errorf("invalid bulk string length: %w", err)
	}
	if n == -1 {
		return "", true, nil
	}
	if n < -1 {
		return "", false, errors.New("invalid negative bulk string length")
	}
	// overflow-safe size check: n is non-negative here, so compare against the
	// remaining budget instead of computing *read+n+2, which can overflow for
	// lengths near math.MaxInt and bypass the limit.
	if maxMessageSize > 0 && n > maxMessageSize-*read-2 {
		return "", false, errors.New("message size exceeds limit")
	}

	buf := make([]byte, n+2)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", false, err
	}
	*read += len(buf)
	if len(buf) < 2 || buf[len(buf)-2] != '\r' || buf[len(buf)-1] != '\n' {
		return "", false, errors.New("bulk string missing CRLF terminator")
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
			return "", errors.New("message size exceeds limit")
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
		return "", errors.New("RESP line missing CRLF terminator")
	}
	return string(out[:len(out)-2]), nil
}

// minArrayElemSize is the smallest possible wire encoding of one array element (e.g. "+\r\n"), used to bound declared array lengths.
// maxPreallocElems caps slice preallocation so a declared length cannot force a huge allocation before any element is read.
const (
	minArrayElemSize = 3
	maxPreallocElems = 1024
)

// checkArrayLen rejects array lengths that could not possibly be encoded
// within maxMessageSize, before any per-element allocation happens
func checkArrayLen(count, maxMessageSize int) error {
	if maxMessageSize > 0 && count > maxMessageSize/minArrayElemSize {
		return errors.New("message size exceeds limit")
	}
	return nil
}

// sanitizeLine makes a value safe for line-based RESP types (simple strings and errors), which must not contain CR or LF
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
