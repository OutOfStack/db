package tiered_test

import (
	"strings"
	"testing"

	"github.com/OutOfStack/db/internal/protocol"
	"github.com/stretchr/testify/require"
)

// TestFormatLimitsReportTooLarge: a key the record format cannot store is refused with TOOLARGE, the code every format
// limit reports, and nothing is written.
func TestFormatLimitsReportTooLarge(t *testing.T) {
	t.Parallel()
	e := open(t, testConfig(t.TempDir()))
	long := strings.Repeat("k", 1<<16)

	for name, set := range map[string]func() error{
		"key":   func() error { return e.Set(t.Context(), "t", long, "v") },
		"table": func() error { return e.Set(t.Context(), long, "k", "v") },
	} {
		err := set()
		require.Error(t, err, name)
		require.Equal(t, protocol.CodeTooLarge, protocol.CodeOf(err), name)
	}
	require.False(t, e.TableExists(t.Context(), "t"), "a refused write must not create the table")
}
