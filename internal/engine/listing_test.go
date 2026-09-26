package engine_test

import (
	"bytes"
	"fmt"
	"iter"
	"slices"
	"testing"

	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/protocol"
)

// mustList unwraps a listing, failing the test on an error.
func mustList(t *testing.T) func([]string, error) []string {
	t.Helper()
	return func(names []string, err error) []string {
		t.Helper()
		if err != nil {
			t.Fatalf("listing: %v", err)
		}
		return names
	}
}

// countingNames yields n names and records how many the consumer actually pulled, so a test can tell a collector that
// stopped at its budget from one that materialized everything and checked afterwards.
func countingNames(n int, pulled *int) iter.Seq[string] {
	return func(yield func(string) bool) {
		for i := range n {
			*pulled++
			if !yield(fmt.Sprintf("name-%08d", i)) {
				return
			}
		}
	}
}

func TestCollectSortedStopsAtTheBudget(t *testing.T) {
	t.Parallel()

	const budget = 1024
	perName := protocol.BulkStringSize("name-00000000")

	var pulled int
	names, err := engine.CollectSorted(countingNames(1_000_000, &pulled), budget)
	if protocol.CodeOf(err) != protocol.CodeTooLarge {
		t.Fatalf("CollectSorted() error = %v, want a %s error", err, protocol.CodeTooLarge)
	}
	if names != nil {
		t.Errorf("CollectSorted() returned %d names alongside the error; a listing is refused, never truncated", len(names))
	}
	// Iteration stops on the first name past the budget: a million-name source is read only as far as the limit reaches.
	if want := budget/perName + 1; pulled != want {
		t.Errorf("pulled %d names from the source, want %d (stop at the first one past the budget)", pulled, want)
	}
}

// TestCollectSortedBoundIsTheEncodedReply pins what the budget measures: the exact bytes of the array reply, header
// included. A listing that encodes to exactly the budget is served; one byte less of budget refuses it.
func TestCollectSortedBoundIsTheEncodedReply(t *testing.T) {
	t.Parallel()

	source := []string{"zeta", "alpha", "mid"}
	var encoded bytes.Buffer
	if err := protocol.WriteReply(&encoded, protocol.BulkStringArray(source)); err != nil {
		t.Fatal(err)
	}
	size := encoded.Len()

	names, err := engine.CollectSorted(slices.Values(source), size)
	if err != nil {
		t.Fatalf("CollectSorted() at exactly the reply size: %v", err)
	}
	if want := []string{"alpha", "mid", "zeta"}; !slices.Equal(names, want) {
		t.Errorf("CollectSorted() = %v, want %v", names, want)
	}
	// Only the array header tips it over here, which is the check that runs after iteration.
	if _, err = engine.CollectSorted(slices.Values(source), size-1); protocol.CodeOf(err) != protocol.CodeTooLarge {
		t.Errorf("CollectSorted() one byte under the reply size error = %v, want %s", err, protocol.CodeTooLarge)
	}
	// An empty listing is the 4-byte "*0\r\n".
	if names, err = engine.CollectSorted(slices.Values([]string(nil)), 4); err != nil || len(names) != 0 {
		t.Errorf("CollectSorted(empty) = %v, %v; want an empty listing", names, err)
	}
}

func TestCollectSortedUnbounded(t *testing.T) {
	t.Parallel()

	var pulled int
	names, err := engine.CollectSorted(countingNames(5000, &pulled), 0)
	if err != nil {
		t.Fatalf("CollectSorted(unbounded) error = %v", err)
	}
	if len(names) != 5000 || !slices.IsSorted(names) {
		t.Errorf("CollectSorted(unbounded) returned %d names, sorted=%v; want all 5000, sorted", len(names),
			slices.IsSorted(names))
	}
}

func TestEngineListingsAreBounded(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	eng := engine.New()
	for i := range 200 {
		if err := eng.Set(ctx, "users", fmt.Sprintf("key-%04d", i), "v"); err != nil {
			t.Fatal(err)
		}
		if err := eng.Set(ctx, fmt.Sprintf("table-%04d", i), "k", "v"); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := eng.Keys(ctx, "users", 512); protocol.CodeOf(err) != protocol.CodeTooLarge {
		t.Errorf("Keys() over the limit error = %v, want %s", err, protocol.CodeTooLarge)
	}
	if _, err := eng.Tables(ctx, 512); protocol.CodeOf(err) != protocol.CodeTooLarge {
		t.Errorf("Tables() over the limit error = %v, want %s", err, protocol.CodeTooLarge)
	}
	if keys := mustList(t)(eng.Keys(ctx, "users", 64<<10)); len(keys) != 200 {
		t.Errorf("Keys() under the limit returned %d keys, want 200", len(keys))
	}
	// A small table stays listable under a limit the large ones exceed.
	if keys := mustList(t)(eng.Keys(ctx, "table-0001", 512)); !slices.Equal(keys, []string{"k"}) {
		t.Errorf("Keys(table-0001) = %v, want [k]", keys)
	}
}
