package engine_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/OutOfStack/db/internal/engine"
)

func TestEngine_Set(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		table   string
		key     string
		value   string
		preSet  bool // set the same key to a different value first
		wantErr bool
	}{
		{"set single key", "t1", "key1", "value1", false, false},
		{"set empty key", "t1", "", "value", false, false},
		{"set empty value", "t1", "key", "", false, false},
		{"set same key twice", "t1", "key1", "newvalue", true, false},
		{"set same key in another table", "t2", "key1", "othervalue", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			eng := engine.New()
			if tt.preSet {
				if err := eng.Set(t.Context(), tt.table, tt.key, "oldvalue"); err != nil {
					t.Fatalf("pre-Set() failed: %v", err)
				}
			}

			if err := eng.Set(t.Context(), tt.table, tt.key, tt.value); (err != nil) != tt.wantErr {
				t.Errorf("Set() error = %v, wantErr %v", err, tt.wantErr)
			}
			// Verify the value was set correctly
			got, err := eng.Get(t.Context(), tt.table, tt.key)
			if err != nil {
				t.Errorf("Get() after Set() failed: %v", err)
			}
			if got != tt.value {
				t.Errorf("Get() after Set() = %v, want %v", got, tt.value)
			}
		})
	}
}

func TestEngine_TablesAreIndependent(t *testing.T) {
	t.Parallel()

	e := engine.New()

	if err := e.Set(t.Context(), "t1", "key", "v1"); err != nil {
		t.Fatalf("Set() failed: %v", err)
	}
	if err := e.Set(t.Context(), "t2", "key", "v2"); err != nil {
		t.Fatalf("Set() failed: %v", err)
	}

	got, err := e.Get(t.Context(), "t1", "key")
	if err != nil || got != "v1" {
		t.Errorf("Get(t1, key) = %q, %v; want %q, nil", got, err, "v1")
	}
	got, err = e.Get(t.Context(), "t2", "key")
	if err != nil || got != "v2" {
		t.Errorf("Get(t2, key) = %q, %v; want %q, nil", got, err, "v2")
	}

	// Deleting from one table must not affect the other
	if err = e.Del(t.Context(), "t1", "key"); err != nil {
		t.Fatalf("Del(t1, key) failed: %v", err)
	}
	if _, err = e.Get(t.Context(), "t1", "key"); err == nil {
		t.Error("Get(t1, key) after Del should have failed")
	}
	if _, err = e.Get(t.Context(), "t2", "key"); err != nil {
		t.Errorf("Get(t2, key) failed after Del in t1: %v", err)
	}
}

func TestEngine_Get(t *testing.T) {
	t.Parallel()

	eng := engine.New()
	tests := []struct {
		name    string
		table   string
		key     string
		want    string
		wantErr bool
	}{
		{"get existing key", "t1", "key1", "value1", false},
		{"get non-existent key", "t1", "key2", "", true},
		{"get key from non-existent table", "missing", "key1", "", true},
		{"get empty key", "t1", "", "", true},
	}

	// Set up test data; subtests only read, so they can share the engine
	eng.Set(t.Context(), "t1", "key1", "value1")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := eng.Get(t.Context(), tt.table, tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEngine_Del(t *testing.T) {
	t.Parallel()

	eng := engine.New()
	tests := []struct {
		name    string
		table   string
		key     string
		wantErr bool
	}{
		{"delete existing key", "t1", "key1", false},
		{"delete non-existent key", "t1", "key2", true},
		{"delete key from non-existent table", "missing", "key1", true},
		{"delete empty key", "t1", "", true},
	}

	// Set up test data; each subtest touches a distinct key, so they can share the engine
	eng.Set(t.Context(), "t1", "key1", "value1")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if err := eng.Del(t.Context(), tt.table, tt.key); (err != nil) != tt.wantErr {
				t.Errorf("Del() error = %v, wantErr %v", err, tt.wantErr)
			}
			// Verify the key was deleted
			if !tt.wantErr {
				_, err := eng.Get(t.Context(), tt.table, tt.key)
				if err == nil {
					t.Errorf("Get() after Del() should have failed")
				}
			}
		})
	}
}

func TestEngine_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	eng := engine.New()
	const numGoroutines = 10
	const numOps = 100

	// Create a map to track expected values
	expectedValues := sync.Map{}

	// Create a channel to signal when all operations are done
	done := make(chan struct{})

	// Start multiple goroutines performing concurrent operations,
	// each goroutine working in its own table and a shared table
	for i := range numGoroutines {
		go func() {
			table := fmt.Sprintf("table-%d", i%3)
			for j := range numOps {
				key := fmt.Sprintf("key-%d-%d", i, j)
				value := fmt.Sprintf("value-%d-%d", i, j)

				// Set value
				if err := eng.Set(t.Context(), table, key, value); err != nil {
					t.Errorf("Set failed: %v", err)
					continue
				}
				expectedValues.Store(table+"/"+key, value)

				// Get value
				got, err := eng.Get(t.Context(), table, key)
				if err != nil {
					t.Errorf("Get failed: %v", err)
					continue
				}
				if got != value {
					t.Errorf("Get returned wrong value: got %v, want %v", got, value)
				}

				// Delete value
				if err = eng.Del(t.Context(), table, key); err != nil {
					t.Errorf("Del failed: %v", err)
					continue
				}
				expectedValues.Delete(table + "/" + key)

				// Verify deletion
				_, err = eng.Get(t.Context(), table, key)
				if err == nil {
					t.Errorf("Get after Del should have failed")
				}
			}
			done <- struct{}{}
		}()
	}

	// Wait for all goroutines to finish
	for range numGoroutines {
		<-done
	}
}
