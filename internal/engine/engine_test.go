package engine_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/OutOfStack/db/internal/engine"
)

func TestEngine_Set(t *testing.T) {
	engine := engine.New()
	tests := []struct {
		name    string
		key     string
		value   string
		wantErr bool
	}{
		{"set single key", "key1", "value1", false},
		{"set empty key", "", "value", false},
		{"set empty value", "key", "", false},
		{"set same key twice", "key1", "newvalue", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := engine.Set(t.Context(), tt.key, tt.value); (err != nil) != tt.wantErr {
				t.Errorf("Set() error = %v, wantErr %v", err, tt.wantErr)
			}
			// Verify the value was set correctly
			got, err := engine.Get(t.Context(), tt.key)
			if err != nil {
				t.Errorf("Get() after Set() failed: %v", err)
			}
			if got != tt.value {
				t.Errorf("Get() after Set() = %v, want %v", got, tt.value)
			}
		})
	}
}

func TestEngine_Get(t *testing.T) {
	engine := engine.New()
	tests := []struct {
		name    string
		key     string
		want    string
		wantErr bool
	}{
		{"get existing key", "key1", "value1", false},
		{"get non-existent key", "key2", "", true},
		{"get empty key", "", "", true},
	}

	// Set up test data
	engine.Set(t.Context(), "key1", "value1")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := engine.Get(t.Context(), tt.key)
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
	engine := engine.New()
	tests := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{"delete existing key", "key1", false},
		{"delete non-existent key", "key2", true},
		{"delete empty key", "", true},
	}

	// Set up test data
	engine.Set(t.Context(), "key1", "value1")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := engine.Del(t.Context(), tt.key); (err != nil) != tt.wantErr {
				t.Errorf("Del() error = %v, wantErr %v", err, tt.wantErr)
			}
			// Verify the key was deleted
			if !tt.wantErr {
				_, err := engine.Get(t.Context(), tt.key)
				if err == nil {
					t.Errorf("Get() after Del() should have failed")
				}
			}
		})
	}
}

func TestEngine_ConcurrentAccess(t *testing.T) {
	engine := engine.New()
	const numGoroutines = 10
	const numOps = 100

	// Create a map to track expected values
	expectedValues := sync.Map{}
	// expectedValues := make(map[string]string)

	// Create a channel to signal when all operations are done
	done := make(chan struct{})

	// Start multiple goroutines performing concurrent operations
	for i := range numGoroutines {
		go func() {
			for j := range numOps {
				key := fmt.Sprintf("key-%d-%d", i, j)
				value := fmt.Sprintf("value-%d-%d", i, j)

				// Set value
				if err := engine.Set(t.Context(), key, value); err != nil {
					t.Errorf("Set failed: %v", err)
					continue
				}
				expectedValues.Store(key, value)

				// Get value
				got, err := engine.Get(t.Context(), key)
				if err != nil {
					t.Errorf("Get failed: %v", err)
					continue
				}
				if got != value {
					t.Errorf("Get returned wrong value: got %v, want %v", got, value)
				}

				// Delete value
				if err = engine.Del(t.Context(), key); err != nil {
					t.Errorf("Del failed: %v", err)
					continue
				}
				expectedValues.Delete(key)

				// Verify deletion
				_, err = engine.Get(t.Context(), key)
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
