package release_test

import (
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"
)

// usage maps a resource name to its current amount.
type usage map[string]int64

// resourceMonitor enforces absolute ceilings and compares the minimum of each window after warmup with the first
// window's minima. Window minima tolerate temporary overlap such as a snapshot written beside the WAL it replaces;
// the finite growth budgets detect sustained growth, not arbitrarily small leaks.
type resourceMonitor struct {
	ceilings, growth      usage
	warmup, window        time.Duration
	begin, windowStart    time.Time
	floor, baseline, peak usage
}

func newResourceMonitor(ceilings, growth usage, warmup, window time.Duration, begin time.Time) *resourceMonitor {
	m := &resourceMonitor{ceilings: ceilings, growth: growth, warmup: warmup, window: window, peak: usage{}}
	m.reset(begin)
	return m
}

// reset starts a new warmup, for example after a restart replaced the process being measured.
func (m *resourceMonitor) reset(now time.Time) {
	m.begin, m.windowStart, m.floor, m.baseline = now, time.Time{}, nil, nil
}

func (m *resourceMonitor) sample(current usage, now time.Time) error {
	for name, value := range current {
		m.peak[name] = max(m.peak[name], value)
		if value > m.ceilings[name] {
			return fmt.Errorf("%s ceiling exceeded: %d > %d", name, value, m.ceilings[name])
		}
	}
	if now.Sub(m.begin) < m.warmup {
		return nil
	}
	if m.floor == nil {
		m.windowStart, m.floor = now, maps.Clone(current)
	}
	for name, value := range current {
		m.floor[name] = min(m.floor[name], value)
	}
	if now.Sub(m.windowStart) < m.window {
		return nil
	}
	if m.baseline == nil {
		m.baseline = maps.Clone(m.floor)
	} else {
		for name, value := range m.floor {
			if value > m.baseline[name]+m.growth[name] {
				return fmt.Errorf("%s sustained growth: window minimum %d, baseline %d", name, value, m.baseline[name])
			}
		}
	}
	m.windowStart, m.floor = now, maps.Clone(current)
	return nil
}

func resources() []string { return []string{"rss_bytes", "disk_bytes", "descriptors"} }

// uniform returns the same amount for every resource, with optional overrides.
func uniform(amount int64, overrides usage) usage {
	u := usage{}
	for _, name := range resources() {
		u[name] = amount
	}
	maps.Copy(u, overrides)
	return u
}

func testMonitor(begin time.Time) *resourceMonitor {
	return newResourceMonitor(usage{"rss_bytes": 100, "disk_bytes": 100, "descriptors": 64}, uniform(8, nil),
		time.Second, time.Second, begin)
}

func at(begin time.Time, seconds float64) time.Time {
	return begin.Add(time.Duration(seconds * float64(time.Second)))
}

func TestResourceMonitorCeilings(t *testing.T) {
	begin := time.Now()
	for name, value := range (usage{"rss_bytes": 101, "disk_bytes": 101, "descriptors": 65}) {
		t.Run(name, func(t *testing.T) {
			err := testMonitor(begin).sample(uniform(10, usage{name: value}), begin)
			if err == nil || !strings.Contains(err.Error(), "ceiling exceeded") {
				t.Fatalf("sample = %v, want a ceiling error", err)
			}
		})
	}
}

func TestResourceMonitorSustainedGrowthBelowCeiling(t *testing.T) {
	begin := time.Now()
	for _, name := range resources() {
		t.Run(name, func(t *testing.T) {
			m := testMonitor(begin)
			grown := uniform(10, usage{name: 19})
			for i, current := range []usage{uniform(10, nil), uniform(10, nil), grown} {
				if err := m.sample(current, at(begin, float64(i+1))); err != nil {
					t.Fatalf("sample %d: %v", i+1, err)
				}
			}
			if err := m.sample(grown, at(begin, 4)); err == nil || !strings.Contains(err.Error(), "sustained growth") {
				t.Fatalf("sample = %v, want a sustained-growth error", err)
			}
		})
	}
}

func TestResourceMonitorIgnoresWarmupAndSpikes(t *testing.T) {
	begin := time.Now()
	m := testMonitor(begin)
	for _, s := range []struct {
		amount  int64
		seconds float64
	}{{60, 0}, {10, 1}, {10, 2}, {60, 2.5}, {10, 3}} {
		if err := m.sample(uniform(s.amount, nil), at(begin, s.seconds)); err != nil {
			t.Fatalf("sample at %vs: %v", s.seconds, err)
		}
	}
	if !maps.Equal(m.baseline, uniform(10, nil)) {
		t.Fatalf("baseline = %v, want the post-warmup minima", m.baseline)
	}
}
