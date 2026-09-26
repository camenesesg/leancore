package aggregator

import (
	"sync"
	"time"

	"leancore/internal/event"
)

// DefaultRetention is the retention window used when none is configured.
const DefaultRetention = 60 * time.Minute

// Counts holds the number of processed and failed payments.
type Counts struct {
	Processed int `json:"processed"`
	Failed    int `json:"failed"`
}

// MinuteCounts holds the counts for the minute starting at Minute (UTC).
type MinuteCounts struct {
	Minute time.Time `json:"minute"`
	Counts
}

// Snapshot is a point-in-time view of the most recent minutes.
type Snapshot struct {
	GeneratedAt time.Time      `json:"generated_at"`
	Minutes     []MinuteCounts `json:"minutes"`
	Totals      Counts         `json:"totals"`
}

// Aggregator keeps per-minute payment counts within a retention window.
// It is safe for concurrent use.
type Aggregator struct {
	retention time.Duration
	now       func() time.Time

	mu      sync.Mutex
	buckets map[time.Time]*Counts
}

// New returns an Aggregator that keeps retention worth of minutes, rounded
// down to whole minutes (at least one). A retention of zero or less means
// DefaultRetention. now is the clock; nil means time.Now.
func New(retention time.Duration, now func() time.Time) *Aggregator {
	if retention <= 0 {
		retention = DefaultRetention
	}
	if retention < time.Minute {
		retention = time.Minute
	}
	if now == nil {
		now = time.Now
	}
	return &Aggregator{
		retention: retention.Truncate(time.Minute),
		now:       now,
		buckets:   make(map[time.Time]*Counts),
	}
}

// RetentionMinutes returns how many minutes, including the current one, the
// aggregator keeps.
func (a *Aggregator) RetentionMinutes() int {
	return int(a.retention / time.Minute)
}

// Record counts e in the minute of its OccurredAt. It returns false, without
// changing any count, when e has an unknown type, falls before the retention
// window, or is more than event.MaxFutureSkew in the future.
func (a *Aggregator) Record(e event.Event) bool {
	now := a.now().UTC()
	if e.OccurredAt.After(now.Add(event.MaxFutureSkew)) {
		return false
	}
	minute := e.OccurredAt.UTC().Truncate(time.Minute)
	oldest := a.oldestMinute(now)
	if minute.Before(oldest) {
		return false
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.buckets[minute]
	if b == nil {
		b = &Counts{}
	}
	switch e.Type {
	case event.TypePaymentProcessed:
		b.Processed++
	case event.TypePaymentFailed:
		b.Failed++
	default:
		return false
	}
	a.buckets[minute] = b
	a.pruneLocked(oldest)
	return true
}

// Snapshot returns the counts for the last n minutes, oldest first and ending
// with the current minute. Minutes without events have zero counts. n is
// clamped to [1, RetentionMinutes()].
func (a *Aggregator) Snapshot(n int) Snapshot {
	n = max(1, min(n, a.RetentionMinutes()))
	now := a.now().UTC()
	current := now.Truncate(time.Minute)
	oldest := a.oldestMinute(now)

	s := Snapshot{GeneratedAt: now, Minutes: make([]MinuteCounts, n)}
	a.mu.Lock()
	defer a.mu.Unlock()
	for i := range s.Minutes {
		m := current.Add(-time.Duration(n-1-i) * time.Minute)
		s.Minutes[i].Minute = m
		// Buckets older than the window may linger until the next Record
		// prunes them; they must not be reported.
		if b := a.buckets[m]; b != nil && !m.Before(oldest) {
			s.Minutes[i].Counts = *b
			s.Totals.Processed += b.Processed
			s.Totals.Failed += b.Failed
		}
	}
	return s
}

// oldestMinute returns the start of the oldest minute inside the window.
func (a *Aggregator) oldestMinute(now time.Time) time.Time {
	return now.Truncate(time.Minute).Add(-a.retention + time.Minute)
}

func (a *Aggregator) pruneLocked(oldest time.Time) {
	for m := range a.buckets {
		if m.Before(oldest) {
			delete(a.buckets, m)
		}
	}
}
