package aggregator

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"leancore/internal/event"
)

// clock is a settable time source for tests.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

func at(hour, minute, sec int) time.Time {
	return time.Date(2026, 9, 25, hour, minute, sec, 0, time.UTC)
}

func processed(t time.Time) event.Event {
	return event.Event{Type: event.TypePaymentProcessed, ID: "p", OccurredAt: t}
}

func failed(t time.Time) event.Event {
	return event.Event{Type: event.TypePaymentFailed, ID: "f", OccurredAt: t}
}

func mustRecord(t *testing.T, a *Aggregator, e event.Event) {
	t.Helper()
	if !a.Record(e) {
		t.Fatalf("Record(%+v) = false, want true", e)
	}
}

func TestNewDefaults(t *testing.T) {
	tests := []struct {
		in   time.Duration
		want int
	}{
		{in: 0, want: 60},
		{in: -time.Minute, want: 60},
		{in: 30 * time.Second, want: 1},
		{in: 90 * time.Second, want: 1},
		{in: 15 * time.Minute, want: 15},
	}
	for _, tc := range tests {
		if got := New(tc.in, nil).RetentionMinutes(); got != tc.want {
			t.Errorf("New(%v).RetentionMinutes() = %d, want %d", tc.in, got, tc.want)
		}
	}
}

// TC-DASH-01: Count successes and failures in the same minute.
func TestSameMinuteCounts(t *testing.T) {
	c := &clock{t: at(12, 5, 30)}
	a := New(DefaultRetention, c.Now)
	mustRecord(t, a, processed(at(12, 5, 0)))
	mustRecord(t, a, processed(at(12, 5, 59)))
	mustRecord(t, a, failed(at(12, 5, 10)))

	s := a.Snapshot(1)
	if len(s.Minutes) != 1 {
		t.Fatalf("len(Minutes) = %d, want 1", len(s.Minutes))
	}
	want := MinuteCounts{Minute: at(12, 5, 0), Counts: Counts{Processed: 2, Failed: 1}}
	if got := s.Minutes[0]; !got.Minute.Equal(want.Minute) || got.Counts != want.Counts {
		t.Errorf("Minutes[0] = %+v, want %+v", got, want)
	}
	if s.Totals != want.Counts {
		t.Errorf("Totals = %+v, want %+v", s.Totals, want.Counts)
	}
	if !s.GeneratedAt.Equal(at(12, 5, 30)) {
		t.Errorf("GeneratedAt = %v, want %v", s.GeneratedAt, at(12, 5, 30))
	}
}

func TestDifferentMinutes(t *testing.T) {
	c := &clock{t: at(12, 6, 30)}
	a := New(DefaultRetention, c.Now)
	mustRecord(t, a, processed(at(12, 5, 30)))
	mustRecord(t, a, processed(at(12, 6, 10)))

	s := a.Snapshot(2)
	for i, m := range []time.Time{at(12, 5, 0), at(12, 6, 0)} {
		got := s.Minutes[i]
		if !got.Minute.Equal(m) || got.Counts != (Counts{Processed: 1}) {
			t.Errorf("Minutes[%d] = %+v, want minute %v with processed 1", i, got, m)
		}
	}
}

func TestNonUTCTimesUseUTCMinute(t *testing.T) {
	c := &clock{t: at(12, 5, 30)}
	a := New(DefaultRetention, c.Now)
	bogota := time.FixedZone("COT", -5*60*60)
	mustRecord(t, a, failed(at(12, 5, 20).In(bogota)))

	s := a.Snapshot(1)
	if s.Minutes[0].Minute.Location() != time.UTC {
		t.Errorf("Minute location = %v, want UTC", s.Minutes[0].Minute.Location())
	}
	if s.Minutes[0].Failed != 1 {
		t.Errorf("Failed = %d, want 1", s.Minutes[0].Failed)
	}
}

// TC-DASH-02: Zero-fill empty minutes.
func TestSnapshotZeroFills(t *testing.T) {
	c := &clock{t: at(12, 10, 0)}
	a := New(DefaultRetention, c.Now)
	mustRecord(t, a, failed(at(12, 8, 15)))

	s := a.Snapshot(3)
	want := []MinuteCounts{
		{Minute: at(12, 8, 0), Counts: Counts{Failed: 1}},
		{Minute: at(12, 9, 0)},
		{Minute: at(12, 10, 0)},
	}
	if len(s.Minutes) != len(want) {
		t.Fatalf("len(Minutes) = %d, want %d", len(s.Minutes), len(want))
	}
	for i := range want {
		if got := s.Minutes[i]; !got.Minute.Equal(want[i].Minute) || got.Counts != want[i].Counts {
			t.Errorf("Minutes[%d] = %+v, want %+v", i, got, want[i])
		}
	}
	if s.Totals != (Counts{Failed: 1}) {
		t.Errorf("Totals = %+v, want {Failed: 1}", s.Totals)
	}
}

func TestSnapshotClampsN(t *testing.T) {
	c := &clock{t: at(12, 0, 0)}
	a := New(10*time.Minute, c.Now)
	tests := []struct {
		n, want int
	}{
		{n: 0, want: 1},
		{n: -5, want: 1},
		{n: 5, want: 5},
		{n: 60, want: 10},
	}
	for _, tc := range tests {
		if got := len(a.Snapshot(tc.n).Minutes); got != tc.want {
			t.Errorf("len(Snapshot(%d).Minutes) = %d, want %d", tc.n, got, tc.want)
		}
	}
}

// TC-DASH-04: Ignore events outside retention.
func TestRecordIgnoresOutOfWindow(t *testing.T) {
	c := &clock{t: at(12, 0, 0)}
	a := New(DefaultRetention, c.Now)
	tests := []struct {
		name string
		e    event.Event
	}{
		{name: "two hours old", e: processed(at(10, 0, 0))},
		{name: "ninety minutes old", e: failed(at(10, 30, 0))},
		{name: "just before window", e: processed(at(11, 0, 59))},
		{name: "beyond future skew", e: processed(at(12, 1, 1))},
		{name: "unknown type", e: event.Event{Type: "payment.refunded", OccurredAt: at(12, 0, 0)}},
	}
	for _, tc := range tests {
		if a.Record(tc.e) {
			t.Errorf("%s: Record() = true, want false", tc.name)
		}
	}
	if s := a.Snapshot(60); s.Totals != (Counts{}) {
		t.Errorf("Totals = %+v, want zero", s.Totals)
	}
	if n := len(a.buckets); n != 0 {
		t.Errorf("len(buckets) = %d, want 0 (no bucket created)", n)
	}
}

func TestRecordAcceptsWindowEdges(t *testing.T) {
	c := &clock{t: at(12, 0, 30)}
	a := New(DefaultRetention, c.Now)
	mustRecord(t, a, processed(at(11, 1, 0)))  // Oldest minute in a 60-minute window.
	mustRecord(t, a, processed(at(12, 1, 30))) // Exactly MaxFutureSkew ahead.
}

func TestOldBucketsArePrunedAndHidden(t *testing.T) {
	c := &clock{t: at(12, 0, 0)}
	a := New(5*time.Minute, c.Now)
	mustRecord(t, a, processed(at(12, 0, 0)))

	// Once the minute leaves the window it must not be reported, even before
	// any Record prunes it.
	c.Set(at(12, 5, 0))
	if s := a.Snapshot(5); s.Totals != (Counts{}) {
		t.Errorf("Totals after window moved = %+v, want zero", s.Totals)
	}

	mustRecord(t, a, failed(at(12, 5, 0)))
	if _, ok := a.buckets[at(12, 0, 0)]; ok {
		t.Error("bucket for 12:00 still present after Record, want pruned")
	}
	if n := len(a.buckets); n != 1 {
		t.Errorf("len(buckets) = %d, want 1", n)
	}
}

func TestSnapshotJSON(t *testing.T) {
	c := &clock{t: at(12, 10, 3)}
	a := New(DefaultRetention, c.Now)
	mustRecord(t, a, processed(at(12, 10, 0)))

	got, err := json.Marshal(a.Snapshot(2))
	if err != nil {
		t.Fatalf("json.Marshal() error: %v", err)
	}
	want := `{"generated_at":"2026-09-25T12:10:03Z",` +
		`"minutes":[{"minute":"2026-09-25T12:09:00Z","processed":0,"failed":0},` +
		`{"minute":"2026-09-25T12:10:00Z","processed":1,"failed":0}],` +
		`"totals":{"processed":1,"failed":0}}`
	if string(got) != want {
		t.Errorf("json.Marshal() =\n%s\nwant\n%s", got, want)
	}
}

// TestConcurrentRecordAndSnapshot is meant to run with -race.
func TestConcurrentRecordAndSnapshot(t *testing.T) {
	c := &clock{t: at(12, 30, 0)}
	a := New(DefaultRetention, c.Now)

	const writers, perWriter = 8, 500
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				ts := at(12, (w+i)%30, 0)
				if i%2 == 0 {
					a.Record(processed(ts))
				} else {
					a.Record(failed(ts))
				}
			}
		}(w)
	}
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				s := a.Snapshot(60)
				var sum Counts
				for _, m := range s.Minutes {
					sum.Processed += m.Processed
					sum.Failed += m.Failed
				}
				if sum != s.Totals {
					t.Errorf("Totals %+v != sum of minutes %+v", s.Totals, sum)
					return
				}
			}
		}()
	}
	wg.Wait()

	s := a.Snapshot(60)
	want := Counts{Processed: writers * perWriter / 2, Failed: writers * perWriter / 2}
	if s.Totals != want {
		t.Errorf("final Totals = %+v, want %+v", s.Totals, want)
	}
}
