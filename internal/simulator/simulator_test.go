package simulator

import (
	"bytes"
	"context"
	"log/slog"
	"math"
	"strings"
	"testing"
	"time"

	"leancore/internal/event"
	"leancore/internal/queue"
)

// drain returns how many queued events of each type q holds.
func drain(t *testing.T, q *queue.MemoryQueue) map[event.Type]int {
	t.Helper()
	counts := make(map[event.Type]int)
	q.Close()
	for {
		e, err := q.Consume(context.Background())
		if err != nil {
			return counts
		}
		if err := e.Validate(time.Now()); err != nil {
			t.Errorf("simulated event %+v is invalid: %v", e, err)
		}
		counts[e.Type]++
	}
}

func run(t *testing.T, q queue.Queue, rate, failureRatio float64, d time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	if err := Run(ctx, q, rate, failureRatio); err != nil {
		t.Fatalf("Run() error: %v", err)
	}
}

func TestRunPublishesBothTypes(t *testing.T) {
	q := queue.NewMemoryQueue(10000)
	run(t, q, 1000, 0.5, 200*time.Millisecond)

	counts := drain(t, q)
	if counts[event.TypePaymentProcessed] == 0 || counts[event.TypePaymentFailed] == 0 {
		t.Errorf("published %v, want both processed and failed events", counts)
	}
}

func TestRunFailureRatioBounds(t *testing.T) {
	tests := []struct {
		ratio float64
		none  event.Type
	}{
		{ratio: 0, none: event.TypePaymentFailed},
		{ratio: 1, none: event.TypePaymentProcessed},
	}
	for _, tc := range tests {
		q := queue.NewMemoryQueue(10000)
		run(t, q, 1000, tc.ratio, 50*time.Millisecond)
		counts := drain(t, q)
		if counts[tc.none] != 0 {
			t.Errorf("ratio %v published %d %s events, want 0", tc.ratio, counts[tc.none], tc.none)
		}
		if len(counts) == 0 {
			t.Errorf("ratio %v published no events", tc.ratio)
		}
	}
}

func TestRunLogsRejectedWhenQueueFull(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	q := queue.NewMemoryQueue(1)
	run(t, q, 1000, 0.5, 50*time.Millisecond)

	if q.Len() != 1 {
		t.Errorf("queue Len() = %d, want 1", q.Len())
	}
	if log := buf.String(); !strings.Contains(log, "simulator events rejected") || !strings.Contains(log, "queue is full") {
		t.Errorf("log = %q, want a queue-full rejection summary", log)
	}
}

func TestRunStopsWhenQueueClosed(t *testing.T) {
	q := queue.NewMemoryQueue(10)
	q.Close()
	done := make(chan error, 1)
	go func() { done <- Run(context.Background(), q, 1000, 0.5) }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run() error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not return after the queue was closed")
	}
}

func TestRunRejectsInvalidArgs(t *testing.T) {
	tests := []struct {
		name          string
		rate, failure float64
	}{
		{name: "zero rate", rate: 0, failure: 0.2},
		{name: "negative rate", rate: -1, failure: 0.2},
		{name: "NaN rate", rate: math.NaN(), failure: 0.2},
		{name: "infinite rate", rate: math.Inf(1), failure: 0.2},
		{name: "ratio below 0", rate: 5, failure: -0.1},
		{name: "ratio above 1", rate: 5, failure: 1.1},
		{name: "NaN ratio", rate: 5, failure: math.NaN()},
	}
	for _, tc := range tests {
		if err := Run(context.Background(), queue.NewMemoryQueue(1), tc.rate, tc.failure); err == nil {
			t.Errorf("%s: Run() succeeded, want error", tc.name)
		}
	}
}
