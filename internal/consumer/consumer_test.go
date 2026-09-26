package consumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"leancore/internal/aggregator"
	"leancore/internal/event"
	"leancore/internal/queue"
)

var now = time.Date(2026, 9, 25, 12, 5, 30, 0, time.UTC)

func newAggregator() *aggregator.Aggregator {
	return aggregator.New(aggregator.DefaultRetention, func() time.Time { return now })
}

func publish(t *testing.T, q queue.Queue, typ event.Type, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := q.Publish(context.Background(), event.Event{Type: typ, ID: "p", OccurredAt: now}); err != nil {
			t.Fatalf("Publish() error: %v", err)
		}
	}
}

func totals(a *aggregator.Aggregator) aggregator.Counts {
	return a.Snapshot(1).Totals
}

func TestRunDeliversPublishedEvents(t *testing.T) {
	q := queue.NewMemoryQueue(10)
	agg := newAggregator()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, q, agg) }()

	publish(t, q, event.TypePaymentProcessed, 2)
	publish(t, q, event.TypePaymentFailed, 1)

	want := aggregator.Counts{Processed: 2, Failed: 1}
	deadline := time.Now().Add(time.Second)
	for totals(agg) != want {
		if time.Now().After(deadline) {
			t.Fatalf("Totals = %+v after 1s, want %+v", totals(agg), want)
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not return after ctx was canceled")
	}
}

// TC-ING-05: Drain queue on shutdown.
func TestRunDrainsAfterClose(t *testing.T) {
	q := queue.NewMemoryQueue(10)
	agg := newAggregator()
	publish(t, q, event.TypePaymentProcessed, 2)
	publish(t, q, event.TypePaymentFailed, 1)
	q.Close()

	done := make(chan error, 1)
	go func() { done <- Run(context.Background(), q, agg) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v, want nil after drain", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not return after the queue was closed")
	}
	if got, want := totals(agg), (aggregator.Counts{Processed: 2, Failed: 1}); got != want {
		t.Errorf("Totals = %+v, want %+v", got, want)
	}
}

func TestRunSkipsIgnoredEvents(t *testing.T) {
	q := queue.NewMemoryQueue(10)
	agg := newAggregator()
	if err := q.Publish(context.Background(), event.Event{Type: event.TypePaymentFailed, ID: "old", OccurredAt: now.Add(-3 * time.Hour)}); err != nil {
		t.Fatalf("Publish() error: %v", err)
	}
	publish(t, q, event.TypePaymentFailed, 1)
	q.Close()

	if err := Run(context.Background(), q, agg); err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	if got, want := totals(agg), (aggregator.Counts{Failed: 1}); got != want {
		t.Errorf("Totals = %+v, want %+v", got, want)
	}
}
