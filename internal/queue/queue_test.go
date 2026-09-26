package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"leancore/internal/event"
)

func newEvent(id string) event.Event {
	return event.Event{Type: event.TypePaymentProcessed, ID: id}
}

func TestNewMemoryQueueCapacity(t *testing.T) {
	tests := []struct {
		in, want int
	}{
		{in: 5, want: 5},
		{in: 0, want: DefaultCapacity},
		{in: -3, want: DefaultCapacity},
	}
	for _, tc := range tests {
		if got := NewMemoryQueue(tc.in).Cap(); got != tc.want {
			t.Errorf("NewMemoryQueue(%d).Cap() = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestFIFOOrder(t *testing.T) {
	ctx := context.Background()
	q := NewMemoryQueue(3)
	for _, id := range []string{"a", "b", "c"} {
		if err := q.Publish(ctx, newEvent(id)); err != nil {
			t.Fatalf("Publish(%q) error: %v", id, err)
		}
	}
	for _, want := range []string{"a", "b", "c"} {
		got, err := q.Consume(ctx)
		if err != nil {
			t.Fatalf("Consume() error: %v", err)
		}
		if got.ID != want {
			t.Errorf("Consume().ID = %q, want %q", got.ID, want)
		}
	}
}

// TC-ING-04 at the queue level, plus the "capacity is configurable" scenario.
func TestPublishWhenFull(t *testing.T) {
	ctx := context.Background()
	q := NewMemoryQueue(2)
	for _, id := range []string{"a", "b"} {
		if err := q.Publish(ctx, newEvent(id)); err != nil {
			t.Fatalf("Publish(%q) error: %v", id, err)
		}
	}
	if err := q.Publish(ctx, newEvent("c")); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("Publish() on full queue error = %v, want ErrQueueFull", err)
	}
	if got := q.Len(); got != 2 {
		t.Errorf("Len() = %d, want 2", got)
	}
}

func TestPublishCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	q := NewMemoryQueue(1)
	if err := q.Publish(ctx, newEvent("a")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish() error = %v, want context.Canceled", err)
	}
	if got := q.Len(); got != 0 {
		t.Errorf("Len() = %d, want 0", got)
	}
}

func TestConsumeBlocksUntilPublish(t *testing.T) {
	q := NewMemoryQueue(1)
	got := make(chan event.Event, 1)
	go func() {
		e, err := q.Consume(context.Background())
		if err != nil {
			t.Errorf("Consume() error: %v", err)
		}
		got <- e
	}()

	select {
	case e := <-got:
		t.Fatalf("Consume() returned %+v before any Publish", e)
	case <-time.After(50 * time.Millisecond):
	}

	if err := q.Publish(context.Background(), newEvent("a")); err != nil {
		t.Fatalf("Publish() error: %v", err)
	}
	select {
	case e := <-got:
		if e.ID != "a" {
			t.Errorf("Consume().ID = %q, want %q", e.ID, "a")
		}
	case <-time.After(time.Second):
		t.Fatal("Consume() did not return after Publish")
	}
}

func TestConsumeCanceledByContext(t *testing.T) {
	q := NewMemoryQueue(1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := q.Consume(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Consume() error = %v, want context.DeadlineExceeded", err)
	}
}

func TestCloseDrainsPendingEvents(t *testing.T) {
	ctx := context.Background()
	q := NewMemoryQueue(3)
	for _, id := range []string{"a", "b", "c"} {
		if err := q.Publish(ctx, newEvent(id)); err != nil {
			t.Fatalf("Publish(%q) error: %v", id, err)
		}
	}
	q.Close()
	q.Close() // Idempotent.

	if err := q.Publish(ctx, newEvent("d")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Publish() after Close error = %v, want ErrClosed", err)
	}
	for _, want := range []string{"a", "b", "c"} {
		got, err := q.Consume(ctx)
		if err != nil {
			t.Fatalf("Consume() error: %v", err)
		}
		if got.ID != want {
			t.Errorf("Consume().ID = %q, want %q", got.ID, want)
		}
	}
	if _, err := q.Consume(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("Consume() on drained queue error = %v, want ErrClosed", err)
	}
}

func TestCloseUnblocksConsumer(t *testing.T) {
	q := NewMemoryQueue(1)
	errc := make(chan error, 1)
	go func() {
		_, err := q.Consume(context.Background())
		errc <- err
	}()
	q.Close()
	select {
	case err := <-errc:
		if !errors.Is(err, ErrClosed) {
			t.Errorf("Consume() error = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Consume() still blocked after Close")
	}
}

// TestConcurrentPublishConsumeClose is meant to run with -race: producers keep
// publishing while Close happens, and every accepted event must be consumed.
func TestConcurrentPublishConsumeClose(t *testing.T) {
	ctx := context.Background()
	q := NewMemoryQueue(16)

	var consumer sync.WaitGroup
	var mu sync.Mutex
	acceptedCount := 0

	var producers sync.WaitGroup
	for p := 0; p < 4; p++ {
		producers.Add(1)
		go func(p int) {
			defer producers.Done()
			for i := 0; i < 200; i++ {
				err := q.Publish(ctx, newEvent(fmt.Sprintf("%d-%d", p, i)))
				switch {
				case err == nil:
					mu.Lock()
					acceptedCount++
					mu.Unlock()
				case errors.Is(err, ErrClosed):
					return
				case errors.Is(err, ErrQueueFull):
				default:
					t.Errorf("Publish() unexpected error: %v", err)
				}
			}
		}(p)
	}

	consumed := 0
	consumer.Add(1)
	go func() {
		defer consumer.Done()
		for {
			if _, err := q.Consume(ctx); err != nil {
				if !errors.Is(err, ErrClosed) {
					t.Errorf("Consume() unexpected error: %v", err)
				}
				return
			}
			consumed++
		}
	}()

	time.Sleep(time.Millisecond)
	q.Close()
	producers.Wait()
	consumer.Wait()

	mu.Lock()
	defer mu.Unlock()
	if consumed != acceptedCount {
		t.Errorf("consumed %d events, want %d accepted", consumed, acceptedCount)
	}
}
