package queue

import (
	"context"
	"errors"
	"sync"

	"leancore/internal/event"
)

// DefaultCapacity is the queue capacity used when none is configured.
const DefaultCapacity = 1000

var (
	// ErrQueueFull is returned by Publish when the queue has no free space.
	ErrQueueFull = errors.New("queue is full")
	// ErrClosed is returned by Publish after Close, and by Consume once a
	// closed queue has been drained.
	ErrClosed = errors.New("queue is closed")
)

// Queue carries payment events from producers to consumers.
type Queue interface {
	// Publish enqueues e without blocking. It returns ErrQueueFull when
	// there is no space and ErrClosed after Close.
	Publish(ctx context.Context, e event.Event) error
	// Consume blocks until an event is available or ctx is done. After
	// Close it keeps returning pending events, then ErrClosed.
	Consume(ctx context.Context) (event.Event, error)
	// Close stops accepting new events. It is safe to call more than once.
	Close()
}

// MemoryQueue is a bounded, in-process FIFO Queue backed by a buffered channel.
type MemoryQueue struct {
	events chan event.Event

	mu     sync.RWMutex // guards closed and closing events
	closed bool
}

var _ Queue = (*MemoryQueue)(nil)

// NewMemoryQueue returns an empty queue holding at most capacity events.
// A capacity below 1 means DefaultCapacity.
func NewMemoryQueue(capacity int) *MemoryQueue {
	if capacity < 1 {
		capacity = DefaultCapacity
	}
	return &MemoryQueue{events: make(chan event.Event, capacity)}
}

// Publish implements Queue.
func (q *MemoryQueue) Publish(ctx context.Context, e event.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// The read lock keeps Close from closing the channel mid-send; the send
	// itself never blocks, so holding the lock is cheap.
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return ErrClosed
	}
	select {
	case q.events <- e:
		return nil
	default:
		return ErrQueueFull
	}
}

// Consume implements Queue.
func (q *MemoryQueue) Consume(ctx context.Context) (event.Event, error) {
	select {
	case e, ok := <-q.events:
		if !ok {
			return event.Event{}, ErrClosed
		}
		return e, nil
	case <-ctx.Done():
		return event.Event{}, ctx.Err()
	}
}

// Close implements Queue.
func (q *MemoryQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	close(q.events)
}

// Len returns the number of events waiting to be consumed.
func (q *MemoryQueue) Len() int { return len(q.events) }

// Cap returns the maximum number of events the queue can hold.
func (q *MemoryQueue) Cap() int { return cap(q.events) }
