package simulator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"time"

	"leancore/internal/event"
	"leancore/internal/queue"
)

// minInterval bounds how often events are published, so the effective rate
// tops out at 1000 events per second.
const minInterval = time.Millisecond

// dropReportInterval is how often rejected events are summarized in the log.
const dropReportInterval = time.Second

// Run publishes random payment events to q at about rate events per second,
// with failureRatio of them being failures, until ctx is done or q is closed.
// Both cases are a normal stop and return nil. Events rejected because q is
// full are dropped and reported in the log once per second.
func Run(ctx context.Context, q queue.Queue, rate, failureRatio float64) error {
	if !(rate > 0) || math.IsInf(rate, 0) {
		return fmt.Errorf("simulator: rate must be a positive number, got %v", rate)
	}
	if !(failureRatio >= 0 && failureRatio <= 1) {
		return fmt.Errorf("simulator: failure ratio must be in [0, 1], got %v", failureRatio)
	}

	interval := max(time.Duration(float64(time.Second)/rate), minInterval)
	publish := time.NewTicker(interval)
	defer publish.Stop()
	report := time.NewTicker(dropReportInterval)
	defer report.Stop()

	var seq, dropped int
	defer func() { logDropped(dropped) }()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-report.C:
			logDropped(dropped)
			dropped = 0
		case <-publish.C:
			seq++
			err := q.Publish(ctx, randomEvent(seq, failureRatio))
			switch {
			case err == nil:
			case errors.Is(err, queue.ErrQueueFull):
				dropped++
			case errors.Is(err, queue.ErrClosed), ctx.Err() != nil:
				return nil
			default:
				return fmt.Errorf("simulator: publishing event: %w", err)
			}
		}
	}
}

func randomEvent(seq int, failureRatio float64) event.Event {
	typ := event.TypePaymentProcessed
	if rand.Float64() < failureRatio {
		typ = event.TypePaymentFailed
	}
	return event.Event{
		Type:       typ,
		ID:         fmt.Sprintf("sim-%d", seq),
		Amount:     math.Round((1+rand.Float64()*499)*100) / 100,
		OccurredAt: time.Now().UTC(),
	}
}

func logDropped(n int) {
	if n > 0 {
		slog.Warn("simulator events rejected", "reason", queue.ErrQueueFull, "count", n)
	}
}
