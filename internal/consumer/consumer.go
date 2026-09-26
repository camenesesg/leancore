package consumer

import (
	"context"
	"errors"
	"log/slog"

	"leancore/internal/event"
	"leancore/internal/queue"
)

// Recorder receives consumed events. It reports whether the event was counted.
type Recorder interface {
	Record(e event.Event) bool
}

// Run consumes events from q and hands them to rec until q is closed and
// drained, in which case it returns nil, or until ctx is done, in which case
// it returns ctx.Err(). To drain pending events on shutdown, close q instead
// of canceling ctx.
func Run(ctx context.Context, q queue.Queue, rec Recorder) error {
	for {
		e, err := q.Consume(ctx)
		if errors.Is(err, queue.ErrClosed) {
			return nil
		}
		if err != nil {
			return err
		}
		if !rec.Record(e) {
			slog.Debug("event ignored by aggregator", "id", e.ID, "type", e.Type, "occurred_at", e.OccurredAt)
		}
	}
}
