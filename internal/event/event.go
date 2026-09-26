package event

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Type identifies the kind of payment event.
type Type string

// Supported event types.
const (
	TypePaymentProcessed Type = "payment.processed"
	TypePaymentFailed    Type = "payment.failed"
)

// MaxFutureSkew is how far in the future an event's OccurredAt may be,
// relative to the server clock, before the event is rejected.
const MaxFutureSkew = time.Minute

// ErrInvalid is wrapped by every error returned from Validate.
var ErrInvalid = errors.New("invalid event")

// Valid reports whether t is a supported event type.
func (t Type) Valid() bool {
	return t == TypePaymentProcessed || t == TypePaymentFailed
}

// Event is a payment outcome published to the queue.
type Event struct {
	Type       Type      `json:"type"`
	ID         string    `json:"id"`
	Amount     float64   `json:"amount,omitempty"`
	OccurredAt time.Time `json:"occurred_at"`
}

// Validate checks e against the event format and, when OccurredAt is unset,
// defaults it to now.
func (e *Event) Validate(now time.Time) error {
	if !e.Type.Valid() {
		return fmt.Errorf("%w: unknown type %q", ErrInvalid, e.Type)
	}
	if strings.TrimSpace(e.ID) == "" {
		return fmt.Errorf("%w: id is required", ErrInvalid)
	}
	if e.Amount < 0 {
		return fmt.Errorf("%w: amount must be >= 0", ErrInvalid)
	}
	if e.OccurredAt.IsZero() {
		e.OccurredAt = now
		return nil
	}
	if e.OccurredAt.After(now.Add(MaxFutureSkew)) {
		return fmt.Errorf("%w: occurred_at is more than %v in the future", ErrInvalid, MaxFutureSkew)
	}
	return nil
}
