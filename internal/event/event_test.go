package event

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestValidate(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 5, 30, 0, time.UTC)
	tests := []struct {
		name    string
		in      Event
		wantErr bool
		wantAt  time.Time
	}{
		{
			name:   "processed with explicit time",
			in:     Event{Type: TypePaymentProcessed, ID: "p-1", Amount: 10.5, OccurredAt: now.Add(-time.Minute)},
			wantAt: now.Add(-time.Minute),
		},
		{
			name:   "failed without occurred_at defaults to now",
			in:     Event{Type: TypePaymentFailed, ID: "p-2"},
			wantAt: now,
		},
		{
			name:   "zero amount is valid",
			in:     Event{Type: TypePaymentProcessed, ID: "p-3", Amount: 0, OccurredAt: now},
			wantAt: now,
		},
		{
			name:   "exactly max future skew is valid",
			in:     Event{Type: TypePaymentProcessed, ID: "p-4", OccurredAt: now.Add(MaxFutureSkew)},
			wantAt: now.Add(MaxFutureSkew),
		},
		{
			name:    "unknown type",
			in:      Event{Type: "payment.refunded", ID: "p-5"},
			wantErr: true,
		},
		{
			name:    "empty type",
			in:      Event{ID: "p-6"},
			wantErr: true,
		},
		{
			name:    "empty id",
			in:      Event{Type: TypePaymentProcessed},
			wantErr: true,
		},
		{
			name:    "blank id",
			in:      Event{Type: TypePaymentProcessed, ID: "   "},
			wantErr: true,
		},
		{
			name:    "negative amount",
			in:      Event{Type: TypePaymentFailed, ID: "p-7", Amount: -1},
			wantErr: true,
		},
		{
			name:    "too far in the future",
			in:      Event{Type: TypePaymentProcessed, ID: "p-8", OccurredAt: now.Add(5 * time.Minute)},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := tc.in
			err := e.Validate(now)
			if tc.wantErr {
				if !errors.Is(err, ErrInvalid) {
					t.Fatalf("Validate() error = %v, want wrapping ErrInvalid", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Validate() unexpected error: %v", err)
			}
			if !e.OccurredAt.Equal(tc.wantAt) {
				t.Errorf("OccurredAt = %v, want %v", e.OccurredAt, tc.wantAt)
			}
		})
	}
}

func TestUnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    Event
		wantErr bool
	}{
		{
			name: "all fields",
			in:   `{"type":"payment.processed","id":"p-1","amount":10.5,"occurred_at":"2026-09-25T12:05:00Z"}`,
			want: Event{Type: TypePaymentProcessed, ID: "p-1", Amount: 10.5, OccurredAt: time.Date(2026, 9, 25, 12, 5, 0, 0, time.UTC)},
		},
		{
			name: "optional fields omitted",
			in:   `{"type":"payment.failed","id":"p-2"}`,
			want: Event{Type: TypePaymentFailed, ID: "p-2"},
		},
		{
			name:    "occurred_at not RFC 3339",
			in:      `{"type":"payment.failed","id":"p-3","occurred_at":"25/09/2026"}`,
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got Event
			err := json.Unmarshal([]byte(tc.in), &got)
			if tc.wantErr {
				if err == nil {
					t.Fatal("json.Unmarshal() succeeded, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("json.Unmarshal() unexpected error: %v", err)
			}
			if got.Type != tc.want.Type || got.ID != tc.want.ID || got.Amount != tc.want.Amount || !got.OccurredAt.Equal(tc.want.OccurredAt) {
				t.Errorf("json.Unmarshal() = %+v, want %+v", got, tc.want)
			}
		})
	}
}
