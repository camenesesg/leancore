package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"leancore/internal/aggregator"
	"leancore/internal/event"
	"leancore/internal/queue"
)

var now = time.Date(2026, 9, 25, 12, 10, 3, 0, time.UTC)

func clock() time.Time { return now }

func newServer(t *testing.T, capacity int, retention time.Duration) (*Server, *queue.MemoryQueue, *aggregator.Aggregator) {
	t.Helper()
	q := queue.NewMemoryQueue(capacity)
	agg := aggregator.New(retention, clock)
	return New(q, agg, clock), q, agg
}

func do(t *testing.T, h http.Handler, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func errorMessage(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var body struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("error body %q is not JSON: %v", rec.Body.String(), err)
	}
	if body.Error == "" {
		t.Errorf("error body %q has empty \"error\"", rec.Body.String())
	}
	return body.Error
}

// TC-ING-01: Accept a valid processed event.
func TestPublishAccepted(t *testing.T) {
	s, q, _ := newServer(t, 10, 0)
	rec := do(t, s, http.MethodPost, "/api/events", `{"type":"payment.processed","id":"p-1","amount":10.5}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body %s", rec.Code, rec.Body)
	}
	if q.Len() != 1 {
		t.Fatalf("queue Len() = %d, want 1", q.Len())
	}
	e, err := q.Consume(context.Background())
	if err != nil {
		t.Fatalf("Consume() error: %v", err)
	}
	if e.Type != event.TypePaymentProcessed || e.ID != "p-1" || e.Amount != 10.5 || !e.OccurredAt.Equal(now) {
		t.Errorf("queued event = %+v, want processed p-1 10.5 at %v", e, now)
	}
}

// TC-ING-02 and TC-ING-03, plus the other invalid inputs listed in the spec.
func TestPublishRejectsInvalid(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "malformed JSON", body: `{"type":`},
		{name: "empty body", body: ``},
		{name: "unknown type", body: `{"type":"payment.refunded","id":"p-2"}`},
		{name: "missing id", body: `{"type":"payment.processed"}`},
		{name: "negative amount", body: `{"type":"payment.failed","id":"p-3","amount":-1}`},
		{name: "occurred_at not RFC 3339", body: `{"type":"payment.failed","id":"p-4","occurred_at":"yesterday"}`},
		{name: "occurred_at too far in the future", body: `{"type":"payment.failed","id":"p-5","occurred_at":"2026-09-25T13:00:00Z"}`},
		{name: "trailing data", body: `{"type":"payment.failed","id":"p-6"} {"x":1}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, q, _ := newServer(t, 10, 0)
			rec := do(t, s, http.MethodPost, "/api/events", tc.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body %s", rec.Code, rec.Body)
			}
			errorMessage(t, rec)
			if q.Len() != 0 {
				t.Errorf("queue Len() = %d, want 0", q.Len())
			}
		})
	}
}

func TestPublishIgnoresUnknownFields(t *testing.T) {
	s, q, _ := newServer(t, 10, 0)
	rec := do(t, s, http.MethodPost, "/api/events", `{"type":"payment.failed","id":"p-1","currency":"COP"}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body %s", rec.Code, rec.Body)
	}
	if q.Len() != 1 {
		t.Errorf("queue Len() = %d, want 1", q.Len())
	}
}

// TC-ING-04: Backpressure when queue is full.
func TestPublishQueueFull(t *testing.T) {
	s, q, _ := newServer(t, 1, 0)
	body := `{"type":"payment.processed","id":"p-1"}`
	if rec := do(t, s, http.MethodPost, "/api/events", body); rec.Code != http.StatusAccepted {
		t.Fatalf("first publish status = %d, want 202", rec.Code)
	}
	rec := do(t, s, http.MethodPost, "/api/events", body)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body %s", rec.Code, rec.Body)
	}
	errorMessage(t, rec)
	if q.Len() != 1 {
		t.Errorf("queue Len() = %d, want 1", q.Len())
	}
}

func TestPublishAfterClose(t *testing.T) {
	s, q, _ := newServer(t, 10, 0)
	q.Close()
	rec := do(t, s, http.MethodPost, "/api/events", `{"type":"payment.processed","id":"p-1"}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	errorMessage(t, rec)
}

func TestPublishBodyTooLarge(t *testing.T) {
	s, q, _ := newServer(t, 10, 0)
	body := `{"type":"payment.processed","id":"` + strings.Repeat("x", MaxBodyBytes) + `"}`
	rec := do(t, s, http.MethodPost, "/api/events", body)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", rec.Code)
	}
	errorMessage(t, rec)
	if q.Len() != 0 {
		t.Errorf("queue Len() = %d, want 0", q.Len())
	}
}

func TestMethodNotAllowed(t *testing.T) {
	s, _, _ := newServer(t, 10, 0)
	if rec := do(t, s, http.MethodGet, "/api/events", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET /api/events status = %d, want 405", rec.Code)
	}
	if rec := do(t, s, http.MethodPost, "/api/stats", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /api/stats status = %d, want 405", rec.Code)
	}
}

func TestStatsExactJSON(t *testing.T) {
	s, _, agg := newServer(t, 10, 0)
	agg.Record(event.Event{Type: event.TypePaymentProcessed, ID: "a", OccurredAt: now})
	agg.Record(event.Event{Type: event.TypePaymentFailed, ID: "b", OccurredAt: now.Add(-time.Minute)})

	rec := do(t, s, http.MethodGet, "/api/stats?minutes=2", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %s", rec.Code, rec.Body)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", cc)
	}
	want := `{"generated_at":"2026-09-25T12:10:03Z",` +
		`"minutes":[{"minute":"2026-09-25T12:09:00Z","processed":0,"failed":1},` +
		`{"minute":"2026-09-25T12:10:00Z","processed":1,"failed":0}],` +
		`"totals":{"processed":1,"failed":1}}` + "\n"
	if got := rec.Body.String(); got != want {
		t.Errorf("body =\n%s\nwant\n%s", got, want)
	}
}

func TestStatsMinutesParam(t *testing.T) {
	tests := []struct {
		query string
		want  int
	}{
		{query: "", want: 15},
		{query: "?minutes=1", want: 1},
		{query: "?minutes=60", want: 60},
	}
	for _, tc := range tests {
		s, _, _ := newServer(t, 10, 0)
		rec := do(t, s, http.MethodGet, "/api/stats"+tc.query, "")
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /api/stats%s status = %d, want 200", tc.query, rec.Code)
		}
		var snap aggregator.Snapshot
		if err := json.Unmarshal(rec.Body.Bytes(), &snap); err != nil {
			t.Fatalf("GET /api/stats%s body is not JSON: %v", tc.query, err)
		}
		if len(snap.Minutes) != tc.want {
			t.Errorf("GET /api/stats%s returned %d minutes, want %d", tc.query, len(snap.Minutes), tc.want)
		}
	}
}

// TC-DASH-03: Reject out-of-range minutes.
func TestStatsRejectsInvalidMinutes(t *testing.T) {
	for _, v := range []string{"0", "-1", "61", "abc", "1.5"} {
		s, _, _ := newServer(t, 10, 0)
		rec := do(t, s, http.MethodGet, "/api/stats?minutes="+v, "")
		if rec.Code != http.StatusBadRequest {
			t.Errorf("minutes=%s status = %d, want 400", v, rec.Code)
			continue
		}
		errorMessage(t, rec)
	}
}

func TestStatsMinutesBoundedByRetention(t *testing.T) {
	s, _, _ := newServer(t, 10, 10*time.Minute)
	if rec := do(t, s, http.MethodGet, "/api/stats?minutes=11", ""); rec.Code != http.StatusBadRequest {
		t.Errorf("minutes=11 with 10m retention status = %d, want 400", rec.Code)
	}
	rec := do(t, s, http.MethodGet, "/api/stats", "")
	var snap aggregator.Snapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &snap); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if len(snap.Minutes) != 10 {
		t.Errorf("default minutes with 10m retention = %d, want 10", len(snap.Minutes))
	}
}

// TC-DASH-05: Serve dashboard page.
func TestIndexPage(t *testing.T) {
	s, _, _ := newServer(t, 10, 0)
	rec := do(t, s, http.MethodGet, "/", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET / status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("Content-Type = %q, want text/html", ct)
	}
	body := rec.Body.String()
	for _, want := range []string{`"/api/stats"`, "REFRESH_MS = 2000", "setTimeout(poll, REFRESH_MS)"} {
		if !strings.Contains(body, want) {
			t.Errorf("page does not contain %q", want)
		}
	}
	// The page must be self-contained: no external scripts, styles or images.
	for _, bad := range []string{`src="http`, `href="http`, "<link "} {
		if strings.Contains(body, bad) {
			t.Errorf("page references an external resource (%q)", bad)
		}
	}
}

func TestUnknownPathNotFound(t *testing.T) {
	s, _, _ := newServer(t, 10, 0)
	if rec := do(t, s, http.MethodGet, "/nope", ""); rec.Code != http.StatusNotFound {
		t.Errorf("GET /nope status = %d, want 404", rec.Code)
	}
}
