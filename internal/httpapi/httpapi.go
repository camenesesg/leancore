package httpapi

import (
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"leancore/internal/aggregator"
	"leancore/internal/event"
	"leancore/internal/queue"
)

const (
	// MaxBodyBytes is the largest request body accepted by POST /api/events.
	MaxBodyBytes = 1 << 20

	defaultMinutes = 15
	maxMinutes     = 60
)

//go:embed web/index.html
var indexHTML []byte

// StatsSource provides the per-minute counts served by GET /api/stats.
type StatsSource interface {
	Snapshot(n int) aggregator.Snapshot
	RetentionMinutes() int
}

// Server serves the HTTP API. It implements http.Handler.
type Server struct {
	q     queue.Queue
	stats StatsSource
	now   func() time.Time
	mux   *http.ServeMux
}

// New returns a Server that publishes events to q and reads counts from
// stats. now is the clock used to validate events; nil means time.Now.
func New(q queue.Queue, stats StatsSource, now func() time.Time) *Server {
	if now == nil {
		now = time.Now
	}
	s := &Server{q: q, stats: stats, now: now, mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /api/events", s.handlePublish)
	s.mux.HandleFunc("GET /api/stats", s.handleStats)
	s.mux.HandleFunc("GET /{$}", s.handleIndex)
	return s
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	e, err := decodeEvent(http.MaxBytesReader(w, r.Body, MaxBodyBytes))
	if err != nil {
		status := http.StatusBadRequest
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			status = http.StatusRequestEntityTooLarge
		}
		slog.Warn("event rejected", "reason", err)
		writeError(w, status, err.Error())
		return
	}
	if err := e.Validate(s.now()); err != nil {
		slog.Warn("event rejected", "id", e.ID, "reason", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	switch err := s.q.Publish(r.Context(), e); {
	case err == nil:
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
	case errors.Is(err, queue.ErrQueueFull):
		slog.Warn("event rejected", "id", e.ID, "reason", err)
		writeError(w, http.StatusServiceUnavailable, "queue is full, retry later")
	default:
		slog.Warn("event not published", "id", e.ID, "reason", err)
		writeError(w, http.StatusServiceUnavailable, "service is not accepting events")
	}
}

// decodeEvent reads exactly one JSON event from r.
func decodeEvent(r io.Reader) (event.Event, error) {
	var e event.Event
	dec := json.NewDecoder(r)
	if err := dec.Decode(&e); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return event.Event{}, err
		}
		return event.Event{}, fmt.Errorf("invalid JSON body: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return event.Event{}, errors.New("invalid JSON body: unexpected data after event")
	}
	return e, nil
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	limit := min(maxMinutes, s.stats.RetentionMinutes())
	n := defaultMinutes
	if v := r.URL.Query().Get("minutes"); v != "" {
		var err error
		n, err = strconv.Atoi(v)
		if err != nil || n < 1 || n > limit {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("minutes must be an integer between 1 and %d", limit))
			return
		}
	}
	n = min(n, limit)
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, s.stats.Snapshot(n))
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Write(indexHTML)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("writing response", "err", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
