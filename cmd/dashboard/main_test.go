package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"leancore/internal/aggregator"
	"leancore/internal/consumer"
	"leancore/internal/event"
)

func defaultConfig(t *testing.T) config {
	t.Helper()
	cfg, err := parseFlags(nil, io.Discard)
	if err != nil {
		t.Fatalf("parseFlags(nil) error: %v", err)
	}
	return cfg
}

func postEvent(t *testing.T, baseURL string, typ event.Type, id string) {
	t.Helper()
	body := fmt.Sprintf(`{"type":%q,"id":%q}`, typ, id)
	resp, err := http.Post(baseURL+"/api/events", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST /api/events error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /api/events status = %d, want 202", resp.StatusCode)
	}
}

func getTotals(t *testing.T, baseURL string) aggregator.Counts {
	t.Helper()
	resp, err := http.Get(baseURL + "/api/stats?minutes=1")
	if err != nil {
		t.Fatalf("GET /api/stats error: %v", err)
	}
	defer resp.Body.Close()
	var snap aggregator.Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatalf("decoding /api/stats: %v", err)
	}
	return snap.Totals
}

func TestParseFlags(t *testing.T) {
	cfg := defaultConfig(t)
	want := config{addr: ":8080", queueSize: 1000, retention: 60 * time.Minute, simulateRate: 5, simulateFailureRatio: 0.2}
	if cfg != want {
		t.Errorf("defaults = %+v, want %+v", cfg, want)
	}

	cfg, err := parseFlags([]string{"-addr", ":9090", "-queue-size", "10", "-retention", "15m", "-simulate", "-simulate-rate", "50", "-simulate-failure-ratio", "0.5"}, io.Discard)
	if err != nil {
		t.Fatalf("parseFlags() error: %v", err)
	}
	want = config{addr: ":9090", queueSize: 10, retention: 15 * time.Minute, simulate: true, simulateRate: 50, simulateFailureRatio: 0.5}
	if cfg != want {
		t.Errorf("parsed = %+v, want %+v", cfg, want)
	}
}

func TestParseFlagsRejectsInvalid(t *testing.T) {
	tests := [][]string{
		{"-queue-size", "0"},
		{"-retention", "30s"},
		{"-simulate", "-simulate-rate", "0"},
		{"-simulate", "-simulate-failure-ratio", "1.5"},
		{"-unknown"},
		{"extra-arg"},
	}
	for _, args := range tests {
		if _, err := parseFlags(args, io.Discard); err == nil {
			t.Errorf("parseFlags(%q) succeeded, want error", args)
		}
	}
}

// TestEndToEnd publishes events over HTTP through the real queue, consumer
// and aggregator, and checks /api/stats reflects them within one second.
func TestEndToEnd(t *testing.T) {
	a := newApp(defaultConfig(t))
	srv := httptest.NewServer(a.handler)
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go consumer.Run(ctx, a.queue, a.agg)

	const processed, failed = 7, 3
	for i := 0; i < processed; i++ {
		postEvent(t, srv.URL, event.TypePaymentProcessed, fmt.Sprintf("p-%d", i))
	}
	for i := 0; i < failed; i++ {
		postEvent(t, srv.URL, event.TypePaymentFailed, fmt.Sprintf("f-%d", i))
	}

	want := aggregator.Counts{Processed: processed, Failed: failed}
	deadline := time.Now().Add(time.Second)
	for {
		got := getTotals(t, srv.URL)
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("/api/stats totals = %+v after 1s, want %+v", got, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestServeDrainsOnShutdown covers TC-ING-05 end to end: events still queued
// when shutdown starts are counted before serve returns.
func TestServeDrainsOnShutdown(t *testing.T) {
	cfg := defaultConfig(t)
	a := newApp(cfg)
	for i := 0; i < 3; i++ {
		e := event.Event{Type: event.TypePaymentFailed, ID: fmt.Sprintf("f-%d", i), OccurredAt: time.Now()}
		if err := a.queue.Publish(context.Background(), e); err != nil {
			t.Fatalf("Publish() error: %v", err)
		}
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Shut down right away; the queued events must still be drained.
	if err := a.serve(ctx, ln); err != nil {
		t.Fatalf("serve() error: %v", err)
	}
	if got := a.agg.Snapshot(1).Totals; got.Failed != 3 {
		t.Errorf("Totals after shutdown = %+v, want 3 failed", got)
	}
	if err := a.queue.Publish(context.Background(), event.Event{Type: event.TypePaymentFailed, ID: "late"}); err == nil {
		t.Error("Publish() after shutdown succeeded, want error")
	}
}

// TestServeWithSimulator runs the full service over a real listener with the
// simulator on, then shuts it down.
func TestServeWithSimulator(t *testing.T) {
	cfg := defaultConfig(t)
	cfg.simulate = true
	cfg.simulateRate = 500
	a := newApp(cfg)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error: %v", err)
	}
	baseURL := "http://" + ln.Addr().String()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.serve(ctx, ln) }()

	deadline := time.Now().Add(2 * time.Second)
	for {
		got := getTotals(t, baseURL)
		if got.Processed > 0 && got.Failed > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("simulated totals = %+v after 2s, want both > 0", got)
		}
		time.Sleep(20 * time.Millisecond)
	}

	resp, err := http.Get(baseURL + "/")
	if err != nil {
		t.Fatalf("GET / error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET / status = %d, want 200", resp.StatusCode)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("serve() error: %v", err)
		}
	case <-time.After(shutdownTimeout + time.Second):
		t.Fatal("serve() did not return after ctx was canceled")
	}
}
