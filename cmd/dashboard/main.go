// Command dashboard consumes payment events from an in-memory queue and
// serves a near-real-time dashboard of per-minute success and failure counts.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"leancore/internal/aggregator"
	"leancore/internal/consumer"
	"leancore/internal/httpapi"
	"leancore/internal/queue"
	"leancore/internal/simulator"
)

// shutdownTimeout bounds how long in-flight HTTP requests may take to finish.
const shutdownTimeout = 5 * time.Second

type config struct {
	addr                 string
	queueSize            int
	retention            time.Duration
	simulate             bool
	simulateRate         float64
	simulateFailureRatio float64
}

func parseFlags(args []string, output io.Writer) (config, error) {
	var cfg config
	fs := flag.NewFlagSet("dashboard", flag.ContinueOnError)
	fs.SetOutput(output)
	fs.StringVar(&cfg.addr, "addr", ":8080", "HTTP listen address")
	fs.IntVar(&cfg.queueSize, "queue-size", queue.DefaultCapacity, "maximum number of queued events")
	fs.DurationVar(&cfg.retention, "retention", aggregator.DefaultRetention, "how long per-minute counts are kept (whole minutes)")
	fs.BoolVar(&cfg.simulate, "simulate", false, "publish random payment events for demos")
	fs.Float64Var(&cfg.simulateRate, "simulate-rate", 5, "simulated events per second")
	fs.Float64Var(&cfg.simulateFailureRatio, "simulate-failure-ratio", 0.2, "fraction of simulated events that fail, in [0, 1]")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if fs.NArg() > 0 {
		return config{}, fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	if cfg.queueSize < 1 {
		return config{}, fmt.Errorf("-queue-size must be at least 1, got %d", cfg.queueSize)
	}
	if cfg.retention < time.Minute {
		return config{}, fmt.Errorf("-retention must be at least 1m, got %v", cfg.retention)
	}
	if cfg.simulate {
		if !(cfg.simulateRate > 0) {
			return config{}, fmt.Errorf("-simulate-rate must be positive, got %v", cfg.simulateRate)
		}
		if !(cfg.simulateFailureRatio >= 0 && cfg.simulateFailureRatio <= 1) {
			return config{}, fmt.Errorf("-simulate-failure-ratio must be in [0, 1], got %v", cfg.simulateFailureRatio)
		}
	}
	return cfg, nil
}

// app holds the wired components of the service.
type app struct {
	cfg     config
	queue   *queue.MemoryQueue
	agg     *aggregator.Aggregator
	handler http.Handler
}

func newApp(cfg config) *app {
	q := queue.NewMemoryQueue(cfg.queueSize)
	agg := aggregator.New(cfg.retention, nil)
	return &app{cfg: cfg, queue: q, agg: agg, handler: httpapi.New(q, agg, nil)}
}

// serve runs the service on ln until ctx is done, then shuts down in order:
// stop accepting HTTP requests, stop the simulator, close the queue, and wait
// for the consumer to drain every pending event.
func (a *app) serve(ctx context.Context, ln net.Listener) error {
	// The consumer does not use ctx: it must keep running after ctx is done
	// so that it can drain the queue once it is closed.
	consumerDone := make(chan error, 1)
	go func() { consumerDone <- consumer.Run(context.Background(), a.queue, a.agg) }()

	simCtx, stopSim := context.WithCancel(context.Background())
	defer stopSim()
	var sim sync.WaitGroup
	if a.cfg.simulate {
		sim.Add(1)
		go func() {
			defer sim.Done()
			if err := simulator.Run(simCtx, a.queue, a.cfg.simulateRate, a.cfg.simulateFailureRatio); err != nil {
				slog.Error("simulator stopped", "err", err)
			}
		}()
	}

	srv := &http.Server{Handler: a.handler, ReadHeaderTimeout: 5 * time.Second}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()
	slog.Info("dashboard listening", "addr", ln.Addr().String(), "simulate", a.cfg.simulate)

	var err error
	select {
	case <-ctx.Done():
		slog.Info("shutting down")
	case err = <-serveErr:
		slog.Error("http server failed", "err", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if serr := srv.Shutdown(shutdownCtx); serr != nil {
		slog.Warn("http shutdown incomplete", "err", serr)
	}
	stopSim()
	sim.Wait()
	a.queue.Close()
	if cerr := <-consumerDone; cerr != nil {
		err = errors.Join(err, fmt.Errorf("consumer: %w", cerr))
	}
	totals := a.agg.Snapshot(a.agg.RetentionMinutes()).Totals
	slog.Info("stopped", "processed", totals.Processed, "failed", totals.Failed)
	return err
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	cfg, err := parseFlags(os.Args[1:], os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "dashboard:", err)
		os.Exit(2)
	}

	ln, err := net.Listen("tcp", cfg.addr)
	if err != nil {
		slog.Error("listen failed", "addr", cfg.addr, "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := newApp(cfg).serve(ctx, ln); err != nil {
		slog.Error("dashboard failed", "err", err)
		os.Exit(1)
	}
}
