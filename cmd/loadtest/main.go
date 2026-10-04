// Command loadtest drives the durable task queue under concurrent, failure-injecting
// load, then verifies the effectively-once invariant and reports throughput and
// p50/p95/p99 latency. Each run appends a git-tagged record to a results file so
// before/after numbers (min-heap rather than fifo queue or group-commit fsync) stay comparable.
//
// Run it from the repo root, e.g.:
//
//	go run ./cmd/loadtest -dur 10s -producers 8 -consumers 8 -failrate 0.2 -label baseline
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"task_queue/internal/queue"
)

// config is the fully-resolved set of knobs for one load-test run. Parsed once in
// main and passed by value to the phases so nothing reaches back into flag state.
type config struct {
	dur        time.Duration // how long producers+consumers run before the drain phase
	producers  int           // concurrent Enqueue goroutines
	consumers  int           // concurrent Dequeue/Ack/Retry goroutines
	failRate   float64       // probability [0,1] a delivery does not cleanly ack
	crashShare float64       // of failures, fraction [0,1] that crash after applying the effect (rest Retry pre-effect)
	maxRetries int           // queue: attempts before a task is sent to the DLQ
	visibility time.Duration // queue: lease duration for an in-flight task
	sweep      time.Duration // queue: how often the background sweeper reclaims leases
	drainTO    time.Duration // max time the post-window drain waits to reach full conservation
	walPath    string        // WAL file; empty => a throwaway temp file we delete on exit
	label      string        // free-form tag recorded with results ("baseline", ...)
	out        string        // results file to append one JSONL record to
}

// main delegates to realMain so deferred cleanup (temp WAL removal, q.Close) runs
// before we translate the conservation verdict into a process exit code.
func main() {
	os.Exit(realMain())
}

func realMain() int {
	cfg, cleanup := parseFlags()
	defer cleanup()

	// No flags on an interactive terminal: walk the user through the knobs.
	if shouldRunWizard() {
		runWizard(&cfg)
	}

	// visibilityTimeout, sweepInterval, maxRetries come straight from the flags so a
	// run can tune the queue's own timing without touching code.
	q, err := queue.NewQueue(cfg.walPath, cfg.visibility, cfg.sweep, cfg.maxRetries)
	if err != nil {
		log.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()

	renderConfig(cfg)

	stats := run(q, cfg)
	renderResults(stats)

	// Persist one JSONL record regardless of verdict: a FAIL is a data point too.
	if err := appendResult(cfg.out, buildRecord(cfg, stats)); err != nil {
		log.Printf("warning: could not write results to %s: %v", cfg.out, err)
	} else {
		fmt.Printf("\nresults appended to %s\n", cfg.out)
	}

	if !stats.pass {
		return 1
	}
	return 0
}

// parseFlags reads the command line into a config. It returns a cleanup func that
// removes the throwaway WAL when the user did not pin one with -wal (so repeated
// runs don't accumulate temp files, and each run starts from an empty queue).
func parseFlags() (config, func()) {
	var cfg config
	flag.DurationVar(&cfg.dur, "dur", 10*time.Second, "how long to run producers+consumers before draining")
	flag.IntVar(&cfg.producers, "producers", 4, "number of concurrent producer goroutines")
	flag.IntVar(&cfg.consumers, "consumers", 4, "number of concurrent consumer goroutines")
	flag.Float64Var(&cfg.failRate, "failrate", 0.2, "probability [0,1] a delivery does not cleanly ack")
	flag.Float64Var(&cfg.crashShare, "crashshare", 0.5, "of failures, fraction [0,1] that crash after applying the effect (rest Retry before it)")
	flag.IntVar(&cfg.maxRetries, "maxretries", 3, "queue: retries before a task goes to the DLQ")
	flag.DurationVar(&cfg.visibility, "visibility", 5*time.Second, "queue: in-flight lease (visibility timeout)")
	flag.DurationVar(&cfg.sweep, "sweep", 500*time.Millisecond, "queue: sweeper interval for reclaiming expired leases")
	flag.DurationVar(&cfg.drainTO, "draintimeout", 60*time.Second, "max time the post-window drain waits to reach full conservation")
	flag.StringVar(&cfg.walPath, "wal", "", "WAL path; empty uses a throwaway temp file deleted on exit")
	flag.StringVar(&cfg.label, "label", "", "label recorded with the results (e.g. baseline, after-minheap)")
	flag.StringVar(&cfg.out, "out", filepath.Join("bench", "results.jsonl"), "results file to append a JSONL record to")
	flag.Parse()

	// Validate the knobs that would otherwise fail deep inside a goroutine.
	if cfg.producers < 1 || cfg.consumers < 1 {
		log.Fatalf("producers and consumers must be >= 1 (got %d, %d)", cfg.producers, cfg.consumers)
	}
	if cfg.failRate < 0 || cfg.failRate > 1 {
		log.Fatalf("failrate must be in [0,1] (got %v)", cfg.failRate)
	}
	if cfg.crashShare < 0 || cfg.crashShare > 1 {
		log.Fatalf("crashshare must be in [0,1] (got %v)", cfg.crashShare)
	}

	cleanup := func() {}
	if cfg.walPath == "" {
		// Unique per run so concurrent invocations don't share a WAL, and so each run
		// replays nothing on open (an empty queue is the honest starting point).
		cfg.walPath = filepath.Join(os.TempDir(), fmt.Sprintf("loadtest-%d.wal", time.Now().UnixNano()))
		cleanup = func() {
			if err := os.Remove(cfg.walPath); err != nil && !os.IsNotExist(err) {
				log.Printf("warning: could not remove temp WAL %s: %v", cfg.walPath, err)
			}
		}
	}
	return cfg, cleanup
}
