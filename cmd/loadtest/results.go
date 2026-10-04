package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// resultRecord is one run's persisted result: identity (when + which commit), the
// config that produced it, the outcome counters, and per-op metrics. One record is
// appended per run as a single JSON line, so a results file is a comparable history
// across code versions (e.g. baseline vs after a min-heap or group-commit fsync).
type resultRecord struct {
	Timestamp string    `json:"timestamp"`
	Label     string    `json:"label,omitempty"`
	Git       gitRecord `json:"git"`
	Config    cfgRecord `json:"config"`
	Counters  cntRecord `json:"counters"`
	WindowSec float64   `json:"window_sec"`
	Ops       opsRecord `json:"ops"`
	Pass      bool      `json:"pass"`
	Shortfall int64     `json:"shortfall"`
}

type gitRecord struct {
	SHA    string `json:"sha"`
	Branch string `json:"branch"`
	Dirty  bool   `json:"dirty"` // uncommitted changes present => numbers aren't tied to a clean commit
}

type cfgRecord struct {
	Dur        string  `json:"dur"`
	Producers  int     `json:"producers"`
	Consumers  int     `json:"consumers"`
	FailRate   float64 `json:"failrate"`
	CrashShare float64 `json:"crashshare"`
	MaxRetries int     `json:"maxretries"`
	Visibility string  `json:"visibility"`
	Sweep      string  `json:"sweep"`
}

type cntRecord struct {
	Produced         int64 `json:"produced"`
	Delivered        int64 `json:"delivered"`
	Redeliveries     int64 `json:"redeliveries"`
	Acked            int64 `json:"acked"`
	Retried          int64 `json:"retried"`
	Crashed          int64 `json:"crashed"`
	DistinctEffects  int   `json:"distinct_effects"`
	DedupedReapplies int64 `json:"deduped_reapplies"`
	DLQLen           int   `json:"dlq_len"`
}

type opsRecord struct {
	Enqueue opReportJSON `json:"enqueue"`
	Dequeue opReportJSON `json:"dequeue"`
	Ack     opReportJSON `json:"ack"`
}

// opReportJSON stores latency as integer microseconds so runs are numerically
// comparable (deltas, charts) without parsing duration strings.
type opReportJSON struct {
	Count     int     `json:"count"`
	OpsPerSec float64 `json:"ops_per_sec"`
	P50us     int64   `json:"p50_us"`
	P95us     int64   `json:"p95_us"`
	P99us     int64   `json:"p99_us"`
}

func opReport(m opMetrics) opReportJSON {
	return opReportJSON{
		Count:     m.count,
		OpsPerSec: m.throughput,
		P50us:     m.p50.Microseconds(),
		P95us:     m.p95.Microseconds(),
		P99us:     m.p99.Microseconds(),
	}
}

// buildRecord assembles the record from the config and the run's stats.
func buildRecord(cfg config, stats runStats) resultRecord {
	sha, branch, dirty := gitInfo()
	return resultRecord{
		Timestamp: time.Now().Format(time.RFC3339),
		Label:     cfg.label,
		Git:       gitRecord{SHA: sha, Branch: branch, Dirty: dirty},
		Config: cfgRecord{
			Dur:        cfg.dur.String(),
			Producers:  cfg.producers,
			Consumers:  cfg.consumers,
			FailRate:   cfg.failRate,
			CrashShare: cfg.crashShare,
			MaxRetries: cfg.maxRetries,
			Visibility: cfg.visibility.String(),
			Sweep:      cfg.sweep.String(),
		},
		Counters: cntRecord{
			Produced:         stats.produced,
			Delivered:        stats.delivered,
			Redeliveries:     stats.delivered - stats.produced,
			Acked:            stats.acked,
			Retried:          stats.retried,
			Crashed:          stats.crashed,
			DistinctEffects:  stats.distinctEffects,
			DedupedReapplies: stats.deliveries - int64(stats.distinctEffects),
			DLQLen:           stats.dlqLen,
		},
		WindowSec: stats.metrics.window.Seconds(),
		Ops: opsRecord{
			Enqueue: opReport(stats.metrics.enqueue),
			Dequeue: opReport(stats.metrics.dequeue),
			Ack:     opReport(stats.metrics.ack),
		},
		Pass:      stats.pass,
		Shortfall: stats.shortfall,
	}
}

// appendResult appends rec as one JSON line to path, creating the parent dir and the
// file if needed. json.Encoder.Encode writes a trailing newline, giving valid JSONL.
func appendResult(path string, rec resultRecord) error {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	return json.NewEncoder(f).Encode(rec)
}

// gitInfo returns the short commit SHA, branch, and whether the tree is dirty. Any
// field is empty/false if git isn't available or this isn't a repo; a run still
// records, just without a commit to tie it to.
func gitInfo() (sha, branch string, dirty bool) {
	sha = runGit("rev-parse", "--short", "HEAD")
	branch = runGit("rev-parse", "--abbrev-ref", "HEAD")
	dirty = runGit("status", "--porcelain") != ""
	return sha, branch, dirty
}

func runGit(args ...string) string {
	out, err := exec.Command("git", args...).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}
