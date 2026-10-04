package main

import (
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"
	"unicode/utf8"
)

// sectionWidth is the total rune width of a "══ title ═══..." header line. Chosen to
// span the widest content block (the metrics table).
const sectionWidth = 52

// section prints a titled header rule, e.g. "══ config ═══════════...".
func section(title string) {
	prefix := "══ " + title + " "
	pad := sectionWidth - utf8.RuneCountInString(prefix)
	if pad < 0 {
		pad = 0
	}
	fmt.Printf("\n%s%s\n", prefix, strings.Repeat("═", pad))
}

// kv prints indented, tab-aligned "label  value" rows. tabwriter sizes the label
// column to the widest label so values line up regardless of label length.
func kv(rows [][2]string) {
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	for _, r := range rows {
		fmt.Fprintf(tw, "  %s\t%s\n", r[0], r[1])
	}
	tw.Flush()
}

// renderConfig echoes the run's knobs before the load starts.
func renderConfig(cfg config) {
	section("config")
	kv([][2]string{
		{"duration", cfg.dur.String()},
		{"producers", fmt.Sprint(cfg.producers)},
		{"consumers", fmt.Sprint(cfg.consumers)},
		{"failrate", fmt.Sprintf("%.2f", cfg.failRate)},
		{"crashshare", fmt.Sprintf("%.2f", cfg.crashShare)},
		{"maxretries", fmt.Sprint(cfg.maxRetries)},
		{"visibility", cfg.visibility.String()},
		{"sweep", cfg.sweep.String()},
		{"draintimeout", cfg.drainTO.String()},
		{"wal", cfg.walPath},
		{"label", cfg.label},
		{"out", cfg.out},
	})
}

// renderResults prints the counters, the throughput/latency table, and the
// conservation verdict after a run.
func renderResults(stats runStats) {
	section("results")
	kv([][2]string{
		{"produced", fmt.Sprint(stats.produced)},
		{"delivered", fmt.Sprintf("%d  (%d redeliveries)", stats.delivered, stats.delivered-stats.produced)},
		{"acked", fmt.Sprint(stats.acked)},
		{"retried (pre-effect)", fmt.Sprint(stats.retried)},
		{"crashed (post-effect)", fmt.Sprint(stats.crashed)},
		{"distinct effects", fmt.Sprint(stats.distinctEffects)},
		{"effect applies", fmt.Sprintf("%d  (%d deduped re-applies)", stats.deliveries, stats.deliveries-int64(stats.distinctEffects))},
		{"dlq length", fmt.Sprint(stats.dlqLen)},
	})

	section("throughput & latency")
	fmt.Printf("  window %s\n", stats.metrics.window.Round(time.Millisecond))
	fmt.Printf("  %-8s %7s %9s %9s %9s %9s\n", "op", "count", "ops/sec", "p50", "p95", "p99")
	printOp("enqueue", stats.metrics.enqueue)
	printOp("dequeue", stats.metrics.dequeue)
	printOp("ack", stats.metrics.ack)

	section("conservation")
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintf(tw, "  effects applied == produced\t%d == %d\t%s\n", stats.distinctEffects, stats.produced, ok(int64(stats.distinctEffects) == stats.produced))
	fmt.Fprintf(tw, "  tasks completed == produced\t%d == %d\t%s\n", stats.acked, stats.produced, ok(stats.acked == stats.produced))
	fmt.Fprintf(tw, "  dlq drained\t%d\t%s\n", stats.dlqLen, ok(stats.dlqLen == 0))
	tw.Flush()
	if stats.pass {
		fmt.Println("  => PASS")
	} else {
		fmt.Printf("  => FAIL (effect shortfall %d)\n", stats.shortfall)
	}
}

// printOp renders one operation's metrics row, right-aligning the numeric columns and
// rounding latencies to microseconds so the table stays legible.
func printOp(name string, m opMetrics) {
	fmt.Printf("  %-8s %7d %9.1f %9s %9s %9s\n",
		name, m.count, m.throughput,
		m.p50.Round(time.Microsecond),
		m.p95.Round(time.Microsecond),
		m.p99.Round(time.Microsecond),
	)
}

// ok renders a boolean check as a compact marker.
func ok(b bool) string {
	if b {
		return "ok"
	}
	return "BAD"
}
