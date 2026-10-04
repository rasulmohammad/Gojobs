package main

import (
	"math"
	"sort"
	"time"
)

// opMetrics summarizes one operation type over the load window: how many happened,
// the sustained rate, and the latency distribution.
type opMetrics struct {
	count      int
	throughput float64 // ops per second over the window
	p50        time.Duration
	p95        time.Duration
	p99        time.Duration
}

// runMetrics holds the per-operation metrics for one run. Enqueue and ack are the
// durable (fsync) ops; dequeue is in-memory only, so comparing them shows where the
// cost lives.
type runMetrics struct {
	window  time.Duration
	enqueue opMetrics
	dequeue opMetrics
	ack     opMetrics
}

// summarize flattens the per-goroutine latency samples, sorts once, and computes
// throughput + percentiles over window. Each inner slice was written by exactly one
// goroutine, so there is nothing to lock here; the caller merges after wg.Wait.
func summarize(perGoroutine [][]time.Duration, window time.Duration) opMetrics {
	var all []time.Duration
	for _, s := range perGoroutine {
		all = append(all, s...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })

	m := opMetrics{count: len(all)}
	if window > 0 {
		m.throughput = float64(len(all)) / window.Seconds()
	}
	m.p50 = percentile(all, 0.50)
	m.p95 = percentile(all, 0.95)
	m.p99 = percentile(all, 0.99)
	return m
}

// percentile returns the p-quantile of an already-sorted slice using the
// nearest-rank method. Returns 0 for an empty slice.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p * float64(len(sorted)))) // 1-indexed nearest rank
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}
