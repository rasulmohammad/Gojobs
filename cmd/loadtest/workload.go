package main

import (
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"task_queue/internal/queue"
)

// runStats collects the counters a run produces. It grows across milestones:
// M2 fills produced; M3 adds acked/retried and the effect-store summary; later
// milestones add latency metrics and the conservation verdict.
type runStats struct {
	produced  int64 // tasks enqueued
	delivered int64 // successful Dequeues (deliveries); exceeds produced by the redelivery count
	acked     int64 // deliveries that applied the effect and acked (window + drain)
	retried   int64 // pre-effect handler failures: Retry, no effect applied
	crashed   int64 // post-effect crashes: effect applied, then no ack (lease will expire)

	distinctEffects int   // unique idemKeys applied == exactly-once effects (must equal produced)
	deliveries      int64 // total effect applications incl. deduped re-applies
	dlqLen          int   // tasks left in the DLQ after the drain

	pass      bool  // conservation verdict: every produced task's effect landed exactly once, DLQ empty
	shortfall int64 // produced - distinctEffects when the verdict fails (0 on pass)

	metrics runMetrics // throughput + p50/p95/p99 over the load window
}

// run drives one load window: it starts the producer (and, from M3, consumer)
// goroutines, lets them hammer the queue for cfg.dur, then signals stop and waits
// for every goroutine to exit before returning the collected counters.
func run(q *queue.Queue, cfg config) runStats {
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var stats runStats
	store := newEffectStore()

	// Per-goroutine latency samples: each goroutine owns one slice (indexed by its
	// id), so there are no shared writes to lock. They're merged after wg.Wait.
	encLat := make([][]time.Duration, cfg.producers)
	deqLat := make([][]time.Duration, cfg.consumers)
	ackLat := make([][]time.Duration, cfg.consumers)

	start := time.Now()
	startProducers(q, cfg, stop, &wg, &stats, encLat)
	startConsumers(q, cfg, stop, &wg, &stats, store, deqLat, ackLat)

	// The load window. Background goroutines do the work; this goroutine just times
	// the window, then broadcasts stop by closing the channel (every goroutine
	// selecting on stop sees it at once).
	time.Sleep(cfg.dur)
	close(stop)
	wg.Wait()
	window := time.Since(start)

	// Summarize window latency/throughput before draining (drain is single-threaded
	// and post-window, so its ops aren't representative and aren't sampled).
	stats.metrics = runMetrics{
		window:  window,
		enqueue: summarize(encLat, window),
		dequeue: summarize(deqLat, window),
		ack:     summarize(ackLat, window),
	}

	// Drain + conservation: single-threaded, no failure injection. Pull everything
	// still ready, replay the whole DLQ, and wait out backoffs/leases until every
	// produced task's effect has landed (or we give up at the drain timeout).
	drain(q, cfg, store, &stats)

	stats.distinctEffects = store.distinct()
	stats.deliveries = store.deliveries()
	stats.dlqLen = q.DLQLen()

	// Full conservation: every task's effect landed exactly once (distinct==produced),
	// every task reached terminal completion (acked==produced, so none left stuck
	// in-flight), and nothing is stranded in the DLQ. acked==produced is the strongest
	// of these and is what the drain loops toward.
	stats.pass = int64(stats.distinctEffects) == stats.produced &&
		stats.acked == stats.produced &&
		stats.dlqLen == 0
	if !stats.pass {
		stats.shortfall = stats.produced - int64(stats.distinctEffects)
	}
	return stats
}

// startProducers launches cfg.producers goroutines. Each enqueues unique-keyed tasks
// as fast as the queue allows until stop closes. Keys are unique per (producer, seq)
// so nothing collapses under enqueue idempotency and produced == distinct tasks.
func startProducers(q *queue.Queue, cfg config, stop <-chan struct{}, wg *sync.WaitGroup, stats *runStats, encLat [][]time.Duration) {
	for p := 0; p < cfg.producers; p++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for seq := 0; ; seq++ {
				select {
				case <-stop:
					return
				default: // stop not closed yet: do one more enqueue
				}

				key := fmt.Sprintf("p%d-%d", id, seq)
				t0 := time.Now()
				if _, err := q.Enqueue([]byte("x"), key); err != nil {
					log.Printf("producer %d: enqueue %s: %v", id, key, err)
					return
				}
				encLat[id] = append(encLat[id], time.Since(t0)) // only this goroutine writes encLat[id]
				atomic.AddInt64(&stats.produced, 1)
			}
		}(p)
	}
}

// startConsumers launches cfg.consumers goroutines. Each pulls a task and, with
// probability cfg.failRate, calls Retry to simulate a failed handler (driving
// backoff and, past maxRetries, the DLQ); otherwise it applies the effect and Acks.
// An empty queue is not an error: Dequeue returns ok=false for both "nothing left"
// and "everything is in backoff", so the consumer briefly yields and tries again.
func startConsumers(q *queue.Queue, cfg config, stop <-chan struct{}, wg *sync.WaitGroup, stats *runStats, store *effectStore, deqLat, ackLat [][]time.Duration) {
	for c := 0; c < cfg.consumers; c++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}

				t0 := time.Now()
				task, ok, err := q.Dequeue()
				if err != nil {
					log.Printf("consumer: dequeue: %v", err)
					return
				}
				if !ok {
					time.Sleep(time.Millisecond) // nothing ready right now; yield, don't busy-spin
					continue
				}
				deqLat[id] = append(deqLat[id], time.Since(t0)) // only successful pops; only this goroutine writes deqLat[id]
				atomic.AddInt64(&stats.delivered, 1)

				// rand/v2 top-level is concurrency-safe and auto-seeded, so each
				// consumer can roll independently without sharing a generator.
				if rand.Float64() < cfg.failRate {
					// A failure. crashShare splits it into the two real-world modes.
					if rand.Float64() < cfg.crashShare {
						// Crash AFTER the side effect: the work happened but the ack
						// never did. The task stays in-flight; its lease expires and
						// the sweeper redelivers it, where apply() dedupes the repeat.
						// This is the case the idempotency key exists for.
						store.apply(task.IdempotencyKey)
						atomic.AddInt64(&stats.crashed, 1)
						continue
					}
					// Handler failed BEFORE the side effect: no effect, explicit Retry
					// (backoff, and the DLQ once retries are exhausted).
					if err := queue.Retry(q, task.ID); err != nil {
						log.Printf("consumer: retry %s: %v", task.ID, err)
						return
					}
					atomic.AddInt64(&stats.retried, 1)
					continue
				}

				// Success: effect applied, then acked.
				store.apply(task.IdempotencyKey)
				t1 := time.Now()
				if err := queue.Ack(q, task.ID); err != nil {
					log.Printf("consumer: ack %s: %v", task.ID, err)
					return
				}
				ackLat[id] = append(ackLat[id], time.Since(t1)) // only this goroutine writes ackLat[id]
				atomic.AddInt64(&stats.acked, 1)
			}
		}(c)
	}
}

// drain finishes the run deterministically: with producers and failure injection
// stopped, it loops until every produced task has reached a terminal ack (acked ==
// produced) or the drain timeout elapses. Each pass (1) acks everything currently
// ready, (2) replays the whole DLQ so tasks that died with no effect get another
// chance, and (3) when nothing was ready, sleeps a sweep interval so backed-off
// tasks and expired crash leases can mature and reappear.
//
// acked == produced (not distinct == produced) is the terminal condition on purpose:
// a crash applies its effect but never acks, so distinct can reach produced while
// those tasks are still stuck in-flight. Waiting for the ack forces the lease to
// expire, the sweeper to redeliver, and the re-delivery to be deduped and acked --
// so nothing is left in limbo. Replaying the DLQ is load-bearing: a task that only
// ever hit pre-effect Retry failures reaches the DLQ with no effect at all.
func drain(q *queue.Queue, cfg config, store *effectStore, stats *runStats) {
	deadline := time.Now().Add(cfg.drainTO)
	for atomic.LoadInt64(&stats.acked) < stats.produced {
		if !time.Now().Before(deadline) {
			return // timed out; the verdict flags the shortfall
		}

		drainedAny := false
		for {
			task, ok, err := q.Dequeue()
			if err != nil {
				log.Printf("drain: dequeue: %v", err)
				return
			}
			if !ok {
				break
			}
			atomic.AddInt64(&stats.delivered, 1)
			store.apply(task.IdempotencyKey)
			if err := queue.Ack(q, task.ID); err != nil {
				log.Printf("drain: ack %s: %v", task.ID, err)
				return
			}
			atomic.AddInt64(&stats.acked, 1)
			drainedAny = true
		}

		for _, dead := range q.ListDeadTasks() {
			if err := q.RequeueDead(dead.ID); err != nil {
				log.Printf("drain: requeue dead %s: %v", dead.ID, err)
				continue
			}
		}

		// Nothing was ready this pass: the remaining tasks are in backoff or in-flight
		// awaiting lease expiry. Wait a sweep interval before looking again.
		if !drainedAny {
			time.Sleep(cfg.sweep)
		}
	}
}
