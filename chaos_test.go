package task_queue

// Verifying correct behavior across producers -> WAL -> Recovery -> Consumers -> Effects

// To simulate the crash we will
// 1) Spawn producers + consumers
// 2) signal every goroutnie to stop and wait for them to exit
// 3) Call reopenQueue() to rebuild the queue from WAL
// 4) Respawn consumers to finish work (counters)

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// consumeLoop is the concurrent consumer: dequeue -> apply -> ack, over and over,
// until the stop channel is closed. Runs as a goroutine, so it reports via
// t.Errorf (goroutine-safe) and never t.Fatalf (which must stay on the test goroutine).
func consumeLoop(t *testing.T, q *Queue, store *effectStore, stop <-chan struct{}) {
	for {
		select {
		case <-stop: // non-blocking check: closed channel fires immediately
			return
		default: // nothing on stop yet, fall through and do work
		}

		task, ok, err := q.Dequeue()
		if err != nil {
			t.Errorf("Dequeue: %v", err)
			return
		}
		if !ok {
			time.Sleep(time.Millisecond) // queue empty right now; yield instead of busy-spinning
			continue
		}

		store.apply(task.IdempotencyKey)
		if err := Ack(q, task.ID); err != nil {
			t.Errorf("Ack: %v", err)
			return
		}
	}
}

func TestEndToEndCrashAtLeastOnce(t *testing.T) {
	q, path := newTestQueue(t)
	store := newEffectStore()

	// Produce a batch of unique tasks, durably, before the crash.
	keys := make([]string, 200)
	for i := range keys {
		keys[i] = fmt.Sprintf("task-%d", i)
	}
	want := countDistinctKeys(keys)
	produce(t, q, keys)

	// Wave 1: concurrent consumers chew on the queue.
	stop := make(chan struct{})
	var wg sync.WaitGroup // waits for all worker goroutines to exit
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			consumeLoop(t, q, store, stop)
		}()
	}

	// The "crash": let them process some, then stop every worker at once.
	time.Sleep(5 * time.Millisecond)
	close(stop) // closing broadcasts to every goroutine selecting on stop
	wg.Wait()

	// Restart: rebuild from the WAL. Tasks that were inflight at the crash were
	// never logged as dequeued, so replay brings them back as ready -> at-least-once.
	q2 := reopenQueue(t, path)

	// Wave 2: finish everything deterministically (single-threaded drain to empty).
	drain(t, q2, store)

	// Guarantee 1: every produced key's effect landed, exactly once (store is idempotent).
	if got := store.count(); got != want {
		t.Fatalf("effects applied = %d, want %d", got, want)
	}
	// Guarantee 2: nothing left stuck in the queue.
	if n := len(q2.index); n != 0 {
		t.Fatalf("queue not drained: %d tasks remain", n)
	}
}
