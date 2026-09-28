package task_queue

/*
Tests to cover:
1. Enqueue
2. Dequeue
3. Ack
4. Retry()
5. Recovery (reopen the queue from the same WAL and check rebuilt state)
*/

import (
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

// newTestQueue creates a queue backed by a throwaway WAL and returns it plus the
// path, so recovery tests can reopen the same log. Both durations are huge so the
// background sweeper never ticks and never reclaims during fast tests, leaving the
// single-threaded assertions (and -race) untouched. The sweeper goroutine is
// stopped at test end via t.Cleanup.
func newTestQueue(t *testing.T) (*Queue, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "q.wal")
	q, err := NewQueue(path, time.Hour, time.Hour)
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	t.Cleanup(func() { q.Close() })
	return q, path
}

// reopenQueue reopens an existing WAL (simulated restart) and returns the rebuilt
// queue. Like newTestQueue, it uses huge durations so the sweeper stays inert and
// registers Close for cleanup.
func reopenQueue(t *testing.T, path string) *Queue {
	t.Helper()
	q, err := NewQueue(path, time.Hour, time.Hour)
	if err != nil {
		t.Fatalf("reopen NewQueue: %v", err)
	}
	t.Cleanup(func() { q.Close() })
	return q
}

// Enqueue: three tasks should land in both the ready slice and the index,
// each with a real (non-nil) unique ID and Status == ready.
func TestEnqueue(t *testing.T) {
	q, _ := newTestQueue(t)

	id1, err := q.Enqueue([]byte("a"), "key-a")
	if err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}
	q.Enqueue([]byte("b"), "key-b")
	q.Enqueue([]byte("c"), "key-c")

	if got := len(q.ready); got != 3 {
		t.Errorf("ready length = %d, want 3", got)
	}
	if got := len(q.index); got != 3 {
		t.Errorf("index length = %d, want 3", got)
	}

	if id1 == uuid.Nil {
		t.Errorf("Enqueue returned the nil UUID, want a real ID")
	}
	// The returned ID must be findable in the index and be ready.
	task, ok := q.index[id1]
	if !ok {
		t.Fatalf("task %s not found in index", id1)
	}
	if task.Status != StateReady {
		t.Errorf("new task Status = %s, want ready", task.Status)
	}
}

// Dequeue: FIFO order, dequeued task becomes inflight, and an empty queue
// reports ok == false with a nil task.
func TestDequeueFIFO(t *testing.T) {
	q, _ := newTestQueue(t)
	first, _ := q.Enqueue([]byte("a"), "key-a")
	second, _ := q.Enqueue([]byte("b"), "key-b")

	t1, ok, err := q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue returned error: %v", err)
	}
	if !ok {
		t.Fatal("Dequeue on non-empty queue returned ok = false")
	}
	if t1.ID != first {
		t.Errorf("first Dequeue returned %s, want %s (FIFO)", t1.ID, first)
	}
	if t1.Status != StateInflight {
		t.Errorf("dequeued task Status = %s, want inflight", t1.Status)
	}

	t2, _, _ := q.Dequeue()
	if t2.ID != second {
		t.Errorf("second Dequeue returned %s, want %s (FIFO)", t2.ID, second)
	}

	// Queue is now empty.
	task, ok, _ := q.Dequeue()
	if ok {
		t.Error("Dequeue on empty queue returned ok = true, want false")
	}
	if task != nil {
		t.Errorf("Dequeue on empty queue returned %v, want nil", task)
	}
}

// Ack: enqueue -> dequeue -> ack removes the task from the index (done tasks
// are dropped from the lookup).
func TestAck(t *testing.T) {
	q, _ := newTestQueue(t)
	id, _ := q.Enqueue([]byte("a"), "key-a")

	q.Dequeue()

	if err := Ack(q, id); err != nil {
		t.Fatalf("Ack returned error: %v", err)
	}
	if _, ok := q.index[id]; ok {
		t.Errorf("after Ack, task still in index, want it removed")
	}
}

// Retry: enqueue -> dequeue -> retry requeues the task: back to ready,
// retry count incremented, and it is handed out again by the next Dequeue.
func TestRetry(t *testing.T) {
	q, _ := newTestQueue(t)
	id, _ := q.Enqueue([]byte("a"), "key-a")

	q.Dequeue() // task is now inflight, ready slice is empty

	if err := Retry(q, id); err != nil {
		t.Fatalf("Retry returned error: %v", err)
	}

	task := q.index[id]
	if task.Status != StateReady {
		t.Errorf("after Retry, Status = %s, want ready", task.Status)
	}
	if task.Retries != 1 {
		t.Errorf("after Retry, Retries = %d, want 1", task.Retries)
	}

	// The retried task should be available to dequeue again.
	again, ok, _ := q.Dequeue()
	if !ok {
		t.Fatal("expected the retried task to be dequeueable again")
	}
	if again.ID != id {
		t.Errorf("re-Dequeue returned %s, want the retried task %s", again.ID, id)
	}
}

// Ack on an unknown ID must return an error (not panic) and must not mutate state.
func TestAckMissingKey(t *testing.T) {
	q, _ := newTestQueue(t)

	err := Ack(q, uuid.New())
	if err == nil {
		t.Fatal("Ack on unknown ID returned nil error, want an error")
	}
	if len(q.index) != 0 {
		t.Errorf("Ack on unknown ID changed index size to %d, want 0", len(q.index))
	}
}

// Retry on an unknown ID must return an error (not panic) and must not requeue anything.
func TestRetryMissingKey(t *testing.T) {
	q, _ := newTestQueue(t)

	err := Retry(q, uuid.New())
	if err == nil {
		t.Fatal("Retry on unknown ID returned nil error, want an error")
	}
	if len(q.ready) != 0 {
		t.Errorf("Retry on unknown ID appended to ready (len = %d), want 0", len(q.ready))
	}
}

// --- Recovery tests: reopen the queue from the same WAL path (simulated restart)
// and assert the state replay rebuilt. ---

// Recovering from an empty log yields an empty queue (no spurious tasks).
func TestRecoverEmpty(t *testing.T) {
	_, path := newTestQueue(t)

	q2 := reopenQueue(t, path)
	if len(q2.ready) != 0 {
		t.Errorf("recovered ready length = %d, want 0", len(q2.ready))
	}
	if len(q2.index) != 0 {
		t.Errorf("recovered index length = %d, want 0", len(q2.index))
	}
}

// Enqueued tasks survive a restart and come back in FIFO order.
func TestRecoverEnqueued(t *testing.T) {
	q1, path := newTestQueue(t)
	a, _ := q1.Enqueue([]byte("a"), "key-a")
	b, _ := q1.Enqueue([]byte("b"), "key-b")
	c, _ := q1.Enqueue([]byte("c"), "key-c")

	q2 := reopenQueue(t, path)

	if len(q2.index) != 3 {
		t.Fatalf("recovered index length = %d, want 3", len(q2.index))
	}

	want := []uuid.UUID{a, b, c}
	for i, wantID := range want {
		got, ok, _ := q2.Dequeue()
		if !ok {
			t.Fatalf("Dequeue #%d after recovery returned ok = false", i)
		}
		if got.ID != wantID {
			t.Errorf("recovered Dequeue #%d = %s, want %s (FIFO)", i, got.ID, wantID)
		}
	}
}

// An acked task stays gone after a restart: it is not in the index and is not
// redelivered, while an un-acked sibling survives.
func TestRecoverAckedTaskGone(t *testing.T) {
	q1, path := newTestQueue(t)
	a, _ := q1.Enqueue([]byte("a"), "key-a")
	b, _ := q1.Enqueue([]byte("b"), "key-b")
	q1.Dequeue() // pop a
	if err := Ack(q1, a); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	q2 := reopenQueue(t, path)

	if _, ok := q2.index[a]; ok {
		t.Errorf("acked task %s present in recovered index, want it gone", a)
	}
	if _, ok := q2.index[b]; !ok {
		t.Errorf("un-acked task %s missing from recovered index", b)
	}

	// Only b should be deliverable, exactly once.
	got, ok, _ := q2.Dequeue()
	if !ok {
		t.Fatal("expected b to be dequeueable after recovery")
	}
	if got.ID != b {
		t.Errorf("recovered Dequeue = %s, want %s (acked task must not resurrect)", got.ID, b)
	}
	if _, ok, _ := q2.Dequeue(); ok {
		t.Error("a second task was dequeueable after recovery, want only b")
	}
}

// The retry count is rebuilt by counting retry records during replay.
func TestRecoverRetryCount(t *testing.T) {
	q1, path := newTestQueue(t)
	a, _ := q1.Enqueue([]byte("a"), "key-a")
	for i := 0; i < 3; i++ {
		q1.Dequeue()
		if err := Retry(q1, a); err != nil {
			t.Fatalf("Retry #%d: %v", i, err)
		}
	}

	q2 := reopenQueue(t, path)

	task, ok := q2.index[a]
	if !ok {
		t.Fatalf("task %s missing from recovered index", a)
	}
	if task.Retries != 3 {
		t.Errorf("recovered Retries = %d, want 3", task.Retries)
	}
	if task.Status != StateReady {
		t.Errorf("recovered Status = %s, want ready", task.Status)
	}
}

// A mixed log recovers to the correct combined final state: dequeue isn't logged
// (so a replays as ready), b was acked (gone), c was retried (ready, Retries=1).
func TestRecoverMixedLog(t *testing.T) {
	q1, path := newTestQueue(t)
	a, _ := q1.Enqueue([]byte("a"), "key-a")
	b, _ := q1.Enqueue([]byte("b"), "key-b")
	c, _ := q1.Enqueue([]byte("c"), "key-c")

	q1.Dequeue() // pop a (not logged)
	if err := Ack(q1, b); err != nil {
		// b isn't at the front; Ack works by ID regardless of ready position.
		t.Fatalf("Ack b: %v", err)
	}
	q1.Dequeue() // pop c
	if err := Retry(q1, c); err != nil {
		t.Fatalf("Retry c: %v", err)
	}

	q2 := reopenQueue(t, path)

	// a: dequeue not logged -> replays as ready.
	if ta, ok := q2.index[a]; !ok {
		t.Errorf("task a missing after recovery")
	} else if ta.Status != StateReady {
		t.Errorf("recovered a Status = %s, want ready", ta.Status)
	}

	// b: acked -> gone.
	if _, ok := q2.index[b]; ok {
		t.Errorf("acked task b present after recovery, want gone")
	}

	// c: retried -> ready with Retries == 1.
	if tc, ok := q2.index[c]; !ok {
		t.Errorf("task c missing after recovery")
	} else {
		if tc.Status != StateReady {
			t.Errorf("recovered c Status = %s, want ready", tc.Status)
		}
		if tc.Retries != 1 {
			t.Errorf("recovered c Retries = %d, want 1", tc.Retries)
		}
	}
}

// --- Phase 4: leasing, sweeper, concurrency ---

// newTestQueueWith creates a queue with explicit lease/sweep durations (for sweeper
// tests) and registers Close for cleanup. Do NOT use it in tests that call Close
// themselves (double close panics).
func newTestQueueWith(t *testing.T, visibility, sweep time.Duration) *Queue {
	t.Helper()
	path := filepath.Join(t.TempDir(), "q.wal")
	q, err := NewQueue(path, visibility, sweep)
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	t.Cleanup(func() { q.Close() })
	return q
}

// waitFor polls cond every 2ms until it returns true or the timeout elapses.
// cond must read shared state under q.mu to stay race-free against the sweeper.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

// Dequeue grants a lease: the task leaves ready, enters indexOfInflight, becomes
// inflight, and gets a LeaseUntil in the future.
func TestDequeueSetsLease(t *testing.T) {
	q, _ := newTestQueue(t) // huge timeout: sweeper stays inert
	id, _ := q.Enqueue([]byte("a"), "key-a")

	before := time.Now()
	task, ok, err := q.Dequeue()
	if err != nil || !ok {
		t.Fatalf("Dequeue: ok=%v err=%v", ok, err)
	}
	if task.ID != id {
		t.Fatalf("Dequeue returned %s, want %s", task.ID, id)
	}
	if task.Status != StateInflight {
		t.Errorf("Status = %s, want inflight", task.Status)
	}
	if _, in := q.indexOfInflight[id]; !in {
		t.Errorf("task not in indexOfInflight after Dequeue")
	}
	if len(q.ready) != 0 {
		t.Errorf("ready length = %d, want 0 after Dequeue", len(q.ready))
	}
	if !task.LeaseUntil.After(before) {
		t.Errorf("LeaseUntil = %v, want after %v", task.LeaseUntil, before)
	}
}

// A single Sweeper pass reclaims a task whose lease has expired: back to ready,
// Retries bumped, and removed from indexOfInflight. Deterministic (manual pass,
// inert background sweeper).
func TestSweeperReclaimsExpiredLease(t *testing.T) {
	q := newTestQueueWith(t, 5*time.Millisecond, time.Hour) // short lease, inert bg sweeper
	id, _ := q.Enqueue([]byte("a"), "key-a")
	if _, ok, _ := q.Dequeue(); !ok {
		t.Fatal("Dequeue returned ok=false")
	}

	time.Sleep(20 * time.Millisecond) // lease (5ms) is now expired

	if err := q.Sweeper(); err != nil {
		t.Fatalf("Sweeper: %v", err)
	}

	if _, in := q.indexOfInflight[id]; in {
		t.Errorf("task still in indexOfInflight after reclaim, want removed")
	}
	if len(q.ready) != 1 {
		t.Fatalf("ready length = %d, want 1 after reclaim", len(q.ready))
	}
	task := q.index[id]
	if task.Status != StateReady {
		t.Errorf("Status = %s, want ready", task.Status)
	}
	if task.Retries != 1 {
		t.Errorf("Retries = %d, want 1", task.Retries)
	}
}

// A Sweeper pass must NOT touch a task whose lease is still valid.
func TestSweeperKeepsValidLease(t *testing.T) {
	q := newTestQueueWith(t, time.Hour, time.Hour) // long lease, inert bg sweeper
	id, _ := q.Enqueue([]byte("a"), "key-a")
	if _, ok, _ := q.Dequeue(); !ok {
		t.Fatal("Dequeue returned ok=false")
	}

	if err := q.Sweeper(); err != nil {
		t.Fatalf("Sweeper: %v", err)
	}

	if _, in := q.indexOfInflight[id]; !in {
		t.Errorf("valid-lease task removed from indexOfInflight, want kept")
	}
	if len(q.ready) != 0 {
		t.Errorf("ready length = %d, want 0 (task should stay inflight)", len(q.ready))
	}
	task := q.index[id]
	if task.Status != StateInflight {
		t.Errorf("Status = %s, want inflight", task.Status)
	}
	if task.Retries != 0 {
		t.Errorf("Retries = %d, want 0 (no reclaim)", task.Retries)
	}
}

// End-to-end: the background sweeper goroutine (not a manual pass) reclaims an
// expired lease on its own.
func TestBackgroundSweeperReclaims(t *testing.T) {
	q := newTestQueueWith(t, 5*time.Millisecond, 2*time.Millisecond) // short lease, fast tick
	id, _ := q.Enqueue([]byte("a"), "key-a")
	if _, ok, _ := q.Dequeue(); !ok {
		t.Fatal("Dequeue returned ok=false")
	}

	reclaimed := waitFor(time.Second, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		return len(q.ready) == 1
	})
	if !reclaimed {
		t.Fatal("background sweeper did not reclaim the expired lease within 1s")
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	if _, in := q.indexOfInflight[id]; in {
		t.Errorf("task still in indexOfInflight after background reclaim")
	}
	if task := q.index[id]; task.Retries != 1 {
		t.Errorf("Retries = %d, want 1", task.Retries)
	}
}

// Many goroutines hammer Enqueue/Dequeue/Ack at once. The load-bearing check is
// running this under `go test -race`: without the mutex it panics with
// "concurrent map writes"; with it, every task is produced and processed exactly
// once (produced == acked, index drained).
func TestConcurrentOpsRace(t *testing.T) {
	q, _ := newTestQueue(t) // huge timeout: sweeper stays inert

	// Task count kept modest: every Enqueue/Ack does a durable fsync, so this is
	// bounded by disk, not CPU. 16 goroutines still give -race plenty of contention.
	const producers = 8
	const perProducer = 25
	total := int64(producers * perProducer)

	var wg sync.WaitGroup

	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				if _, err := q.Enqueue([]byte("x"), "k"); err != nil {
					t.Errorf("Enqueue: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	var acked int64
	const consumers = 8
	for c := 0; c < consumers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				task, ok, err := q.Dequeue()
				if err != nil {
					t.Errorf("Dequeue: %v", err)
					return
				}
				if !ok {
					return // queue drained
				}
				if err := Ack(q, task.ID); err != nil {
					t.Errorf("Ack: %v", err)
					return
				}
				atomic.AddInt64(&acked, 1)
			}
		}()
	}
	wg.Wait()

	if acked != total {
		t.Errorf("acked = %d, want %d (conservation: produced == processed)", acked, total)
	}
	if len(q.index) != 0 {
		t.Errorf("index length = %d, want 0 after draining", len(q.index))
	}
}

// Close stops the sweeper cleanly: it returns promptly (doesn't deadlock) even
// with the sweeper actively ticking. This test owns the Close, so it does NOT use
// a helper that also registers Close (double close would panic).
func TestCloseStopsSweeper(t *testing.T) {
	path := filepath.Join(t.TempDir(), "q.wal")
	q, err := NewQueue(path, time.Hour, 2*time.Millisecond) // fast tick: sweeper is active
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}

	done := make(chan struct{})
	go func() {
		q.Close()
		close(done)
	}()

	select {
	case <-done: // Close returned: close(stop) + wg.Wait() completed
	case <-time.After(time.Second):
		t.Fatal("Close did not return within 1s (sweeper likely not stopping)")
	}
}
