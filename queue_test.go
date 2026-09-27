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
	"testing"

	"github.com/google/uuid"
)

// newTestQueue creates a queue backed by a throwaway WAL and returns it plus the
// path, so recovery tests can reopen the same log.
func newTestQueue(t *testing.T) (*Queue, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "q.wal")
	q, err := NewQueue(path)
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	return q, path
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

	q2, err := NewQueue(path)
	if err != nil {
		t.Fatalf("reopen NewQueue: %v", err)
	}
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

	q2, err := NewQueue(path)
	if err != nil {
		t.Fatalf("reopen NewQueue: %v", err)
	}

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

	q2, err := NewQueue(path)
	if err != nil {
		t.Fatalf("reopen NewQueue: %v", err)
	}

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

	q2, err := NewQueue(path)
	if err != nil {
		t.Fatalf("reopen NewQueue: %v", err)
	}

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

	q2, err := NewQueue(path)
	if err != nil {
		t.Fatalf("reopen NewQueue: %v", err)
	}

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
