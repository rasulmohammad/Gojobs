package task_queue

/*
Tests to cover:
1. Enqueue
2. Dequeue
3. Ack
4. Nack()
*/

import (
	"testing"

	"github.com/google/uuid"
)

// Enqueue: three tasks should land in both the ready slice and the index,
// each with a real (non-nil) unique ID and Status == ready.
func TestEnqueue(t *testing.T) {
	q := NewQueue()

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
	q := NewQueue()
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

// Ack: enqueue -> dequeue -> ack marks the task done.
func TestAck(t *testing.T) {
	q := NewQueue()
	id, _ := q.Enqueue([]byte("a"), "key-a")

	q.Dequeue()

	if err := Ack(q, id); err != nil {
		t.Fatalf("Ack returned error: %v", err)
	}
	if got := q.index[id].Status; got != StateDone {
		t.Errorf("after Ack, Status = %s, want done", got)
	}
}

// Nack: enqueue -> dequeue -> nack requeues the task: back to ready,
// retry count incremented, and it is handed out again by the next Dequeue.
func TestNack(t *testing.T) {
	q := NewQueue()
	id, _ := q.Enqueue([]byte("a"), "key-a")

	q.Dequeue() // task is now inflight, ready slice is empty

	if err := Nack(q, id); err != nil {
		t.Fatalf("Nack returned error: %v", err)
	}

	task := q.index[id]
	if task.Status != StateReady {
		t.Errorf("after Nack, Status = %s, want ready", task.Status)
	}
	if task.Retries != 1 {
		t.Errorf("after Nack, Retries = %d, want 1", task.Retries)
	}

	// The nacked task should be available to dequeue again.
	again, ok, _ := q.Dequeue()
	if !ok {
		t.Fatal("expected the nacked task to be dequeueable again")
	}
	if again.ID != id {
		t.Errorf("re-Dequeue returned %s, want the nacked task %s", again.ID, id)
	}
}

// Ack on an unknown ID must return an error (not panic) and must not mutate state.
func TestAckMissingKey(t *testing.T) {
	q := NewQueue()

	err := Ack(q, uuid.New())
	if err == nil {
		t.Fatal("Ack on unknown ID returned nil error, want an error")
	}
	if len(q.index) != 0 {
		t.Errorf("Ack on unknown ID changed index size to %d, want 0", len(q.index))
	}
}

// Nack on an unknown ID must return an error (not panic) and must not requeue anything.
func TestNackMissingKey(t *testing.T) {
	q := NewQueue()

	err := Nack(q, uuid.New())
	if err == nil {
		t.Fatal("Nack on unknown ID returned nil error, want an error")
	}
	if len(q.ready) != 0 {
		t.Errorf("Nack on unknown ID appended to ready (len = %d), want 0", len(q.ready))
	}
}
