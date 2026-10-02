package task_queue

import (
	"sync"
	"testing"

	"github.com/google/uuid"
)

// Phase 6.2 — idempotent enqueue (dedup on IdempotencyKey).
// Contract under test: silent no-op (duplicate returns the existing ID, nil
// error, no second task, no second WAL record), first-write-wins, empty key opts
// out, dedup index rebuilt on replay, keys persist forever.
//
// These compile against the current API, so they fail *behaviorally* (today's
// Enqueue always mints a new ID) until 6.2 is implemented.

// Same key twice -> one task; the second call returns the first ID.
func TestEnqueueDedupSameKey(t *testing.T) {
	q, _ := newTestQueue(t)

	id1, err := q.Enqueue([]byte("a"), "dup")
	if err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}
	id2, err := q.Enqueue([]byte("a-again"), "dup")
	if err != nil {
		t.Fatalf("second Enqueue: %v", err)
	}

	if id2 != id1 {
		t.Errorf("duplicate Enqueue returned %s, want existing ID %s", id2, id1)
	}
	if got := len(q.ready); got != 1 {
		t.Errorf("ready length = %d, want 1 (duplicate must not create a task)", got)
	}
	if got := len(q.index); got != 1 {
		t.Errorf("index length = %d, want 1", got)
	}
}

// First-write-wins: the stored task keeps the FIRST payload.
func TestEnqueueDedupFirstWriteWins(t *testing.T) {
	q, _ := newTestQueue(t)

	id, _ := q.Enqueue([]byte("first"), "dup")
	q.Enqueue([]byte("second"), "dup")

	if got := len(q.index); got != 1 {
		t.Errorf("index length = %d, want 1 (second enqueue must be deduped)", got)
	}
	got, ok := q.index[id]
	if !ok {
		t.Fatalf("task %s missing from index", id)
	}
	if string(got.Payload) != "first" {
		t.Errorf("stored payload = %q, want %q (first write wins)", got.Payload, "first")
	}
}

// Same key 100x -> exactly one task and exactly one durable record. Recovery is
// the oracle for "one record": a second ENQUEUE record would rebuild a second task.
func TestEnqueueDedupManyTimesOneRecord(t *testing.T) {
	q, path := newTestQueue(t)

	var first uuid.UUID
	for i := 0; i < 100; i++ {
		id, err := q.Enqueue([]byte("x"), "dup")
		if err != nil {
			t.Fatalf("Enqueue #%d: %v", i, err)
		}
		if i == 0 {
			first = id
		} else if id != first {
			t.Fatalf("Enqueue #%d returned %s, want %s", i, id, first)
		}
	}
	if got := len(q.ready); got != 1 {
		t.Errorf("ready length = %d, want 1 after 100 same-key enqueues", got)
	}

	q2 := reopenQueue(t, path)
	if got := len(q2.index); got != 1 {
		t.Errorf("recovered index length = %d, want 1 (only one ENQUEUE record should exist)", got)
	}
}

// An empty idempotency key opts out of dedup: each call is its own task.
func TestEnqueueEmptyKeyNoDedup(t *testing.T) {
	q, _ := newTestQueue(t)

	id1, _ := q.Enqueue([]byte("a"), "")
	id2, _ := q.Enqueue([]byte("b"), "")

	if id1 == id2 {
		t.Error("two empty-key enqueues returned the same ID, want distinct tasks")
	}
	if got := len(q.ready); got != 2 {
		t.Errorf("ready length = %d, want 2 (empty key must not dedup)", got)
	}
}

// Dedup state is rebuilt from the WAL: after a restart, re-enqueuing a key used
// before recovery is still deduped to the original task.
func TestEnqueueDedupSurvivesRecovery(t *testing.T) {
	q1, path := newTestQueue(t)
	orig, _ := q1.Enqueue([]byte("a"), "dup")

	q2 := reopenQueue(t, path)
	again, err := q2.Enqueue([]byte("a-again"), "dup")
	if err != nil {
		t.Fatalf("post-recovery Enqueue: %v", err)
	}

	if again != orig {
		t.Errorf("post-recovery duplicate returned %s, want original %s", again, orig)
	}
	if got := len(q2.index); got != 1 {
		t.Errorf("index length = %d after recovery + duplicate, want 1", got)
	}
}

// Concurrent enqueues of the SAME key collapse to exactly one task, and every
// caller gets the same ID. This pins dedup's check-then-insert atomicity under
// contention: the lock must be held across BOTH steps, or two goroutines could
// each see "key absent" and create a task. `go test -race` can't catch that logic
// race on its own (the map is still lock-protected per access); this assertion can.
func TestEnqueueDedupConcurrentSameKey(t *testing.T) {
	q, _ := newTestQueue(t)

	const goroutines = 50
	ids := make([]uuid.UUID, goroutines) // each goroutine writes its own index
	errs := make([]error, goroutines)

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ids[g], errs[g] = q.Enqueue([]byte("x"), "shared")
		}()
	}
	wg.Wait()

	for g, err := range errs {
		if err != nil {
			t.Fatalf("Enqueue #%d: %v", g, err)
		}
	}

	// Exactly one task created, despite 50 concurrent same-key enqueues.
	if got := len(q.index); got != 1 {
		t.Errorf("index length = %d, want 1 (concurrent same-key enqueues must collapse)", got)
	}
	if got := len(q.ready); got != 1 {
		t.Errorf("ready length = %d, want 1", got)
	}

	// Every caller received the same deduped ID.
	for g := 1; g < goroutines; g++ {
		if ids[g] != ids[0] {
			t.Fatalf("goroutine %d got ID %s, want %s (all callers share the deduped ID)", g, ids[g], ids[0])
		}
	}
}

// Persist-forever consequence (accepted tradeoff): a key stays deduped even after
// its task is Acked and gone from index. Re-enqueuing is a no-op returning the
// original (now-dangling) ID; no new task is created.
func TestEnqueueDedupPersistsAfterAck(t *testing.T) {
	q, _ := newTestQueue(t)

	orig, _ := q.Enqueue([]byte("a"), "dup")
	if _, ok, _ := q.Dequeue(); !ok {
		t.Fatal("Dequeue: ok=false")
	}
	if err := Ack(q, orig); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	again, err := q.Enqueue([]byte("a"), "dup")
	if err != nil {
		t.Fatalf("re-Enqueue after ack: %v", err)
	}
	if again != orig {
		t.Errorf("re-Enqueue returned %s, want original %s (key persists forever)", again, orig)
	}
	if got := len(q.ready); got != 0 {
		t.Errorf("ready length = %d, want 0 (dedup must not re-create the acked task)", got)
	}
	if _, ok := q.index[again]; ok {
		t.Error("returned ID is back in index; persist-forever returns a dangling ID by design")
	}
}
