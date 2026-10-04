package task_queue

//   1. effectStore — a toy *idempotent* endpoint. Applying the same idemKey
//      more than once still counts once. This is what makes the queue's
//      at-least-once delivery look effectively-once at the effect level.
//   2. produce    — a producer: enqueue a batch of keys.
//   3. drain      — a consumer: pull tasks and apply their effect, until empty.

import (
	"sync"
	"testing"
)

// effectStore is the stand-in for a real idempotent consumer endpoint. Needs a mutex because we'll spawn in multiple goroutines later on
type effectStore struct {
	mu      sync.Mutex
	applied map[string]int // idemKey -> number of effects recorded for it
}

func newEffectStore() *effectStore {
	return &effectStore{applied: make(map[string]int)}
}

// Applying the records effects idempotently
func (s *effectStore) apply(idemKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// TODO: record idemKey's effect exactly once.

	_, ok := s.applied[idemKey]
	if ok {
		return
	}

	s.applied[idemKey] += 1
}

// count returns how many distinct keys have had an effect applied. We're using this to assert equal counts in our tests
func (s *effectStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.applied)
}

// produce enqueues one task per key.
func produce(t *testing.T, q *Queue, keys []string) {
	t.Helper()
	for _, k := range keys {
		// Throw error if we can't even open our queue
		if _, err := q.Enqueue([]byte(k), k); err != nil {
			t.Fatalf("Enqueue(%q): %v", k, err)
		}
	}
}

// drain is a single-threaded consumer: pull ready tasks and apply each effect until the queue has nothing ready, then return.
func drain(t *testing.T, q *Queue, store *effectStore) {
	t.Helper()
	for {
		task, ok, err := q.Dequeue()
		if err != nil {
			t.Fatalf("Dequeue(): %v", err)
		}

		// Empty queue (this is synchronous call after our enqueue()'s, so it's fine to return. During non-synchronous calls we should have backoff logic instead)
		if !ok {
			return
		}

		// Apply effect of task (successfully dequeued)
		store.apply(task.IdempotencyKey)
		if err := Ack(q, task.ID); err != nil {
			t.Fatalf("Ack(%q): %v", task.ID, err)
		}
	}
}

// TestHarnessBasic for 1 gouroutine. Produce 3 tasks & ensure we get 3 back
func TestHarnessBasic(t *testing.T) {
	q, _ := newTestQueue(t)
	store := newEffectStore()

	produce(t, q, []string{"k1", "k2", "k3"})
	drain(t, q, store)

	if got := store.count(); got != 3 {
		t.Fatalf("effect store count = %d, want 3", got)
	}
}
