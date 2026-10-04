package main

import "sync"

// effectStore is the load test's stand-in for an idempotent consumer endpoint:
// applying the same idempotency key more than once still records a single effect.
// That is what turns the queue's at-least-once *delivery* into exactly-once
// *effect*, and it is the ledger the end-of-run conservation check reads.
//
// It mirrors the effectStore pattern in internal/queue/harness_test.go, which lives
// in a _test.go file and so cannot be imported from this binary.
type effectStore struct {
	mu      sync.Mutex
	applied map[string]int // idemKey -> times delivered (>=1); len == distinct effects
	total   int64          // total apply calls, including duplicate deliveries
}

func newEffectStore() *effectStore {
	return &effectStore{applied: make(map[string]int)}
}

// apply records one delivery of key. The effect is idempotent (distinct counts it
// once no matter how often it arrives); the per-key tally and total still climb so
// we can report how many duplicate deliveries the at-least-once path produced.
func (s *effectStore) apply(key string) {
	s.mu.Lock()
	s.applied[key]++
	s.total++
	s.mu.Unlock()
}

// distinct is the number of unique keys whose effect landed: the exactly-once count
// the conservation check compares against the number of tasks produced.
func (s *effectStore) distinct() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.applied)
}

// deliveries is the total apply calls including duplicates; deliveries-distinct is
// the number of redeliveries the run absorbed without double-applying an effect.
func (s *effectStore) deliveries() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.total
}
