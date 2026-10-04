package main

import "testing"

// Applying the same key twice must land one distinct effect (idempotent) while both
// deliveries are still counted. This is the exactly-once-effect property the whole
// conservation check leans on.
func TestEffectStoreIdempotent(t *testing.T) {
	s := newEffectStore()

	s.apply("k1")
	s.apply("k1") // duplicate delivery of the same task
	s.apply("k2")

	if got := s.distinct(); got != 2 {
		t.Errorf("distinct() = %d, want 2", got)
	}
	if got := s.deliveries(); got != 3 {
		t.Errorf("deliveries() = %d, want 3 (two of k1 + one of k2)", got)
	}
}
