package queue

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

// A malformed / truncated record body must return an error, never panic with a
// slice-out-of-range. Covers the variable-length Enqueue path and the fixed Dead path.
func TestDecodeRecordTruncatedBodyNoPanic(t *testing.T) {
	enqFields, err := encodeEnqueuePayload(EnqueuePayload{
		ID:             uuid.New(),
		IdempotencyKey: "key",
		Payload:        []byte("payload"),
		EnqueuedAt:     time.Now(),
		Priority:       3,
	})
	if err != nil {
		t.Fatalf("encodeEnqueuePayload: %v", err)
	}
	deadFields, err := encodeDeadPayload(DeadPayload{ID: uuid.New(), Retries: 2, FailedAt: time.Now()})
	if err != nil {
		t.Fatalf("encodeDeadPayload: %v", err)
	}

	records := map[string][]byte{
		"enqueue": encodeRecord(OperationEnqueue, enqFields),
		"dead":    encodeRecord(OperationDead, deadFields),
	}

	for name, full := range records {
		t.Run(name, func(t *testing.T) {
			// Every prefix shorter than the whole record is a torn body. Copy into
			// an exact-size slice (cap == len) so a reslice past the end panics,
			// matching the make([]byte, n) buffer decodeFrame hands us in real replay.
			for n := 1; n < len(full); n++ {
				torn := make([]byte, n)
				copy(torn, full[:n])
				if _, _, err := decodeRecord(torn); err == nil {
					t.Errorf("decodeRecord(%s[:%d]) = nil error, want error for truncated body", name, n)
				}
			}
		})
	}
}
