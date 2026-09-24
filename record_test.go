package task_queue

/*
Tests to cover (record.go):
 1. OperationType.String
 2. Enqueue payload round trip
 3. Enqueue payload round trip with empty fields
 4. Enqueue payload encoded byte layout
 5. Dequeue payload round trip
 6. Dequeue payload round trip with empty owner
 7. Ack payload round trip
 8. Retry payload round trip
 9. Dead payload round trip
10. encodeRecord prepends the op tag
11. Record round trip for every op
12. decodeRecord on an empty body
13. decodeRecord on an unknown op
*/

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/google/uuid"
)

// String: each defined op returns its exact name; anything else is "unknown".
func TestOperationTypeString(t *testing.T) {
	if got := OperationEnqueue.String(); got != "enqueue" {
		t.Errorf("OperationEnqueue.String() = %q, want \"enqueue\"", got)
	}
	if got := OperationDequeue.String(); got != "dequeue" {
		t.Errorf("OperationDequeue.String() = %q, want \"dequeue\"", got)
	}
	if got := OperationAck.String(); got != "ack" {
		t.Errorf("OperationAck.String() = %q, want \"ack\"", got)
	}
	if got := OperationRetry.String(); got != "retry" {
		t.Errorf("OperationRetry.String() = %q, want \"retry\"", got)
	}
	if got := OperationDead.String(); got != "dead" {
		t.Errorf("OperationDead.String() = %q, want \"dead\"", got)
	}
	// Zero value and an out-of-range value both fall through to the default.
	if got := OperationType(0).String(); got != "unknown" {
		t.Errorf("OperationType(0).String() = %q, want \"unknown\"", got)
	}
	if got := OperationType(99).String(); got != "unknown" {
		t.Errorf("OperationType(99).String() = %q, want \"unknown\"", got)
	}
}

// Enqueue round trip: encode a fully-populated payload, decode it, and confirm
// every field comes back unchanged.
func TestEnqueuePayloadRoundTrip(t *testing.T) {
	want := EnqueuePayload{
		ID:             uuid.New(),
		IdempotencyKey: "key-abc",
		Payload:        []byte("hello world"),
		EnqueuedAt:     time.Date(2026, 9, 24, 12, 0, 0, 123456789, time.UTC),
		Priority:       7,
	}

	b, err := encodeEnqueuePayload(want)
	if err != nil {
		t.Fatalf("encodeEnqueuePayload returned error: %v", err)
	}

	got, err := decodeEnqueuePayload(b)
	if err != nil {
		t.Fatalf("decodeEnqueuePayload returned error: %v", err)
	}

	if got.ID != want.ID {
		t.Errorf("ID = %s, want %s", got.ID, want.ID)
	}
	if got.IdempotencyKey != want.IdempotencyKey {
		t.Errorf("IdempotencyKey = %q, want %q", got.IdempotencyKey, want.IdempotencyKey)
	}
	if !bytes.Equal(got.Payload, want.Payload) {
		t.Errorf("Payload = %q, want %q", got.Payload, want.Payload)
	}
	if !got.EnqueuedAt.Equal(want.EnqueuedAt) {
		t.Errorf("EnqueuedAt = %v, want %v", got.EnqueuedAt, want.EnqueuedAt)
	}
	if got.Priority != want.Priority {
		t.Errorf("Priority = %d, want %d", got.Priority, want.Priority)
	}
}

// Enqueue round trip with empty variable-length fields: an empty key and an
// empty payload (length prefix = 0) must survive.
func TestEnqueuePayloadRoundTripEmptyFields(t *testing.T) {
	want := EnqueuePayload{
		ID:             uuid.New(),
		IdempotencyKey: "",
		Payload:        []byte{},
		EnqueuedAt:     time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC),
		Priority:       0,
	}

	b, err := encodeEnqueuePayload(want)
	if err != nil {
		t.Fatalf("encodeEnqueuePayload returned error: %v", err)
	}

	got, err := decodeEnqueuePayload(b)
	if err != nil {
		t.Fatalf("decodeEnqueuePayload returned error: %v", err)
	}

	if got.IdempotencyKey != "" {
		t.Errorf("IdempotencyKey = %q, want empty string", got.IdempotencyKey)
	}
	if len(got.Payload) != 0 {
		t.Errorf("Payload len = %d, want 0", len(got.Payload))
	}
	if !got.EnqueuedAt.Equal(want.EnqueuedAt) {
		t.Errorf("EnqueuedAt = %v, want %v", got.EnqueuedAt, want.EnqueuedAt)
	}
}

// Encoded byte layout: the wire format is
// [16 ID][4 keyLen][key][4 payloadLen][payload][8 time][4 priority].
// Check the total length and the fixed-position fields so a field-order or
// field-size change is caught here.
func TestEnqueuePayloadEncodeByteLayout(t *testing.T) {
	p := EnqueuePayload{
		ID:             uuid.New(),
		IdempotencyKey: "key-abc", // 7 bytes
		Payload:        []byte("hello"), // 5 bytes
		EnqueuedAt:     time.Unix(0, 0).UTC(),
		Priority:       1,
	}

	b, err := encodeEnqueuePayload(p)
	if err != nil {
		t.Fatalf("encodeEnqueuePayload returned error: %v", err)
	}

	wantLen := 16 + 4 + len(p.IdempotencyKey) + 4 + len(p.Payload) + 8 + 4
	if len(b) != wantLen {
		t.Fatalf("encoded length = %d, want %d", len(b), wantLen)
	}

	// First 16 bytes are the raw ID.
	if !bytes.Equal(b[:16], p.ID[:]) {
		t.Errorf("bytes[:16] = %x, want ID %x", b[:16], p.ID[:])
	}
	// Next 4 bytes are the key length prefix.
	if got := binary.BigEndian.Uint32(b[16:20]); got != uint32(len(p.IdempotencyKey)) {
		t.Errorf("key length prefix = %d, want %d", got, len(p.IdempotencyKey))
	}
	// The key bytes follow the prefix.
	if got := string(b[20 : 20+len(p.IdempotencyKey)]); got != p.IdempotencyKey {
		t.Errorf("key bytes = %q, want %q", got, p.IdempotencyKey)
	}
}

// Dequeue round trip: ID, LeaseUntil, and Owner come back unchanged.
func TestDequeuePayloadRoundTrip(t *testing.T) {
	want := DequeuePayload{
		ID:         uuid.New(),
		LeaseUntil: time.Date(2026, 9, 24, 13, 30, 0, 42, time.UTC),
		Owner:      "worker-1",
	}

	b, err := encodeDequeuePayload(want)
	if err != nil {
		t.Fatalf("encodeDequeuePayload returned error: %v", err)
	}

	got, err := decodeDequeuePayload(b)
	if err != nil {
		t.Fatalf("decodeDequeuePayload returned error: %v", err)
	}

	if got.ID != want.ID {
		t.Errorf("ID = %s, want %s", got.ID, want.ID)
	}
	if !got.LeaseUntil.Equal(want.LeaseUntil) {
		t.Errorf("LeaseUntil = %v, want %v", got.LeaseUntil, want.LeaseUntil)
	}
	if got.Owner != want.Owner {
		t.Errorf("Owner = %q, want %q", got.Owner, want.Owner)
	}
}

// Dequeue round trip with an empty owner (length prefix = 0).
func TestDequeuePayloadRoundTripEmptyOwner(t *testing.T) {
	want := DequeuePayload{
		ID:         uuid.New(),
		LeaseUntil: time.Date(2026, 9, 24, 13, 30, 0, 0, time.UTC),
		Owner:      "",
	}

	b, err := encodeDequeuePayload(want)
	if err != nil {
		t.Fatalf("encodeDequeuePayload returned error: %v", err)
	}

	got, err := decodeDequeuePayload(b)
	if err != nil {
		t.Fatalf("decodeDequeuePayload returned error: %v", err)
	}

	if got.Owner != "" {
		t.Errorf("Owner = %q, want empty string", got.Owner)
	}
	if got.ID != want.ID {
		t.Errorf("ID = %s, want %s", got.ID, want.ID)
	}
}

// Ack round trip: the ID survives and the encoded body is exactly 16 bytes.
func TestAckPayloadRoundTrip(t *testing.T) {
	want := AckPayload{ID: uuid.New()}

	b, err := encodeAckPayload(want)
	if err != nil {
		t.Fatalf("encodeAckPayload returned error: %v", err)
	}
	if len(b) != 16 {
		t.Errorf("encoded length = %d, want 16", len(b))
	}

	got, err := decodeAckPayload(b)
	if err != nil {
		t.Fatalf("decodeAckPayload returned error: %v", err)
	}
	if got.ID != want.ID {
		t.Errorf("ID = %s, want %s", got.ID, want.ID)
	}
}

// Retry round trip: same shape as Ack.
func TestRetryPayloadRoundTrip(t *testing.T) {
	want := RetryPayload{ID: uuid.New()}

	b, err := encodeRetryPayload(want)
	if err != nil {
		t.Fatalf("encodeRetryPayload returned error: %v", err)
	}
	if len(b) != 16 {
		t.Errorf("encoded length = %d, want 16", len(b))
	}

	got, err := decodeRetryPayload(b)
	if err != nil {
		t.Fatalf("decodeRetryPayload returned error: %v", err)
	}
	if got.ID != want.ID {
		t.Errorf("ID = %s, want %s", got.ID, want.ID)
	}
}

// Dead round trip: same shape as Ack.
func TestDeadPayloadRoundTrip(t *testing.T) {
	want := DeadPayload{ID: uuid.New()}

	b, err := encodeDeadPayload(want)
	if err != nil {
		t.Fatalf("encodeDeadPayload returned error: %v", err)
	}
	if len(b) != 16 {
		t.Errorf("encoded length = %d, want 16", len(b))
	}

	got, err := decodeDeadPayload(b)
	if err != nil {
		t.Fatalf("decodeDeadPayload returned error: %v", err)
	}
	if got.ID != want.ID {
		t.Errorf("ID = %s, want %s", got.ID, want.ID)
	}
}

// encodeRecord must put the op tag in byte 0 and leave the field bytes untouched
// after it.
func TestEncodeRecordPrependsOpTag(t *testing.T) {
	fields := []byte{0xDE, 0xAD, 0xBE, 0xEF}

	rec := encodeRecord(OperationAck, fields)

	if len(rec) != len(fields)+1 {
		t.Fatalf("record length = %d, want %d", len(rec), len(fields)+1)
	}
	if rec[0] != byte(OperationAck) {
		t.Errorf("record[0] = %d, want %d", rec[0], byte(OperationAck))
	}
	if !bytes.Equal(rec[1:], fields) {
		t.Errorf("record[1:] = %x, want %x", rec[1:], fields)
	}
}

// Full record round trip for every op: encode the fields, wrap them with
// encodeRecord, then decodeRecord and confirm the op and the decoded payload
// match what went in.
func TestRecordRoundTripAllOps(t *testing.T) {
	id := uuid.New()

	// Enqueue
	{
		want := EnqueuePayload{
			ID:             id,
			IdempotencyKey: "k",
			Payload:        []byte("p"),
			EnqueuedAt:     time.Date(2026, 9, 24, 12, 0, 0, 5, time.UTC),
			Priority:       3,
		}
		fields, err := encodeEnqueuePayload(want)
		if err != nil {
			t.Fatalf("encodeEnqueuePayload: %v", err)
		}
		op, payload, err := decodeRecord(encodeRecord(OperationEnqueue, fields))
		if err != nil {
			t.Fatalf("decodeRecord(enqueue): %v", err)
		}
		if op != OperationEnqueue {
			t.Errorf("op = %s, want enqueue", op)
		}
		got, ok := payload.(EnqueuePayload)
		if !ok {
			t.Fatalf("payload type = %T, want EnqueuePayload", payload)
		}
		if got.ID != want.ID || got.IdempotencyKey != want.IdempotencyKey ||
			!bytes.Equal(got.Payload, want.Payload) || !got.EnqueuedAt.Equal(want.EnqueuedAt) ||
			got.Priority != want.Priority {
			t.Errorf("enqueue payload = %+v, want %+v", got, want)
		}
	}

	// Dequeue
	{
		want := DequeuePayload{
			ID:         id,
			LeaseUntil: time.Date(2026, 9, 24, 12, 5, 0, 0, time.UTC),
			Owner:      "w1",
		}
		fields, err := encodeDequeuePayload(want)
		if err != nil {
			t.Fatalf("encodeDequeuePayload: %v", err)
		}
		op, payload, err := decodeRecord(encodeRecord(OperationDequeue, fields))
		if err != nil {
			t.Fatalf("decodeRecord(dequeue): %v", err)
		}
		if op != OperationDequeue {
			t.Errorf("op = %s, want dequeue", op)
		}
		got, ok := payload.(DequeuePayload)
		if !ok {
			t.Fatalf("payload type = %T, want DequeuePayload", payload)
		}
		if got.ID != want.ID || !got.LeaseUntil.Equal(want.LeaseUntil) || got.Owner != want.Owner {
			t.Errorf("dequeue payload = %+v, want %+v", got, want)
		}
	}

	// Ack
	{
		fields, err := encodeAckPayload(AckPayload{ID: id})
		if err != nil {
			t.Fatalf("encodeAckPayload: %v", err)
		}
		op, payload, err := decodeRecord(encodeRecord(OperationAck, fields))
		if err != nil {
			t.Fatalf("decodeRecord(ack): %v", err)
		}
		if op != OperationAck {
			t.Errorf("op = %s, want ack", op)
		}
		got, ok := payload.(AckPayload)
		if !ok {
			t.Fatalf("payload type = %T, want AckPayload", payload)
		}
		if got.ID != id {
			t.Errorf("ack ID = %s, want %s", got.ID, id)
		}
	}

	// Retry
	{
		fields, err := encodeRetryPayload(RetryPayload{ID: id})
		if err != nil {
			t.Fatalf("encodeRetryPayload: %v", err)
		}
		op, payload, err := decodeRecord(encodeRecord(OperationRetry, fields))
		if err != nil {
			t.Fatalf("decodeRecord(retry): %v", err)
		}
		if op != OperationRetry {
			t.Errorf("op = %s, want retry", op)
		}
		got, ok := payload.(RetryPayload)
		if !ok {
			t.Fatalf("payload type = %T, want RetryPayload", payload)
		}
		if got.ID != id {
			t.Errorf("retry ID = %s, want %s", got.ID, id)
		}
	}

	// Dead
	{
		fields, err := encodeDeadPayload(DeadPayload{ID: id})
		if err != nil {
			t.Fatalf("encodeDeadPayload: %v", err)
		}
		op, payload, err := decodeRecord(encodeRecord(OperationDead, fields))
		if err != nil {
			t.Fatalf("decodeRecord(dead): %v", err)
		}
		if op != OperationDead {
			t.Errorf("op = %s, want dead", op)
		}
		got, ok := payload.(DeadPayload)
		if !ok {
			t.Fatalf("payload type = %T, want DeadPayload", payload)
		}
		if got.ID != id {
			t.Errorf("dead ID = %s, want %s", got.ID, id)
		}
	}
}

// decodeRecord on an empty body must return an error, not panic.
func TestDecodeRecordEmptyBody(t *testing.T) {
	op, payload, err := decodeRecord([]byte{})
	if err == nil {
		t.Fatal("decodeRecord(empty) returned nil error, want an error")
	}
	if payload != nil {
		t.Errorf("payload = %v, want nil", payload)
	}
	if op != 0 {
		t.Errorf("op = %d, want 0", op)
	}
}

// decodeRecord on a body whose op tag is unknown must return an error and echo
// back the unknown op value.
func TestDecodeRecordUnknownOp(t *testing.T) {
	op, payload, err := decodeRecord([]byte{0xFF})
	if err == nil {
		t.Fatal("decodeRecord(unknown op) returned nil error, want an error")
	}
	if op != OperationType(0xFF) {
		t.Errorf("op = %d, want 255", op)
	}
	if payload != nil {
		t.Errorf("payload = %v, want nil", payload)
	}
}
