package task_queue

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// TODO: decoders currently assume well-formed input. A truncated buffer or a
// length prefix larger than the remaining bytes will panic (slice out of range)
// instead of returning an error. Add bounds checking / error returns next, then
// add truncation tests to record_test.go.

type OperationType uint8

const (
	OperationEnqueue OperationType = iota + 1
	OperationDequeue
	OperationAck
	OperationRetry
	OperationDead
)

func (o OperationType) String() string {
	switch o {
	case OperationEnqueue:
		return "enqueue"
	case OperationDequeue:
		return "dequeue"
	case OperationAck:
		return "ack"
	case OperationRetry:
		return "retry"
	case OperationDead:
		return "dead"
	default:
		return "unknown"
	}
}

// Wire format (one frame per append):
// [len uint32][crc32 uint32][type uint8][payload]

// It's better to define only what each operation actually needs because not all operations need the same fields
// but since we had to later encode/decode the struct/[]byte, it is now operation specific. More functions
type EnqueuePayload struct {
	ID             uuid.UUID
	IdempotencyKey string
	Payload        []byte
	EnqueuedAt     time.Time
	Priority       int
}

// Encoding only fields. General rule, if the size of a field is fixed, no prefix needed. If not, add a prefix
// We encode in the order as defined in the struct
func encodeEnqueuePayload(p EnqueuePayload) ([]byte, error) {
	var buf bytes.Buffer
	var scratch [8]byte

	// buf only takes a slice. ID is already [16]byte so no conversion necessary here
	buf.Write(p.ID[:])

	// IdempotencyKey not fixed length, which means we need to length prefix it. 32 bits/8 bytes is more than enough
	IdempotencyKeyLength := uint32(len(p.IdempotencyKey))
	binary.BigEndian.PutUint32(scratch[:4], IdempotencyKeyLength)
	buf.Write(scratch[:4]) // len prefix
	buf.WriteString(p.IdempotencyKey)

	// Payload not fixed length, we need to length prefix.
	PayloadLength := uint32(len(p.Payload))
	binary.BigEndian.PutUint32(scratch[:4], PayloadLength)
	buf.Write(scratch[:4])
	buf.Write(p.Payload)

	// time.Time cant be serialized directly (wall clock, monotonic reading, and a *Location pointer built in)
	// Use nanoseconds unix time instead -- need 8 bytes for this since 4 bytes max < current nanoseconds
	EnqueuedAtNanoseconds := uint64(p.EnqueuedAt.UnixNano())
	binary.BigEndian.PutUint64(scratch[:8], EnqueuedAtNanoseconds)
	buf.Write(scratch[:8])

	// Known value, 4 bytes. Assumes priority is >= 0 / non-negative
	binary.BigEndian.PutUint32(scratch[:4], uint32(p.Priority))
	buf.Write(scratch[:4])

	// buf.Bytes() returns everything we appended, in order.
	return buf.Bytes(), nil
}

func decodeEnqueuePayload(b []byte) (EnqueuePayload, error) {
	// A running offset for clearer naming convention
	bytePos := 0
	//First 16 bytes is ID:
	ID, err := uuid.FromBytes(b[:16])
	if err != nil {
		return EnqueuePayload{}, err
	}
	bytePos += 16

	//Idempotency key
	IdempotencyKeyLength := int(binary.BigEndian.Uint32(b[bytePos : bytePos+4]))
	bytePos += 4
	IdempotencyKey := string(b[bytePos : bytePos+IdempotencyKeyLength])
	bytePos += IdempotencyKeyLength

	//Payload
	PayloadLength := int(binary.BigEndian.Uint32(b[bytePos : bytePos+4]))
	bytePos += 4
	Payload := b[bytePos : bytePos+PayloadLength]
	bytePos += PayloadLength

	// Enqueued at
	EnqueuedAtNanoseconds := int64(binary.BigEndian.Uint64(b[bytePos : bytePos+8]))
	bytePos += 8
	EnqueuedAt := time.Unix(0, EnqueuedAtNanoseconds).UTC()

	//Priority
	Priority := int(binary.BigEndian.Uint32(b[bytePos : bytePos+4]))

	return EnqueuePayload{
		ID:             ID,
		IdempotencyKey: IdempotencyKey,
		Payload:        Payload,
		EnqueuedAt:     EnqueuedAt,
		Priority:       Priority,
	}, nil
}

type DequeuePayload struct {
	ID         uuid.UUID
	LeaseUntil time.Time
	Owner      string
}

func encodeDequeuePayload(p DequeuePayload) ([]byte, error) {
	var buf bytes.Buffer
	var scratch [8]byte

	//ID
	buf.Write(p.ID[:])

	//LeaseUntil
	LeaseUntilNanoseconds := uint64(p.LeaseUntil.UnixNano())
	binary.BigEndian.PutUint64(scratch[:8], LeaseUntilNanoseconds)
	buf.Write(scratch[:8])

	//Owner
	OwnerLength := uint32(len(p.Owner))
	binary.BigEndian.PutUint32(scratch[:4], OwnerLength)
	buf.Write(scratch[:4])
	buf.WriteString(p.Owner)

	return buf.Bytes(), nil
}

func decodeDequeuePayload(b []byte) (DequeuePayload, error) {
	bytePos := 0

	// ID
	ID, err := uuid.FromBytes(b[:16])
	if err != nil {
		return DequeuePayload{}, err
	}
	bytePos += 16

	//Time
	LeaseUntilNanoseconds := int64(binary.BigEndian.Uint64(b[bytePos : bytePos+8]))
	bytePos += 8
	LeaseUntil := time.Unix(0, LeaseUntilNanoseconds).UTC()

	//Owner
	OwnerLength := int(binary.BigEndian.Uint32(b[bytePos : bytePos+4]))
	bytePos += 4
	Owner := string(b[bytePos : bytePos+OwnerLength])

	return DequeuePayload{
		ID:         ID,
		LeaseUntil: LeaseUntil,
		Owner:      Owner,
	}, nil

}

type AckPayload struct {
	ID uuid.UUID
}

func encodeAckPayload(p AckPayload) ([]byte, error) {
	var buf bytes.Buffer
	//ID
	buf.Write(p.ID[:])
	return buf.Bytes(), nil
}

func decodeAckPayload(b []byte) (AckPayload, error) {
	ID, err := uuid.FromBytes(b[:16])
	if err != nil {
		return AckPayload{}, err
	}

	return AckPayload{
		ID: ID,
	}, nil
}

type RetryPayload struct {
	ID uuid.UUID
}

func encodeRetryPayload(p RetryPayload) ([]byte, error) {
	var buf bytes.Buffer
	//ID
	buf.Write(p.ID[:])
	return buf.Bytes(), nil
}

func decodeRetryPayload(b []byte) (RetryPayload, error) {
	ID, err := uuid.FromBytes(b[:16])
	if err != nil {
		return RetryPayload{}, err
	}

	return RetryPayload{
		ID: ID,
	}, nil
}

type DeadPayload struct {
	ID uuid.UUID
}

func encodeDeadPayload(p DeadPayload) ([]byte, error) {
	var buf bytes.Buffer
	//ID
	buf.Write(p.ID[:])
	return buf.Bytes(), nil
}

func decodeDeadPayload(b []byte) (DeadPayload, error) {
	ID, err := uuid.FromBytes(b[:16])
	if err != nil {
		return DeadPayload{}, err
	}

	return DeadPayload{
		ID: ID,
	}, nil
}

// --- record layer: op tag + dispatch ---
// Body layout is [op uint8][op-specific fields]. The op tag lives in the BODY,
// not in the WAL frame, so the frame stays opaque ([len][crc][payload]) and wal.go
// never needs to know about operations. (Update encodeFrame/decodeFrame in wal.go
// to drop the op parameter accordingly.)

// encodeRecord prepends the operation tag to already-encoded field bytes.
func encodeRecord(op OperationType, fields []byte) []byte {
	return append([]byte{byte(op)}, fields...)
}

// decodeRecord reads the leading op tag and dispatches to the matching field
// decoder. Returns the op plus the decoded payload; callers type-assert on op.
func decodeRecord(body []byte) (OperationType, interface{}, error) {
	if len(body) == 0 {
		return 0, nil, fmt.Errorf("decodeRecord: empty body")
	}

	// We do this because when we encodeRecord() we append the operation to the 1st byte. The rest are the fields bytes
	op := OperationType(body[0])
	fields := body[1:]

	switch op {
	case OperationEnqueue:
		p, err := decodeEnqueuePayload(fields)
		return op, p, err
	case OperationDequeue:
		p, err := decodeDequeuePayload(fields)
		return op, p, err
	case OperationAck:
		p, err := decodeAckPayload(fields)
		return op, p, err
	case OperationRetry:
		p, err := decodeRetryPayload(fields)
		return op, p, err
	case OperationDead:
		p, err := decodeDeadPayload(fields)
		return op, p, err
	default:
		return op, nil, fmt.Errorf("decodeRecord: unknown op %d", op)
	}
}
