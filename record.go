package task_queue

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

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

func encodeEnqueuePayload(p EnqueuePayload) ([]byte, error) {
	panic("TODO: implement in 2.1")
}

func decodeEnqueuePayload(b []byte) (EnqueuePayload, error) {
	panic("TODO: implement in 2.1")
}


type DequeuePayload struct {
    ID         uuid.UUID
    LeaseUntil time.Time
    Owner      string
}

func encodeDequeuePayload(p DequeuePayload) ([]byte, error) {
	panic("TODO: implement in 2.1")
}

func decodeDequeuePayload(b []byte) (DequeuePayload, error) {
	panic("TODO: implement in 2.1")
}


type AckPayload struct {
    ID uuid.UUID
}

func encodeAckPayload(p AckPayload) ([]byte, error) {
	panic("TODO: implement in 2.1")
}

func decodeAckPayload(b []byte) (AckPayload, error) {
	panic("TODO: implement in 2.1")
}

type RetryPayload struct {
    ID uuid.UUID
}

func encodeRetryPayload(p RetryPayload) ([]byte, error) {
	panic("TODO: implement in 2.1")
}

func decodeRetryPayload(b []byte) (RetryPayload, error) {
	panic("TODO: implement in 2.1")
}

type DeadPayload struct {
    ID uuid.UUID
}

func encodeDeadPayload(p DeadPayload) ([]byte, error) {
	panic("TODO: implement in 2.1")
}

func decodeDeadPayload(b []byte) (DeadPayload, error) {
	panic("TODO: implement in 2.1")
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
