package task_queue

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// readN returns the next n bytes starting at pos, plus the advanced position, or
// an error if the body is too short. Decoders read through it instead of slicing
// b directly, so a truncated or malformed body returns an error rather than
// panicking with a slice-out-of-range.
func readN(b []byte, pos, n int) ([]byte, int, error) {
	if n < 0 || pos+n > len(b) {
		return nil, pos, fmt.Errorf("decode: need %d bytes at offset %d, have %d", n, pos, len(b))
	}
	return b[pos : pos+n], pos + n, nil
}

type OperationType uint8

const (
	OperationEnqueue OperationType = iota + 1
	OperationDequeue
	OperationAck
	OperationRetry
	OperationDead
	OperationRequeue
	OperationPurge
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
	case OperationRequeue:
		return "requeue"
	case OperationPurge:
		return "purge"
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
	//First 16 bytes is ID:
	idBytes, pos, err := readN(b, 0, 16)
	if err != nil {
		return EnqueuePayload{}, err
	}
	ID, err := uuid.FromBytes(idBytes)
	if err != nil {
		return EnqueuePayload{}, err
	}

	//Idempotency key (4-byte length prefix, then that many bytes)
	keyLenBytes, pos, err := readN(b, pos, 4)
	if err != nil {
		return EnqueuePayload{}, err
	}
	keyBytes, pos, err := readN(b, pos, int(binary.BigEndian.Uint32(keyLenBytes)))
	if err != nil {
		return EnqueuePayload{}, err
	}

	//Payload (same length-prefixed shape)
	payloadLenBytes, pos, err := readN(b, pos, 4)
	if err != nil {
		return EnqueuePayload{}, err
	}
	Payload, pos, err := readN(b, pos, int(binary.BigEndian.Uint32(payloadLenBytes)))
	if err != nil {
		return EnqueuePayload{}, err
	}

	// Enqueued at
	enqBytes, pos, err := readN(b, pos, 8)
	if err != nil {
		return EnqueuePayload{}, err
	}
	EnqueuedAt := time.Unix(0, int64(binary.BigEndian.Uint64(enqBytes))).UTC()

	//Priority
	prioBytes, _, err := readN(b, pos, 4)
	if err != nil {
		return EnqueuePayload{}, err
	}

	return EnqueuePayload{
		ID:             ID,
		IdempotencyKey: string(keyBytes),
		Payload:        Payload,
		EnqueuedAt:     EnqueuedAt,
		Priority:       int(binary.BigEndian.Uint32(prioBytes)),
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
	// ID
	idBytes, pos, err := readN(b, 0, 16)
	if err != nil {
		return DequeuePayload{}, err
	}
	ID, err := uuid.FromBytes(idBytes)
	if err != nil {
		return DequeuePayload{}, err
	}

	//Time
	leaseBytes, pos, err := readN(b, pos, 8)
	if err != nil {
		return DequeuePayload{}, err
	}
	LeaseUntil := time.Unix(0, int64(binary.BigEndian.Uint64(leaseBytes))).UTC()

	//Owner (4-byte length prefix, then that many bytes)
	ownerLenBytes, pos, err := readN(b, pos, 4)
	if err != nil {
		return DequeuePayload{}, err
	}
	ownerBytes, _, err := readN(b, pos, int(binary.BigEndian.Uint32(ownerLenBytes)))
	if err != nil {
		return DequeuePayload{}, err
	}

	return DequeuePayload{
		ID:         ID,
		LeaseUntil: LeaseUntil,
		Owner:      string(ownerBytes),
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
	idBytes, _, err := readN(b, 0, 16)
	if err != nil {
		return AckPayload{}, err
	}
	ID, err := uuid.FromBytes(idBytes)
	if err != nil {
		return AckPayload{}, err
	}
	return AckPayload{ID: ID}, nil
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
	idBytes, _, err := readN(b, 0, 16)
	if err != nil {
		return RetryPayload{}, err
	}
	ID, err := uuid.FromBytes(idBytes)
	if err != nil {
		return RetryPayload{}, err
	}
	return RetryPayload{ID: ID}, nil
}

type DeadPayload struct {
	ID       uuid.UUID
	Retries  int
	FailedAt time.Time
}

func encodeDeadPayload(p DeadPayload) ([]byte, error) {
	var buf bytes.Buffer
	var scratch [8]byte
	//ID
	buf.Write(p.ID[:])
	// Retries: fixed 4 bytes, matches how Priority is encoded above. Absolute
	// count so a replayed DLQ entry is self-describing.
	binary.BigEndian.PutUint32(scratch[:4], uint32(p.Retries))
	buf.Write(scratch[:4])
	// FailedAt: 8-byte unix nanos, same encoding as EnqueuedAt.
	binary.BigEndian.PutUint64(scratch[:8], uint64(p.FailedAt.UnixNano()))
	buf.Write(scratch[:8])
	return buf.Bytes(), nil
}

func decodeDeadPayload(b []byte) (DeadPayload, error) {
	idBytes, pos, err := readN(b, 0, 16)
	if err != nil {
		return DeadPayload{}, err
	}
	ID, err := uuid.FromBytes(idBytes)
	if err != nil {
		return DeadPayload{}, err
	}
	retBytes, pos, err := readN(b, pos, 4)
	if err != nil {
		return DeadPayload{}, err
	}
	failBytes, _, err := readN(b, pos, 8)
	if err != nil {
		return DeadPayload{}, err
	}

	return DeadPayload{
		ID:       ID,
		Retries:  int(binary.BigEndian.Uint32(retBytes)),
		FailedAt: time.Unix(0, int64(binary.BigEndian.Uint64(failBytes))).UTC(),
	}, nil
}

// RequeuePayload and PurgePayload are ID-only, same shape as RetryPayload: the
// task's full state already lives in the DLQ, so the record just names which one.
type RequeuePayload struct {
	ID uuid.UUID
}

func encodeRequeuePayload(p RequeuePayload) ([]byte, error) {
	var buf bytes.Buffer
	buf.Write(p.ID[:])
	return buf.Bytes(), nil
}

func decodeRequeuePayload(b []byte) (RequeuePayload, error) {
	idBytes, _, err := readN(b, 0, 16)
	if err != nil {
		return RequeuePayload{}, err
	}
	ID, err := uuid.FromBytes(idBytes)
	if err != nil {
		return RequeuePayload{}, err
	}
	return RequeuePayload{ID: ID}, nil
}

type PurgePayload struct {
	ID uuid.UUID
}

func encodePurgePayload(p PurgePayload) ([]byte, error) {
	var buf bytes.Buffer
	buf.Write(p.ID[:])
	return buf.Bytes(), nil
}

func decodePurgePayload(b []byte) (PurgePayload, error) {
	idBytes, _, err := readN(b, 0, 16)
	if err != nil {
		return PurgePayload{}, err
	}
	ID, err := uuid.FromBytes(idBytes)
	if err != nil {
		return PurgePayload{}, err
	}
	return PurgePayload{ID: ID}, nil
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
	case OperationRequeue:
		p, err := decodeRequeuePayload(fields)
		return op, p, err
	case OperationPurge:
		p, err := decodePurgePayload(fields)
		return op, p, err
	default:
		return op, nil, fmt.Errorf("decodeRecord: unknown op %d", op)
	}
}
