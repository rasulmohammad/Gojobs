package task_queue

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ready is our FIFO queue to deliver the next task via Dequeue()
// index is our lookup: an ID maps to a task for O(1) lookup
// wal is the durable log; every mutation is appended (and fsync'd) here
// before it becomes visible in memory.
type Queue struct {
	ready []*Task
	index map[uuid.UUID]*Task
	wal   *WAL
}

// NewQueue opens (or creates) the WAL at walPath and returns a Queue backed by
// it. The WAL is mandatory: without it the queue can't guarantee durability, so
// construction fails if the log can't be opened.
func NewQueue(walPath string) (*Queue, error) {
	w, err := OpenWAL(walPath)
	if err != nil {
		return nil, err
	}

	q := &Queue{
		index: make(map[uuid.UUID]*Task),
		wal:   w,
	}

	// Read each record from the loop with Replay().
	// Each record should be creating a new task or altering
	// a newly created one in the loop(it's possible for multiple
	// records to be referencing the same task)
	// This means we need to create our ready slice & index map
	if err = w.Replay(q.applyRecord); err != nil {
		return nil, err
	}

	return q, nil
}

// Receives the record bytes (frames payload) which follows a
// [operation tag: 1 byte][op-specific: non-fixed size] format according
// to how we encode records (encodeRecord() in record.go)
func (q *Queue) applyRecord(record []byte) error {
	op, payload, err := decodeRecord(record)
	if err != nil {
		return err
	}

	switch op {
	case OperationEnqueue:
		p := payload.(EnqueuePayload)
		t := &Task{
			ID:             p.ID,
			IdempotencyKey: p.IdempotencyKey,
			Payload:        p.Payload,
			Status:         StateReady,
			EnqueuedAt:     p.EnqueuedAt,
			Priority:       p.Priority,
		}
		q.ready = append(q.ready, t)
		q.index[t.ID] = t
	case OperationDequeue:
		p := payload.(DequeuePayload)
		q.index[p.ID].Status = StateInflight
	case OperationAck:
		p := payload.(AckPayload)
		delete(q.index, p.ID)
		// Dequeue isn't logged, so replay never removed this task from ready.
		// Drop it here so an acked task isn't redelivered after recovery.
		for i, t := range q.ready {
			if t.ID == p.ID {
				// Removing acked task from ready queue (... operator since append expects individual elements)
				q.ready = append(q.ready[:i], q.ready[i+1:]...)
				break
			}
		}
	case OperationRetry:
		p := payload.(RetryPayload)
		t := q.index[p.ID]
		t.Retries += 1
		q.index[p.ID].Status = StateReady
	case OperationDead:
		p := payload.(DeadPayload)
		q.index[p.ID].Status = StateDead
	default:
		return err
	}

	return nil
}

// Enqueue creates the task, stores it into the data structures
func (q *Queue) Enqueue(payload []byte, idemKey string) (uuid.UUID, error) {

	t := &Task{
		ID:             uuid.New(),
		IdempotencyKey: idemKey,
		Payload:        payload,
		Status:         StateReady,
		EnqueuedAt:     time.Now(),
	}

	// Write-ahead: durably record the enqueue before it exists in memory.
	// encode the op-specific fields, prepend the op tag, append+fsync.
	fields, err := encodeEnqueuePayload(EnqueuePayload{
		ID:             t.ID,
		IdempotencyKey: t.IdempotencyKey,
		Payload:        t.Payload,
		EnqueuedAt:     t.EnqueuedAt,
		Priority:       t.Priority,
	})
	if err != nil {
		return uuid.Nil, err
	}
	if _, err := q.wal.Append(encodeRecord(OperationEnqueue, fields)); err != nil {
		// Fail closed: the record isn't durable, so the task must not become
		// visible. Return the error and touch nothing in memory.
		return uuid.Nil, err
	}

	// The fsync succeeded, so the task is now durable. Only now do we make it
	// visible in memory; if we crashed right here, replay would rebuild it.
	q.ready = append(q.ready, t)
	q.index[t.ID] = t

	return t.ID, nil
}

// Dequeue pops the task from the front of the ready slice & updates status. True = Successful pop
func (q *Queue) Dequeue() (*Task, bool, error) {
	// Ensuring non-empty array
	if len(q.ready) > 0 {
		t := q.ready[0]
		q.ready = q.ready[1:]

		// For now, we're not making dequeue durable
		// if a process fails while we're in-flight, we'll just replay
		// the task as ready
		// fields, err := encodeDequeuePayload(DequeuePayload{ID: t.ID, LeaseUntil: t.LeaseUntil, Owner: t.Owner})
		// if err != nil {
		// 	return nil, false, err
		// }

		// _, err = q.wal.Append(fields)
		// if err != nil {
		// 	return nil, false, err
		// }

		// Updating task itself since map holds a pointer to it
		t.Status = StateInflight

		return t, true, nil
	}

	return nil, false, nil
}

// Consumer uses Ack() when successfully finishes the task
func Ack(q *Queue, id uuid.UUID) error {

	t, ok := q.index[id]
	if !ok {
		return fmt.Errorf("task %s not found", id)
	}

	fields, err := encodeAckPayload(AckPayload{ID: id})
	if err != nil {
		return err
	}
	record := encodeRecord(OperationAck, fields)
	_, err = q.wal.Append(record)
	if err != nil {
		return err
	}
	t.Status = StateDone
	delete(q.index, id) // task is done; drop from lookup

	return nil
}

// Consumer uses Retry() when we are unsuccessful --> Allow retry
func Retry(q *Queue, id uuid.UUID) error {
	t, ok := q.index[id]
	if !ok {
		return fmt.Errorf("task %s not found", id)
	}

	t.Retries += 1

	fields, err := encodeRetryPayload(RetryPayload{ID: id})
	if err != nil {
		return err
	}

	_, err = q.wal.Append(encodeRecord(OperationRetry, fields))
	if err != nil {
		return err
	}

	t.Status = StateReady

	q.ready = append(q.ready, t)
	return nil
}
