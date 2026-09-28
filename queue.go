package task_queue

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ready is our FIFO queue to deliver the next task via Dequeue()
// index is our lookup: an ID maps to a task for O(1) lookup
// wal is the durable log; every mutation is appended (and fsync'd) here
// before it becomes visible in memory.
type Queue struct {
	ready             []*Task
	index             map[uuid.UUID]*Task
	indexOfInflight   map[uuid.UUID]*Task
	wal               *WAL
	mu                sync.Mutex
	stop              chan struct{}  // Close() closes this to tell the sweeper to exit
	wg                sync.WaitGroup // waits for the sweeper goroutine to actually finish
	visibilityTimeout time.Duration  // how long a worker can hog a task for
	sweepInterval     time.Duration  // how often the sweeper wakes to check for expired leases
}

// NewQueue opens (or creates) the WAL at walPath and returns a Queue backed by
// it. The WAL is mandatory: without it the queue can't guarantee durability, so
// construction fails if the log can't be opened.
// visibilityTimeout is how long a dequeued task's lease lasts; sweepInterval is
// how often the background sweeper checks for expired leases. Tick more often
// than the timeout so an expired lease isn't left sitting for long.
func NewQueue(walPath string, visibilityTimeout, sweepInterval time.Duration) (*Queue, error) {
	w, err := OpenWAL(walPath)
	if err != nil {
		return nil, err
	}

	q := &Queue{
		index:             make(map[uuid.UUID]*Task),
		indexOfInflight:   map[uuid.UUID]*Task{},
		wal:               w,
		stop:              make(chan struct{}),
		visibilityTimeout: visibilityTimeout,
		sweepInterval:     sweepInterval,
	}

	// Read each record from the loop with Replay().
	// Each record should be creating a new task or altering
	// a newly created one in the loop(it's possible for multiple
	// records to be referencing the same task)
	// This means we need to create our ready slice & index map
	if err = w.Replay(q.applyRecord); err != nil {
		return nil, err
	}

	// Starts the background sweeper. Non-blocking, NewQueue continues to return q, nil instantly after runSweeper
	// integer 1 here represents total amount of routines we want to wait for
	q.wg.Add(1)
	go q.runSweeper(q.sweepInterval)

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
		// q.ready = q.ready[1:] // We cant just assume the dequeued task is at the front of the array due to the nature of log record writing. Currently fine because Dequeue isn't durable() but needs flagging
		q.indexOfInflight[p.ID] = q.index[p.ID] // add to inflight
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

// Sweeper that scans through the existing set of in-flight tasks, checks if the task.LeaseUntil < time.Now() and Retries() it on true
func (q *Queue) Sweeper() error {
	q.mu.Lock() // Blocking queue operations while we move back into our queue / update state
	defer q.mu.Unlock()

	for id, t := range q.indexOfInflight {
		if t.LeaseUntil.Before(time.Now()) {
			err := retryLocked(q, id)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// runSweeper is the background goroutine that drives Sweeper on a fixed
// interval. It loops until Close() closes q.stop, then returns so the goroutine
// exits (no leak). It holds no lock itself; each Sweeper pass takes the lock.
func (q *Queue) runSweeper(interval time.Duration) {
	// Whenever runSweeper returns, it decrements back from 1 -> 0 (our total count of routines)
	defer q.wg.Done()

	// Drops 1 value into the channel (ticket.C) every interval X amount of seconds
	ticker := time.NewTicker(interval)
	defer ticker.Stop() // turns off the ticker so we dont waste resources after sweeper is killed

	for {
		// We wait for one channel event to happen
		select {
		case <-ticker.C: // event was a tick
			_ = q.Sweeper()
		// We need to have a stopper for when we end the sweeper (or else we have a goroutine leak / zombie sweeper)
		case <-q.stop: // event was our stop being closed
			return
		}
	}
}

// Close actually stops our goroutines from processing, then waits for mid-process routines to finish. This is supposed to be invoked once we know we want to end the program / close queues, etc..
func (q *Queue) Close() {
	close(q.stop) // closes the channel stored in q.stop
	q.wg.Wait()
}

// Enqueue creates the task, stores it into the data structures
func (q *Queue) Enqueue(payload []byte, idemKey string) (uuid.UUID, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
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
	q.mu.Lock()
	defer q.mu.Unlock()
	// Ensuring non-empty array
	if len(q.ready) > 0 {
		t := q.ready[0]
		q.ready = q.ready[1:]

		// Updating task itself since map holds a pointer to it
		t.Status = StateInflight
		q.indexOfInflight[t.ID] = t

		t.LeaseUntil = time.Now().Add(q.visibilityTimeout)

		return t, true, nil
	}

	return nil, false, nil
}

// Consumer uses Ack() when successfully finishes the task
func Ack(q *Queue, id uuid.UUID) error {
	q.mu.Lock()
	defer q.mu.Unlock()
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
	delete(q.indexOfInflight, id) // No longer inflight
	delete(q.index, id)           // task is done; drop from lookup

	return nil
}

// Consumer uses Retry() when we are unsuccessful --> Allow retry.
// It owns the locking, then delegates the actual state transition to
// retryLocked so the lease sweeper (which already holds q.mu) can reuse the
// same logic without double-locking.
func Retry(q *Queue, id uuid.UUID) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return retryLocked(q, id)
}

// retryLocked performs the retry state transition: bump Retries, write-ahead
// the RETRY record, and put the task back on ready.
// PRECONDITION: the caller already holds q.mu. It does NOT lock, so both Retry
// (worker path) and the sweeper (already holding the lock) can call it without
// deadlocking on the non-reentrant mutex.
func retryLocked(q *Queue, id uuid.UUID) error {
	t, ok := q.index[id]
	if !ok {
		return fmt.Errorf("task %s not found", id)
	}

	fields, err := encodeRetryPayload(RetryPayload{ID: id})
	if err != nil {
		return err
	}

	_, err = q.wal.Append(encodeRecord(OperationRetry, fields))
	if err != nil {
		return err
	}

	t.Status = StateReady
	t.Retries += 1

	q.ready = append(q.ready, t)
	delete(q.indexOfInflight, id)
	return nil
}
