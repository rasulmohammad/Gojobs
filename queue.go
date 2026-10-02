package task_queue

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	// Backoff constraints
	baseDelay = 1 * time.Second
	maxDelay  = 30 * time.Second
)

// ready is our FIFO queue to deliver the next task via Dequeue()
// index is our lookup: an ID maps to a task for O(1) lookup
// wal is the durable log; every mutation is appended (and fsync'd) here
// before it becomes visible in memory.
type Queue struct {
	ready             []*Task
	index             map[uuid.UUID]*Task
	indexOfInflight   map[uuid.UUID]*Task
	indexOfIdemKeys   map[string]*uuid.UUID // Deduplicating task entries into queue
	wal               *WAL
	mu                sync.Mutex
	stop              chan struct{}       // Close() closes this to tell the sweeper to exit
	wg                sync.WaitGroup      // waits for the sweeper goroutine to actually finish
	visibilityTimeout time.Duration       // how long a worker can hog a task for
	sweepInterval     time.Duration       // how often the sweeper wakes to check for expired leases
	maxRetries        int                 // Total amount of retries we allow a task before we move into DLQ
	dlq               map[uuid.UUID]*Task // Contains all tasks that need manual inspection
}

// NewQueue opens (or creates) the WAL at walPath and returns a Queue backed by
// it. The WAL is mandatory: without it the queue can't guarantee durability, so
// construction fails if the log can't be opened.
// visibilityTimeout is how long a dequeued task's lease lasts; sweepInterval is
// how often the background sweeper checks for expired leases. Tick more often
// than the timeout so an expired lease isn't left sitting for long.
func NewQueue(walPath string, visibilityTimeout, sweepInterval time.Duration, maxRetries int) (*Queue, error) {
	w, err := OpenWAL(walPath)
	if err != nil {
		return nil, err
	}

	q := &Queue{
		index:             make(map[uuid.UUID]*Task),
		indexOfInflight:   make(map[uuid.UUID]*Task),
		indexOfIdemKeys:   make(map[string]*uuid.UUID),
		wal:               w,
		stop:              make(chan struct{}),
		visibilityTimeout: visibilityTimeout,
		sweepInterval:     sweepInterval,
		maxRetries:        maxRetries,
		dlq:               make(map[uuid.UUID]*Task),
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
		_, ok := q.indexOfIdemKeys[t.IdempotencyKey]
		if t.IdempotencyKey != "" && ok {
			break
		}
		q.ready = append(q.ready, t)
		q.index[t.ID] = t
		q.indexOfIdemKeys[t.IdempotencyKey] = &t.ID
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
		t.Status = StateReady
		delete(q.indexOfInflight, t.ID)
		// Intentionally allowing a retried task to be available immediately -- no harm in letting it be retried 1x. Only during WAL replay cases
		t.AvailableAt = time.Now()
	case OperationDead:
		p := payload.(DeadPayload)
		t := q.index[p.ID]
		for i, t2 := range q.ready {
			if t.ID == t2.ID {
				// Removing dead task from ready queue (because we dont log dequeue)
				q.ready = append(q.ready[:i], q.ready[i+1:]...)
				break
			}
		}
		t.Status = StateDead
		// Absolute count from the record, not a delta, so it's robust to changes
		// in retry accounting.
		t.Retries = p.Retries
		t.FailedAt = p.FailedAt
		q.dlq[t.ID] = t
		delete(q.indexOfInflight, t.ID)
		delete(q.index, t.ID)
	case OperationRequeue:
		p := payload.(RequeuePayload)
		// A dead task lives in dlq (not index) at replay time; move it back.
		t := q.dlq[p.ID]
		t.Status = StateReady
		t.Retries = 0
		t.AvailableAt = time.Time{}
		t.FailedAt = time.Time{}
		delete(q.dlq, p.ID)
		q.index[p.ID] = t
		q.ready = append(q.ready, t)
	case OperationPurge:
		p := payload.(PurgePayload)
		delete(q.dlq, p.ID)
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

	// Deduplicate task enqueue
	_, ok := q.indexOfIdemKeys[idemKey]
	if idemKey != "" && ok {
		return *q.indexOfIdemKeys[idemKey], nil
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
	q.indexOfIdemKeys[idemKey] = &t.ID

	return t.ID, nil
}

// Dequeue pops the task from the front of the ready slice & updates status. True = Successful pop
func (q *Queue) Dequeue() (*Task, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	// Ensuring non-empty array
	now := time.Now()
	for i, t := range q.ready {
		// Only process tasks that are ready
		if t.AvailableAt.After(now) {
			continue
		}

		// We have a ready task
		q.ready = append(q.ready[:i], q.ready[i+1:]...)

		// Updating task itself since map holds a pointer to it
		t.Status = StateInflight
		t.LeaseUntil = now.Add(q.visibilityTimeout)
		q.indexOfInflight[t.ID] = t
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

	taskWillDieAfterRetry := t.Retries+1 > q.maxRetries

	if taskWillDieAfterRetry { // WAL should have a Dead() payload written to it so it knows --> DLQ
		// Move into DLQ & remove from retry and return. First make durable though
		// t.Retries is still pre-increment here; the tail below bumps it to this
		// same value, so store t.Retries+1 as the final count.
		// Capture one timestamp so the persisted record and the in-memory task agree.
		failedAt := time.Now()
		fields, err := encodeDeadPayload(DeadPayload{ID: id, Retries: t.Retries + 1, FailedAt: failedAt})
		if err != nil {
			return err
		}

		_, err = q.wal.Append(encodeRecord(OperationDead, fields))
		if err != nil {
			return err
		}

		t.Status = StateDead
		t.FailedAt = failedAt
		q.dlq[id] = t
		delete(q.index, id)

	} else { // WAL should have Retry() payload written to it so it knows --> task back in Ready[]
		fields, err := encodeRetryPayload(RetryPayload{ID: id})
		if err != nil {
			return err
		}

		_, err = q.wal.Append(encodeRecord(OperationRetry, fields))
		if err != nil {
			return err
		}

		t.Status = StateReady
		t.AvailableAt = time.Now().Add(generateBackoff(t.Retries))
		q.ready = append(q.ready, t)

	}
	t.Retries += 1
	delete(q.indexOfInflight, id)
	return nil
}

func generateBackoff(retryAttempt int) time.Duration {
	// exponential ceiling in nanoseconds: base * 2^attempt
	exp := float64(baseDelay) * math.Pow(2, float64(retryAttempt))
	if exp > float64(maxDelay) { // cap (also catches +Inf for huge attempts)
		exp = float64(maxDelay)
	}
	// full jitter: uniform in [0, exp)
	return time.Duration(rand.Float64() * exp)
}

/*
	Accessor functions for our DLQ split by Accessor functions and then mutating functions
	Accessor functions:
*/

// ListDeadTasks returns copies of every task in the DLQ, most recent failure
// first. Copies (not the stored *Task) so callers can't mutate live state
// outside the lock. Cannot fail, so no error return.
func (q *Queue) ListDeadTasks() []Task {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Copy the structs out while holding the lock, then sort the copies.
	deadTasks := make([]Task, 0, len(q.dlq))
	for _, t := range q.dlq {
		deadTasks = append(deadTasks, *t)
	}

	// Most recent failure first.
	sort.Slice(deadTasks, func(i, j int) bool {
		return deadTasks[i].FailedAt.After(deadTasks[j].FailedAt)
	})

	return deadTasks
}

// DeadTask returns a copy of one dead task, or an error if it isn't in the DLQ.
func (q *Queue) DeadTask(id uuid.UUID) (Task, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	t, ok := q.dlq[id]
	if !ok {
		return Task{}, fmt.Errorf("task %s not found in DLQ", id)
	}
	return *t, nil
}

// DLQLen returns the number of tasks currently in the DLQ.
func (q *Queue) DLQLen() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.dlq)
}

/*
Mutating functions
*/

// RequeueDead revives a dead task back onto the ready queue, preserving its
// identity (same ID/payload) but resetting its retry history so it gets a fresh
// set of attempts. WAL-first: the REQUEUE record is durable before any in-memory
// change, so recovery replays the revival.
func (q *Queue) RequeueDead(id uuid.UUID) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	t, ok := q.dlq[id]
	if !ok {
		return fmt.Errorf("task %s not found in DLQ", id)
	}

	fields, err := encodeRequeuePayload(RequeuePayload{ID: id})
	if err != nil {
		return err
	}
	if _, err := q.wal.Append(encodeRecord(OperationRequeue, fields)); err != nil {
		return err
	}

	// Durable now: fresh start on ready.
	t.Status = StateReady
	t.Retries = 0
	t.AvailableAt = time.Time{}
	t.FailedAt = time.Time{}
	delete(q.dlq, id)
	q.index[id] = t
	q.ready = append(q.ready, t)
	return nil
}

// PurgeDeadTask permanently removes a dead task from the DLQ so the map doesn't
// grow unbounded. WAL-first so the removal survives recovery.
func (q *Queue) PurgeDeadTask(id uuid.UUID) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.dlq[id]; !ok {
		return fmt.Errorf("task %s not found in DLQ", id)
	}

	fields, err := encodePurgePayload(PurgePayload{ID: id})
	if err != nil {
		return err
	}
	if _, err := q.wal.Append(encodeRecord(OperationPurge, fields)); err != nil {
		return err
	}

	delete(q.dlq, id)
	return nil
}
