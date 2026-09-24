package task_queue

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ready is our FIFO queue to deliver the next task via Dequeue() 
// index is our lookup: an ID maps to a task for O(1) lookup
type Queue struct {
	ready []*Task 
	index map[uuid.UUID]*Task
}

// Initializing our map
func NewQueue() *Queue {
	return &Queue{
		index: make(map[uuid.UUID]*Task),
	}
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

	q.ready = append(q.ready, t)
	q.index[t.ID] = t 

	return t.ID, nil
}

// Dequeue pops the task from the front of the ready slice & updates status
func (q *Queue) Dequeue() (*Task, bool, error) {
	// Ensuring non-empty array
	if len(q.ready) > 0{
		t := q.ready[0]
		q.ready = q.ready[1:]

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

	t.Status = StateDone

	return nil
}

// Consumer uses nack() when we are unsuccessful --> Allow retry
func Nack(q *Queue, id uuid.UUID) error {
	t, ok := q.index[id]
	if !ok {
		return fmt.Errorf("task %s not found", id)
	}

	t.Retries += 1
	t.Status = StateReady

	q.ready = append(q.ready, t)
	return nil
}