package task_queue
import (
	"github.com/google/uuid"
	"time"
)

type TaskState uint8

const (
	StateUnknown TaskState = iota
	StateReady
	StateInflight
	StateDone
	StateDead
)

func (s TaskState) String() string {
	switch s {
	case StateReady:
		return "ready"
	case StateInflight:
		return "inflight"
	case StateDone:
		return "done"
	case StateDead:
		return "dead"
	default:
		return "unknown"
	}

}


// Defining task struct inside here
type Task struct {
	ID uuid.UUID  // UUID identifier for the job
	IdempotencyKey string
	Payload []byte
	Status TaskState 
	Retries int
	Priority int // Consider how we can define this (this is for priority queues since some jobs are more important than others)
	
	// Know when the job was enqueued & leaseUntil acts like visibility timeout from AWS SQS (a lock)
	EnqueuedAt time.Time
	LeaseUntil time.Time
	Owner string  
}