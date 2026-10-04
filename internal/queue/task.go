package queue

import (
	"fmt"
	"time"

	"github.com/google/uuid"
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
	ID             uuid.UUID // UUID identifier for the job
	IdempotencyKey string
	Payload        []byte
	Status         TaskState
	Retries        int
	AvailableAt    time.Time // When we can dequeue the task again (after retries)
	FailedAt       time.Time
	Priority       int // Consider how we can define this (this is for priority queues since some jobs are more important than others)

	// Know when the job was enqueued & leaseUntil acts like visibility timeout from AWS SQS (a lock)
	EnqueuedAt time.Time
	LeaseUntil time.Time
	Owner      string
}

// String renders a Task as a readable multi-line block.
// Because it has this method, *Task and Task both satisfy fmt.Stringer,
// so fmt.Println(t) / fmt.Printf("%v", t) call it automatically.
func (t Task) String() string {
	owner := t.Owner
	if owner == "" {
		owner = "-"
	}
	return fmt.Sprintf(
		"Task %s\n"+
			"  Status:       %s\n"+
			"  Retries:      %d\n"+
			"  Priority:     %d\n"+
			"  Payload:      %d bytes\n"+
			"  IdempKey:     %s\n"+
			"  Owner:        %s\n"+
			"  EnqueuedAt:   %s\n"+
			"  AvailableAt:  %s\n"+
			"  LeaseUntil:   %s\n"+
			"  FailedAt:     %s",
		t.ID,
		t.Status, // reuses TaskState.String()
		t.Retries,
		t.Priority,
		len(t.Payload),
		t.IdempotencyKey,
		owner,
		fmtTime(t.EnqueuedAt),
		fmtTime(t.AvailableAt),
		fmtTime(t.LeaseUntil),
		fmtTime(t.FailedAt),
	)
}

// fmtTime prints "-" for the zero time so unset fields don't show as 0001-01-01.
func fmtTime(ts time.Time) string {
	if ts.IsZero() {
		return "-"
	}
	return ts.Format(time.RFC3339)
}
