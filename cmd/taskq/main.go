// Command taskq is a small demo that drives the durable task queue end to end:
// it enqueues a few tasks, then dequeues, "processes", and acks each one until
// the queue is empty. Run it with `go run ./cmd/taskq` from the repo root.
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"task_queue/internal/queue"
)

func main() {
	// The WAL lives under data/ (gitignored). Create the dir so a fresh clone
	// can run the demo without any setup.
	if err := os.MkdirAll("data", 0o755); err != nil {
		log.Fatalf("mkdir data: %v", err)
	}
	path := filepath.Join("data", "queue.wal")

	// visibilityTimeout 30s, sweep every 5s, up to 3 retries before the DLQ.
	q, err := queue.NewQueue(path, 30*time.Second, 5*time.Second, 3)
	if err != nil {
		log.Fatalf("NewQueue: %v", err)
	}
	defer q.Close()

	// Produce a few tasks. The second arg is the idempotency key; reusing a key
	// makes a repeat enqueue a no-op, which is why we key each task uniquely here.
	keys := []string{"email-1", "email-2", "email-3"}
	for _, k := range keys {
		id, err := q.Enqueue([]byte("payload for "+k), k)
		if err != nil {
			log.Fatalf("Enqueue(%s): %v", k, err)
		}
		fmt.Printf("enqueued %s -> %s\n", k, id)
	}

	// Consume: pull ready tasks and ack each one until nothing is left.
	fmt.Println("--- draining ---")
	for {
		t, ok, err := q.Dequeue()
		if err != nil {
			log.Fatalf("Dequeue: %v", err)
		}
		if !ok {
			break // queue empty
		}

		fmt.Printf("processing %s (%s)\n", t.ID, string(t.Payload))
		if err := queue.Ack(q, t.ID); err != nil {
			log.Fatalf("Ack(%s): %v", t.ID, err)
		}
	}

	fmt.Printf("done. dead-letter tasks: %d\n", q.DLQLen())
}
