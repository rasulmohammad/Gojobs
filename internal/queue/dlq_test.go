package queue

import (
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
)

// newQueueMax is like newTestQueue but lets a test pick maxRetries. maxRetries=0
// means the first Retry kills the task, which keeps death/DLQ tests free of the
// multi-retry + backoff dance. Huge lease/sweep durations keep the background
// sweeper inert.
func newQueueMax(t *testing.T, maxRetries int) (*Queue, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "q.wal")
	q, err := NewQueue(path, time.Hour, time.Hour, maxRetries)
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	t.Cleanup(func() { q.Close() })
	return q, path
}

// killTask enqueues, dequeues, then retries a task once. On a maxRetries=0 queue
// that single Retry sends it to the DLQ. Returns the task ID.
func killTask(t *testing.T, q *Queue, payload string) uuid.UUID {
	t.Helper()
	id, err := q.Enqueue([]byte(payload), "k-"+payload)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, ok, _ := q.Dequeue(); !ok {
		t.Fatal("Dequeue: ok=false")
	}
	if err := Retry(q, id); err != nil {
		t.Fatalf("Retry: %v", err)
	}
	return id
}

// --- death / recovery core ---

// A task that exceeds maxRetries lands in the DLQ, dead, and is gone from every
// live collection.
func TestRetryToDeath(t *testing.T) {
	q, _ := newQueueMax(t, 0)
	id := killTask(t, q, "a")

	q.mu.Lock()
	defer q.mu.Unlock()

	dt, ok := q.dlq[id]
	if !ok {
		t.Fatal("task not in dlq after death")
	}
	if dt.Status != StateDead {
		t.Errorf("Status = %s, want dead", dt.Status)
	}
	if dt.Retries != 1 {
		t.Errorf("Retries = %d, want 1", dt.Retries)
	}
	if dt.FailedAt.IsZero() {
		t.Error("FailedAt is zero, want set")
	}
	if _, in := q.index[id]; in {
		t.Error("dead task still in index")
	}
	if _, in := q.indexOfInflight[id]; in {
		t.Error("dead task still in indexOfInflight")
	}
	for _, rt := range q.ready {
		if rt.ID == id {
			t.Error("dead task still in ready")
		}
	}
}

// A dead task survives a restart: the DEAD record round-trips, including Retries
// and FailedAt, and it does not reappear on ready.
func TestRecoverDead(t *testing.T) {
	q, path := newQueueMax(t, 0)
	id := killTask(t, q, "a")

	q.mu.Lock()
	wantRetries := q.dlq[id].Retries
	wantFailedAt := q.dlq[id].FailedAt
	q.mu.Unlock()

	q2 := reopenQueue(t, path)
	q2.mu.Lock()
	defer q2.mu.Unlock()

	dt, ok := q2.dlq[id]
	if !ok {
		t.Fatal("dead task missing from dlq after recovery")
	}
	if dt.Retries != wantRetries {
		t.Errorf("recovered Retries = %d, want %d", dt.Retries, wantRetries)
	}
	if !dt.FailedAt.Equal(wantFailedAt) {
		t.Errorf("recovered FailedAt = %v, want %v", dt.FailedAt, wantFailedAt)
	}
	for _, rt := range q2.ready {
		if rt.ID == id {
			t.Error("dead task reappeared in ready after recovery")
		}
	}
}

// With maxRetries=2 the task survives two retries and dies on the third failure.
// Confirms the strict-greater boundary.
func TestRetriesBeforeDeath(t *testing.T) {
	q, _ := newQueueMax(t, 2)
	id, _ := q.Enqueue([]byte("a"), "k")

	for i := 1; i <= 3; i++ {
		if _, ok, _ := q.Dequeue(); !ok {
			t.Fatalf("Dequeue before retry #%d: ok=false", i)
		}
		if err := Retry(q, id); err != nil {
			t.Fatalf("Retry #%d: %v", i, err)
		}
		// Clear backoff so the next Dequeue can re-lease it (no-op once dead).
		q.mu.Lock()
		if rt, ok := q.index[id]; ok {
			rt.AvailableAt = time.Time{}
		}
		q.mu.Unlock()
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.dlq[id]; !ok {
		t.Fatal("task not dead after exceeding maxRetries")
	}
	if _, in := q.index[id]; in {
		t.Error("dead task still in index")
	}
}

// --- read accessors ---

func TestListDeadTasksSorted(t *testing.T) {
	q, _ := newQueueMax(t, 0)
	first := killTask(t, q, "first")
	time.Sleep(2 * time.Millisecond) // ensure distinct FailedAt
	second := killTask(t, q, "second")

	list := q.ListDeadTasks()
	if len(list) != 2 {
		t.Fatalf("ListDeadTasks len = %d, want 2", len(list))
	}
	if list[0].ID != second {
		t.Errorf("list[0] = %s, want most-recent %s", list[0].ID, second)
	}
	if list[1].ID != first {
		t.Errorf("list[1] = %s, want oldest %s", list[1].ID, first)
	}

	// Copies are isolated: mutating a returned Task must not touch stored state.
	list[0].Retries = 9999
	for _, dt := range q.ListDeadTasks() {
		if dt.ID == second && dt.Retries == 9999 {
			t.Error("mutating a returned Task changed the stored DLQ task; not a copy")
		}
	}
}

func TestDeadTask(t *testing.T) {
	q, _ := newQueueMax(t, 0)
	id := killTask(t, q, "a")

	got, err := q.DeadTask(id)
	if err != nil {
		t.Fatalf("DeadTask: %v", err)
	}
	if got.ID != id {
		t.Errorf("DeadTask ID = %s, want %s", got.ID, id)
	}

	if _, err := q.DeadTask(uuid.New()); err == nil {
		t.Error("DeadTask on unknown id returned nil error, want error")
	}
}

func TestDLQLen(t *testing.T) {
	q, _ := newQueueMax(t, 0)
	if n := q.DLQLen(); n != 0 {
		t.Errorf("initial DLQLen = %d, want 0", n)
	}
	killTask(t, q, "a")
	killTask(t, q, "b")
	if n := q.DLQLen(); n != 2 {
		t.Errorf("DLQLen = %d, want 2", n)
	}
}

// --- mutators ---

func TestRequeueDead(t *testing.T) {
	q, path := newQueueMax(t, 0)
	id := killTask(t, q, "a")

	if err := q.RequeueDead(id); err != nil {
		t.Fatalf("RequeueDead: %v", err)
	}

	q.mu.Lock()
	if _, in := q.dlq[id]; in {
		t.Error("task still in dlq after requeue")
	}
	rt, in := q.index[id]
	if !in {
		t.Fatal("requeued task missing from index")
	}
	if rt.Status != StateReady {
		t.Errorf("Status = %s, want ready", rt.Status)
	}
	if rt.Retries != 0 {
		t.Errorf("Retries = %d, want 0 (reset)", rt.Retries)
	}
	if !rt.AvailableAt.IsZero() {
		t.Errorf("AvailableAt = %v, want zero", rt.AvailableAt)
	}
	if !rt.FailedAt.IsZero() {
		t.Errorf("FailedAt = %v, want zero", rt.FailedAt)
	}
	foundInReady := false
	for _, x := range q.ready {
		if x.ID == id {
			foundInReady = true
		}
	}
	q.mu.Unlock()
	if !foundInReady {
		t.Error("requeued task not in ready")
	}

	// Revival survives recovery as a live (not dead) task.
	q2 := reopenQueue(t, path)
	q2.mu.Lock()
	defer q2.mu.Unlock()
	if _, in := q2.dlq[id]; in {
		t.Error("requeued task back in dlq after recovery")
	}
	if _, in := q2.index[id]; !in {
		t.Error("requeued task missing from index after recovery")
	}
}

func TestRequeueDeadMissing(t *testing.T) {
	q, _ := newQueueMax(t, 0)
	if err := q.RequeueDead(uuid.New()); err == nil {
		t.Error("RequeueDead on unknown id returned nil error, want error")
	}
}

func TestPurgeDeadTask(t *testing.T) {
	q, path := newQueueMax(t, 0)
	id := killTask(t, q, "a")

	if err := q.PurgeDeadTask(id); err != nil {
		t.Fatalf("PurgeDeadTask: %v", err)
	}
	if n := q.DLQLen(); n != 0 {
		t.Errorf("DLQLen after purge = %d, want 0", n)
	}

	q2 := reopenQueue(t, path)
	q2.mu.Lock()
	defer q2.mu.Unlock()
	if _, in := q2.dlq[id]; in {
		t.Error("purged task reappeared in dlq after recovery")
	}
}

func TestPurgeDeadTaskMissing(t *testing.T) {
	q, _ := newQueueMax(t, 0)
	if err := q.PurgeDeadTask(uuid.New()); err == nil {
		t.Error("PurgeDeadTask on unknown id returned nil error, want error")
	}
}

// --- backoff ---

func TestGenerateBackoffBounds(t *testing.T) {
	for attempt := 0; attempt <= 5; attempt++ {
		exp := float64(baseDelay) * math.Pow(2, float64(attempt))
		if exp > float64(maxDelay) {
			exp = float64(maxDelay)
		}
		upper := time.Duration(exp)
		for i := 0; i < 100; i++ {
			d := generateBackoff(attempt)
			if d < 0 || d > upper {
				t.Fatalf("generateBackoff(%d) = %v, want in [0, %v]", attempt, d, upper)
			}
		}
	}
}

func TestBackoffBlocksDequeue(t *testing.T) {
	q, _ := newQueueMax(t, 1000) // high so the retry doesn't kill the task
	id, _ := q.Enqueue([]byte("a"), "k")
	if _, ok, _ := q.Dequeue(); !ok {
		t.Fatal("Dequeue: ok=false")
	}
	if err := Retry(q, id); err != nil {
		t.Fatalf("Retry: %v", err)
	}

	if _, ok, _ := q.Dequeue(); ok {
		t.Fatal("task dequeueable immediately after retry, want backing off")
	}

	q.mu.Lock()
	q.index[id].AvailableAt = time.Time{}
	q.mu.Unlock()

	if _, ok, _ := q.Dequeue(); !ok {
		t.Fatal("task not dequeueable after clearing backoff")
	}
}
