package queue

// readyHeap is a binary min-heap of ready/waiting tasks, ordered so the root is
// always the task that becomes available soonest. Dequeue peeks the root: if the
// root is not yet due, nothing is, so the old O(n) scan over backed-off tasks
// collapses to O(1); popping the due task is O(log n).
//
// Ordering key: AvailableAt ascending (earliest-available first), then
// EnqueuedAt ascending as a stable FIFO tie-break among tasks that share an
// availability time (fresh enqueues all carry the zero AvailableAt, so they fall
// back to enqueue order). It implements container/heap.Interface; callers drive
// it with heap.Push / heap.Pop / heap.Init, never the raw methods.
type readyHeap []*Task

func (h readyHeap) Len() int { return len(h) }

func (h readyHeap) Less(i, j int) bool {
	if !h[i].AvailableAt.Equal(h[j].AvailableAt) {
		return h[i].AvailableAt.Before(h[j].AvailableAt)
	}
	return h[i].EnqueuedAt.Before(h[j].EnqueuedAt)
}

func (h readyHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *readyHeap) Push(x any) { *h = append(*h, x.(*Task)) }

func (h *readyHeap) Pop() any {
	old := *h
	n := len(old)
	t := old[n-1]
	old[n-1] = nil // let the popped task be GC'd once it leaves ready
	*h = old[:n-1]
	return t
}
