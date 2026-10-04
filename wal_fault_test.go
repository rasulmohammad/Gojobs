package task_queue

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
)

var errSimulatedCrash = errors.New("simulated crash mid-append")

// failAfterWriter forwards writes to the real file but "crashes" after a byte budget.
type failAfterWriter struct {
	under   syncWriter //holds the real file
	budget  int
	written int
}

func (f *failAfterWriter) Write(p []byte) (int, error) {
	remaining := f.budget - f.written
	if len(p) <= remaining {
		f.written += len(p)
		return f.under.Write(p)
	}
	n, _ := f.under.Write(p[:remaining]) // p[:remaining]: write only the bytes that fit before the crash
	f.written += n
	return n, errSimulatedCrash
}

func (f *failAfterWriter) Sync() error {
	return f.under.Sync()
}

func TestAppendCrashIsAllOrNothing(t *testing.T) {
	record := []byte("enqueue-task-1")
	frame, err := encodeFrame(record)
	if err != nil {
		t.Fatalf("encodeFrame: %v", err)
	}

	for n := 0; n <= len(frame); n++ {
		t.Run(fmt.Sprintf("crash-after-%d", n), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "q.wal")
			w, err := OpenWAL(path)
			if err != nil {
				t.Fatalf("OpenWAL: %v", err)
			}
			w.w = &failAfterWriter{under: w.w, budget: n} // swap the real writer for one that dies after n bytes

			_, appendErr := w.Append(record)
			full := n == len(frame)

			// Invariant A: a partial write fails closed (error returned, offset untouched).
			if !full {
				if appendErr == nil {
					t.Fatalf("Append succeeded despite crash after %d/%d bytes", n, len(frame))
				}
				if w.offset != 0 {
					t.Errorf("offset = %d after failed append, want 0", w.offset)
				}
			} else if appendErr != nil {
				t.Fatalf("Append failed on a full write: %v", appendErr)
			}

			// Invariant B: replay recovers the whole record or nothing, never a fragment.
			want := 0
			if full {
				want = 1
			}
			if got := replayAll(t, path); len(got) != want {
				t.Fatalf("recovered %d records after crash at %d/%d bytes, want %d", len(got), n, len(frame), want)
			}
		})
	}
}
