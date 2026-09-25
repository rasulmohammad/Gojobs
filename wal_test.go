package task_queue

/*
Tests to cover:
Frame layer (in-memory):
1. encodeFrame/decodeFrame round trip (incl. empty payload)
2. sequential frames through one reader + EOF at end
3. empty reader -> io.EOF
4. corrupt CRC -> error
5. truncated frame (short record, short header) -> error
File layer (temp dir):
6. OpenWAL new file -> offset 0
7. OpenWAL reopen -> offset seeded from file size
8. Append -> correct start offsets + w.offset bookkeeping
9. Append -> Replay round trip in FIFO order
10. Replay on empty WAL -> apply never called
*/

import (
	"bytes"
	"io"
	"path/filepath"
	"testing"
)

// encodeFrame then decodeFrame returns the identical payload, including the
// zero-length case.
func TestEncodeDecodeFrameRoundTrip(t *testing.T) {
	cases := map[string][]byte{
		"normal": []byte("hello world"),
		"empty":  {},
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			frame, err := encodeFrame(payload)
			if err != nil {
				t.Fatalf("encodeFrame returned error: %v", err)
			}
			got, err := decodeFrame(bytes.NewReader(frame))
			if err != nil {
				t.Fatalf("decodeFrame returned error: %v", err)
			}
			if !bytes.Equal(got, payload) {
				t.Errorf("round trip = %q, want %q", got, payload)
			}
		})
	}
}

// Two frames concatenated into one reader decode back in order, and a third
// decode hits io.EOF. Proves the reader cursor advances across frames.
func TestDecodeFrameSequential(t *testing.T) {
	f1, _ := encodeFrame([]byte("first"))
	f2, _ := encodeFrame([]byte("second"))

	r := bytes.NewReader(append(append([]byte{}, f1...), f2...))

	got1, err := decodeFrame(r)
	if err != nil {
		t.Fatalf("first decodeFrame error: %v", err)
	}
	if string(got1) != "first" {
		t.Errorf("first frame = %q, want %q", got1, "first")
	}

	got2, err := decodeFrame(r)
	if err != nil {
		t.Fatalf("second decodeFrame error: %v", err)
	}
	if string(got2) != "second" {
		t.Errorf("second frame = %q, want %q", got2, "second")
	}

	if _, err := decodeFrame(r); err != io.EOF {
		t.Errorf("third decodeFrame error = %v, want io.EOF", err)
	}
}

// decodeFrame on an empty reader returns io.EOF exactly (the signal Replay
// uses to stop cleanly), not a generic error.
func TestDecodeFrameEmptyReaderEOF(t *testing.T) {
	_, err := decodeFrame(bytes.NewReader(nil))
	if err != io.EOF {
		t.Errorf("decodeFrame on empty reader = %v, want io.EOF", err)
	}
}

// Flipping a byte in the payload makes the recomputed CRC disagree with the
// stored one, so decodeFrame reports corruption.
func TestDecodeFrameCorruptCRC(t *testing.T) {
	frame, _ := encodeFrame([]byte("payload"))
	// Byte 8 is the first payload byte (after the 8-byte header). Corrupt it.
	frame[8] ^= 0xFF

	if _, err := decodeFrame(bytes.NewReader(frame)); err == nil {
		t.Fatal("decodeFrame on corrupt frame returned nil error, want a CRC error")
	}
}

// A frame whose header promises more bytes than are present, or a header that
// is itself cut short, must error (and not be mistaken for a clean io.EOF).
func TestDecodeFrameTruncated(t *testing.T) {
	t.Run("short record", func(t *testing.T) {
		frame, _ := encodeFrame([]byte("full payload"))
		// Drop the last few payload bytes: header still claims the full length.
		truncated := frame[:len(frame)-3]

		_, err := decodeFrame(bytes.NewReader(truncated))
		if err == nil {
			t.Fatal("decodeFrame on truncated record returned nil error, want an error")
		}
		if err == io.EOF {
			t.Error("decodeFrame on truncated record returned io.EOF, want a non-EOF error")
		}
	})

	t.Run("short header", func(t *testing.T) {
		// Only 4 bytes: not even a full 8-byte header.
		_, err := decodeFrame(bytes.NewReader([]byte{0, 0, 0, 1}))
		if err == nil {
			t.Fatal("decodeFrame on short header returned nil error, want an error")
		}
		if err == io.EOF {
			t.Error("decodeFrame on short header returned io.EOF, want a non-EOF error")
		}
	})
}

// OpenWAL on a brand-new path creates the file and starts offset at 0.
func TestOpenWALNewFileOffsetZero(t *testing.T) {
	path := filepath.Join(t.TempDir(), "new.wal")

	w, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL returned error: %v", err)
	}
	if w.offset != 0 {
		t.Errorf("new WAL offset = %d, want 0", w.offset)
	}
	if _, err := w.file.Stat(); err != nil {
		t.Errorf("expected file to exist after OpenWAL, stat error: %v", err)
	}
}

// Reopening an existing WAL seeds offset from the file size (Stat().Size()),
// so appends continue at the true end rather than restarting at 0.
func TestOpenWALReopenOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reopen.wal")

	w1, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("first OpenWAL error: %v", err)
	}
	if _, err := w1.Append([]byte("aaaa")); err != nil {
		t.Fatalf("Append error: %v", err)
	}
	if _, err := w1.Append([]byte("bbbbbb")); err != nil {
		t.Fatalf("Append error: %v", err)
	}

	w2, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen OpenWAL error: %v", err)
	}
	if w2.offset != w1.offset {
		t.Errorf("reopened offset = %d, want %d (file size)", w2.offset, w1.offset)
	}
}

// Append returns the start offset of each record and advances w.offset by the
// full frame size (8-byte header + payload) each time.
func TestAppendOffsets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offsets.wal")
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL error: %v", err)
	}

	rec1 := []byte("hello")
	rec2 := []byte("world!!")

	off1, err := w.Append(rec1)
	if err != nil {
		t.Fatalf("first Append error: %v", err)
	}
	if off1 != 0 {
		t.Errorf("first append start offset = %d, want 0", off1)
	}

	off2, err := w.Append(rec2)
	if err != nil {
		t.Fatalf("second Append error: %v", err)
	}
	wantOff2 := int64(8 + len(rec1))
	if off2 != wantOff2 {
		t.Errorf("second append start offset = %d, want %d", off2, wantOff2)
	}

	wantEnd := int64(8+len(rec1)) + int64(8+len(rec2))
	if w.offset != wantEnd {
		t.Errorf("final w.offset = %d, want %d", w.offset, wantEnd)
	}
}

// The core durability test: append several records, then Replay (which opens a
// fresh read handle, simulating recovery) hands each record to apply in FIFO
// order.
func TestAppendReplayRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replay.wal")
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL error: %v", err)
	}

	records := [][]byte{
		[]byte("enqueue-1"),
		[]byte("dequeue-1"),
		[]byte("ack-1"),
	}
	for _, rec := range records {
		if _, err := w.Append(rec); err != nil {
			t.Fatalf("Append(%q) error: %v", rec, err)
		}
	}

	var replayed [][]byte
	err = w.Replay(func(record []byte) error {
		// Copy: decodeFrame's buffer is safe to keep, but be explicit.
		cp := append([]byte{}, record...)
		replayed = append(replayed, cp)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay error: %v", err)
	}

	if len(replayed) != len(records) {
		t.Fatalf("replayed %d records, want %d", len(replayed), len(records))
	}
	for i := range records {
		if !bytes.Equal(replayed[i], records[i]) {
			t.Errorf("replayed[%d] = %q, want %q (order matters)", i, replayed[i], records[i])
		}
	}
}

// Replay on a freshly opened, empty WAL never calls apply and returns nil.
func TestReplayEmptyWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.wal")
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL error: %v", err)
	}

	called := false
	err = w.Replay(func(record []byte) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("Replay on empty WAL error: %v", err)
	}
	if called {
		t.Error("apply was called on an empty WAL, want it never called")
	}
}
