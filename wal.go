package task_queue

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// This is the log manager. Owns file handle / how to append bytes. It is not an entry in the log
type WAL struct {
	file   *os.File // open queue.wal
	offset int64    // points to the end of the file
}

// Open the WAL file so we can write in it
func OpenWAL(path string) (*WAL, error) {
	// Create the file if DNE, only write from the writer, write goes to the end of the file
	// 0644 is unix perm to allow open
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	// Stat lets us see stats of the file like the size of it. Which we need for offset when we create the struct
	info, err := f.Stat()
	if err != nil {
		f.Close() // Avoiding leak
		return nil, err
	}

	return &WAL{file: f, offset: info.Size()}, nil
}

// Takes in the encoded record and writes it into our WAL
// Returning offset could be good for steady-state (no crash) indexing
func (w *WAL) Append(record []byte) (offset int64, err error) {
	// 1. Encode the frame
	frame, err := encodeFrame(record)
	if err != nil {
		return w.offset, err
	}

	start := w.offset

	// 2. w.file.Write() to the file with the frame
	n, err := w.file.Write(frame)
	if err != nil {
		return 0, err
	}

	// Currently fsync per record. Lowest throughput though
	// consider accumulating several appends and fsync'ing a batch.
	// adds some latency due to postponing ack to consumer until the batch submission
	err = w.file.Sync() // actually writes to disk. Write() writes to OS cache
	if err != nil {
		return 0, err
	}

	// 3. Update w.offset by how many bytes you just wrote
	w.offset += int64(n)
	return start, nil // End of record position = start

}

// Replay means our memory queue died and we need to be able to read the record
// this function is only responsible for reading the log. It doesn't know if we want to
// re-enqueue, ack it, etc. Decodes each record and hands it to the apply function to handle
func (w *WAL) Replay(apply func(record []byte) error) error {
	//Create the reader
	f, err := os.Open(w.file.Name())
	if err != nil {
		return err
	}
	defer f.Close() // Closes the read handle when Replay() returns

	r := bufio.NewReader(f)

	// Look by decoding frame, if cursor hits the end, we have no more frames
	for {
		frame, err := decodeFrame(r)
		if err == io.EOF {
			break // Clean end - no more frames left
		}
		if err != nil {
			return err // Real problem
		}
		if err := apply(frame); err != nil {
			return err
		}
	}
	return nil
}

// encodeFrame writes [len uint32][crc32 uint32][record].
func encodeFrame(record []byte) ([]byte, error) {
	var buf bytes.Buffer
	var scratch [4]byte

	// Find out how many record bytes follow the first 8 bytes
	binary.BigEndian.PutUint32(scratch[:], uint32(len(record)))
	buf.Write(scratch[:])

	// crc
	crc := crc32.ChecksumIEEE(record)
	binary.BigEndian.PutUint32(scratch[:], crc)
	buf.Write(scratch[:])

	// record payload
	buf.Write(record)

	return buf.Bytes(), nil
}

// decodeFrame reads one framed record and returns the record bytes.
// r is a flat sequence of bytes
// Note that this is specifically for decoding a singular frame, and is blind
// to positioning. Luckily, io.Reader has it's own internal position, so reading
// it automatically advances the cursor.
func decodeFrame(r io.Reader) ([]byte, error) {

	// ReadFull fills a buffer we give it (header) to it's maximum capacity
	// Hence why we must give it a size (8 represents the header bytes)
	header := make([]byte, 8)
	_, err := io.ReadFull(r, header)
	if err != nil {
		return nil, err
	}
	recordLength := int(binary.BigEndian.Uint32(header[0:4]))
	storedCRC := binary.BigEndian.Uint32((header[4:8]))

	// Now read exactly recordLength bytes for the record itself.
	// We only know the size now, so we make() the buffer here.
	record := make([]byte, recordLength)
	_, err = io.ReadFull(r, record)
	if err != nil {
		// Got the header but not the whole record = truncated/corrupt frame.
		return nil, err
	}

	// Verify integrity: recompute the checksum and compare to the stored one.
	if crc32.ChecksumIEEE(record) != storedCRC {
		return nil, fmt.Errorf("decodeFrame: crc mismatch (corrupt record)")
	}

	return record, nil
}
