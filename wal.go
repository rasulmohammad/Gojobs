package task_queue

import (
	// "bytes"
	// "encoding/binary"
	// "fmt"
	// "hash/crc32"
	"io"
	"os"
)

// This is the log manager. Owns file handle / how to append bytes. It is not an entry in the log
type WAL struct {
	file   *os.File // open queue.wal
	offset int64    // bytes written thus far
}

// Open the WAL file so we can write in it
func OpenWAL(path string) (*WAL, error) {
	panic("TODO: implement in 2.1")
}

// Takes in the encoded task and writes it into our WAL
func (w *WAL) Append(payload []byte) (offset int64, err error) {
	panic("TODO: implement in 2.1")
}

// Replay means our memory queue died and we need to be able to read the record and enqueue it again
func (w *WAL) Replay(apply func(payload []byte) error) error {
	panic("TODO: implement in 2.1")
}

// encodeFrame writes [len uint32][crc32 uint32][payload].
func encodeFrame(payload []byte) ([]byte, error) {
	panic("TODO: implement in 2.1")
}

// decodeFrame reads one framed record and returns the payload bytes.
func decodeFrame(r io.Reader) ([]byte, error) {
	panic("TODO: implement in 2.1")
}
