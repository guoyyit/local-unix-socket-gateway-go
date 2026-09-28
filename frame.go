package lsg

import (
	"encoding/binary"
	"hash/crc32"
	"io"
)

// frameHeaderSize is |length uint32 BE|crc32 IEEE uint32 BE|.
const frameHeaderSize = 8

var crcTable = crc32.MakeTable(crc32.IEEE)

// AppendFrame appends a length-prefixed, checksummed frame of payload to dst.
func AppendFrame(dst, payload []byte) []byte {
	var hdr [frameHeaderSize]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(hdr[4:8], crc32.Checksum(payload, crcTable))
	dst = append(dst, hdr[:]...)
	return append(dst, payload...)
}

// FrameReader reassembles framed messages from a byte stream, transparently
// handling short reads and frame coalescing (sticky/half packets).
type FrameReader struct {
	r          io.Reader
	maxPayload uint32

	hdr  [frameHeaderSize]byte
	hbuf []byte // leftover header bytes between Read calls
	body []byte // leftover body bytes between Read calls

	length uint32 // declared length of the frame currently being read
	bodyN  int    // body bytes already read for the current frame
}

// NewFrameReader creates a FrameReader rejecting frames above maxPayload.
func NewFrameReader(r io.Reader, maxPayload uint32) *FrameReader {
	return &FrameReader{r: r, maxPayload: maxPayload}
}

// ReadFrame reads exactly one complete frame from the underlying stream.
// It performs as many underlying reads as required (short reads are
// retried), validates the length bound and the CRC, and returns a fresh
// payload slice owned by the caller. Multiple frames arriving in one
// segment are separated; a frame split across segments is reassembled.
func (r *FrameReader) ReadFrame() ([]byte, error) {
	// Phase 1: read a full 8-byte header.
	if _, err := io.ReadFull(r.r, r.hdr[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(r.hdr[0:4])
	wantCRC := binary.BigEndian.Uint32(r.hdr[4:8])
	if length > r.maxPayload {
		return nil, ErrFrameTooLarge
	}

	// Phase 2: read exactly length payload bytes (handles half packets).
	payload := make([]byte, length)
	if _, err := io.ReadFull(r.r, payload); err != nil {
		return nil, err
	}
	if crc32.Checksum(payload, crcTable) != wantCRC {
		// Do not return any payload bytes on corruption: the caller
		// receives only the error and must terminate this connection.
		return nil, ErrFrameCorrupt
	}
	return payload, nil
}
