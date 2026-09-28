package lsg

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// chunkReader slices every Read into maxChunk-sized fragments to force
// half packets and repeated short reads.
type chunkReader struct {
	mu       sync.Mutex
	buf      []byte
	maxChunk int
}

func (r *chunkReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.buf) == 0 {
		return 0, io.EOF
	}
	n := r.maxChunk
	if n > len(r.buf) {
		n = len(r.buf)
	}
	if n > len(p) {
		n = len(p)
	}
	copy(p, r.buf[:n])
	r.buf = r.buf[n:]
	return n, nil
}

func buildFrames(payloads ...[]byte) []byte {
	var b []byte
	for _, p := range payloads {
		b = AppendFrame(b, p)
	}
	return b
}

func TestFrameReassemblyHalfPackets(t *testing.T) {
	msgs := [][]byte{
		[]byte("a"),
		bytes.Repeat([]byte("x"), 1),
		bytes.Repeat([]byte("y"), 300),
		[]byte("zzz"),
		nil, // empty payload is legal
	}
	wire := buildFrames(msgs...)
	// Read one byte at a time: maximally fragmented half packets.
	r := &chunkReader{buf: wire, maxChunk: 1}
	fr := NewFrameReader(r, 1<<20)
	for i, want := range msgs {
		got, err := fr.ReadFrame()
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("frame %d mismatch: got %d bytes want %d", i, len(got), len(want))
		}
	}
	if _, err := fr.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("want EOF, got %v", err)
	}
}

func TestFrameStickyPackets(t *testing.T) {
	// Many frames delivered in a single large Read.
	var msgs [][]byte
	for i := 0; i < 200; i++ {
		msgs = append(msgs, []byte("message-number-"+itoa(i)))
	}
	wire := buildFrames(msgs...)
	r := &chunkReader{buf: wire, maxChunk: len(wire)}
	fr := NewFrameReader(r, 1<<20)
	for i, want := range msgs {
		got, err := fr.ReadFrame()
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if string(got) != string(want) {
			t.Fatalf("frame %d mismatch", i)
		}
	}
}

func TestFrameCorruptTerminates(t *testing.T) {
	good := AppendFrame(nil, []byte("payload"))
	bad := append([]byte(nil), good...)
	bad[len(bad)-1] ^= 0xFF // flip payload byte -> CRC mismatch

	srv, cl := net.Pipe()
	go func() { _, _ = srv.Write(bad); time.Sleep(20 * time.Millisecond); _ = srv.Close() }()
	fr := NewFrameReader(cl, 1<<20)
	if _, err := fr.ReadFrame(); !errors.Is(err, ErrFrameCorrupt) {
		t.Fatalf("want ErrFrameCorrupt, got %v", err)
	}
	_ = cl.Close()
}

func TestFrameTooLarge(t *testing.T) {
	big := AppendFrame(nil, make([]byte, 1024))
	r := bytes.NewReader(big)
	fr := NewFrameReader(r, 512)
	if _, err := fr.ReadFrame(); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("want ErrFrameTooLarge, got %v", err)
	}
}

// End-to-end: corrupt frame kills only its own connection, other clients
// keep exchanging frames.
func TestFrameCorruptIsolatesConnection(t *testing.T) {
	g, _ := startTestGW(t, DefaultConfig(""), func(c *Conn, p []byte) {
		_ = c.Send(p)
	})
	ok1, err := Dial(g.Config().SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer ok1.Close()
	ok2, err := Dial(g.Config().SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer ok2.Close()

	// Raw client that sends a valid header claiming CRC then bad bytes.
	raw, err := dialRaw(g.Config().SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	// Send a frame whose CRC won't match.
	bad := AppendFrame(nil, []byte("abc"))
	bad[4+2] ^= 0x01 // flip last payload byte -> CRC mismatch
	_, _ = raw.Write(bad)
	time.Sleep(150 * time.Millisecond)
	_ = raw.Close()

	for i := 0; i < 3; i++ {
		for _, cl := range []*Client{ok1, ok2} {
			msg := []byte("still-here-" + itoa(i))
			if err := cl.Send(msg); err != nil {
				t.Fatalf("healthy client send: %v", err)
			}
			got, err := cl.Recv()
			if err != nil {
				t.Fatalf("healthy client recv after neighbor corruption: %v", err)
			}
			if !bytes.Equal(got, msg) {
				t.Fatalf("mismatch %q", got)
			}
		}
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
