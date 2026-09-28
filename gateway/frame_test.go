package gateway

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

// 逐字节读取的 Reader：模拟最极端的半包。
type byteReader struct{ data []byte }

func (r *byteReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	p[0] = r.data[0]
	r.data = r.data[1:]
	return 1, nil
}

func TestFramePartialReads(t *testing.T) {
	codec := NewFrameCodec(1024)
	payloads := [][]byte{[]byte("a"), []byte("bb"), []byte("three"), make([]byte, 300)}
	for i := range payloads {
		for j := range payloads[i] {
			payloads[i][j] = byte(i + 1)
		}
	}
	var stream bytes.Buffer
	for _, p := range payloads {
		f, err := codec.Encode(p)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		stream.Write(f)
	}
	r := &byteReader{data: append([]byte(nil), stream.Bytes()...)}
	header := make([]byte, frameHeaderSize)
	for i, want := range payloads {
		got, err := codec.ReadFrame(r, header)
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("frame %d payload mismatch: len=%d want=%d", i, len(got), len(want))
		}
	}
}

func TestFrameCoalescedWrites(t *testing.T) {
	codec := NewFrameCodec(1024)
	// 三帧在一次写入中全部到达（粘包）。
	var buf bytes.Buffer
	msgs := [][]byte{[]byte("hello"), []byte("unix"), []byte("gateway")}
	for _, m := range msgs {
		f, _ := codec.Encode(m)
		buf.Write(f)
	}
	header := make([]byte, frameHeaderSize)
	for _, want := range msgs {
		got, err := codec.ReadFrame(&buf, header)
		if err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}
		if string(got) != string(want) {
			t.Fatalf("got %q want %q", got, want)
		}
	}
}

func TestFrameCorruptChecksum(t *testing.T) {
	codec := NewFrameCodec(1024)
	f, _ := codec.Encode([]byte("payload"))
	f[len(f)-1] ^= 0xFF // 翻转 payload 末位
	_, err := codec.ReadFrame(bytes.NewReader(f), make([]byte, frameHeaderSize))
	if !errors.Is(err, ErrFrameCorrupt) {
		t.Fatalf("got %v want ErrFrameCorrupt", err)
	}
}

func TestFrameBadMagicAndLength(t *testing.T) {
	codec := NewFrameCodec(64)
	badMagic := make([]byte, frameHeaderSize+1)
	binary.LittleEndian.PutUint32(badMagic[0:4], 0xdeadbeef)
	if _, err := codec.ReadFrame(bytes.NewReader(badMagic), make([]byte, frameHeaderSize)); !errors.Is(err, ErrFrameCorrupt) {
		t.Fatalf("bad magic: got %v", err)
	}

	badLen := make([]byte, frameHeaderSize)
	binary.LittleEndian.PutUint32(badLen[0:4], frameMagic)
	binary.LittleEndian.PutUint32(badLen[4:8], 65) // 超过 maxPayload
	if _, err := codec.ReadFrame(bytes.NewReader(badLen), make([]byte, frameHeaderSize)); !errors.Is(err, ErrFrameCorrupt) {
		t.Fatalf("bad length: got %v", err)
	}
}

// failWriter 始终写 0 字节，验证短写被识别而不是死循环。
type failWriter struct{}

func (failWriter) Write(p []byte) (int, error) { return 0, nil }

func TestFrameZeroWrite(t *testing.T) {
	codec := NewFrameCodec(128)
	if err := codec.WriteFrame(failWriter{}, []byte("x")); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("got %v want ErrShortWrite", err)
	}
}

func TestFrameTooLarge(t *testing.T) {
	codec := NewFrameCodec(4)
	if _, err := codec.Encode(make([]byte, 5)); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("got %v", err)
	}
}
