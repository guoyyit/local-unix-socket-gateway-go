package lsg

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
	"net"
	"time"
)

// Client is a thin framed client for a local Unix domain socket gateway.
// The kernel supplies the client's UID/GID to the server from the
// connection itself; no credentials are sent on the wire.
type Client struct {
	path string
	nc   net.Conn
	br   *bufio.Reader

	rmu chan struct{} // serializes Recv
	wmu chan struct{} // serializes Send
}

// Dial connects to the gateway at path.
func Dial(path string) (*Client, error) {
	nc, err := net.DialTimeout("unix", path, 5*time.Second)
	if err != nil {
		return nil, err
	}
	c := &Client{
		path: path,
		nc:   nc,
		br:   bufio.NewReaderSize(nc, 64*1024),
		rmu:  make(chan struct{}, 1),
		wmu:  make(chan struct{}, 1),
	}
	c.rmu <- struct{}{}
	c.wmu <- struct{}{}
	return c, nil
}

// LocalUID is informational; servers derive identity from the kernel.
func (c *Client) LocalUID() (uint32, bool) {
	if uc, ok := c.nc.(*net.UnixConn); ok {
		rc, err := uc.SyscallConn()
		if err != nil {
			return 0, false
		}
		var uid uint32
		ok := false
		_ = rc.Control(func(fd uintptr) {
			if cred, err := peerCredentials(fd); err == nil {
				uid, ok = cred.UID, true
			}
		})
		return uid, ok
	}
	return 0, false
}

// Send writes one length-prefixed, checksummed frame. Short writes are
// retried so a frame is written atomically with respect to other Senders.
func (c *Client) Send(payload []byte) error {
	if len(payload) > int(^uint32(0)) {
		return ErrFrameTooLarge
	}
	<-c.wmu
	defer func() { c.wmu <- struct{}{} }()

	hdr := make([]byte, frameHeaderSize, frameHeaderSize+len(payload))
	binary.BigEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(hdr[4:8], crc32.Checksum(payload, crcTable))
	if err := writeFull(c.nc, hdr); err != nil {
		return err
	}
	return writeFull(c.nc, payload)
}

// Recv reads one complete frame. It transparently handles short reads and
// multiple frames coalesced by the transport (sticky packets); frames split
// across transport segments are reassembled. A length or checksum failure
// yields ErrFrameTooLarge / ErrFrameCorrupt.
func (c *Client) Recv() ([]byte, error) {
	<-c.rmu
	defer func() { c.rmu <- struct{}{} }()

	var hdr [frameHeaderSize]byte
	if _, err := io.ReadFull(c.br, hdr[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(hdr[0:4])
	wantCRC := binary.BigEndian.Uint32(hdr[4:8])
	payload := make([]byte, length)
	if _, err := io.ReadFull(c.br, payload); err != nil {
		return nil, err
	}
	if crc32.Checksum(payload, crcTable) != wantCRC {
		return nil, ErrFrameCorrupt
	}
	return payload, nil
}

// Close releases the client connection.
func (c *Client) Close() error { return c.nc.Close() }
