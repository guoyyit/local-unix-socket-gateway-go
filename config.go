package lsg

import (
	"errors"
	"fmt"
	"path/filepath"
	"time"
)

// Config is the desired configuration of a Gateway.
//
// Zero values are replaced by defaults documented on each field via
// DefaultConfig / withDefaults.
type Config struct {
	// SocketPath is the filesystem path of the listening Unix domain socket.
	SocketPath string

	// DirMode is the mode used when creating parent directories.
	DirMode uint32
	// SocketMode is the mode applied to the listening socket file.
	SocketMode uint32

	// AllowUIDs lists UIDs permitted to connect. An empty list allows any
	// authenticated UID (subject to group checks).
	AllowUIDs []int32
	// AllowGIDs lists GIDs permitted to connect; supplementary groups of
	// the peer are included in the match. Empty allows any group.
	AllowGIDs []int32

	// MaxPayload is the maximum accepted frame payload size in bytes.
	MaxPayload uint32

	// MaxQueuedBytes is the per-connection cap on queued+in-flight send
	// bytes. Sends beyond the cap are rejected with ErrBackpressure.
	MaxQueuedBytes int

	// IdleLease is the maximum time a connection may stay without any
	// read or write activity before it is reclaimed.
	IdleLease time.Duration

	// DrainTimeout bounds how long Shutdown waits for admitted sends to
	// be delivered. Zero means wait until all admitted sends finish.
	DrainTimeout time.Duration
}

// DefaultConfig returns a Config populated with safe defaults for path.
func DefaultConfig(socketPath string) Config {
	c := Config{SocketPath: socketPath}
	return c.withDefaults()
}

func (c Config) withDefaults() Config {
	cfg := c
	if cfg.DirMode == 0 {
		cfg.DirMode = 0o700
	}
	if cfg.SocketMode == 0 {
		cfg.SocketMode = 0o600
	}
	if cfg.MaxPayload == 0 {
		cfg.MaxPayload = 1 << 20 // 1 MiB
	}
	if cfg.MaxQueuedBytes <= 0 {
		cfg.MaxQueuedBytes = 1 << 20 // 1 MiB
	}
	if cfg.IdleLease <= 0 {
		cfg.IdleLease = 5 * time.Minute
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 10 * time.Second
	}
	return cfg
}

// sendBufferBytes chooses the per-connection kernel send buffer so a slow
// reader makes writes block and the userspace backpressure budget remains
// effective. It is clamped to a usable range because operating systems
// impose minimum and maximum SO_SNDBUF values.
func sendBufferBytes(c Config) int {
	n := c.MaxQueuedBytes
	const minBuf = 4096
	const maxBuf = 4 << 20 // 4 MiB
	if n < minBuf {
		return minBuf
	}
	if n > maxBuf {
		return maxBuf
	}
	return n
}

// Validate reports configuration errors before the gateway starts.
func (c Config) Validate() error {
	if c.SocketPath == "" {
		return errors.New("lsg: SocketPath must not be empty")
	}
	if !filepath.IsAbs(c.SocketPath) {
		return errors.New("lsg: SocketPath must be absolute")
	}
	if len(c.SocketPath) > maxSockPathLen {
		return fmt.Errorf("lsg: SocketPath length %d exceeds platform sockaddr_un limit (%d); choose a shorter path (e.g. under /tmp)", len(c.SocketPath), maxSockPathLen)
	}
	if c.SocketMode&0o170000 != 0 || c.SocketMode > 0o777 {
		return errors.New("lsg: SocketMode must be a valid permission mode (<=0777)")
	}
	if c.MaxPayload == 0 {
		return errors.New("lsg: MaxPayload must be > 0 after defaulting")
	}
	if c.MaxQueuedBytes <= 0 {
		return errors.New("lsg: MaxQueuedBytes must be > 0 after defaulting")
	}
	if c.IdleLease <= 0 {
		return errors.New("lsg: IdleLease must be > 0 after defaulting")
	}
	if c.DrainTimeout < 0 {
		return errors.New("lsg: DrainTimeout must be >= 0")
	}
	return nil
}
