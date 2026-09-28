package lsg

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// gatewayState values describe the lifecycle of a Gateway.
type gatewayState int32

const (
	stateNew gatewayState = iota
	stateRunning
	stateShuttingDown
	stateStopped
)

func (s gatewayState) String() string {
	switch s {
	case stateNew:
		return "new"
	case stateRunning:
		return "running"
	case stateShuttingDown:
		return "shutting_down"
	default:
		return "stopped"
	}
}

// atomicConfig stores the current desired Config as an immutable snapshot.
type atomicConfig struct{ v atomic.Pointer[Config] }

func (a *atomicConfig) load() Config {
	p := a.v.Load()
	if p == nil {
		return Config{}
	}
	return *p
}

func (a *atomicConfig) store(c Config) { a.v.Store(&c) }

// Stats is a point-in-time snapshot of gateway runtime state. It is
// distinct from Config: stats describe what is, config describes intent.
// After a restart, Stats resets while Config is reloaded from input.
type Stats struct {
	State         string
	StartedAt     time.Time
	ActiveConns   int
	AcceptedTotal uint64
	RejectedTotal uint64
	FramesIn      uint64
	FramesOut     uint64
	BytesIn       int64
	BytesOut      int64
	CurrentConfig Config
}

// Gateway is a local Unix domain socket gateway.
type Gateway struct {
	cfg atomicConfig
	h   HandleFunc

	state atomic.Int32

	mu         sync.Mutex
	ln         *net.UnixListener
	conns      map[uint64]*Conn
	startedAt  time.Time
	listenPath string

	nextID   atomic.Uint64
	rejected atomic.Uint64

	connWG sync.WaitGroup // connection goroutines

	runDone      chan struct{}
	readyCh      chan struct{}
	readyOnce    sync.Once
	closeRunOnce sync.Once
	runResult    atomic.Value // error
	runOnce      sync.Once
	shutdownOnce sync.Once
}

// Ready returns a channel closed once the gateway has bound its listener
// and is accepting connections. Callers that dial immediately after
// starting Run in a separate goroutine should wait on this rather than on a
// state transition.
func (g *Gateway) Ready() <-chan struct{} { return g.readyCh }

func (g *Gateway) markReady() { g.readyOnce.Do(func() { close(g.readyCh) }) }

// New constructs a Gateway with validated, defaulted configuration. The
// gateway does not touch the filesystem until Run.
func New(cfg Config, h HandleFunc) (*Gateway, error) {
	cfg = cfg.withDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if h == nil {
		return nil, errors.New("lsg: handler must not be nil")
	}
	g := &Gateway{
		h:          h,
		conns:      make(map[uint64]*Conn),
		listenPath: cfg.SocketPath,
		runDone:    make(chan struct{}),
		readyCh:    make(chan struct{}),
	}
	g.cfg.store(cfg)
	g.state.Store(int32(stateNew))
	return g, nil
}

func (g *Gateway) getState() gatewayState { return gatewayState(g.state.Load()) }

func (g *Gateway) addWG(delta int) {
	if delta > 0 {
		g.connWG.Add(delta)
	}
}

func (g *Gateway) doneWG() { g.connWG.Done() }

// Run prepares the listening path (creating parent directories, setting
// permissions, safely reclaiming stale sockets, refusing a live one),
// serves connections until ctx is canceled, the listener fails fatally, or
// Shutdown is called. Run returns after listener teardown; use Shutdown to
// drain and clean up deterministically.
func (g *Gateway) Run(ctx context.Context) (retErr error) {
	if !g.state.CompareAndSwap(int32(stateNew), int32(stateRunning)) {
		// Shutdown already requested before Run: nothing to start.
		if g.getState() == stateStopped {
			g.closeRunOnce.Do(func() { close(g.runDone) })
			return nil
		}
		return errors.New("lsg: Run called more than once or gateway already stopped")
	}
	defer g.finishRun()

	cfg := g.cfg.load()
	if err := prepareListenPath(cfg.SocketPath, cfg.SocketMode, cfg.DirMode); err != nil {
		g.runResult.Store(err)
		g.markReady()
		return err
	}

	addr, err := net.ResolveUnixAddr("unix", cfg.SocketPath)
	if err != nil {
		g.runResult.Store(err)
		g.markReady()
		return err
	}
	ln, err := net.ListenUnix("unix", addr)
	if err != nil {
		g.runResult.Store(err)
		g.markReady()
		return err
	}
	// Defend against umask and enforce the declared mode explicitly.
	if err := os.Chmod(cfg.SocketPath, os.FileMode(cfg.SocketMode)); err != nil {
		_ = ln.Close()
		_ = os.Remove(cfg.SocketPath)
		g.runResult.Store(err)
		g.markReady()
		return err
	}
	ln.SetUnlinkOnClose(false) // cleanup is owned explicitly by Shutdown/Run

	g.mu.Lock()
	g.ln = ln
	g.startedAt = time.Now()
	g.mu.Unlock()

	// If Shutdown raced us before the listener was installed.
	if g.getState() != stateRunning {
		_ = ln.Close()
		_ = os.Remove(cfg.SocketPath)
		g.markReady()
		return nil
	}
	g.markReady()

	stopAccept := make(chan struct{})
	defer close(stopAccept)
	go func() {
		select {
		case <-ctx.Done():
		case <-stopAccept:
		}
		_ = ln.Close()
	}()
	go g.leaseReaper(ctx)

	var serveErr error
	for {
		nc, err := ln.AcceptUnix()
		if err != nil {
			if g.getState() != stateRunning {
				serveErr = nil // deliberate shutdown
			} else if errors.Is(err, net.ErrClosed) && ctx.Err() != nil {
				serveErr = nil
			} else {
				serveErr = err
			}
			break
		}
		g.connWG.Add(1)
		go g.handleAccept(nc)
	}

	g.connWG.Wait()
	g.mu.Lock()
	g.ln = nil
	g.mu.Unlock()

	// ctx cancellation without Shutdown still cleans the socket path.
	if g.getState() == stateRunning {
		g.state.Store(int32(stateShuttingDown))
		_ = os.Remove(cfg.SocketPath)
	}
	g.state.Store(int32(stateStopped))
	if serveErr != nil {
		g.runResult.Store(serveErr)
	}
	return serveErr
}

// finishRun closes runDone exactly once and marks the gateway stopped so a
// waiting Shutdown can always progress.
func (g *Gateway) finishRun() {
	g.state.Store(int32(stateStopped))
	// Best-effort socket cleanup if a failure path left the inode behind.
	if g.listenPath != "" {
		if fi, err := os.Lstat(g.listenPath); err == nil && isSocket(fi) {
			_ = os.Remove(g.listenPath)
		}
	}
	g.closeRunOnce.Do(func() { close(g.runDone) })
}

// Shutdown stops accepting connections, drains admitted sends within the
// configured drain timeout, cancels waiting/idle connections and removes
// the listening path. Sends submitted during shutdown get a definite error.
// It is safe to call multiple times and from a handler.
func (g *Gateway) Shutdown(ctx context.Context) error {
	g.shutdownOnce.Do(func() {
		// If Run never started, release any waiter immediately.
		if !g.state.CompareAndSwap(int32(stateNew), int32(stateStopped)) {
			g.state.CompareAndSwap(int32(stateRunning), int32(stateShuttingDown))
		}
	})

	g.mu.Lock()
	ln := g.ln
	conns := make([]*Conn, 0, len(g.conns))
	for _, c := range g.conns {
		conns = append(conns, c)
	}
	path := g.listenPath
	g.mu.Unlock()

	if g.getState() == stateStopped && ln == nil {
		// Run never ran or has fully finished; just ensure path is gone.
		if path != "" {
			if fi, err := os.Lstat(path); err == nil && isSocket(fi) {
				_ = os.Remove(path)
			}
		}
		<-g.runDone
		return nil
	}

	if ln != nil {
		_ = ln.Close() // stop accepting new connections
	}

	cfg := g.cfg.load()
	drainDeadline := time.Time{}
	if cfg.DrainTimeout > 0 {
		drainDeadline = time.Now().Add(cfg.DrainTimeout)
	}
	cmd := drainCmd{deadline: drainDeadline}
	for _, c := range conns {
		select {
		case c.drain <- cmd:
		default:
			// A drain/close is already pending or the connection is dying.
		}
	}

	// Wait for connections to drain out, bounded by the caller's ctx.
	done := make(chan struct{})
	go func() {
		g.connWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		// Force-reclaim anything still here.
		for _, c := range conns {
			c.closeWith(ErrShutdown)
		}
		<-done
	}

	if path != "" {
		if fi, err := os.Lstat(path); err == nil && isSocket(fi) {
			_ = os.Remove(path)
		}
	}
	<-g.runDone
	if v := g.runResult.Load(); v != nil {
		if e, ok := v.(error); ok && e != nil {
			return e
		}
	}
	return nil
}

// UpdateConfig atomically updates the desired configuration.
//
// Effectiveness rules:
//   - SocketPath changes are rejected (the listener address is immutable;
//     restart the gateway to relocate it).
//   - SocketMode/DirMode are applied to new paths and re-applied to the
//     existing listening socket immediately.
//   - AllowUIDs/AllowGIDs apply to newly accepted connections immediately;
//     established connections stay authenticated for their lifetime.
//   - MaxPayload applies to new connections; established connections keep
//     their accept-time frame limit to avoid mid-stream framing changes.
//   - MaxQueuedBytes applies to all existing connections on their very next
//     admission/queue decision (no frame is dropped retroactively).
//   - IdleLease applies to all existing connections at the next reaper tick
//     (shorter leases reclaim promptly; longer leases extend promptly).
//   - DrainTimeout applies to the next Shutdown.
func (g *Gateway) UpdateConfig(cfg Config) error {
	cur := g.cfg.load()
	// SocketPath may be left empty in a partial update to mean "unchanged".
	// Any non-empty value that differs is rejected because the listen
	// address is immutable for the life of a process.
	if cfg.SocketPath != "" && cfg.SocketPath != cur.SocketPath {
		return errors.New("lsg: SocketPath cannot be changed on a running gateway; restart to relocate")
	}
	// An update is a partial overlay: zero-valued scalar fields inherit
	// the current value so callers can change one parameter without
	// resending every other one. Slice fields are always explicit.
	merged := cur.mergeFrom(cfg)
	if err := merged.validateExplicit(); err != nil {
		return err
	}
	g.cfg.store(merged)
	// Re-apply socket mode to the live inode.
	if g.getState() == stateRunning {
		if err := os.Chmod(merged.SocketPath, os.FileMode(merged.SocketMode)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// mergeFrom overlays non-zero fields of override onto c. Slices are always
// treated as explicit values (including nil, which clears the allow list).
func (c Config) mergeFrom(override Config) Config {
	out := c
	if override.DirMode != 0 {
		out.DirMode = override.DirMode
	}
	if override.SocketMode != 0 {
		out.SocketMode = override.SocketMode
	}
	out.AllowUIDs = override.AllowUIDs
	out.AllowGIDs = override.AllowGIDs
	if override.MaxPayload != 0 {
		out.MaxPayload = override.MaxPayload
	}
	if override.MaxQueuedBytes != 0 {
		out.MaxQueuedBytes = override.MaxQueuedBytes
	}
	if override.IdleLease != 0 {
		out.IdleLease = override.IdleLease
	}
	if override.DrainTimeout != 0 {
		out.DrainTimeout = override.DrainTimeout
	}
	return out
}

// validateExplicit validates a fully populated config, rejecting negative
// values that withDefaults would otherwise mask.
func (c Config) validateExplicit() error {
	if err := c.Validate(); err != nil {
		return err
	}
	if c.MaxQueuedBytes < 0 {
		return errors.New("lsg: MaxQueuedBytes must be >= 0")
	}
	if c.IdleLease < 0 {
		return errors.New("lsg: IdleLease must be >= 0")
	}
	return nil
}

// Config returns the current desired configuration snapshot.
func (g *Gateway) Config() Config { return g.cfg.load() }

// Stats returns a point-in-time runtime snapshot; it never mixes runtime
// state into configuration.
func (g *Gateway) Stats() Stats {
	g.mu.Lock()
	conns := make([]*Conn, 0, len(g.conns))
	var fin, fout uint64
	var bin, bout int64
	for _, c := range g.conns {
		conns = append(conns, c)
		s := c.Stats()
		fin += s.FramesIn
		fout += s.FramesOut
		bin += s.BytesIn
		bout += s.BytesOut
	}
	started := g.startedAt
	g.mu.Unlock()

	return Stats{
		State:         g.getState().String(),
		StartedAt:     started,
		ActiveConns:   len(conns),
		AcceptedTotal: acceptedTotal(g),
		RejectedTotal: rejectedTotal(g),
		FramesIn:      fin,
		FramesOut:     fout,
		BytesIn:       bin,
		BytesOut:      bout,
		CurrentConfig: g.cfg.load(),
	}
}

func acceptedTotal(g *Gateway) uint64 { return g.nextID.Load() }
func rejectedTotal(g *Gateway) uint64 { return g.rejected.Load() }

// Conns returns runtime snapshots of all active connections.
func (g *Gateway) Conns() []ConnStats {
	g.mu.Lock()
	out := make([]ConnStats, 0, len(g.conns))
	for _, c := range g.conns {
		out = append(out, c.Stats())
	}
	g.mu.Unlock()
	return out
}

func (g *Gateway) registerConn(c *Conn) {
	g.mu.Lock()
	g.conns[c.id] = c
	g.mu.Unlock()
}

func (g *Gateway) unregisterConn(id uint64) {
	g.mu.Lock()
	delete(g.conns, id)
	g.mu.Unlock()
}

func (g *Gateway) addRejected() { g.rejected.Add(1) }

func (g *Gateway) handleAccept(nc *net.UnixConn) {
	defer g.connWG.Done()

	raw, err := nc.SyscallConn()
	if err != nil {
		_ = nc.Close()
		g.addRejected()
		return
	}
	var cred PeerCred
	var cerr error
	if err := raw.Control(func(fd uintptr) {
		cred, cerr = peerCredentials(fd)
	}); err != nil {
		_ = nc.Close()
		g.addRejected()
		return
	}
	if cerr != nil {
		_ = nc.Close()
		g.addRejected()
		return
	}

	cfg := g.cfg.load()
	ok, err := authorize(cfg, cred)
	if err != nil || !ok {
		// Rejected identity: close before any application bytes are
		// exchanged. No handler runs, no business data is exposed.
		_ = nc.Close()
		g.addRejected()
		return
	}

	// Bound the kernel send buffer so that a slow reader makes writes
	// block and the userspace backpressure budget stays meaningful:
	// admitted data cannot exceed MaxQueuedBytes of kernel+queued bytes.
	if berr := raw.Control(func(fd uintptr) {
		cerr = setSendBuffer(fd, sendBufferBytes(cfg))
	}); berr != nil || cerr != nil {
		_ = nc.Close()
		g.addRejected()
		return
	}

	id := g.nextID.Add(1)
	c := newConn(g, id, nc, cred, cfg.MaxPayload)
	g.registerConn(c)
	c.start(g.h)
}

// leaseReaper periodically closes connections that exceed the current idle
// lease. Idle is defined by absence of both read and write activity.
func (g *Gateway) leaseReaper(ctx context.Context) {
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-g.runDone:
			return
		case now := <-t.C:
			if g.getState() != stateRunning {
				return
			}
			lease := g.cfg.load().IdleLease
			g.mu.Lock()
			conns := make([]*Conn, 0, len(g.conns))
			for _, c := range g.conns {
				conns = append(conns, c)
			}
			g.mu.Unlock()
			for _, c := range conns {
				if c.idle(lease, now) {
					c.closeWith(ErrIdleLeaseExpired)
				}
			}
		}
	}
}

// prepareListenPath creates parent directories, safely reclaims a stale
// socket file and refuses to start when the path is held by a live listener.
func prepareListenPath(path string, sockMode, dirMode uint32) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, os.FileMode(dirMode)); err != nil {
		return err
	}
	fi, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	// Never replace something that is not a socket (symlink, regular file
	// planted by another user, etc.): fail loudly instead.
	if fi.Mode()&os.ModeSocket == 0 {
		return &os.PathError{Op: "bind", Path: path, Err: errNotSocket}
	}
	if socketInUse(path) {
		return ErrSocketInUse
	}
	// Stale, unreferenced socket from a previous (crashed) process:
	// reclaim it before binding.
	return os.Remove(path)
}

var errNotSocket = errors.New("path exists and is not a socket; refusing to replace it")

func isSocket(fi os.FileInfo) bool { return fi.Mode()&os.ModeSocket != 0 }
