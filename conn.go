package lsg

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// HandleFunc is invoked serially per connection with each authenticated,
// intact frame payload. The slice is only valid during the call; handlers
// must copy data they retain.
type HandleFunc func(c *Conn, payload []byte)

// ConnStats is a point-in-time, runtime snapshot of one connection.
// Runtime stats are intentionally separate from the desired Config.
type ConnStats struct {
	ID          uint64
	UID         uint32
	GID         uint32
	PID         int32
	CreatedAt   time.Time
	FramesIn    uint64
	FramesOut   uint64
	BytesIn     int64
	BytesOut    int64
	QueuedBytes int64
}

// sendReq is one admitted outbound frame.
type sendReq struct {
	payload []byte
	done    chan error // buffered(1); settled exactly once
}

// offer is a request to admit a frame into the send budget.
type offer struct {
	req   sendReq
	block bool // Send blocks; TrySend wants an immediate decision
	reply chan offerReply
}

type offerReply struct {
	admitted bool
	err      error
}

type writeResult struct {
	req *sendReq
	err error
}

type drainCmd struct {
	deadline time.Time // zero = drain until empty
}

// Conn is one authenticated client connection.
type Conn struct {
	id        uint64
	gw        *Gateway
	nc        net.Conn
	cred      PeerCred
	createdAt time.Time

	maxPayload uint32 // fixed at accept time

	framesIn  atomic.Uint64
	framesOut atomic.Uint64
	bytesIn   atomic.Int64
	bytesOut  atomic.Int64

	lastActive atomic.Int64 // unix nanos
	queuedN    atomic.Int64 // queued + in-flight payload bytes

	// offerCh is buffered so a sender can deliver its offer even when the
	// dispatcher is momentarily busy; admission itself is always decided
	// by the dispatcher against the live budget.
	offerCh chan offer
	// writeCh has capacity 1: it hands exactly one frame at a time to the
	// single writer. The dispatcher only sends when no frame is in flight.
	writeCh  chan sendReq
	resultCh chan writeResult
	drain    chan drainCmd
	closeCh  chan struct{}
	done     chan struct{}

	closeOnce sync.Once
	reason    atomic.Value // error
}

func newConn(gw *Gateway, id uint64, nc net.Conn, cred PeerCred, maxPayload uint32) *Conn {
	c := &Conn{
		id:         id,
		gw:         gw,
		nc:         nc,
		cred:       cred,
		createdAt:  time.Now(),
		maxPayload: maxPayload,
		offerCh:    make(chan offer, offerChanDepth),
		writeCh:    make(chan sendReq, 1),
		resultCh:   make(chan writeResult, 1),
		drain:      make(chan drainCmd, 1),
		closeCh:    make(chan struct{}),
		done:       make(chan struct{}),
	}
	c.lastActive.Store(time.Now().UnixNano())
	return c
}

// offerChanDepth bounds offers parked at the channel while the dispatcher
// is processing; admission decisions still enforce the byte budget.
const offerChanDepth = 256

func (c *Conn) ID() uint64     { return c.id }
func (c *Conn) Peer() PeerCred { return c.cred }

func (c *Conn) closeWith(reason error) {
	c.closeOnce.Do(func() {
		if reason == nil {
			reason = ErrConnClosed
		}
		c.reason.Store(reason)
		close(c.closeCh)
	})
}

func (c *Conn) closedReason() error {
	if v := c.reason.Load(); v != nil {
		return v.(error)
	}
	return nil
}

func (c *Conn) touch() {
	c.lastActive.Store(time.Now().UnixNano())
}

// idle reports whether the connection has shown no activity for lease.
func (c *Conn) idle(lease time.Duration, now time.Time) bool {
	last := time.Unix(0, c.lastActive.Load())
	return now.Sub(last) >= lease
}

// Send enqueues a framed payload, applying the connection's current
// backpressure budget. It blocks until delivered, the connection dies, or
// the gateway shuts down; the result is always deterministic.
func (c *Conn) Send(payload []byte) error { return c.submit(payload, true) }

// TrySend attempts to enqueue a framed payload without blocking. It returns
// nil as soon as the frame is admitted into the bounded per-connection send
// budget (delivery then completes asynchronously), ErrBackpressure when the
// budget is currently full, or a definite error if the connection is dead.
func (c *Conn) TrySend(payload []byte) error { return c.submit(payload, false) }

func (c *Conn) submit(payload []byte, block bool) error {
	if err := c.closedReason(); err != nil {
		return err
	}
	if uint32(len(payload)) > c.maxPayload {
		return ErrFrameTooLarge
	}
	o := offer{
		req:   sendReq{payload: payload, done: make(chan error, 1)},
		block: block,
		reply: make(chan offerReply, 1),
	}

	if !block {
		select {
		case c.offerCh <- o:
		case <-c.closeCh:
			return c.closedReason()
		default:
			return ErrBackpressure
		}
		// TrySend resolves at the admission boundary. A nil result means
		// the frame was accepted into the bounded queue and will either be
		// delivered or settled with a definite error when the connection
		// terminates; it is never silently dropped.
		select {
		case rep := <-o.reply:
			if !rep.admitted {
				return rep.err
			}
			return nil
		case <-c.closeCh:
			return c.closedReason()
		}
	}

	// Send blocks through admission AND delivery, returning the definite
	// delivery result.
	select {
	case c.offerCh <- o:
	case <-c.closeCh:
		return c.closedReason()
	}
	var admitted bool
	select {
	case rep := <-o.reply:
		if !rep.admitted {
			return rep.err
		}
		admitted = true
	case <-c.closeCh:
		return c.closedReason()
	}
	if !admitted {
		return ErrBackpressure
	}
	// Admitted: settled exactly once by the dispatcher.
	return <-o.req.done
}

// Close reclaims the connection and fails admitted but unsent sends.
func (c *Conn) Close(reason error) error {
	c.closeWith(reason)
	<-c.done
	return nil
}

func (c *Conn) Stats() ConnStats {
	return ConnStats{
		ID:          c.id,
		UID:         c.cred.UID,
		GID:         c.cred.GID,
		PID:         c.cred.PID,
		CreatedAt:   c.createdAt,
		FramesIn:    c.framesIn.Load(),
		FramesOut:   c.framesOut.Load(),
		BytesIn:     c.bytesIn.Load(),
		BytesOut:    c.bytesOut.Load(),
		QueuedBytes: c.queuedN.Load(),
	}
}

// start launches reader, writer and dispatcher goroutines.
func (c *Conn) start(handler HandleFunc) {
	var ioWG sync.WaitGroup
	ioWG.Add(2) // reader + writer
	go c.readLoop(handler, &ioWG)
	go c.writeLoop(&ioWG)

	c.gw.addWG(1)
	go c.dispatch(&ioWG)
}

func (c *Conn) readLoop(handler HandleFunc, ioWG *sync.WaitGroup) {
	defer ioWG.Done()
	fr := NewFrameReader(c.nc, c.maxPayload)
	for {
		payload, err := fr.ReadFrame()
		if err != nil {
			if errors.Is(err, io.EOF) {
				c.closeWith(io.EOF)
			} else {
				c.closeWith(err)
			}
			return
		}
		c.framesIn.Add(1)
		c.bytesIn.Add(int64(len(payload)))
		c.touch()
		c.serve(handler, payload)
	}
}

func (c *Conn) serve(handler HandleFunc, payload []byte) {
	defer func() {
		if r := recover(); r != nil {
			c.closeWith(errHandlerPanic(r))
		}
	}()
	handler(c, payload)
}

func (c *Conn) writeLoop(ioWG *sync.WaitGroup) {
	defer ioWG.Done()
	for {
		select {
		case <-c.closeCh:
			return
		case req := <-c.writeCh:
			buf := AppendFrame(make([]byte, 0, frameHeaderSize+len(req.payload)), req.payload)
			err := writeFull(c.nc, buf)
			select {
			case c.resultCh <- writeResult{req: &req, err: err}:
			case <-c.closeCh:
				return
			}
		}
	}
}

// writeFull writes all of buf, retrying short writes.
func writeFull(w io.Writer, buf []byte) error {
	for len(buf) > 0 {
		n, err := w.Write(buf)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		buf = buf[n:]
	}
	return nil
}

// dispatch is the single owner of the send queue and the only goroutine that
// decides admission, so backpressure accounting is race-free. The writer is
// decoupled: a slow consumer blocks the writer, not admission control.
func (c *Conn) dispatch(ioWG *sync.WaitGroup) {
	defer func() {
		c.gw.unregisterConn(c.id)
		close(c.done)
		c.gw.doneWG()
	}()

	var (
		queue       []sendReq
		inflight    *sendReq
		waiting     []offer
		draining    bool
		drainTimer  *time.Timer
		drainTimerC <-chan time.Time
	)

	used := func() int64 {
		var n int64
		for _, r := range queue {
			n += int64(len(r.payload))
		}
		if inflight != nil {
			n += int64(len(inflight.payload))
		}
		return n
	}

	setQueued := func() { c.queuedN.Store(used()) }

	canFit := func(size int) bool {
		return used()+int64(size) <= int64(c.gw.cfg.load().MaxQueuedBytes)
	}

	settle := func(r *sendReq, err error) {
		if r == nil {
			return
		}
		if err == nil {
			c.framesOut.Add(1)
			c.bytesOut.Add(int64(len(r.payload)))
		}
		r.done <- err
	}

	reply := func(o offer, admitted bool, err error) {
		o.reply <- offerReply{admitted: admitted, err: err}
	}

	// tryLaunch hands the queued head to the idle writer. It never blocks:
	// it is only called with inflight==nil, and writeCh has capacity 1, so
	// a writer that has finished the previous frame accepts immediately.
	tryLaunch := func() {
		if inflight != nil || len(queue) == 0 {
			return
		}
		req := queue[0]
		queue = queue[1:]
		select {
		case c.writeCh <- req:
			inflight = &req
			setQueued()
		default:
			// Should not happen given the capacity-1/inflight invariant;
			// put it back rather than drop it.
			queue = append([]sendReq{req}, queue...)
		}
	}

	wakeWaiters := func() {
		for len(waiting) > 0 {
			if draining {
				o := waiting[0]
				waiting = waiting[1:]
				reply(o, false, ErrShutdown)
				continue
			}
			o := waiting[0]
			if !canFit(len(o.req.payload)) {
				return
			}
			waiting = waiting[1:]
			queue = append(queue, o.req)
			reply(o, true, nil)
		}
		setQueued()
	}

	admit := func(o offer) {
		if r := c.closedReason(); r != nil {
			reply(o, false, r)
			return
		}
		if draining {
			reply(o, false, ErrShutdown)
			return
		}
		// FIFO: once any sender is waiting, everyone joins the line so a
		// small frame cannot jump ahead of a blocked large one.
		if len(waiting) > 0 {
			if o.block {
				waiting = append(waiting, o)
			} else {
				reply(o, false, ErrBackpressure)
			}
			return
		}
		if canFit(len(o.req.payload)) {
			queue = append(queue, o.req)
			setQueued()
			reply(o, true, nil)
			tryLaunch()
			return
		}
		if o.block {
			waiting = append(waiting, o)
			return
		}
		reply(o, false, ErrBackpressure)
	}

	finish := func(reason error) {
		c.closeWith(reason)
		if drainTimer != nil {
			drainTimer.Stop()
		}
		_ = c.nc.Close() // unblocks reader and a slow writer
		for _, o := range waiting {
			reply(o, false, errOr(reason, ErrShutdown))
		}
		for i := range queue {
			queue[i].done <- errOr(reason, ErrConnClosed)
		}
		if inflight != nil {
			// The writer may still deliver a result after nc.Close; prefer
			// that definite outcome if it is already available.
			select {
			case res := <-c.resultCh:
				settle(res.req, res.err)
			default:
				inflight.done <- errOr(reason, io.ErrClosedPipe)
			}
		}
		ioWG.Wait()
	}

	for {
		select {
		case o := <-c.offerCh:
			admit(o)

		case res := <-c.resultCh:
			settle(res.req, res.err)
			inflight = nil
			c.touch()
			if res.err != nil {
				setQueued()
				finish(res.err)
				return
			}
			wakeWaiters()
			tryLaunch()
			setQueued()
			if draining && inflight == nil && len(queue) == 0 && len(waiting) == 0 {
				finish(nil)
				return
			}

		case cmd := <-c.drain:
			draining = true
			rem := waiting
			waiting = nil
			for _, o := range rem {
				reply(o, false, ErrShutdown)
			}
			if !cmd.deadline.IsZero() {
				d := time.Until(cmd.deadline)
				if d <= 0 {
					finish(ErrShutdown)
					return
				}
				t := time.NewTimer(d)
				drainTimer = t
				drainTimerC = t.C
			}
			if inflight == nil && len(queue) == 0 {
				finish(nil)
				return
			}

		case <-drainTimerC:
			finish(ErrShutdown)
			return

		case <-c.closeCh:
			finish(c.closedReason())
			return
		}
	}
}

func errOr(reason, fallback error) error {
	if reason != nil {
		return reason
	}
	return fallback
}

type handlerPanicError struct{ v interface{} }

func (e handlerPanicError) Error() string { return "lsg: frame handler panicked" }

func errHandlerPanic(v interface{}) error { return handlerPanicError{v: v} }
