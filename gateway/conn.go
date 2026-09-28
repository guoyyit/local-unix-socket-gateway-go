package gateway

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

// outMsg 是发送队列中的一条待写帧。
type outMsg struct {
	// frame 已包含帧头，可直接写入内核。
	frame []byte
	// result 缓冲为 1：写出结果恰好投递一次，写入方永不阻塞。
	// nil 表示整帧已写入内核；非 nil 表示确定失败原因。
	result chan error
}

// connCtx 携带连接关闭信号的 context。
type connCtx struct {
	context.Context
	done chan struct{}
}

func (c connCtx) Done() <-chan struct{} { return c.done }

// Conn 是服务端侧的单连接句柄，对 Handler 暴露。
type Conn struct {
	raw    *net.UnixConn
	writer io.Writer // 默认等于 raw；测试可替换为确定性的可控 writer
	codec  *FrameCodec
	cfg    Config
	peer   PeerCred
	bootID string

	frameTokens chan struct{}      // 帧计数背压令牌，容量=SendQueueFrames
	byteSem     *weightedSemaphore // 字节背压，容量=SendQueueBytes
	sendQ       chan *outMsg       // 实际发送队列
	ctx         context.Context    // 关闭时取消，供 Handler 感知
	cancelCtx   context.CancelFunc

	idleMu      sync.Mutex
	lastActive  time.Time
	idleTimeout time.Duration

	stateMu  sync.Mutex
	stopping bool // 优雅停止中：拒绝新 Send，已入队数据等待排空
	closed   bool

	closeOnce    sync.Once
	rawCloseOnce sync.Once
	closedSig    chan struct{}
	doneSig      chan struct{} // 读写循环全部退出后关闭
	drain        chan struct{} // beginDrain 时关闭
	events       chan struct{} // 关闭或进入排空时关闭，统一唤醒背压等待
	eventsOnce   sync.Once
	closeReason  atomicError

	wg sync.WaitGroup // 读循环、写循环、空闲看门狗
}

// newConn 创建连接句柄（尚未启动读写循环）。
func newConn(raw *net.UnixConn, cfg Config, peer PeerCred, bootID string) *Conn {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Conn{
		raw:         raw,
		writer:      raw,
		codec:       NewFrameCodec(cfg.MaxFrameSize),
		cfg:         cfg,
		peer:        peer,
		bootID:      bootID,
		frameTokens: make(chan struct{}, cfg.SendQueueFrames),
		byteSem:     newWeightedSemaphore(cfg.SendQueueBytes),
		sendQ:       make(chan *outMsg, cfg.SendQueueFrames),
		ctx:         ctx,
		cancelCtx:   cancel,
		closedSig:   make(chan struct{}),
		doneSig:     make(chan struct{}),
		drain:       make(chan struct{}),
		events:      make(chan struct{}),
		lastActive:  time.Now(),
		idleTimeout: cfg.IdleTimeout,
	}
	c.ctx = connCtx{Context: ctx, done: c.closedSig}
	return c
}

// serve 启动读循环、写循环与空闲租约看门狗，阻塞直到连接终止。
func (c *Conn) serve(handler Handler) {
	c.wg.Add(3)
	go c.readLoop(handler)
	go c.writeLoop()
	go c.idleLoop()
	c.wg.Wait()
	// 连接结束：取消 handler context，唤醒所有等待者并释放底层 fd。
	c.cancelCtx()
	c.closeRaw()
	close(c.doneSig)
}

// Context 返回随连接关闭而取消的 context。
func (c *Conn) Context() context.Context { return c.ctx }

// Done 在连接终止后关闭。
func (c *Conn) Done() <-chan struct{} { return c.closedSig }

// Err 返回连接终止原因（未终止时为 nil）。
func (c *Conn) Err() error { return c.closeReason.Load() }

// Peer 返回连接对端身份。
func (c *Conn) Peer() PeerCred { return c.peer }

// BootID 返回服务端启动代际标识。
func (c *Conn) BootID() string { return c.bootID }

// touch 记录一次成功收发活动，续租空闲租约。
func (c *Conn) touch() {
	c.idleMu.Lock()
	c.lastActive = time.Now()
	c.idleMu.Unlock()
}

// idleLoop 是空闲租约看门狗：
// 超过 IdleTimeout 没有任何收发活动即回收连接并释放资源。
func (c *Conn) idleLoop() {
	defer c.wg.Done()
	if c.idleTimeout <= 0 {
		return
	}
	for {
		c.idleMu.Lock()
		wait := c.idleTimeout - time.Since(c.lastActive)
		c.idleMu.Unlock()
		if wait < 0 {
			c.fail(ErrIdleLeaseExpired)
			return
		}
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
			c.idleMu.Lock()
			idle := time.Since(c.lastActive) >= c.idleTimeout
			c.idleMu.Unlock()
			if idle {
				c.fail(ErrIdleLeaseExpired)
				return
			}
		case <-c.closedSig:
			timer.Stop()
			return
		}
	}
}

// readLoop 持续解码帧（处理半包/粘包），损坏帧立即终止本连接，
// 不影响服务器上的其他连接。
func (c *Conn) readLoop(handler Handler) {
	defer c.wg.Done()
	header := make([]byte, frameHeaderSize)
	for {
		payload, err := c.codec.ReadFrame(c.raw, header)
		if err != nil {
			if !c.isClosed() {
				c.fail(classifyReadErr(err))
			}
			return
		}
		c.touch()
		// 单连接内消息串行处理；handler 可通过 Send 异步投递任意数量回复。
		handler.HandleMessage(c.ctx, c, payload)
	}
}

// writeLoop 从有界队列取帧并完整写出；优雅停止开始后改为排空模式。
//
// 设计要点：写操作使用阻塞模式（无写截止时间），慢消费者通过有界队列
// 施加背压而非用超时杀连接；连接关闭（含排空超时强杀）时关闭底层 fd，
// 正在进行的阻塞写会被立即打断并得到确定错误，不会泄漏 goroutine。
func (c *Conn) writeLoop() {
	defer c.wg.Done()
	for {
		select {
		case msg := <-c.sendQ:
			c.writeOne(msg)
		case <-c.drainSig():
			c.drainRemaining()
			return
		case <-c.closedSig:
			// 连接被关闭时不会再有正在进行的写（关闭 fd 会让写循环走到这里）。
			c.abortAll(c.closedErr())
			return
		}
	}
}

// drainSig 返回优雅排空信号（stopping 时由 beginDrain 关闭）。
// 通道在连接构造时就固定，避免反复新建导致 select 饥饿。
func (c *Conn) drainSig() <-chan struct{} { return c.drain }

// writeOne 阻塞写出单帧（循环处理短写），归还背压令牌，并向 Send 投递
// 唯一一次确定结果：成功为 nil，失败为确定原因。
// 写出期间若连接被关闭（如排空超时强杀），底层 fd 关闭让 writeRaw 立即失败，
// 返回连接的确定终止原因（如 ErrDrainTimeout），而不是底层的 closed 错误。
func (c *Conn) writeOne(msg *outMsg) {
	err := writeRaw(c.writer, msg.frame)
	c.releaseTokens(len(msg.frame))
	if err == nil {
		c.touch()
		msg.result <- nil // buffered channel，永不阻塞
		return
	}
	if !c.isClosed() {
		// 真实写错误（对端关闭、fd 异常等）：终结连接。
		c.fail(err)
	}
	msg.result <- c.closedErr()
}

// drainRemaining 排空模式：依次写出队列中所有“已确认入队”的帧。
// 排空期间不再 select drain/closed 信号——是否超时由 Shutdown 外部决定：
// 超时后它调用 fail 关闭 fd，当前阻塞写立即失败，后续帧也快速失败，
// 绝不让任何已入队 Send 悬挂。
func (c *Conn) drainRemaining() {
	for len(c.sendQ) > 0 {
		if c.isClosed() {
			c.abortAll(c.closedErr())
			return
		}
		msg := <-c.sendQ
		c.writeOne(msg)
	}
	// 队列已空：已确认数据全部写出，安静结束本连接。
	c.fail(ErrServerStopping)
}

// abortAll 让队列中剩余帧全部得到确定的失败结果并归还令牌。
func (c *Conn) abortAll(err error) {
	for {
		select {
		case msg := <-c.sendQ:
			c.releaseTokens(len(msg.frame))
			msg.result <- err
			continue
		default:
			return
		}
	}
}

// releaseTokens 归还帧计数与字节背压令牌。
func (c *Conn) releaseTokens(frameLen int) {
	<-c.frameTokens
	c.byteSem.Release(frameLen)
}

// Send 提交一帧待发送数据：
//   - 队列达到背压上限时按 ctx 阻塞等待，绝不静默丢帧；
//   - 优雅停止开始后新提交立即返回 ErrServerStopping（确定结果）；
//   - 一旦入队成功，该帧一定会被写出或得到确定错误（关闭/排空超时）；
//   - ctx 在排队等待期间取消会直接放弃入队；入队之后取消不再影响数据去向，
//     但调用方可能提前收到 ctx 错误，真实结果由内部回收。
func (c *Conn) Send(ctx context.Context, payload []byte) error {
	if uint32(len(payload)) > c.cfg.MaxFrameSize {
		return ErrFrameTooLarge
	}
	if c.isClosed() {
		return c.closedErr()
	}
	if c.isStopping() {
		return ErrServerStopping
	}

	// 第一级背压：帧计数。
	select {
	case c.frameTokens <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closedSig:
		return c.closedErr()
	}
	frameSize := frameHeaderSize + len(payload)
	// 第二级背压：排队字节总量。
	if err := c.byteSem.Acquire(ctx, frameSize, c.events, c.isClosed, c.isStopping); err != nil {
		<-c.frameTokens
		if errors.Is(err, errStopping) {
			return ErrServerStopping
		}
		return err
	}

	msg := &outMsg{
		frame:  mustEncode(c.codec, payload),
		result: make(chan error, 1),
	}

	// 获取令牌后、入队前再次检查终止状态，避免泄漏令牌。
	c.stateMu.Lock()
	if c.closed {
		c.stateMu.Unlock()
		c.releaseTokens(frameSize)
		return c.closedErr()
	}
	if c.stopping {
		c.stateMu.Unlock()
		c.releaseTokens(frameSize)
		return ErrServerStopping
	}
	c.sendQ <- msg // 持有帧令牌，容量必然足够，不阻塞
	c.stateMu.Unlock()

	select {
	case err := <-msg.result:
		return err
	case <-ctx.Done():
		// 帧仍会按 drain 规则被写出或失败，结果不能丢弃：后台回收。
		go func() { <-msg.result }()
		return ctx.Err()
	}
}

func (c *Conn) isStopping() bool {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.stopping || c.closed
}

func (c *Conn) isClosed() bool {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.closed
}

func (c *Conn) closedErr() error {
	if err := c.Err(); err != nil {
		return err
	}
	return ErrConnectionClosed
}

// beginDrain 进入优雅停止：拒绝新 Send，写循环排空已入队数据。
func (c *Conn) beginDrain() {
	c.stateMu.Lock()
	if c.closed || c.stopping {
		c.stateMu.Unlock()
		return
	}
	c.stopping = true
	d := c.drain
	c.stateMu.Unlock()
	close(d)
	c.signalEvents()
}

// signalEvents 关闭统一事件通道（幂等），唤醒所有背压等待者。
func (c *Conn) signalEvents() {
	c.eventsOnce.Do(func() { close(c.events) })
}

// closeRaw 幂等关闭底层 fd，避免 fail 与 serve 收尾之间重复关闭
// （fd 号可能被复用，重复 Close 有误关无关文件的风险）。
func (c *Conn) closeRaw() {
	c.rawCloseOnce.Do(func() {
		if c.raw != nil {
			_ = c.raw.Close()
		}
	})
}

// fail 以确定原因终止连接（幂等）。
func (c *Conn) fail(reason error) {
	c.closeOnce.Do(func() {
		if reason == nil {
			reason = ErrConnectionClosed
		}
		c.closeReason.Store(reason)
		c.stateMu.Lock()
		c.closed = true
		c.stateMu.Unlock()
		c.signalEvents()
		c.cancelCtx()
		if c.raw != nil {
			_ = c.raw.Close()
		}
		close(c.closedSig)
	})
}

// Close 立即终止连接；队列中尚未写出的帧得到 ErrConnectionClosed。
func (c *Conn) Close() error {
	c.fail(ErrConnectionClosed)
	<-c.doneSig
	return nil
}

// waitDone 等待内部循环完全退出（供服务器注销使用）。
func (c *Conn) waitDone() { <-c.doneSig }

// classifyReadErr 把网络读错误归类：超时（含租约关闭导致的读失败）等。
func classifyReadErr(err error) error {
	if errors.Is(err, ErrFrameCorrupt) {
		return ErrFrameCorrupt
	}
	return err
}

func mustEncode(codec *FrameCodec, payload []byte) []byte {
	frame, err := codec.Encode(payload)
	if err != nil {
		// Send 入口已检查长度，理论不可达。
		panic(err)
	}
	return frame
}

// leaseDeadline 计算租约到期时间（供测试与诊断使用）。
func leaseDeadline(lastActive time.Time, idleTimeout time.Duration) time.Time {
	return lastActive.Add(idleTimeout)
}
