package gateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func acquireNoEvents(sem *weightedSemaphore, n int) error {
	events := make(chan struct{})
	return sem.Acquire(context.Background(), n, events,
		func() bool { return false }, func() bool { return false })
}

func TestWeightedSemaphoreBlocksAndWakes(t *testing.T) {
	sem := newWeightedSemaphore(10)
	if err := acquireNoEvents(sem, 6); err != nil {
		t.Fatal(err)
	}
	got := make(chan error, 1)
	go func() { got <- acquireNoEvents(sem, 5) }()
	time.Sleep(30 * time.Millisecond)
	select {
	case err := <-got:
		t.Fatalf("acquire should block under backpressure, got %v", err)
	default:
	}
	sem.Release(6)
	select {
	case err := <-got:
		if err != nil {
			t.Fatalf("acquire after release: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("acquire was not woken after release")
	}
}

func TestWeightedSemaphoreOversize(t *testing.T) {
	sem := newWeightedSemaphore(4)
	if err := acquireNoEvents(sem, 5); !errors.Is(err, ErrSendQueueFull) {
		t.Fatalf("got %v want ErrSendQueueFull", err)
	}
}

func TestWeightedSemaphoreClosedAndStopping(t *testing.T) {
	sem := newWeightedSemaphore(2)
	if err := acquireNoEvents(sem, 2); err != nil {
		t.Fatal(err)
	}

	stopping := make(chan struct{})
	var isStopping atomic.Bool
	errc := make(chan error, 1)
	go func() {
		errc <- sem.Acquire(context.Background(), 1, stopping,
			func() bool { return false }, func() bool { return isStopping.Load() })
	}()
	time.Sleep(30 * time.Millisecond)
	isStopping.Store(true)
	close(stopping)
	if err := <-errc; !errors.Is(err, errStopping) {
		t.Fatalf("stopping: got %v", err)
	}

	closed := make(chan struct{})
	var isClosed atomic.Bool
	go func() {
		errc <- sem.Acquire(context.Background(), 1, closed,
			func() bool { return isClosed.Load() }, func() bool { return false })
	}()
	time.Sleep(30 * time.Millisecond)
	isClosed.Store(true)
	close(closed)
	if err := <-errc; !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("closed: got %v", err)
	}
}

// newTestConn 构造不持有真实 fd 的连接（writer 可注入），仅启动写循环，
// 用于确定性地验证有界队列、背压与排空，不依赖各平台 socket 缓冲行为。
func newTestConn(t *testing.T, cfg Config, w io.Writer) *Conn {
	t.Helper()
	cfg, err := cfg.Validate()
	if err != nil {
		t.Fatal(err)
	}
	c := newConn(nil, cfg, PeerCred{}, "test-boot")
	c.writer = w
	c.wg.Add(1)
	writeDone := make(chan struct{})
	go func() {
		c.writeLoop()
		close(writeDone)
	}()
	t.Cleanup(func() {
		c.fail(ErrConnectionClosed)
		<-writeDone // 等待写循环退出，避免测试结束后仍有 goroutine 操作队列
	})
	return c
}

// gateWriter 模拟慢/停住的消费者：block 非 nil 时写操作阻塞到其关闭；
// 解封后按普通 buffer 完整接收，便于校验送达内容与顺序。
type gateWriter struct {
	mu       sync.Mutex
	block    chan struct{}
	received bytes.Buffer
}

func newGateWriter() *gateWriter {
	g := &gateWriter{block: make(chan struct{})}
	return g
}

func (g *gateWriter) Write(p []byte) (int, error) {
	g.mu.Lock()
	block := g.block
	g.mu.Unlock()
	if block != nil {
		<-block
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.received.Write(p)
}

func (g *gateWriter) unblock() {
	g.mu.Lock()
	b := g.block
	g.block = nil
	g.mu.Unlock()
	if b != nil {
		close(b)
	}
}

func (g *gateWriter) bytes() []byte {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.received.Bytes()
}

// TestConnBackpressureBlocksAndNoLoss：队列满时 Send 阻塞等待（背压），
// 消费者恢复后全部帧完整有序送达，无静默丢失。
func TestConnBackpressureBlocksAndNoLoss(t *testing.T) {
	cfg := Config{
		SocketPath:      "/tmp/x.sock",
		AllowedUIDs:     []uint32{0},
		SendQueueFrames: 2,
		SendQueueBytes:  1 << 20,
	}
	gw := newGateWriter()
	c := newTestConn(t, cfg, gw)

	codec := NewFrameCodec(cfg.MaxFrameSize)
	payloads := [][]byte{[]byte("f0"), []byte("f1"), []byte("f2"), []byte("f3")}
	frames := make([][]byte, len(payloads))
	for i, p := range payloads {
		f, _ := codec.Encode(p)
		frames[i] = f
	}

	done := make(chan error, len(payloads))
	for i := range payloads {
		payload := bytes.Clone(payloads[i])
		go func() { done <- c.Send(context.Background(), payload) }()
	}

	// 只有 2 帧能进入队列：第 3、4 帧必须因背压等待，不能提前成功返回。
	time.Sleep(150 * time.Millisecond)
	if n := countDone(done); n != 0 {
		t.Fatalf("no Send should complete while writer blocked, got %d", n)
	}

	gw.unblock()
	for i := 0; i < len(payloads); i++ {
		if err := <-done; err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}

	// 校验送达字节包含四帧完整数据（并发入队不保证顺序，但不允许丢帧或损坏）。
	delivered := gw.bytes()
	codec2 := NewFrameCodec(cfg.MaxFrameSize)
	hdr := make([]byte, frameHeaderSize)
	gotPayloads := map[string]int{}
	for len(delivered) > 0 {
		p, err := codec2.ReadFrame(bytes.NewReader(delivered), hdr)
		if err != nil {
			t.Fatalf("decode delivered frame: %v", err)
		}
		gotPayloads[string(p)]++
		f, _ := codec2.Encode(p)
		delivered = delivered[len(f):]
	}
	for _, p := range payloads {
		if gotPayloads[string(p)] != 1 {
			t.Fatalf("payload %q delivered %d times, want 1", p, gotPayloads[string(p)])
		}
	}
}

func countDone[T any](ch chan T) int {
	n := 0
	for {
		select {
		case <-ch:
			n++
		default:
			return n
		}
	}
}

// TestConnBackpressureByteLimit：字节总量打满同样施加背压。
func TestConnBackpressureByteLimit(t *testing.T) {
	cfg := Config{
		SocketPath:      "/tmp/x.sock",
		AllowedUIDs:     []uint32{0},
		SendQueueFrames: 100,
		SendQueueBytes:  frameHeaderSize + 10, // 仅够一帧 10 字节 payload
	}
	gw := newGateWriter()
	c := newTestConn(t, cfg, gw)

	done := make(chan error, 2)
	go func() { done <- c.Send(context.Background(), make([]byte, 10)) }()
	time.Sleep(50 * time.Millisecond)
	go func() { done <- c.Send(context.Background(), make([]byte, 10)) }()
	time.Sleep(150 * time.Millisecond)
	if n := countDone(done); n != 0 {
		t.Fatalf("second send should block on byte limit, completed=%d", n)
	}
	gw.unblock()
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
}

// TestConnBackpressureCtxCancel：等待令牌期间可被 ctx 取消，之后连接仍可用。
func TestConnBackpressureCtxCancel(t *testing.T) {
	cfg := Config{
		SocketPath:      "/tmp/x.sock",
		AllowedUIDs:     []uint32{0},
		SendQueueFrames: 1,
		SendQueueBytes:  1 << 20,
	}
	gw := newGateWriter()
	c := newTestConn(t, cfg, gw)

	// 占住唯一帧位（writer 阻塞 -> 帧令牌不归还）。
	go func() { _ = c.Send(context.Background(), []byte("hold")) }()
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- c.Send(ctx, []byte("wait")) }()
	time.Sleep(100 * time.Millisecond)
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v want context.Canceled", err)
	}

	// 解封后连接仍然可正常发送。
	gw.unblock()
	if err := c.Send(context.Background(), []byte("after")); err != nil {
		t.Fatalf("send after cancel: %v", err)
	}
}
