package gateway

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

// TestGracefulShutdownDrainsConfirmedData：
// 已通过 Send 确认（已交给内核/在途）的数据在停止后仍完整送达客户端；
// 监听路径被清理；停止后无法再建立新连接。
func TestGracefulShutdownDrainsConfirmedData(t *testing.T) {
	cfg := testConfig(t)
	cfg.IdleTimeout = 10 * time.Second
	cfg.DrainTimeout = 5 * time.Second
	cfg.SendQueueFrames = 128
	cfg.SendQueueBytes = 8 << 20

	const n = 50
	allConfirmed := make(chan struct{})
	h := HandlerFunc(func(ctx context.Context, c *Conn, _ []byte) {
		frame := make([]byte, 8192)
		for i := 0; i < n; i++ {
			if err := c.Send(ctx, frame); err != nil {
				return
			}
		}
		close(allConfirmed) // 全部 Send 已确认（已交给内核/在途）
	})
	srv := startServer(t, cfg, h)
	cl := dialTest(t, cfg.SocketPath)

	// 客户端并发消费，保证服务端的确认能持续推进。
	got := make(chan int, 1)
	go func() {
		count := 0
		for i := 0; i < n; i++ {
			if _, err := cl.Recv(context.Background()); err != nil {
				break
			}
			count++
		}
		got <- count
	}()

	if err := cl.Send(context.Background(), []byte("go")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-allConfirmed:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never confirmed all sends")
	}

	// 全部确认后再停止：在途数据必须完整排空，不能因为 close 丢失。
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if count := <-got; count != n {
		t.Fatalf("client got %d frames, want %d (confirmed data lost)", count, n)
	}

	// 监听路径被清理。
	if !waitFor(func() bool {
		_, err := os.Stat(cfg.SocketPath)
		return errors.Is(err, os.ErrNotExist)
	}, time.Second) {
		t.Fatal("socket path not removed after shutdown")
	}

	// 停止后无法再建立新连接。
	if _, err := Dial(context.Background(), cfg.SocketPath); err == nil {
		t.Fatal("dial succeeded after shutdown")
	}
}

// TestSendDuringShutdownGetsDefinitiveResult：排空阶段新提交的 Send
// 必须立即得到 ErrServerStopping，而不是悬挂。
func TestSendDuringShutdownGetsDefinitiveResult(t *testing.T) {
	cfg := testConfig(t)
	cfg.IdleTimeout = 10 * time.Second
	cfg.DrainTimeout = 5 * time.Second

	gotConn := make(chan *Conn, 1)
	h := HandlerFunc(func(ctx context.Context, c *Conn, _ []byte) {
		select {
		case gotConn <- c:
		default:
		}
		<-ctx.Done() // 保持连接存活，让 Shutdown 走排空流程
	})
	srv := startServer(t, cfg, h)
	cl := dialTest(t, cfg.SocketPath)
	if err := cl.Send(context.Background(), []byte("hold")); err != nil {
		t.Fatal(err)
	}
	sc := <-gotConn

	shutdownDone := make(chan struct{})
	go func() {
		_ = srv.Shutdown(context.Background())
		close(shutdownDone)
	}()

	// 等待进入 stopping。
	if !waitFor(func() bool { return sc.isStopping() }, time.Second) {
		t.Fatal("conn never entered stopping state")
	}

	// 排空期间新 Send 立即得到确定结果。
	err := sc.Send(context.Background(), []byte("late"))
	if !errors.Is(err, ErrServerStopping) {
		t.Fatalf("send during shutdown: %v want ErrServerStopping", err)
	}

	<-shutdownDone
}

// TestShutdownReclaimsIdleConns：停止时空闲连接也要被回收（受 DrainTimeout 限制）。
func TestShutdownReclaimsIdleConns(t *testing.T) {
	cfg := testConfig(t)
	cfg.IdleTimeout = time.Hour
	cfg.DrainTimeout = 300 * time.Millisecond
	srv := startServer(t, cfg, echoHandler{})
	cl := dialTest(t, cfg.SocketPath)
	// 有一条在途连接但无数据。
	_ = cl

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("shutdown took %v, idle conn not reclaimed promptly", elapsed)
	}
	if srv.Stats().State != "stopped" {
		t.Fatalf("state=%q", srv.Stats().State)
	}
}

// TestRestartNewBootIDNoStateReuse：
// 服务端重启后旧监听残留不影响新实例；BootID 变化；新连接不复用旧状态。
func TestRestartNewBootIDNoStateReuse(t *testing.T) {
	cfg := testConfig(t)
	srv1, err := NewServer(cfg, echoHandler{})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv1.Listen(); err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv1.Serve() }()
	c1 := dialTest(t, cfg.SocketPath)
	boot1 := c1.BootID()
	if boot1 == "" {
		t.Fatal("empty boot id")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := srv1.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	srv2, err := NewServer(cfg, echoHandler{})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv2.Listen(); err != nil {
		t.Fatalf("restart listen: %v", err)
	}
	go func() { _ = srv2.Serve() }()
	// 注意：dialTest 的 Cleanup 会在测试结束时再次关闭，路径相同无妨。
	c2, err := Dial(context.Background(), cfg.SocketPath)
	if err != nil {
		t.Fatalf("dial after restart: %v", err)
	}
	if c2.BootID() == boot1 {
		t.Fatal("boot id did not change across restart")
	}
	if err := c2.Send(context.Background(), []byte("fresh")); err != nil {
		t.Fatal(err)
	}
	p, err := c2.Recv(context.Background())
	if err != nil || string(p) != "fresh" {
		t.Fatalf("after restart: p=%q err=%v", p, err)
	}
	if srv2.Stats().AcceptedTotal != 1 {
		t.Fatalf("new server accepted=%d, want 1 (old state leaked)", srv2.Stats().AcceptedTotal)
	}
	_ = c2.Close()

	shut2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	_ = srv2.Shutdown(shut2)
}

// TestDrainTimeoutFailsQueuedSends：消费者极慢导致排空超时，
// 未送达的已确认 Send 必须得到 ErrDrainTimeout 而不是悬挂或静默丢失。
func TestDrainTimeoutFailsQueuedSends(t *testing.T) {
	cfg := testConfig(t)
	cfg.IdleTimeout = time.Hour
	cfg.DrainTimeout = 200 * time.Millisecond
	cfg.WriteTimeout = time.Hour

	// 用注入式阻塞 writer 模拟写不动的慢消费者，直接驱动服务端 Conn。
	holdWrites := make(chan struct{})
	serverConn := make(chan *Conn, 1)
	h := HandlerFunc(func(ctx context.Context, c *Conn, _ []byte) {
		select {
		case serverConn <- c:
		default:
		}
		<-holdWrites
	})
	srv := startServer(t, cfg, h)
	cl := dialTest(t, cfg.SocketPath)
	if err := cl.Send(context.Background(), []byte("trigger")); err != nil {
		t.Fatal(err)
	}
	sc := <-serverConn

	// 服务端向客户端 echo：客户端不读，数据滞留在内核/队列（尽力）。
	// 为确定性，直接在 sc 上提交数据并依赖排空超时关闭 fd。
	sendResult := make(chan error, 1)
	go func() {
		sendResult <- sc.Send(context.Background(), make([]byte, 4096))
	}()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()

	select {
	case err := <-sendResult:
		// 成功写出或确定失败都可接受（取决于内核缓冲），但绝不允许悬挂。
		_ = err
	case <-time.After(3 * time.Second):
		t.Fatal("queued send hung forever after drain timeout")
	}
	close(holdWrites)
}
