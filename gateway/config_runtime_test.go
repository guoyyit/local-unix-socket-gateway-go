package gateway

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestConfigAffectsNewConnsOnly：更新租约后，新连接用新值，旧连接保留快照。
func TestConfigAffectsNewConnsOnly(t *testing.T) {
	cfg := testConfig(t)
	cfg.IdleTimeout = 5 * time.Second // 旧连接长租约
	srv := startServer(t, cfg, echoHandler{})

	old := dialTest(t, cfg.SocketPath)

	// 动态缩短租约。
	if err := srv.UpdateConfig(func(c *Config) { c.IdleTimeout = 150 * time.Millisecond }); err != nil {
		t.Fatal(err)
	}
	if got := srv.Config().IdleTimeout; got != 150*time.Millisecond {
		t.Fatalf("current idle=%v want 150ms", got)
	}

	// 新连接采用短租约：空闲约 150ms 被回收。
	fresh := dialTest(t, cfg.SocketPath)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := fresh.Recv(ctx); err == nil {
		t.Fatal("new conn should be reclaimed by the shortened lease")
	}

	// 旧连接仍持有长租约快照，在 400ms 后依然可收发。
	time.Sleep(400 * time.Millisecond)
	if err := old.Send(context.Background(), []byte("still-alive")); err != nil {
		t.Fatalf("old conn snapshot lease broken: %v", err)
	}
	if _, err := old.Recv(context.Background()); err != nil {
		t.Fatalf("old conn recv: %v", err)
	}
}

// TestConfigBackpressureAppliesToNewConns：新帧上限只作用于新连接：
// 更新后建立的连接发送超限帧被服务端终止；更新前建立的连接仍用旧上限。
func TestConfigBackpressureAppliesToNewConns(t *testing.T) {
	cfg := testConfig(t)
	cfg.IdleTimeout = 10 * time.Second
	cfg.MaxFrameSize = 4096
	srv := startServer(t, cfg, echoHandler{})

	// 更新前建立的连接使用旧上限。
	old := dialTest(t, cfg.SocketPath, WithMaxFrameSize(4096))

	if err := srv.UpdateConfig(func(c *Config) { c.MaxFrameSize = 32 }); err != nil {
		t.Fatal(err)
	}

	// 新连接（原始层）发 33 字节帧 -> 超过新上限，被服务端终止。
	raw := rawDial(t, cfg.SocketPath)
	serverCodec := NewFrameCodec(4096) // 客户端用旧上限编码才能发出 33 字节
	hdr := make([]byte, frameHeaderSize)
	if _, err := serverCodec.ReadFrame(raw, hdr); err != nil {
		t.Fatalf("handshake: %v", err)
	}
	big := make([]byte, 33)
	frame, err := serverCodec.Encode(big)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Write(frame); err != nil {
		t.Fatal(err)
	}
	_ = raw.SetReadDeadline(deadlineIn(2 * time.Second))
	buf := make([]byte, 16)
	if _, err := raw.Read(buf); err == nil {
		t.Fatal("oversize frame should terminate the new connection")
	}

	// 旧连接仍允许 4096 以内的帧。
	if err := old.Send(context.Background(), make([]byte, 100)); err != nil {
		t.Fatalf("old conn rejected by new frame limit: %v", err)
	}
	if p, err := old.Recv(context.Background()); err != nil || len(p) != 100 {
		t.Fatalf("old conn recv: len=%d err=%v", len(p), err)
	}
}

// TestConfigInvalidUpdateRejected：非法更新被整体拒绝，旧配置继续生效。
func TestConfigInvalidUpdateRejected(t *testing.T) {
	cfg := testConfig(t)
	srv := startServer(t, cfg, echoHandler{})
	before := srv.Config()

	if err := srv.UpdateConfig(func(c *Config) { c.SendQueueFrames = -1 }); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("got %v want ErrInvalidConfig", err)
	}
	if got := srv.Config().SendQueueFrames; got != before.SendQueueFrames {
		t.Fatalf("old config mutated: frames=%d want %d", got, before.SendQueueFrames)
	}

	// 服务仍可用。
	cl := dialTest(t, cfg.SocketPath)
	if err := cl.Send(context.Background(), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Recv(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestStatsDistinctFromConfig：运行态（BootID/State/计数）与配置通过不同入口获取，
// 且重启后 BootID 改变。
func TestStatsDistinctFromConfigAndRestart(t *testing.T) {
	cfg := testConfig(t)
	srv := startServer(t, cfg, echoHandler{})
	cl := dialTest(t, cfg.SocketPath)
	if err := cl.Send(context.Background(), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Recv(context.Background()); err != nil {
		t.Fatal(err)
	}

	st := srv.Stats()
	if st.State != "running" {
		t.Fatalf("state=%q", st.State)
	}
	if st.BootID == "" {
		t.Fatal("empty boot id in stats")
	}
	if st.BootID != cl.BootID() {
		t.Fatalf("stats boot=%q client boot=%q", st.BootID, cl.BootID())
	}
	if st.AcceptedTotal < 1 {
		t.Fatalf("accepted=%d", st.AcceptedTotal)
	}
	// 配置对象不含运行态字段，二者类型与入口均不同。
	_ = srv.Config()
}
