package gateway

import (
	"context"
	"testing"
	"time"
)

// TestIdleLeaseExpires：连接在 IdleTimeout 内无任何收发活动时被服务端回收。
func TestIdleLeaseExpires(t *testing.T) {
	cfg := testConfig(t)
	cfg.IdleTimeout = 200 * time.Millisecond
	cfg.DrainTimeout = time.Second
	srv := startServer(t, cfg, echoHandler{})
	cl := dialTest(t, cfg.SocketPath)

	// 不发送任何消息，等待租约到期；服务端关闭后客户端读得到确定 EOF。
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := cl.Recv(ctx); err == nil {
		t.Fatal("expected error after idle lease expiry, got nil")
	}

	if !waitFor(func() bool { return srv.Stats().ActiveConns == 0 }, 2*time.Second) {
		t.Fatalf("idle conn not reclaimed, active=%d", srv.Stats().ActiveConns)
	}
}

// TestIdleLeaseRenewedByTraffic：持续有收发活动时连接不会被租约回收。
func TestIdleLeaseRenewedByTraffic(t *testing.T) {
	cfg := testConfig(t)
	cfg.IdleTimeout = 150 * time.Millisecond
	startServer(t, cfg, echoHandler{})
	cl := dialTest(t, cfg.SocketPath)

	ctx := context.Background()
	deadline := time.Now().Add(700 * time.Millisecond)
	for time.Now().Before(deadline) {
		if err := cl.Send(ctx, []byte("ping")); err != nil {
			t.Fatalf("send: %v", err)
		}
		if _, err := cl.Recv(ctx); err != nil {
			t.Fatalf("recv: %v", err)
		}
		time.Sleep(60 * time.Millisecond) // 小于租约，持续续租
	}
}
