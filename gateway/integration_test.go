package gateway

import (
	"context"
	"sync"
	"syscall"
	"testing"
	"time"
)

// TestMultipleClientsConcurrent：多个本地客户端并发收发，互不影响。
func TestMultipleClientsConcurrent(t *testing.T) {
	cfg := testConfig(t)
	cfg.IdleTimeout = 10 * time.Second
	startServer(t, cfg, echoHandler{})

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cl := dialTest(t, cfg.SocketPath)
			for j := 0; j < 20; j++ {
				msg := []byte{byte(id), byte(j)}
				if err := cl.Send(context.Background(), msg); err != nil {
					t.Errorf("client %d send %d: %v", id, j, err)
					return
				}
				got, err := cl.Recv(context.Background())
				if err != nil {
					t.Errorf("client %d recv %d: %v", id, j, err)
					return
				}
				if len(got) != 2 || got[0] != byte(id) || got[1] != byte(j) {
					t.Errorf("client %d frame %d corrupted: %v", id, j, got)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}

// TestGIDOnlyPolicy：仅按用户组白名单放行当前用户。
func TestGIDOnlyPolicy(t *testing.T) {
	cfg := testConfig(t)
	cfg.AllowedUIDs = nil
	cfg.AllowedGIDs = []uint32{uint32(syscall.Getgid())}
	startServer(t, cfg, echoHandler{})
	cl := dialTest(t, cfg.SocketPath)
	if err := cl.Send(context.Background(), []byte("g")); err != nil {
		t.Fatal(err)
	}
	if p, err := cl.Recv(context.Background()); err != nil || string(p) != "g" {
		t.Fatalf("p=%q err=%v", p, err)
	}
}

// TestBootIDStableWithinServerLifetime：同一服务器实例上所有连接 BootID 一致。
func TestBootIDStableWithinServerLifetime(t *testing.T) {
	cfg := testConfig(t)
	srv := startServer(t, cfg, echoHandler{})
	var ids []string
	for i := 0; i < 3; i++ {
		cl := dialTest(t, cfg.SocketPath)
		ids = append(ids, cl.BootID())
	}
	for _, id := range ids {
		if id != srv.Stats().BootID {
			t.Fatalf("boot id mismatch: %q vs %q", id, srv.Stats().BootID)
		}
	}
}

// TestLongFrameEcho：跨越多个内核缓冲的大帧完整往返。
func TestLongFrameEcho(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxFrameSize = 1 << 20
	startServer(t, cfg, echoHandler{})
	cl := dialTest(t, cfg.SocketPath, WithMaxFrameSize(1<<20))

	payload := make([]byte, 256*1024)
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	if err := cl.Send(context.Background(), payload); err != nil {
		t.Fatal(err)
	}
	got, err := cl.Recv(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(payload) {
		t.Fatalf("length %d want %d", len(got), len(payload))
	}
	for i := range got {
		if got[i] != payload[i] {
			t.Fatalf("byte %d mismatch", i)
		}
	}
}
