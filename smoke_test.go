package lsg

import (
	"context"
	"testing"
	"time"
)

func startTestGW(t *testing.T, cfg Config, h HandleFunc) (*Gateway, context.CancelFunc) {
	t.Helper()
	cfg.SocketPath = shortSocketPath(t)
	ctx, cancel := context.WithCancel(context.Background())
	g, err := New(cfg, h)
	if err != nil {
		cancel()
		t.Fatalf("New: %v", err)
	}
	errc := make(chan error, 1)
	go func() { errc <- g.Run(ctx) }()
	select {
	case <-g.Ready():
	case err := <-errc:
		cancel()
		t.Fatalf("gateway Run exited before ready: %v", err)
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatalf("gateway never became ready")
	}
	if g.Stats().State != "running" {
		cancel()
		t.Fatalf("gateway ready but state=%s", g.Stats().State)
	}
	t.Cleanup(func() {
		ctx2, c2 := context.WithTimeout(context.Background(), 3*time.Second)
		defer c2()
		_ = g.Shutdown(ctx2)
		cancel()
	})
	return g, cancel
}

func TestSmokeEcho(t *testing.T) {
	g, _ := startTestGW(t, DefaultConfig(""), func(c *Conn, p []byte) {
		_ = c.Send(append([]byte("echo:"), p...))
	})
	_ = g
	cl, err := Dial(g.Config().SocketPath)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cl.Close()
	for _, msg := range []string{"hello", "", "world-1234567890"} {
		if err := cl.Send([]byte(msg)); err != nil {
			t.Fatalf("send %q: %v", msg, err)
		}
		got, err := cl.Recv()
		if err != nil {
			t.Fatalf("recv %q: %v", msg, err)
		}
		if string(got) != "echo:"+msg {
			t.Fatalf("mismatch: got %q want echo:%q", got, msg)
		}
	}
}
