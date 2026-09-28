package lsg

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

// After Shutdown the listen path is removed and new dials fail.
func TestShutdownRemovesPathAndStopsAccept(t *testing.T) {
	cfg := DefaultConfig(shortSocketPath(t))
	cfg.DrainTimeout = time.Second
	path := cfg.SocketPath
	g, err := New(cfg, func(c *Conn, p []byte) {})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = g.Run(context.Background()) }()
	waitReady(t, g, 2_000)

	sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := g.Shutdown(sctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("listen path not removed: %v", err)
	}
	if _, err := Dial(path); err == nil {
		t.Fatal("dial after shutdown should fail")
	}
}

// Admitted sends to a slow consumer are drained during shutdown and fully
// delivered, within the drain timeout; the consumer gets every byte.
func TestShutdownDrainsAdmittedData(t *testing.T) {
	cfg := DefaultConfig("")
	cfg.MaxQueuedBytes = 1 << 20
	cfg.DrainTimeout = 3 * time.Second
	g, _ := startTestGW(t, cfg, func(c *Conn, p []byte) {})

	cl, err := Dial(g.Config().SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	srv := waitConnCount(g, 1)[0]

	const n = 20
	delivered := make(chan error, n)
	go func() {
		for i := 0; i < n; i++ {
			delivered <- srv.Send([]byte("frame-" + itoa(i)))
		}
	}()
	// Give sends time to be admitted, then shut down while consumer drains.
	time.Sleep(100 * time.Millisecond)
	sctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	done := make(chan struct{})
	go func() { _ = g.Shutdown(sctx); close(done) }()

	got := map[string]bool{}
	deadline := time.Now().Add(4 * time.Second)
	for len(got) < n && time.Now().Before(deadline) {
		b, err := cl.Recv()
		if err != nil {
			break
		}
		got[string(b)] = true
	}
	cancel()
	<-done
	if len(got) != n {
		t.Fatalf("drain lost data: got %d/%d frames", len(got), n)
	}
	for i := 0; i < n; i++ {
		if !got["frame-"+itoa(i)] {
			t.Fatalf("missing frame-%d after drain", i)
		}
	}
	for i := 0; i < n; i++ {
		if err := <-delivered; err != nil {
			t.Fatalf("admitted send %d got error: %v", i, err)
		}
	}
}

// A send submitted after shutdown begins gets a definite error rather than
// hanging.
func TestShutdownNewSendGetsDefiniteError(t *testing.T) {
	cfg := DefaultConfig("")
	cfg.IdleLease = 100 * time.Millisecond
	g, _ := startTestGW(t, cfg, func(c *Conn, p []byte) {})

	cl, err := Dial(g.Config().SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	srv := waitConnCount(g, 1)[0]

	// Block the connection on a slow peer so shutdown cannot drain fast;
	// then new sends must be rejected with ErrShutdown.
	sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	shdone := make(chan struct{})
	go func() {
		// First saturate kernel buffer by admitting a payload and never
		// reading; shutdown then cancels after drain timeout.
		_ = srv.TrySend(make([]byte, 1<<16))
		_ = g.Shutdown(sctx)
		close(shdone)
	}()
	time.Sleep(100 * time.Millisecond)
	err = srv.Send([]byte("during-shutdown"))
	cancel()
	<-shdone
	if err == nil {
		t.Fatal("send during shutdown must return a definite error")
	}
	if !errors.Is(err, ErrShutdown) && !errors.Is(err, ErrConnClosed) &&
		!errors.Is(err, ErrIdleLeaseExpired) {
		t.Fatalf("unexpected error during shutdown: %v", err)
	}
}

// Shutdown is idempotent.
func TestShutdownIdempotent(t *testing.T) {
	path := shortSocketPath(t)
	g, err := New(DefaultConfig(path), func(c *Conn, p []byte) {})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = g.Run(context.Background()) }()
	waitReady(t, g, 2_000)
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := g.Shutdown(ctx); err != nil {
			cancel()
			t.Fatalf("shutdown #%d: %v", i, err)
		}
		cancel()
	}
}
