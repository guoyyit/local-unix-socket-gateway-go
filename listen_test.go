package lsg

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
)

// A stale (unreferenced) socket file left by a crashed process must be
// safely reclaimed.
func TestStaleSocketReclaimed(t *testing.T) {
	path := shortSocketPath(t)
	l1, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(path); err != nil || fi.Mode()&os.ModeSocket == 0 {
		t.Fatalf("socket not present: %v %v", fi, err)
	}
	// Simulate crash: close listener but leave the inode, and disable
	// the runtime's auto-unlink.
	l1.(*net.UnixListener).SetUnlinkOnClose(false)
	_ = l1.Close()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stale socket should still exist: %v", err)
	}

	g, err := New(DefaultConfig(path), func(c *Conn, p []byte) {})
	if err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- g.Run(context.Background()) }()
	waitReady(t, g, 2_000)

	cl, err := Dial(path)
	if err != nil {
		t.Fatalf("dial after reclaim: %v", err)
	}
	_ = cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2_000_000_000)
	defer cancel()
	if err := g.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("socket path should be cleaned up, stat err=%v", err)
	}
}

// A socket currently held by a live listener must refuse startup.
func TestLiveSocketRefused(t *testing.T) {
	path := shortSocketPath(t)
	g1, err := New(DefaultConfig(path), func(c *Conn, p []byte) {})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = g1.Run(context.Background()) }()
	waitReady(t, g1, 2_000)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2_000_000_000)
		defer cancel()
		_ = g1.Shutdown(ctx)
	})

	g2, err := New(DefaultConfig(path), func(c *Conn, p []byte) {})
	if err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- g2.Run(context.Background()) }()
	select {
	case err := <-errc:
		if !errors.Is(err, ErrSocketInUse) {
			t.Fatalf("want ErrSocketInUse, got %v", err)
		}
	case <-after(2_000_000_000):
		t.Fatal("second Run did not fail on live socket")
	}
}

// A non-socket at the path must not be silently replaced.
func TestNonSocketPathNotReplaced(t *testing.T) {
	path := shortSocketPath(t)
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	g, err := New(DefaultConfig(path), func(c *Conn, p []byte) {})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Run(context.Background()); err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2_000_000_000)
		defer cancel()
		_ = g.Shutdown(ctx)
		t.Fatal("expected startup to fail over a non-socket path")
	}
	data, _ := os.ReadFile(path)
	if string(data) != "x" {
		t.Fatal("regular file was modified or removed")
	}
}

// The listening socket must honor the configured permission mode.
func TestSocketModeApplied(t *testing.T) {
	path := shortSocketPath(t)
	cfg := DefaultConfig(path)
	cfg.SocketMode = 0o640
	g, err := New(cfg, func(c *Conn, p []byte) {})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = g.Run(context.Background()) }()
	waitReady(t, g, 2_000)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2_000_000_000)
		defer cancel()
		_ = g.Shutdown(ctx)
	})
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o640 {
		t.Fatalf("perm = %o, want 0o640", perm)
	}
}
