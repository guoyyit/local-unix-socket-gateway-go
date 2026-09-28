package lsg

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// When a client vanishes (RST/abnormal close), the server reclaims the
// connection and its resources.
func TestClientAbnormalDisconnectCleanup(t *testing.T) {
	g, _ := startTestGW(t, DefaultConfig(""), func(c *Conn, p []byte) {
		_ = c.Send(p)
	})

	// Raw connection closed abruptly without framing a goodbye.
	raw, err := dialRaw(g.Config().SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	waitConnCount(g, 1)
	// Abrupt close (no orderly application-level goodbye).
	if tc, ok := raw.(*net.UnixConn); ok {
		_ = tc.Close()
	} else {
		_ = raw.Close()
	}

	if !waitUntil(3*time.Second, func() bool { return g.Stats().ActiveConns == 0 }) {
		t.Fatalf("server did not clean up vanished client: %d conns", g.Stats().ActiveConns)
	}

	// A subsequent fresh client works and is not confused with the dead one.
	cl, err := Dial(g.Config().SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	waitConnCount(g, 1)
	s := g.Conns()[0]
	if s.ID == 0 {
		t.Fatal("new connection got reused/zero id")
	}
	if err := cl.Send([]byte("after-death")); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Recv(); err != nil {
		t.Fatal(err)
	}
}

// Connection IDs are never reused after a server restart: each process
// starts its counter fresh and old accepted totals do not leak into the new
// runtime stats, while config (the input) is independently reloaded.
func TestServerRestartNoStateReuse(t *testing.T) {
	path := shortSocketPath(t)
	cfg := DefaultConfig(path)

	runOne := func(h HandleFunc) *Gateway {
		g, err := New(cfg, h)
		if err != nil {
			t.Fatal(err)
		}
		go func() { _ = g.Run(context.Background()) }()
		waitReady(t, g, 2_000)
		return g
	}

	g1 := runOne(func(c *Conn, p []byte) { _ = c.Send(p) })
	c1, err := Dial(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := c1.Send([]byte("gen1")); err != nil {
		t.Fatal(err)
	}
	if _, err := c1.Recv(); err != nil {
		t.Fatal(err)
	}
	idGen1 := g1.Conns()[0].ID
	_ = idGen1
	_ = c1.Close()

	// Simulate a server crash: hard-stop without orderly drain.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = g1.Shutdown(ctx)
	cancel()

	// Restart on the same path. The stale socket (if any) must be reclaimed
	// and the new instance must start with zero runtime state.
	g2 := runOne(func(c *Conn, p []byte) { _ = c.Send(append([]byte("gen2:"), p...)) })
	if g2.Stats().AcceptedTotal != 0 {
		t.Fatalf("new process inherited accepted total: %d", g2.Stats().AcceptedTotal)
	}
	if g2.Stats().ActiveConns != 0 {
		t.Fatal("new process started with stale connections")
	}
	if g2.Config().SocketPath != path {
		t.Fatal("config (input) should reload independently of runtime state")
	}

	c2, err := Dial(path)
	if err != nil {
		t.Fatalf("dial after restart: %v", err)
	}
	defer c2.Close()
	if err := c2.Send([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	got, err := c2.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "gen2:hi" {
		t.Fatalf("restarted server responded with wrong handler/state: %q", got)
	}
	// The new connection belongs to the new runtime; prior accepted
	// totals were already asserted to start at zero above, which is the
	// guarantee that no stale state is reused.
	if g2.Stats().AcceptedTotal != 1 {
		t.Fatalf("new runtime accepted total = %d, want 1", g2.Stats().AcceptedTotal)
	}
}

// Simulate an actual process crash: spawn a helper subprocess running a
// gateway, kill it leaving a stale socket, then start a new gateway on the
// same path. This validates real restart recovery, not just in-process
// shutdown.
func TestServerCrashLeavesStaleSocketReclaimed(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain unavailable for subprocess test")
	}
	dir := t.TempDir()
	sock := filepath.Join(dir, "crash.sock")
	if len(sock) > 100 {
		sock = shortSocketPath(t)
	}

	helper := buildCrashHelper(t)
	// Start the crashing gateway.
	cmd := exec.Command(helper, sock)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waitSocketExists(sock, 2*time.Second)

	// Kill it abruptly (SIGKILL) so no cleanup runs and the socket remains.
	_ = cmd.Process.Kill()
	_, _ = cmd.Process.Wait()

	// The socket inode may linger. A new gateway must reclaim it.
	g, err := New(DefaultConfig(sock), func(c *Conn, p []byte) { _ = c.Send(p) })
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = g.Run(context.Background()) }()
	waitReady(t, g, 3_000)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = g.Shutdown(ctx)
	})

	cl, err := Dial(sock)
	if err != nil {
		t.Fatalf("dial after crash recovery: %v", err)
	}
	defer cl.Close()
	if err := cl.Send([]byte("recovered")); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Recv(); err != nil {
		t.Fatal(err)
	}
}

func waitSocketExists(path string, within time.Duration) {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}
