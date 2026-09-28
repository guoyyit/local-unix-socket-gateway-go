package lsg

import (
	"os"
	"sync/atomic"
	"testing"
	"time"
)

var (
	testPathN  atomic.Uint64
	testRunTag = os.Getpid()
)

// shortSocketPath returns a unique socket path short enough for the
// platform sockaddr_un limit. Go's t.TempDir() embeds the (potentially very
// long) test name, so on Darwin it can exceed SUNPATHLEN; use /tmp instead.
// The directory embeds the pid so repeated `go test -count=N` invocations
// (each a fresh process with counters reset) never reuse a pathname from a
// crashed prior process.
func shortSocketPath(t *testing.T) string {
	t.Helper()
	n := testPathN.Add(1)
	dir := "/tmp/lsg-p" + itoa(testRunTag) + "-" + itoa(int(n))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		panic(err)
	}
	path := dir + "/s.sock"
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return path
}

// socketDir returns the directory portion of a shortSocketPath.
func socketDir(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[:i]
		}
	}
	return "/tmp"
}

func after(d time.Duration) <-chan time.Time { return time.After(d) }

// waitConnCount blocks until the gateway reports exactly n active
// connections, then returns the underlying server-side *Conn objects.
func waitConnCount(g *Gateway, n int) []*Conn {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		g.mu.Lock()
		if len(g.conns) == n {
			out := make([]*Conn, 0, n)
			for _, c := range g.conns {
				out = append(out, c)
			}
			g.mu.Unlock()
			return out
		}
		g.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	panic("waitConnCount: connection count not reached")
}

// waitReady blocks until the gateway reports it is accepting connections.
func waitReady(t *testing.T, g *Gateway, ms int) {
	t.Helper()
	select {
	case <-g.Ready():
	case <-time.After(time.Duration(ms) * time.Millisecond):
		t.Fatalf("gateway did not become ready; state=%s", g.Stats().State)
	}
}
