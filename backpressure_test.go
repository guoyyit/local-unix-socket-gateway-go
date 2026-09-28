package lsg

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// A connection whose peer never reads trips the per-connection backpressure
// budget: TrySend is rejected with ErrBackpressure, and a sibling
// connection keeps working unaffected.
func TestBackpressureSlowConsumer(t *testing.T) {
	cfg := DefaultConfig("")
	cfg.MaxQueuedBytes = 4096

	var mu sync.Mutex
	hit := map[uint64]bool{}

	g, _ := startTestGW(t, cfg, func(c *Conn, p []byte) {
		if string(p) != "flood" {
			_ = c.Send(p) // echo for the healthy client
			return
		}
		block := make([]byte, 1024)
		for i := 0; i < 100000; i++ {
			if err := c.TrySend(block); err != nil {
				if errors.Is(err, ErrBackpressure) {
					mu.Lock()
					hit[c.ID()] = true
					mu.Unlock()
				}
				return
			}
		}
	})

	slow, err := Dial(g.Config().SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Close()
	healthy, err := Dial(g.Config().SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer healthy.Close()

	if err := slow.Send([]byte("flood")); err != nil {
		t.Fatal(err)
	}

	// Wait for backpressure on the slow connection.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(hit)
		mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	gotBP := len(hit) > 0
	mu.Unlock()
	if !gotBP {
		t.Fatal("expected ErrBackpressure on slow consumer")
	}

	// The healthy connection must still be fully serviceable.
	for i := 0; i < 5; i++ {
		msg := []byte("alive-" + itoa(i))
		if err := healthy.Send(msg); err != nil {
			t.Fatalf("healthy send: %v", err)
		}
		got, err := healthy.Recv()
		if err != nil {
			t.Fatalf("healthy recv: %v", err)
		}
		if string(got) != string(msg) {
			t.Fatalf("healthy mismatch %q", got)
		}
	}
}

// A blocked Send completes once the consumer drains; admitted data is never
// lost.
func TestBackpressureBlockingSendCompletesOnDrain(t *testing.T) {
	cfg := DefaultConfig("")
	cfg.MaxQueuedBytes = 8192
	g, _ := startTestGW(t, cfg, func(c *Conn, p []byte) {})

	cl, err := Dial(g.Config().SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	srv := waitConnCount(g, 1)[0]

	result := make(chan error, 1)
	go func() {
		for i := 0; i < 4; i++ {
			if err := srv.TrySend(make([]byte, 2048)); err != nil {
				result <- err
				return
			}
		}
		// Parks until the client drains.
		result <- srv.Send([]byte("release-payload"))
	}()

	time.Sleep(100 * time.Millisecond)
	deadline := time.Now().Add(3 * time.Second)
	gotRelease := false
	for time.Now().Before(deadline) && !gotRelease {
		_ = cl.nc.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		b, err := cl.Recv()
		if err != nil {
			continue
		}
		if string(b) == "release-payload" {
			gotRelease = true
		}
	}
	if !gotRelease {
		t.Fatal("blocking Send did not complete after drain")
	}
	if err := <-result; err != nil {
		t.Fatalf("blocking send result: %v", err)
	}
}

func newestConnID(g *Gateway) uint64 {
	var id uint64
	for _, s := range g.Conns() {
		if s.ID > id {
			id = s.ID
		}
	}
	return id
}
