package lsg

import (
	"errors"
	"testing"
	"time"
)

// An idle connection is reclaimed after the lease; an active connection is
// not.
func TestIdleLeaseReclaimsIdle(t *testing.T) {
	cfg := DefaultConfig("")
	cfg.IdleLease = 300 * time.Millisecond
	g, _ := startTestGW(t, cfg, func(c *Conn, p []byte) {
		_ = c.Send(p)
	})

	idle, err := Dial(g.Config().SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer idle.Close()

	// Confirm the idle connection works first.
	if err := idle.Send([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	if _, err := idle.Recv(); err != nil {
		t.Fatal(err)
	}

	// Stay silent past the lease; the server must close it.
	idle.nc.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 8)
	if _, err := idle.nc.Read(buf); err == nil {
		t.Fatal("expected idle connection to be closed by lease reaper")
	}

	waitUntil(3*time.Second, func() bool { return g.Stats().ActiveConns == 0 })
	if n := g.Stats().ActiveConns; n != 0 {
		t.Fatalf("expected 0 active conns after lease, got %d", n)
	}
}

// A busy connection that exchanges frames within the lease is never reaped.
func TestIdleLeaseKeepsActive(t *testing.T) {
	cfg := DefaultConfig("")
	cfg.IdleLease = 200 * time.Millisecond
	g, _ := startTestGW(t, cfg, func(c *Conn, p []byte) { _ = c.Send(p) })

	cl, err := Dial(g.Config().SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	deadline := time.Now().Add(1500 * time.Millisecond)
	i := 0
	for time.Now().Before(deadline) {
		msg := []byte("beat-" + itoa(i))
		if err := cl.Send(msg); err != nil {
			t.Fatalf("active conn closed early at beat %d: %v", i, err)
		}
		got, err := cl.Recv()
		if err != nil {
			t.Fatalf("active conn recv at beat %d: %v", i, err)
		}
		if string(got) != string(msg) {
			t.Fatalf("mismatch at beat %d: %q", i, got)
		}
		i++
		time.Sleep(80 * time.Millisecond) // < lease
	}
}

// Sends to a lease-reclaimed connection get a definite error.
func TestIdleLeaseSendError(t *testing.T) {
	cfg := DefaultConfig("")
	cfg.IdleLease = 200 * time.Millisecond
	g, _ := startTestGW(t, cfg, func(c *Conn, p []byte) {})

	cl, err := Dial(g.Config().SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	conns := waitConnCount(g, 1)
	srv := conns[0]

	time.Sleep(700 * time.Millisecond) // allow reaper to reclaim
	err = srv.Send([]byte("late"))
	if err == nil {
		t.Fatal("expected definite error sending to reclaimed connection")
	}
	if !errors.Is(err, ErrIdleLeaseExpired) {
		t.Fatalf("want ErrIdleLeaseExpired, got %v", err)
	}
}

func waitUntil(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}
