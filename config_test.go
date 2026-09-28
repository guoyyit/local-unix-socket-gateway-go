package lsg

import (
	"errors"
	"os"
	"testing"
	"time"
)

// Updating AllowUIDs affects new connections immediately; established
// connections stay usable.
func TestUpdateConfigAuthAffectsNewConns(t *testing.T) {
	cfg := DefaultConfig("") // initially allow any local user
	g, _ := startTestGW(t, cfg, func(c *Conn, p []byte) { _ = c.Send(p) })

	old, err := Dial(g.Config().SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	waitConnCount(g, 1) // synchronize on server-side accept/registration

	// Lock down to an impossible UID.
	cfg2 := g.Config()
	cfg2.AllowUIDs = []int32{int32(os.Getuid() + 900000)}
	if err := g.UpdateConfig(cfg2); err != nil {
		t.Fatal(err)
	}

	// Established connection remains authenticated and works.
	if err := old.Send([]byte("still-ok")); err != nil {
		t.Fatalf("established conn broken by config update: %v", err)
	}
	if _, err := old.Recv(); err != nil {
		t.Fatalf("established conn recv: %v", err)
	}

	// New connection is rejected.
	bad, err := Dial(g.Config().SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer bad.Close()
	bad.nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := bad.nc.Read(make([]byte, 8)); err == nil {
		t.Fatal("new connection after lockdown should be rejected")
	}
	if !waitUntil(2*time.Second, func() bool { return g.Stats().RejectedTotal >= 1 }) {
		t.Fatalf("expected rejected count >=1, got %d", g.Stats().RejectedTotal)
	}
}

// Updating SocketMode is re-applied to the live listener.
func TestUpdateConfigMode(t *testing.T) {
	g, _ := startTestGW(t, DefaultConfig(""), func(c *Conn, p []byte) {})
	c := g.Config()
	c.SocketMode = 0o600
	if err := g.UpdateConfig(c); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(g.Config().SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("perm=%o", fi.Mode().Perm())
	}
}

// SocketPath cannot change on a running gateway.
func TestUpdateConfigPathImmutable(t *testing.T) {
	g, _ := startTestGW(t, DefaultConfig(""), func(c *Conn, p []byte) {})
	c := g.Config()
	c.SocketPath = "/tmp/lsg-test/different.sock"
	if err := g.UpdateConfig(c); err == nil {
		t.Fatal("expected error changing SocketPath")
	}
}

// MaxQueuedBytes applies to an existing connection at the next admission.
func TestUpdateConfigBackpressureLive(t *testing.T) {
	cfg := DefaultConfig("")
	cfg.MaxQueuedBytes = 1 << 20
	g, _ := startTestGW(t, cfg, func(c *Conn, p []byte) {})

	cl, err := Dial(g.Config().SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	srv := waitConnCount(g, 1)[0]

	// Shrink budget below what the client will consume; do not read.
	c2 := g.Config()
	c2.MaxQueuedBytes = 2048
	if err := g.UpdateConfig(c2); err != nil {
		t.Fatal(err)
	}
	gotBP := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := srv.TrySend(make([]byte, 1024)); err != nil {
			if errors.Is(err, ErrBackpressure) {
				gotBP = true
				break
			}
		}
	}
	if !gotBP {
		t.Fatal("new backpressure limit did not apply to existing connection")
	}
}

// Invalid config updates are rejected atomically (old config survives).
func TestUpdateConfigInvalidRejected(t *testing.T) {
	g, _ := startTestGW(t, DefaultConfig(""), func(c *Conn, p []byte) {})
	before := g.Config().IdleLease
	c := g.Config()
	c.IdleLease = -1
	if err := g.UpdateConfig(c); err == nil {
		t.Fatal("expected invalid update to fail")
	}
	if g.Config().IdleLease != before {
		t.Fatal("invalid update mutated live config")
	}
}
