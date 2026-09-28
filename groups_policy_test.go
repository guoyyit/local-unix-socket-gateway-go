package lsg

import (
	"os"
	"testing"
	"time"
)

// A peer whose primary GID is listed is admitted.
func TestGIDPolicyPrimaryMatch(t *testing.T) {
	cfg := DefaultConfig("")
	cfg.AllowGIDs = []int32{int32(os.Getgid())}
	g, _ := startTestGW(t, cfg, func(c *Conn, p []byte) { _ = c.Send(p) })
	cl, err := Dial(g.Config().SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	waitConnCount(g, 1)
	if err := cl.Send([]byte("ok")); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Recv(); err != nil {
		t.Fatal(err)
	}
}

// A peer whose GID is not listed is rejected and no data is returned.
func TestGIDPolicyMismatchRejected(t *testing.T) {
	cfg := DefaultConfig("")
	// A GID this user does not have.
	cfg.AllowGIDs = []int32{int32(os.Getgid() + 70000)}
	g, _ := startTestGW(t, cfg, func(c *Conn, p []byte) {
		t.Errorf("handler must not run for rejected group")
	})
	cl, err := Dial(g.Config().SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if !waitUntil(2*time.Second, func() bool { return g.Stats().RejectedTotal >= 1 }) {
		t.Fatalf("expected rejection, stats=%+v", g.Stats())
	}
	cl.nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := cl.nc.Read(make([]byte, 8)); err == nil {
		t.Fatal("expected server to close rejected-group connection")
	}
}

// Empty allow lists admit any authenticated local user.
func TestEmptyPolicyAllowsLocal(t *testing.T) {
	g, _ := startTestGW(t, DefaultConfig(""), func(c *Conn, p []byte) { _ = c.Send(p) })
	cl, err := Dial(g.Config().SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := cl.Send([]byte("open")); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Recv(); err != nil {
		t.Fatal(err)
	}
}
