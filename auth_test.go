package lsg

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

const dialHelperEnv = "LSG_DIAL_HELPER"

// TestUIDPolicyRejectsOtherUser runs the current test binary as a child
// under a different UID ("nobody") and confirms the gateway rejects it.
func TestUIDPolicyRejectsOtherUser(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("requires root to switch UIDs")
	}
	nobody, err := lookupUID("nobody")
	if err != nil {
		t.Skipf("nobody user unavailable: %v", err)
	}

	path := shortSocketPath(t)
	// The unprivileged child must be able to traverse the directory.
	if err := os.Chmod(socketDir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig(path)
	// Only the root UID may connect.
	cfg.AllowUIDs = []int32{0}
	cfg.SocketMode = 0o666
	g, err := New(cfg, func(c *Conn, p []byte) {
		t.Errorf("handler must never run for rejected identity")
	})
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

	cmd := exec.Command(os.Args[0], "-test.run=TestHelperDial")
	cmd.Env = append(os.Environ(), dialHelperEnv+"="+path)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Credential: &syscall.Credential{Uid: nobody, Gid: -2 & 0xffffffff},
	}
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("nobody dial unexpectedly succeeded: %s", out)
	}
	time.Sleep(200 * time.Millisecond)
	if g.Stats().RejectedTotal < 1 {
		t.Fatalf("expected >=1 rejected connection, got stats=%+v", g.Stats())
	}
}

// TestHelperDial is the unprivileged child process: it attempts a connect.
// The gateway closes rejected connections before any data is exchanged.
func TestHelperDial(t *testing.T) {
	path := os.Getenv(dialHelperEnv)
	if path == "" {
		return
	}
	conn, err := dialRaw(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial failed: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	// A rejected connection is closed by the server without reading our
	// frame; writing should eventually surface EOF/EPIPE.
	conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 4096)
	for i := range buf {
		buf[i] = 'A'
	}
	for i := 0; i < 100; i++ {
		if _, err := conn.Write(buf); err != nil {
			fmt.Fprintf(os.Stderr, "write after rejection: %v\n", err)
			os.Exit(1)
		}
	}
	os.Exit(0)
}

func TestAuthSameUserAllowed(t *testing.T) {
	cfg := DefaultConfig("")
	cfg.AllowUIDs = []int32{int32(os.Getuid())}
	g, _ := startTestGW(t, cfg, func(c *Conn, p []byte) {
		_ = c.Send(p)
	})
	cl, err := Dial(g.Config().SocketPath)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cl.Close()
	if err := cl.Send([]byte("ok")); err != nil {
		t.Fatalf("send: %v", err)
	}
	if _, err := cl.Recv(); err != nil {
		t.Fatalf("recv: %v", err)
	}
}

func TestAuthWrongUIDRejectedLocally(t *testing.T) {
	cfg := DefaultConfig("")
	// A UID that cannot exist in normal tests.
	cfg.AllowUIDs = []int32{int32(os.Getuid() + 100000)}
	g, _ := startTestGW(t, cfg, func(c *Conn, p []byte) {
		t.Errorf("handler must not run")
	})
	cl, err := Dial(g.Config().SocketPath)
	if err != nil {
		t.Fatalf("dial itself should succeed at transport: %v", err)
	}
	defer cl.Close()
	// Wait until the server has processed and rejected this connection.
	if !waitUntil(2*time.Second, func() bool { return g.Stats().RejectedTotal >= 1 }) {
		t.Fatalf("connection not rejected, stats=%+v", g.Stats())
	}
	// A write may surface the server-side close in several platform forms.
	_ = cl.Send([]byte("data"))
	// The server must close without ever responding.
	cl.nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 16)
	if _, err := cl.nc.Read(buf); err == nil {
		t.Fatal("expected connection to be closed by server")
	}
}
