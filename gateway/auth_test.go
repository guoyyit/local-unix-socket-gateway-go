package gateway

import (
	"context"
	"errors"
	"io"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestAllowsPolicy(t *testing.T) {
	cred := PeerCred{UID: 1000, GID: 100, Groups: []uint32{200, 300}}
	cases := []struct {
		name string
		uids []uint32
		gids []uint32
		want bool
	}{
		{"empty lists deny all", nil, nil, false},
		{"uid hit", []uint32{1000}, nil, true},
		{"uid miss", []uint32{9999}, nil, false},
		{"primary gid hit", nil, []uint32{100}, true},
		{"supplementary gid hit", nil, []uint32{300}, true},
		{"gid miss", nil, []uint32{999}, false},
		{"uid hit and gid hit", []uint32{1000}, []uint32{100}, true},
		{"uid hit but gid miss", []uint32{1000}, []uint32{999}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := allows(cred, tc.uids, tc.gids); got != tc.want {
				t.Fatalf("allows=%v want %v", got, tc.want)
			}
		})
	}
}

func TestAuthRejectedUID(t *testing.T) {
	cfg := testConfig(t)
	cfg.AllowedUIDs = []uint32{uint32(syscall.Getuid()) + 777777}
	cfg.AllowedGIDs = nil // 仅按 UID 拒绝
	startServer(t, cfg, echoHandler{})

	// 高层客户端：握手帧永远不会到来。
	_, err := Dial(context.Background(), cfg.SocketPath)
	if !errors.Is(err, ErrAuthRejected) {
		t.Fatalf("Dial err=%v want ErrAuthRejected", err)
	}
}

func TestAuthRejectedLeaksNoData(t *testing.T) {
	cfg := testConfig(t)
	cfg.AllowedUIDs = []uint32{uint32(syscall.Getuid()) + 777777}
	cfg.AllowedGIDs = []uint32{uint32(syscall.Getgid()) + 777777}
	srv := startServer(t, cfg, echoHandler{})

	raw := rawDial(t, cfg.SocketPath)
	buf := make([]byte, 64)
	_ = raw.SetReadDeadline(deadlineIn(2 * time.Second))
	n, err := raw.Read(buf)
	if n != 0 {
		t.Fatalf("rejected connection leaked %d bytes", n)
	}
	if !errors.Is(err, io.EOF) {
		t.Fatalf("read err=%v want EOF", err)
	}
	if !waitFor(func() bool { return srv.Stats().RejectedTotal >= 1 }, 2*time.Second) {
		t.Fatalf("rejected counter=%d", srv.Stats().RejectedTotal)
	}
}

func TestPeerCredOnAcceptedConn(t *testing.T) {
	cfg := testConfig(t)
	gotCred := make(chan PeerCred, 1)
	h := HandlerFunc(func(_ context.Context, c *Conn, _ []byte) {
		select {
		case gotCred <- c.Peer():
		default:
		}
		_ = c.Send(context.Background(), []byte("ok"))
	})
	srv := startServer(t, cfg, h)
	cl := dialTest(t, cfg.SocketPath)
	if err := cl.Send(context.Background(), []byte("whoami")); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Recv(context.Background()); err != nil {
		t.Fatal(err)
	}
	cred := <-gotCred
	if int(cred.UID) != syscall.Getuid() {
		t.Fatalf("peer uid=%d want %d", cred.UID, syscall.Getuid())
	}
	if srv.Stats().AcceptedTotal != 1 {
		t.Fatalf("accepted=%d", srv.Stats().AcceptedTotal)
	}
}

func TestSocketPermissions(t *testing.T) {
	cfg := testConfig(t)
	cfg.SocketMode = 0o600
	startServer(t, cfg, echoHandler{})
	fi, err := os.Stat(cfg.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode()&os.ModeSocket == 0 {
		t.Fatal("path is not a socket")
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Fatalf("perm=%o want 600", perm)
	}
}
