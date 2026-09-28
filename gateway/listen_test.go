package gateway

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func deadlineIn(d time.Duration) time.Time {
	return time.Now().Add(d)
}

func TestListenRejectsPathInUse(t *testing.T) {
	cfg := testConfig(t)
	startServer(t, cfg, echoHandler{})

	dup, err := NewServer(cfg, echoHandler{})
	if err != nil {
		t.Fatal(err)
	}
	err = dup.Serve()
	if !errors.Is(err, ErrSocketInUse) {
		t.Fatalf("Serve err=%v want ErrSocketInUse", err)
	}
}

func TestListenRejectsNonSocketPath(t *testing.T) {
	cfg := testConfig(t)
	if err := os.MkdirAll(filepath.Dir(cfg.SocketPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.SocketPath, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	srv, err := NewServer(cfg, echoHandler{})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Serve(); !errors.Is(err, ErrSocketInUse) {
		t.Fatalf("Serve err=%v want ErrSocketInUse", err)
	}
	// 普通文件绝不能被误删。
	if _, err := os.Stat(cfg.SocketPath); err != nil {
		t.Fatalf("regular file was removed: %v", err)
	}
}

// 在路径上绑定一个没有任何监听者的 AF_UNIX 套接字，模拟上次进程崩溃的遗留文件。
func bindStaleSocket(t *testing.T, path string) {
	t.Helper()
	fd, err := unix.Socket(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.Bind(fd, &unix.SockaddrUnix{Name: path}); err != nil {
		t.Fatal(err)
	}
	if err := unix.Close(fd); err != nil {
		t.Fatal(err)
	}
}

func TestStaleSocketReclaimed(t *testing.T) {
	cfg := testConfig(t)
	if err := os.MkdirAll(filepath.Dir(cfg.SocketPath), 0o700); err != nil {
		t.Fatal(err)
	}
	bindStaleSocket(t, cfg.SocketPath)
	if _, err := os.Stat(cfg.SocketPath); err != nil {
		t.Fatal(err)
	}
	startServer(t, cfg, echoHandler{}) // 启动成功即说明遗留文件被识别并回收
	cl := dialTest(t, cfg.SocketPath)
	if err := cl.Send(context.Background(), []byte("alive")); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Recv(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestInvalidConfig(t *testing.T) {
	if _, err := NewServer(Config{}, echoHandler{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty config err=%v", err)
	}
}
