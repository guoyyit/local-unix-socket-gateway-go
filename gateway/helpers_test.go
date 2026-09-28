package gateway

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

var tempRootOnce struct {
	sync.Once
	dir string
}

// shortTempDir 返回长度可控的临时目录：t.TempDir() 在 macOS 上路径很长，
// 长测试名 + 套接字文件名可能超过 sun_path 上限。
func shortTempDir(t *testing.T) string {
	t.Helper()
	tempRootOnce.Once.Do(func() {
		base, err := os.MkdirTemp("/private/tmp", "gw-test-")
		if err != nil {
			t.Fatalf("mk temp root: %v", err)
		}
		tempRootOnce.dir = base
	})
	dir, err := os.MkdirTemp(tempRootOnce.dir, "t-")
	if err != nil {
		t.Fatalf("mk temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func testConfig(t *testing.T) Config {
	t.Helper()
	uid := uint32(syscall.Getuid())
	gid := uint32(syscall.Getgid())
	return Config{
		SocketPath:      filepath.Join(shortTempDir(t), "g.sock"),
		SocketMode:      0o600,
		AllowedUIDs:     []uint32{uid},
		AllowedGIDs:     []uint32{gid},
		MaxFrameSize:    1 << 20,
		SendQueueFrames: 64,
		SendQueueBytes:  4 << 20,
		IdleTimeout:     500 * time.Millisecond,
		WriteTimeout:    5 * time.Second,
		DrainTimeout:    3 * time.Second,
	}
}

func startServer(t *testing.T, cfg Config, h Handler) *Server {
	t.Helper()
	srv, err := NewServer(cfg, h)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	// 同步建立监听，避免“路径上存在旧文件”与“监听已就绪”之间的启动竞态。
	if err := srv.Listen(); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if _, err := os.Stat(cfg.SocketPath); err != nil {
		t.Fatalf("socket missing after Listen: %v", err)
	}
	go func() { _ = srv.Serve() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return srv
}

func dialTest(t *testing.T, path string, opts ...DialOption) *Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := Dial(ctx, path, opts...)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func waitFor(fn func() bool, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return fn()
}

// echoHandler 原样回显收到的每条消息。
type echoHandler struct{}

func (echoHandler) HandleMessage(_ context.Context, c *Conn, payload []byte) {
	_ = c.Send(context.Background(), payload)
}

// blockOnceHandler 在收到第一条消息后阻塞，直到 release 被关闭，然后回显。
type blockOnceHandler struct {
	release chan struct{}
	once    sync.Once
}

func newBlockOnceHandler() *blockOnceHandler {
	return &blockOnceHandler{release: make(chan struct{})}
}

func (h *blockOnceHandler) HandleMessage(_ context.Context, c *Conn, payload []byte) {
	h.once.Do(func() {
		<-h.release
	})
	_ = c.Send(context.Background(), payload)
}

// rawDial 建立未经握手的底层 Unix 连接（用于协议级测试）。
func rawDial(t *testing.T, path string) *net.UnixConn {
	t.Helper()
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatalf("DialUnix: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}
