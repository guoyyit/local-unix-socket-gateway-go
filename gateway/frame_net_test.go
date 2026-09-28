package gateway

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"
)

// TestCorruptFrameTerminatesOnlyThatConn：一条损坏帧只终止对应连接，
// 其他连接继续正常收发。
func TestCorruptFrameTerminatesOnlyThatConn(t *testing.T) {
	cfg := testConfig(t)
	srv := startServer(t, cfg, echoHandler{})

	bad := rawDial(t, cfg.SocketPath)
	good := dialTest(t, cfg.SocketPath)

	// bad 连接先消费握手帧。
	codec := NewFrameCodec(cfg.MaxFrameSize)
	if _, err := codec.ReadFrame(bad, make([]byte, frameHeaderSize)); err != nil {
		t.Fatal(err)
	}

	// 发送一个 CRC 错误的帧。
	frame, _ := codec.Encode([]byte("evil"))
	frame[len(frame)-1] ^= 0xFF
	if _, err := bad.Write(frame); err != nil {
		t.Fatal(err)
	}
	// 服务端检测到损坏后关闭该连接。
	_ = bad.SetReadDeadline(deadlineIn(2 * time.Second))
	buf := make([]byte, 16)
	if _, err := bad.Read(buf); err == nil {
		t.Fatal("corrupt connection not closed by server")
	}

	// 好连接不受影响：连续两次收发成功。
	for i := 0; i < 2; i++ {
		if err := good.Send(context.Background(), []byte("ok")); err != nil {
			t.Fatalf("good conn send %d: %v", i, err)
		}
		p, err := good.Recv(context.Background())
		if err != nil {
			t.Fatalf("good conn recv %d: %v", i, err)
		}
		if string(p) != "ok" {
			t.Fatalf("good conn payload %q", p)
		}
	}

	// 损坏连接最终从活动计数移除，且不影响 AcceptedTotal。
	if !waitFor(func() bool { return srv.Stats().ActiveConns == 1 }, 2*time.Second) {
		t.Fatalf("active=%d want 1", srv.Stats().ActiveConns)
	}
}

// TestTruncatedFrame：只发送帧头（声明 10 字节但无 payload）即关闭/停住，
// 服务端读循环必须把该连接当作异常终止，而不是永久挂起或影响他人。
func TestTruncatedFrame(t *testing.T) {
	cfg := testConfig(t)
	srv := startServer(t, cfg, echoHandler{})
	bad := rawDial(t, cfg.SocketPath)
	codec := NewFrameCodec(cfg.MaxFrameSize)
	if _, err := codec.ReadFrame(bad, make([]byte, frameHeaderSize)); err != nil {
		t.Fatal(err)
	}
	hdr := make([]byte, frameHeaderSize)
	binary.LittleEndian.PutUint32(hdr[0:4], frameMagic)
	binary.LittleEndian.PutUint32(hdr[4:8], 10)
	binary.LittleEndian.PutUint32(hdr[8:12], 0)
	if _, err := bad.Write(hdr); err != nil {
		t.Fatal(err)
	}
	_ = bad.Close() // 客户端异常退出：服务端应读到 EOF 并清理
	if !waitFor(func() bool { return srv.Stats().ActiveConns == 0 }, 2*time.Second) {
		t.Fatalf("active=%d, truncated conn not cleaned", srv.Stats().ActiveConns)
	}
}

// TestClientAbruptDisconnect：客户端建立会话后直接关闭（模拟异常退出），
// 服务端必须清理残留状态；之后新连接仍可正常工作（不复用旧状态）。
func TestClientAbruptDisconnect(t *testing.T) {
	cfg := testConfig(t)
	srv := startServer(t, cfg, echoHandler{})

	cl1 := dialTest(t, cfg.SocketPath)
	if err := cl1.Send(context.Background(), []byte("before-crash")); err != nil {
		t.Fatal(err)
	}
	if _, err := cl1.Recv(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 模拟异常退出：直接关闭底层连接。
	_ = cl1.Close()
	if !waitFor(func() bool { return srv.Stats().ActiveConns == 0 }, 2*time.Second) {
		t.Fatalf("active=%d after client crash", srv.Stats().ActiveConns)
	}

	// 新连接不应复用任何旧状态：正常握手、收发。
	cl2 := dialTest(t, cfg.SocketPath)
	if cl2.BootID() == "" {
		t.Fatal("empty boot id")
	}
	if err := cl2.Send(context.Background(), []byte("after-crash")); err != nil {
		t.Fatal(err)
	}
	p, err := cl2.Recv(context.Background())
	if err != nil || string(p) != "after-crash" {
		t.Fatalf("new conn after crash: p=%q err=%v", p, err)
	}
}

// TestSendAfterClientGoneReturnsError：对端消失后继续 Send 必须得到确定错误，
// 不允许悬挂或静默成功。
func TestSendAfterClientGoneReturnsError(t *testing.T) {
	cfg := testConfig(t)
	gotConn := make(chan *Conn, 1)
	h := HandlerFunc(func(ctx context.Context, c *Conn, _ []byte) {
		select {
		case gotConn <- c:
		default:
		}
	})
	startServer(t, cfg, h)
	cl := dialTest(t, cfg.SocketPath)
	if err := cl.Send(context.Background(), []byte("hi")); err != nil {
		t.Fatal(err)
	}
	sc := <-gotConn
	_ = cl.Close() // 客户端消失

	// 服务端向已消失连接写，应在有限时间内得到确定错误。
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := sc.Send(ctx, []byte("nobody-home"))
	if err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("send to gone peer: err=%v, want deterministic failure", err)
	}
}

// TestServerHandlesChunkedClientWrites：客户端把帧拆成任意小块逐字节发送（极端半包），
// 服务端必须正确重组并回显。
func TestServerHandlesChunkedClientWrites(t *testing.T) {
	cfg := testConfig(t)
	cfg.IdleTimeout = 10 * time.Second
	startServer(t, cfg, echoHandler{})

	cl, err := Dial(context.Background(), cfg.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	codec := NewFrameCodec(cfg.MaxFrameSize)
	frame, _ := codec.Encode([]byte("chunked-payload"))

	// 通过底层 fd 逐字节写（需要拿到 raw conn）。用 raw 连接自行读握手。
	raw := rawDial(t, cfg.SocketPath)
	if _, err := codec.ReadFrame(raw, make([]byte, frameHeaderSize)); err != nil {
		t.Fatalf("handshake: %v", err)
	}
	for _, b := range frame {
		if _, err := raw.Write([]byte{b}); err != nil {
			t.Fatalf("chunk write: %v", err)
		}
	}
	got, err := codec.ReadFrame(raw, make([]byte, frameHeaderSize))
	if err != nil {
		t.Fatalf("recv chunked echo: %v", err)
	}
	if string(got) != "chunked-payload" {
		t.Fatalf("got %q", got)
	}
	_ = cl.Close()
}
