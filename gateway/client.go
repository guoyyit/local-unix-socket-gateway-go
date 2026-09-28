package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

// Client 是网关的本地客户端。每次 Dial 都是一条全新连接，
// BootID 可帮助调用方发现服务端重启，绝不跨代际复用旧会话状态。
type Client struct {
	raw    *net.UnixConn
	codec  *FrameCodec
	bootID string

	writeMu sync.Mutex // 一次只能有一个完整帧在写出
	closeMu sync.Mutex
	closed  bool
	closeR  atomicError
}

type dialOptions struct {
	maxFrameSize uint32
}

// DialOption 配置客户端可选项。
type DialOption func(*dialOptions)

// WithMaxFrameSize 覆盖客户端默认的最大帧大小。
// 通常应与服务端配置保持一致；默认与服务端默认值相同。
func WithMaxFrameSize(n uint32) DialOption {
	return func(o *dialOptions) { o.maxFrameSize = n }
}

// Dial 连接到 path 上的网关，完成协议握手并读取服务端代际标识。
// 若对端基于调用方身份拒绝访问，会在不发送任何数据的情况下关闭连接，
// 本方法读到 EOF 后返回 ErrAuthRejected；握手帧本身损坏则返回 ErrFrameCorrupt。
func Dial(ctx context.Context, path string, opts ...DialOption) (*Client, error) {
	o := &dialOptions{maxFrameSize: defaultMaxFrameSize}
	for _, opt := range opts {
		opt(o)
	}

	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "unix", path)
	if err != nil {
		return nil, err
	}
	raw := conn.(*net.UnixConn)
	c := &Client{
		raw:   raw,
		codec: NewFrameCodec(o.maxFrameSize),
	}

	// 读取服务端握手帧。认证被拒时对端直接关闭，这里得到 EOF，不泄露任何数据。
	header := make([]byte, frameHeaderSize)
	if dl, ok := ctx.Deadline(); ok {
		_ = raw.SetReadDeadline(dl)
	}
	helloFrame, err := c.codec.ReadFrame(raw, header)
	_ = raw.SetReadDeadline(time.Time{})
	if err != nil {
		_ = raw.Close()
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, ErrAuthRejected
		}
		return nil, err
	}
	var hello helloMsg
	if err := json.Unmarshal(helloFrame, &hello); err != nil || hello.Proto != protoName {
		_ = raw.Close()
		return nil, ErrFrameCorrupt
	}
	c.bootID = hello.BootID
	return c, nil
}

// Send 发送一帧并确保完整写出。
// ctx 取消通过写截止时间生效；若取消发生在写中途，流已无法保证边界，
// 连接会被强制关闭（后续操作得到确定错误），而不会把半帧留给对端。
func (c *Client) Send(ctx context.Context, payload []byte) error {
	if c.isClosed() {
		return ErrConnectionClosed
	}
	if uint32(len(payload)) > c.codec.MaxPayload() {
		return ErrFrameTooLarge
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if dl, ok := ctx.Deadline(); ok {
		_ = c.raw.SetWriteDeadline(dl)
	}
	done := make(chan struct{})
	if ctx.Done() != nil {
		go func() {
			select {
			case <-ctx.Done():
				// 打断进行中的写；连接随后按“流不可恢复”关闭。
				_ = c.raw.SetWriteDeadline(time.Now())
			case <-done:
			}
		}()
	}
	err := c.codec.WriteFrame(c.raw, payload)
	close(done)
	_ = c.raw.SetWriteDeadline(time.Time{})
	if err != nil {
		if isCtxNetErr(ctx, err) {
			c.fail(ctx.Err())
			return ctx.Err()
		}
		c.fail(err)
		return err
	}
	return nil
}

// Recv 接收一帧（自动处理半包/粘包）。
// ctx 取消时强制关闭连接，避免帧边界被部分读取污染。
func (c *Client) Recv(ctx context.Context) ([]byte, error) {
	if c.isClosed() {
		return nil, ErrConnectionClosed
	}
	header := make([]byte, frameHeaderSize)
	type res struct {
		payload []byte
		err     error
	}
	ch := make(chan res, 1)
	go func() {
		p, err := c.codec.ReadFrame(c.raw, header)
		ch <- res{p, err}
	}()
	if dl, ok := ctx.Deadline(); ok {
		_ = c.raw.SetReadDeadline(dl)
	}
	// 若读结果已经就绪则优先消费，避免帧已完整却因 ctx 恰好完成而误关连接。
	select {
	case r := <-ch:
		_ = c.raw.SetReadDeadline(time.Time{})
		if r.err != nil {
			c.fail(r.err)
			return nil, r.err
		}
		return r.payload, nil
	default:
	}
	select {
	case r := <-ch:
		_ = c.raw.SetReadDeadline(time.Time{})
		if r.err != nil {
			c.fail(r.err)
			return nil, r.err
		}
		return r.payload, nil
	case <-ctx.Done():
		_ = c.raw.SetReadDeadline(time.Now()) // 打断后台读
		<-ch                                  // 等待读 goroutine 返回后再标记，避免竞争
		c.fail(ctx.Err())
		return nil, ctx.Err()
	}
}

// BootID 返回服务端本次启动的唯一标识；与上一次连接取值不同说明服务端已重启。
func (c *Client) BootID() string { return c.bootID }

// Close 关闭客户端连接。
func (c *Client) Close() error {
	c.fail(ErrConnectionClosed)
	return nil
}

func (c *Client) isClosed() bool {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	return c.closed
}

func (c *Client) fail(reason error) {
	c.closeMu.Lock()
	if c.closed {
		c.closeMu.Unlock()
		return
	}
	c.closed = true
	c.closeMu.Unlock()
	c.closeR.Store(reason)
	_ = c.raw.Close()
}

// isCtxNetErr 判断写失败是否由 ctx 取消触发（表现为超时网络错误）。
func isCtxNetErr(ctx context.Context, err error) bool {
	if ctx.Err() == nil {
		return false
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}
