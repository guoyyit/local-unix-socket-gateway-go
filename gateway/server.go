package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// 协议握手帧：连接通过认证后，服务端发出的第一帧必须是 hello。
// 客户端可用 boot_id 识别服务端是否已经重启，避免复用旧代际的会话假设。
const protoName = "gwf/v1"

type helloMsg struct {
	Proto  string `json:"proto"`
	BootID string `json:"boot_id"`
}

// Handler 处理服务端收到的一条消息，可通过 conn 发送任意数量回复。
type Handler interface {
	HandleMessage(ctx context.Context, conn *Conn, payload []byte)
}

// HandlerFunc 是 Handler 的函数适配。
type HandlerFunc func(ctx context.Context, conn *Conn, payload []byte)

// HandleMessage 实现 Handler。
func (f HandlerFunc) HandleMessage(ctx context.Context, conn *Conn, payload []byte) {
	f(ctx, conn, payload)
}

// Stats 是与配置严格区分的运行态快照：重启后 BootID/StartedAt 变化，
// 而 Config 是纯声明式数据，二者通过不同方法获取。
type Stats struct {
	BootID        string
	StartedAt     time.Time
	ActiveConns   int
	AcceptedTotal uint64
	RejectedTotal uint64
	State         string // "running" | "stopping" | "stopped"
}

// Server 是本地 Unix 域套接字网关。
type Server struct {
	cfgStore *ConfigStore
	handler  Handler

	mu         sync.Mutex
	listener   *net.UnixListener
	socketIno  uint64 // 监听套接字文件 inode，停止时用于确认文件仍属于自己
	socketDev  uint64
	conns      map[*Conn]struct{}
	bootID     string
	startedAt  time.Time
	state      string
	acceptDone chan struct{}

	accepted atomic.Uint64
	rejected atomic.Uint64
}

// NewServer 创建网关（尚未监听，也未生成运行态）。
func NewServer(cfg Config, handler Handler) (*Server, error) {
	if handler == nil {
		return nil, fmt.Errorf("%w: nil handler", ErrInvalidConfig)
	}
	store, err := NewConfigStore(cfg)
	if err != nil {
		return nil, err
	}
	return &Server{
		cfgStore: store,
		handler:  handler,
		conns:    make(map[*Conn]struct{}),
		bootID:   newBootID(),
		state:    "new",
	}, nil
}

// newBootID 生成本次启动的唯一代际标识。
func newBootID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand 失败属于环境性灾难，退化为时间戳仍可区分重启。
		return fmt.Sprintf("ts-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// Listen 同步完成监听路径检查、回收、绑定与权限设置。
// 重复调用且监听已存在时返回 nil；服务已停止后重新 Listen 会返回错误。
func (s *Server) Listen() error {
	s.mu.Lock()
	if s.listener != nil {
		s.mu.Unlock()
		return nil
	}
	if s.state == "stopping" || s.state == "stopped" {
		s.mu.Unlock()
		return ErrServerClosed
	}
	s.mu.Unlock()
	return s.listen()
}

// Serve 阻塞运行 accept 循环，直到 Shutdown 完成。
// 若尚未调用 Listen，会先同步建立监听（保留便捷的一步启动用法）。
func (s *Server) Serve() error {
	if err := s.Listen(); err != nil {
		return err
	}
	s.mu.Lock()
	s.state = "running"
	s.startedAt = time.Now()
	s.acceptDone = make(chan struct{})
	s.mu.Unlock()

	defer close(s.acceptDone)
	for {
		raw, err := s.listener.AcceptUnix()
		if err != nil {
			s.mu.Lock()
			stopping := s.state == "stopping"
			s.mu.Unlock()
			if stopping {
				return nil
			}
			return err
		}
		go s.serveConn(raw)
	}
}

// listen 安全建立监听：
//   - 路径已存在且可连接（有服务在监听）-> 拒绝启动；
//   - 路径存在但连接被拒绝 -> 遗留的未使用套接字文件，回收；
//   - 非套接字文件占位 -> 拒绝启动，避免误删普通文件。
func (s *Server) listen() error {
	cfg := s.cfgStore.Load()
	path := cfg.SocketPath

	if fi, err := os.Stat(path); err == nil {
		if fi.Mode()&os.ModeSocket == 0 {
			return fmt.Errorf("%w: %s exists and is not a socket", ErrSocketInUse, path)
		}
		switch err := probeConnect(path); {
		case err == nil:
			return fmt.Errorf("%w: %s accepts connections", ErrSocketInUse, path)
		case isRefusedErr(err):
			// ECONNREFUSED/ENOENT：文件存在但无人监听（遗留套接字），安全回收。
			if err := os.Remove(path); err != nil {
				return fmt.Errorf("reclaim stale socket %s: %w", path, err)
			}
		default:
			// 权限不足等：不擅自删除，直接失败（默认安全）。
			return fmt.Errorf("%w: cannot verify existing socket %s: %v", ErrSocketInUse, path, err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return err
	}
	// 内核默认按 0o666&~umask 创建，显式 Chmod 强制配置权限。
	if err := os.Chmod(path, cfg.SocketMode); err != nil {
		_ = ln.Close()
		_ = os.Remove(path)
		return err
	}
	dev, ino, err := statInode(path)
	if err != nil {
		_ = ln.Close()
		_ = os.Remove(path)
		return err
	}

	s.mu.Lock()
	s.listener = ln
	s.socketDev = dev
	s.socketIno = ino
	s.mu.Unlock()
	return nil
}

// probeConnect 探测路径上是否有服务在监听。
// 返回非 nil 表示无法建立连接；调用方用 isRefusedErr 区分“遗留套接字”
// 与“权限不足等不可安全处理”的错误。
func probeConnect(path string) error {
	c, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return err
	}
	_ = c.Close()
	return nil
}

// isRefusedErr 判断错误是否为“路径上无人监听”：
// Linux/macOS 均返回 ECONNREFUSED；竞态删除时可能得到 ENOENT/ErrNotExist。
func isRefusedErr(err error) bool {
	if errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ENOENT) ||
		errors.Is(err, os.ErrNotExist) {
		return true
	}
	return false
}

// rejectConn 拒绝不被接受的连接：先半关闭写端发送 FIN（对端读到确定 EOF），
// 再释放 fd，绝不写入任何业务数据。
func rejectConn(raw *net.UnixConn) {
	_ = raw.CloseWrite()
	_ = raw.Close()
}

// serveConn 执行认证 -> 注册 -> 握手 -> 帧服务。
// 认证失败或服务器正在停止：关闭原始连接，不写任何业务数据。
func (s *Server) serveConn(raw *net.UnixConn) {
	cfg := s.cfgStore.Load() // Accept 时刻固化配置快照

	cred, err := getPeerCred(raw)
	if err != nil || !allows(*cred, cfg.AllowedUIDs, cfg.AllowedGIDs) {
		rejectConn(raw)
		s.rejected.Add(1)
		return
	}
	if cfg.SendBufferBytes > 0 {
		_ = raw.SetWriteBuffer(cfg.SendBufferBytes)
	}

	c := newConn(raw, cfg, *cred, s.bootID)
	// 先注册再握手：保证停止窗口内已 accept 的连接也被 Shutdown 跟踪等待，
	// 不会成为“停止后仍在运行”的游离 goroutine。
	if !s.register(c) {
		rejectConn(raw)
		s.rejected.Add(1)
		return
	}
	s.accepted.Add(1)

	// 认证通过后发送协议握手帧（服务端第一条数据）。
	hello, err := json.Marshal(helloMsg{Proto: protoName, BootID: s.bootID})
	if err != nil {
		c.fail(ErrConnectionClosed)
		s.unregister(c)
		return
	}
	// 握手是协议控制帧，使用硬上限编解码器，不随业务 MaxFrameSize 变化。
	codec := NewFrameCodec(hardMaxPayload)
	_ = raw.SetWriteDeadline(time.Now().Add(cfg.WriteTimeout))
	if err := codec.WriteFrame(raw, hello); err != nil {
		c.fail(ErrConnectionClosed)
		s.unregister(c)
		return
	}
	_ = raw.SetWriteDeadline(time.Time{})

	c.serve(s.handler)
	s.unregister(c)
}

func (s *Server) register(c *Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == "stopping" || s.state == "stopped" {
		return false
	}
	s.conns[c] = struct{}{}
	return true
}

func (s *Server) unregister(c *Conn) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
}

// Shutdown 优雅停止：
//  1. 进入 stopping，不再注册新连接并关闭监听器（停止接收新连接）；
//  2. 所有现存连接开始排空：拒绝新 Send，已确认入队数据按 DrainTimeout 排空；
//  3. 空闲/排空超时的连接被强制回收；
//  4. 确认监听路径仍属于自己后删除，最后关闭监听套接字。
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.state == "stopped" {
		s.mu.Unlock()
		return nil
	}
	s.state = "stopping"
	ln := s.listener
	conns := make([]*Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	// 停止接收新连接：从文件系统摘除路径并关闭监听器。
	// 先摘路径再 close，避免关闭与删除之间被他人抢占同名路径。
	if ln != nil {
		s.removeOwnSocket()
		_ = ln.Close()
	}

	for _, c := range conns {
		c.beginDrain()
	}

	// 等待连接排空/退出。每个连接的等待上限取“ctx 剩余时间”与该连接
	// 快照的 DrainTimeout 的较小值；超时后强制回收（关闭 fd 打断阻塞写）。
	timer := time.NewTimer(0)
	defer timer.Stop()
	for _, c := range conns {
		remaining := timeUntil(ctx)
		if c.cfg.DrainTimeout > 0 && c.cfg.DrainTimeout < remaining {
			remaining = c.cfg.DrainTimeout
		}
		if remaining <= 0 {
			c.fail(ErrDrainTimeout)
			c.waitDone()
			continue
		}
		timer.Reset(remaining)
		select {
		case <-c.doneSig:
		case <-timer.C:
			c.fail(ErrDrainTimeout)
			c.waitDone()
		}
	}

	s.mu.Lock()
	s.state = "stopped"
	s.listener = nil
	s.mu.Unlock()
	return nil
}

// removeOwnSocket 仅当路径上的套接字文件 inode 仍属于本实例时删除。
func (s *Server) removeOwnSocket() {
	cfg := s.cfgStore.Load()
	dev, ino, err := statInode(cfg.SocketPath)
	if err != nil {
		return
	}
	s.mu.Lock()
	mine := dev == s.socketDev && ino == s.socketIno
	s.mu.Unlock()
	if mine {
		_ = os.Remove(cfg.SocketPath)
	}
}

// UpdateConfig 动态更新配置。
// fn 在当前快照的副本上修改；校验失败则更新被整体拒绝、旧配置继续生效。
func (s *Server) UpdateConfig(fn func(*Config)) error {
	cfg := s.cfgStore.Load().Clone()
	fn(&cfg)
	return s.cfgStore.Store(cfg)
}

// Config 返回当前配置快照。
func (s *Server) Config() Config { return s.cfgStore.Load() }

// Stats 返回运行态快照（与配置可区分）。
func (s *Server) Stats() Stats {
	s.mu.Lock()
	state := s.state
	if state == "new" {
		state = "stopped"
	}
	if state == "" {
		state = "stopped"
	}
	st := Stats{
		BootID:      s.bootID,
		StartedAt:   s.startedAt,
		ActiveConns: len(s.conns),
		State:       state,
	}
	s.mu.Unlock()
	st.AcceptedTotal = s.accepted.Load()
	st.RejectedTotal = s.rejected.Load()
	return st
}

func timeUntil(ctx context.Context) time.Duration {
	if dl, ok := ctx.Deadline(); ok {
		return time.Until(dl)
	}
	// 无截止时间时给出一个很长但有限的等待。
	return time.Hour
}
