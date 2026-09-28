package gateway

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

// 默认参数。测试可通过 Config 显式覆盖。
const (
	defaultSocketMode     os.FileMode   = 0o600
	defaultMaxFrameSize   uint32        = 1 << 20 // 1 MiB
	defaultSendQueueFrame int           = 128
	defaultSendQueueBytes int           = 16 << 20 // 16 MiB
	defaultIdleTimeout    time.Duration = 5 * time.Minute
	defaultWriteTimeout   time.Duration = 30 * time.Second
	defaultDrainTimeout   time.Duration = 10 * time.Second
)

// Config 是网关的可动态更新配置。
//
// 生效规则（见 README“动态配置”）：
//   - 所有连接在 Accept 时固化一份 Config 快照，整个连接生命周期只使用该快照；
//   - 更新配置只影响后续建立的新连接；
//   - 调用方若希望缩短/取消已建立连接的租约，应显式关闭连接。
type Config struct {
	// SocketPath 监听的 Unix 域套接字路径。
	SocketPath string
	// SocketMode 监听套接字文件权限（仅受 umask 影响，创建后会显式 Chmod 兜底）。
	SocketMode os.FileMode

	// AllowedUIDs / AllowedGIDs 允许访问的本地用户/用户组白名单。
	// 两者皆为空时拒绝所有连接（默认安全）。
	AllowedUIDs []uint32
	AllowedGIDs []uint32

	// MaxFrameSize 单帧 payload 上限（字节），0 表示使用默认值。
	MaxFrameSize uint32

	// SendQueueFrames 每连接发送队列最大帧数（背压上限之一）。
	SendQueueFrames int
	// SendQueueBytes 每连接发送队列最大字节数（背压上限之一，按帧长度计）。
	SendQueueBytes int

	// SendBufferBytes 服务端每连接的内核发送缓冲 SO_SNDBUF 大小；0 表示使用系统默认。
	SendBufferBytes int

	// IdleTimeout 空闲租约：连接在此时长内没有任何成功收发活动即被回收；0=默认值。
	IdleTimeout time.Duration
	// WriteTimeout 单次内核写超时，防止极慢对端长期占用。
	WriteTimeout time.Duration
	// DrainTimeout 优雅停止时排空“已确认入队但尚未写出”数据的最长等待时间。
	DrainTimeout time.Duration
}

// Clone 返回配置的深拷贝，切片字段不受后续修改影响。
func (c Config) Clone() Config {
	out := c
	if c.AllowedUIDs != nil {
		out.AllowedUIDs = append([]uint32(nil), c.AllowedUIDs...)
	}
	if c.AllowedGIDs != nil {
		out.AllowedGIDs = append([]uint32(nil), c.AllowedGIDs...)
	}
	return out
}

// Validate 检查配置合法性并填充默认值，返回归一化后的新配置。
func (c Config) Validate() (Config, error) {
	out := c.Clone()
	if out.SocketPath == "" {
		return out, ErrInvalidConfig
	}
	if len(out.SocketPath) >= maxUnixSocketPathLen {
		return out, fmt.Errorf("%w: socket path too long (%d bytes, max %d)",
			ErrInvalidConfig, len(out.SocketPath), maxUnixSocketPathLen-1)
	}
	if out.SocketMode == 0 {
		out.SocketMode = defaultSocketMode
	}
	if out.MaxFrameSize == 0 {
		out.MaxFrameSize = defaultMaxFrameSize
	}
	if out.MaxFrameSize > hardMaxPayload {
		return out, ErrInvalidConfig
	}
	if out.SendQueueFrames == 0 {
		out.SendQueueFrames = defaultSendQueueFrame
	}
	if out.SendQueueBytes == 0 {
		out.SendQueueBytes = defaultSendQueueBytes
	}
	if out.SendQueueFrames < 1 || out.SendQueueBytes < 1 {
		return out, ErrInvalidConfig
	}
	if out.SendBufferBytes < 0 {
		return out, ErrInvalidConfig
	}
	if out.IdleTimeout == 0 {
		out.IdleTimeout = defaultIdleTimeout
	}
	if out.IdleTimeout < 0 {
		return out, ErrInvalidConfig
	}
	if out.WriteTimeout == 0 {
		out.WriteTimeout = defaultWriteTimeout
	}
	if out.WriteTimeout < 0 {
		return out, ErrInvalidConfig
	}
	if out.DrainTimeout == 0 {
		out.DrainTimeout = defaultDrainTimeout
	}
	if out.DrainTimeout < 0 {
		return out, ErrInvalidConfig
	}
	return out, nil
}

// ConfigStore 以原子方式保存当前生效配置。
// 新连接取最新快照；已建立连接在 Accept 时固化快照（生效规则见 Config 注释）。
type ConfigStore struct {
	current atomic.Pointer[Config]
}

// NewConfigStore 校验配置并创建配置存储。
func NewConfigStore(cfg Config) (*ConfigStore, error) {
	normalized, err := cfg.Validate()
	if err != nil {
		return nil, err
	}
	s := &ConfigStore{}
	s.current.Store(&normalized)
	return s, nil
}

// Load 取当前配置快照（安全读取，无需加锁）。
func (s *ConfigStore) Load() Config {
	return *s.current.Load()
}

// Store 校验并原子更新当前配置。
func (s *ConfigStore) Store(cfg Config) error {
	normalized, err := cfg.Validate()
	if err != nil {
		return err
	}
	s.current.Store(&normalized)
	return nil
}
