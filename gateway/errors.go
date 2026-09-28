package gateway

import "errors"

// 包内统一错误定义。
var (
	ErrNotImplemented   = errors.New("gateway: not implemented")
	ErrServerClosed     = errors.New("gateway: server is closed")
	ErrServerStopping   = errors.New("gateway: server is stopping")
	ErrFrameTooLarge    = errors.New("gateway: frame payload exceeds configured maximum")
	ErrFrameCorrupt     = errors.New("gateway: frame header or checksum is corrupt")
	ErrConnectionClosed = errors.New("gateway: connection closed")
	ErrIdleLeaseExpired = errors.New("gateway: idle lease expired")
	ErrSendQueueFull    = errors.New("gateway: per-connection send queue full")
	ErrAuthRejected     = errors.New("gateway: peer credentials rejected by access policy")
	ErrSocketInUse      = errors.New("gateway: listening path exists and is already in use")
	ErrInvalidConfig    = errors.New("gateway: invalid configuration")
	ErrDrainTimeout     = errors.New("gateway: graceful drain timed out")
	ErrServerRestarted  = errors.New("gateway: peer identified a different server generation")
)
