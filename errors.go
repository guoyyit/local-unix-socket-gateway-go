package lsg

import "errors"

var (
	// ErrSocketInUse is returned when the configured listen path already
	// exists and belongs to a live listener.
	ErrSocketInUse = errors.New("lsg: listen path is already in use by another process")

	// ErrPermissionDenied is returned (client side) or recorded (server
	// side) when a peer does not match the configured UID/GID policy.
	ErrPermissionDenied = errors.New("lsg: peer credentials rejected by access policy")

	// ErrFrameTooLarge is returned when a frame advertises a payload
	// longer than the configured maximum.
	ErrFrameTooLarge = errors.New("lsg: frame exceeds configured maximum payload size")

	// ErrFrameCorrupt is returned when a frame checksum does not match.
	ErrFrameCorrupt = errors.New("lsg: frame checksum mismatch")

	// ErrBackpressure is returned when a send cannot be admitted within
	// the per-connection send buffer budget.
	ErrBackpressure = errors.New("lsg: send rejected by per-connection backpressure limit")

	// ErrIdleLeaseExpired is returned for pending sends when a connection
	// is reclaimed because its idle lease elapsed.
	ErrIdleLeaseExpired = errors.New("lsg: connection reclaimed after idle lease expired")

	// ErrShutdown is returned for sends that cannot be completed because
	// the gateway is shutting down or has stopped.
	ErrShutdown = errors.New("lsg: gateway is shutting down")

	// ErrConnClosed is returned for operations on an already closed connection.
	ErrConnClosed = errors.New("lsg: connection closed")
)
