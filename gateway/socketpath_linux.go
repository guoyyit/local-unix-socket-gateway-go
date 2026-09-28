//go:build linux

package gateway

// Linux sockaddr_un.sun_path 为 108 字节。
const maxUnixSocketPathLen = 108
