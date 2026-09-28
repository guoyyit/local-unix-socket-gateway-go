//go:build darwin

package gateway

// macOS SUN_LEN 路径缓冲为 104 字节。
const maxUnixSocketPathLen = 104
