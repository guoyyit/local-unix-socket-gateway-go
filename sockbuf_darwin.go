//go:build darwin

package lsg

import "syscall"

func setSendBuffer(fd uintptr, bytes int) error {
	return syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, bytes)
}
