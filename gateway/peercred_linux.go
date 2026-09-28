//go:build linux

package gateway

import "golang.org/x/sys/unix"

// getPeerCredOS 在 Linux 上通过 SO_PEERCRED 读取 ucred（含 pid/uid/gid）。
func getPeerCredOS(fd int) (*PeerCred, error) {
	u, err := unix.GetsockoptUcred(fd, unix.SOL_SOCKET, unix.SO_PEERCRED)
	if err != nil {
		return nil, err
	}
	return &PeerCred{PID: int(u.Pid), UID: u.Uid, GID: u.Gid}, nil
}
