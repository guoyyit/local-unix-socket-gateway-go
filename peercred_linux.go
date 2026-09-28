//go:build linux

package lsg

import "syscall"

func peerCredentials(fd uintptr) (PeerCred, error) {
	uc, err := syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	if err != nil {
		return PeerCred{}, err
	}
	return PeerCred{UID: uc.Uid, GID: uc.Gid, PID: uc.Pid}, nil
}
