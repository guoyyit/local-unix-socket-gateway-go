//go:build darwin

package lsg

import (
	"syscall"
	"unsafe"
)

// LOCAL_PEERCRED on Darwin returns a struct xucred through SOL_LOCAL.
const (
	solLocal      = 0
	localPeerCred = 0x001
	xucredVersion = 0
	xucredNGroups = 16
)

// xucred mirrors <ucred.h> struct xucred on Darwin (amd64 and arm64).
type xucred struct {
	Version uint32
	Uid     uint32
	Ngroups int16
	Groups  [xucredNGroups]uint32
}

func peerCredentials(fd uintptr) (PeerCred, error) {
	var cred xucred
	cred.Version = xucredVersion
	length := uint32(unsafe.Sizeof(cred))
	_, _, errno := syscall.RawSyscall6(
		syscall.SYS_GETSOCKOPT,
		fd,
		uintptr(solLocal),
		uintptr(localPeerCred),
		uintptr(unsafe.Pointer(&cred)),
		uintptr(unsafe.Pointer(&length)),
		0,
	)
	if errno != 0 {
		return PeerCred{}, errno
	}
	gid := uint32(0)
	if cred.Ngroups > 0 {
		gid = cred.Groups[0]
	}
	return PeerCred{UID: cred.Uid, GID: gid}, nil
}
