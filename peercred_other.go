//go:build !darwin && !linux

package lsg

import "errors"

var errPeerCredUnsupported = errors.New("lsg: peer credentials unsupported on this platform")

func peerCredentials(fd uintptr) (PeerCred, error) {
	return PeerCred{}, errPeerCredUnsupported
}
