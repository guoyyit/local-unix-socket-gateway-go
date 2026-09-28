//go:build !darwin && !linux

package lsg

import "errors"

var errSockBufUnsupported = errors.New("lsg: SO_SNDBUF unsupported on this platform")

func setSendBuffer(fd uintptr, bytes int) error { return errSockBufUnsupported }
