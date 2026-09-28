//go:build !darwin && !linux

package lsg

// Conservative default on other platforms.
const maxSockPathLen = 103
