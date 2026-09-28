//go:build linux

package lsg

// Linux defines UNIX_PATH_MAX = 108, so 107 usable characters.
const maxSockPathLen = 107
