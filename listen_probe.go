package lsg

import (
	"errors"
	"net"
	"syscall"
)

// socketInUse reports whether a Unix socket at path currently has a live
// listener.
//
// A connect() to an AF_UNIX pathname socket returns:
//   - success: a live listener accepted the probe -> in use;
//   - ECONNREFUSED: the inode exists but no listener is bound (stale socket
//     left by a crashed process) -> safe to reclaim;
//   - anything else: conservatively treated as in-use so startup refuses
//     rather than replacing an unknown object.
//
// The accepted probe connection is closed immediately; it carries no
// application data and a running gateway only sees one immediate EOF.
func socketInUse(path string) bool {
	conn, err := net.DialTimeout("unix", path, probeDialTimeout)
	if err != nil {
		return !errors.Is(err, syscall.ECONNREFUSED)
	}
	_ = conn.Close()
	return true
}
