//go:build darwin

package lsg

// maxSockPathLen is the maximum usable length of a Unix domain socket
// pathname (sockaddr_un.sun_path minus the terminating NUL byte).
// Darwin defines SUNPATHLEN = 104, so 103 usable characters.
const maxSockPathLen = 103
