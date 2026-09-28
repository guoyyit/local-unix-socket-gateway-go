//go:build darwin || linux

package lsg

import (
	"net"
	"os/user"
)

func lookupUID(name string) (uint32, error) {
	u, err := user.Lookup(name)
	if err != nil {
		return 0, err
	}
	var id uint32
	if _, err := parseUint32(u.Uid, &id); err != nil {
		return 0, err
	}
	return id, nil
}

func parseUint32(s string, out *uint32) (int, error) {
	n := 0
	for _, ch := range s {
		if ch < '0' || ch > '9' {
			return 0, errBadNumber
		}
		n = n*10 + int(ch-'0')
	}
	*out = uint32(n)
	return n, nil
}

var errBadNumber = &numError{}

type numError struct{}

func (*numError) Error() string { return "bad number" }

func dialRaw(path string) (net.Conn, error) { return net.Dial("unix", path) }
