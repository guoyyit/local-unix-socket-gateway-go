package lsg

import (
	"os/user"
	"strconv"
)

// groupsForUID resolves the primary and supplementary group IDs of uid
// through the operating system's user database. The kernel-provided
// primary GID is checked separately by callers.
func groupsForUID(uid uint32) ([]uint32, error) {
	u, err := user.LookupId(strconv.FormatUint(uint64(uid), 10))
	if err != nil {
		return nil, err
	}
	var out []uint32
	if u.Gid != "" {
		if gid, err := strconv.ParseUint(u.Gid, 10, 32); err == nil {
			out = append(out, uint32(gid))
		}
	}
	supp, err := u.GroupIds()
	if err != nil {
		return out, err
	}
	for _, s := range supp {
		gid, err := strconv.ParseUint(s, 10, 32)
		if err != nil {
			continue
		}
		out = append(out, uint32(gid))
	}
	return out, nil
}
