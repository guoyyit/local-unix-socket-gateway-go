package lsg

// PeerCred is the authenticated operating-system identity of a connected
// local peer, obtained from the kernel (never from wire data).
type PeerCred struct {
	UID uint32
	GID uint32
	// PID is the peer process id where the platform provides it; zero
	// otherwise. PIDs can be reused, so they are informational only and
	// never used as an authorization input.
	PID int32
}

// authorize applies the user/group access policy in cfg to an authenticated
// peer. Failures while resolving supplementary groups fail closed.
//
// Rules:
//   - When AllowUIDs is empty, any authenticated UID passes the user check;
//     otherwise the UID must be listed.
//   - When AllowGIDs is empty, any group passes; otherwise at least one of
//     the peer's primary and supplementary GIDs must be listed.
func authorize(cfg Config, cred PeerCred) (bool, error) {
	if len(cfg.AllowUIDs) > 0 {
		ok := false
		for _, uid := range cfg.AllowUIDs {
			if uint32(uid) == cred.UID {
				ok = true
				break
			}
		}
		if !ok {
			return false, nil
		}
	}

	if len(cfg.AllowGIDs) > 0 {
		gids, err := groupsForUID(cred.UID)
		if err != nil {
			// Fail closed: do not admit an identity whose groups we
			// cannot verify against the policy.
			return false, err
		}
		// The kernel-provided primary GID is authoritative even if name
		// service resolution is degraded.
		gids = append(gids, cred.GID)
		ok := false
		for _, have := range gids {
			for _, want := range cfg.AllowGIDs {
				if have == uint32(want) {
					ok = true
					break
				}
			}
			if ok {
				break
			}
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}
