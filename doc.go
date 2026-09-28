// Package lsg implements a controlled local Unix domain socket gateway.
//
// Local clients authenticate with their operating-system peer credentials
// (UID/GID), exchange length-prefixed and checksummed data frames, and are
// subject to per-connection send backpressure limits and idle leases. The
// gateway supports dynamic configuration updates, graceful shutdown and
// recovery from stale socket files and dead peers.
package lsg
