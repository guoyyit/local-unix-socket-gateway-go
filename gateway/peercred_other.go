//go:build !darwin && !linux

package gateway

// getPeerCredOS 在不支持对端凭证查询的平台上直接失败（拒绝优于放行）。
func getPeerCredOS(fd int) (*PeerCred, error) {
	return nil, ErrAuthRejected
}
