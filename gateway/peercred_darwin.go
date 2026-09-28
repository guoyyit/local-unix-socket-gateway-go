//go:build darwin

package gateway

import "golang.org/x/sys/unix"

// getPeerCredOS 在 macOS/Darwin 上通过 LOCAL_PEERCRED 读取 xucred。
// 注意 xucred 没有独立 gid 字段，Groups[0] 即主用户组。
func getPeerCredOS(fd int) (*PeerCred, error) {
	xu, err := unix.GetsockoptXucred(fd, unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
	if err != nil {
		return nil, err
	}
	cred := &PeerCred{UID: xu.Uid}
	if xu.Ngroups > 0 {
		cred.GID = xu.Groups[0]
		for i := int(1); i < int(xu.Ngroups) && i < len(xu.Groups); i++ {
			cred.Groups = append(cred.Groups, xu.Groups[i])
		}
	}
	return cred, nil
}
