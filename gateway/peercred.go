package gateway

import (
	"net"
)

// PeerCred 是与操作系统无关的调用方身份描述，来源于内核在连接建立时
// 记录的 SO_PEERCRED / LOCAL_PEERCRED，客户端无法伪造。
type PeerCred struct {
	UID    uint32   // 真实用户 ID
	GID    uint32   // 主用户组 ID
	Groups []uint32 // 附加组（不含主组；若平台可得）
	PID    int      // 调用方进程 ID（若平台可得，0 表示不可用）
}

// getPeerCred 通过操作系统提供的对端凭证查询获取连接调用方身份。
func getPeerCred(conn *net.UnixConn) (*PeerCred, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return nil, err
	}
	var (
		cred *PeerCred
		cerr error
	)
	if err := raw.Control(func(fd uintptr) {
		cred, cerr = getPeerCredOS(int(fd))
	}); err != nil {
		return nil, err
	}
	if cerr != nil {
		return nil, cerr
	}
	return cred, nil
}

// allows 判断身份是否被访问策略接受。
//
// 策略语义：
//   - AllowedUIDs 非空时，UID 必须命中；
//   - AllowedGIDs 非空时，GID 或任一附加组必须命中；
//   - 两者皆为空时拒绝所有身份（默认安全，避免“空白名单=放行所有人”的歧义）。
func allows(cred PeerCred, allowedUIDs, allowedGIDs []uint32) bool {
	if len(allowedUIDs) == 0 && len(allowedGIDs) == 0 {
		return false
	}
	uidOK := len(allowedUIDs) == 0
	for _, id := range allowedUIDs {
		if cred.UID == id {
			uidOK = true
			break
		}
	}
	gidOK := len(allowedGIDs) == 0
	if !gidOK {
		for _, id := range allowedGIDs {
			if cred.GID == id {
				gidOK = true
				break
			}
			for _, g := range cred.Groups {
				if g == id {
					gidOK = true
					break
				}
			}
			if gidOK {
				break
			}
		}
	}
	return uidOK && gidOK
}
