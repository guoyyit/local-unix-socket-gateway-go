//go:build darwin || linux

package gateway

import (
	"os"
	"syscall"
)

// statInode 返回文件所在设备号与 inode。
func statInode(path string) (dev, ino uint64, err error) {
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		return 0, 0, &os.PathError{Op: "stat", Path: path, Err: err}
	}
	return uint64(st.Dev), uint64(st.Ino), nil
}
