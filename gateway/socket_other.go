//go:build !darwin && !linux

package gateway

// statInode 在非 Unix 平台不可用；包仍可编译，但监听会被前置校验拦住。
func statInode(path string) (dev, ino uint64, err error) {
	return 0, 0, ErrInvalidConfig
}
