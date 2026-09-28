package gateway

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// errStopping 是信号量获取期间连接进入优雅停止的内部哨兵错误。
var errStopping = errors.New("gateway: connection is draining")

// weightedSemaphore 是带总权重上限的计数信号量，
// 用于按“在途字节数”施加每连接发送背压。
//
// 配额变化通过 sync.Cond 广播；等待者被唤醒后重新检查配额，
// 未抢到配额的等待者继续 Wait，不会丢失唤醒。
// 关闭/排空/ctx 取消等外部事件由调用方通过 wake 通道通知并 Broadcast。
type weightedSemaphore struct {
	mu   sync.Mutex
	cur  int
	max  int
	cond *sync.Cond
}

func newWeightedSemaphore(max int) *weightedSemaphore {
	s := &weightedSemaphore{max: max}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// Acquire 获取 n 个配额。n > max 时返回 ErrSendQueueFull，避免永久等待。
// 调用方保证在配额释放、ctx 取消、连接关闭、进入排空时广播 wakeCond 并
// 同步 closed/stopping/ctx 的状态；本方法不自己启动 goroutine，避免锁竞争。
func (s *weightedSemaphore) Acquire(ctx context.Context, n int, wake <-chan struct{}, isClosed, isStopping func() bool) error {
	if n > s.max {
		return ErrSendQueueFull
	}
	s.mu.Lock()
	for s.cur+n > s.max {
		if isClosed() {
			s.mu.Unlock()
			return ErrConnectionClosed
		}
		if isStopping() {
			s.mu.Unlock()
			return errStopping
		}
		if err := ctx.Err(); err != nil {
			s.mu.Unlock()
			return err
		}
		s.waitWake(ctx, wake)
	}
	// 被唤醒后再次确认终止状态。
	if isClosed() {
		s.mu.Unlock()
		return ErrConnectionClosed
	}
	if isStopping() {
		s.mu.Unlock()
		return errStopping
	}
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return err
	}
	s.cur += n
	s.mu.Unlock()
	return nil
}

// waitWake 在持有 s.mu 时等待一次“可能发生了状态变化”的通知。
// wake 是外部事件（关闭/排空）信号；ctx 取消由每个 Send 自行处理：
// 我们额外启动一个仅在本次等待期间存活的 goroutine 来广播 cond，
// 但该 goroutine 不获取 s.mu，只做 Broadcast（Broadcast 无需与 Wait 持锁互斥）。
func (s *weightedSemaphore) waitWake(ctx context.Context, wake <-chan struct{}) {
	done := make(chan struct{})
	stop := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			s.cond.Broadcast()
		case <-wake:
			s.cond.Broadcast()
		case <-stop:
		}
		close(done)
	}()
	s.cond.Wait()
	close(stop)
	<-done
}

// Release 归还 n 个配额并广播唤醒等待者。
func (s *weightedSemaphore) Release(n int) {
	s.mu.Lock()
	s.cur -= n
	if s.cur < 0 {
		s.cur = 0
	}
	s.cond.Broadcast()
	s.mu.Unlock()
}

// atomicError 对 error 的原子读写包装。
type atomicError struct {
	v atomic.Pointer[errorBox]
}

type errorBox struct{ err error }

func (a *atomicError) Store(err error) {
	a.v.Store(&errorBox{err: err})
}

func (a *atomicError) Load() error {
	if b := a.v.Load(); b != nil {
		return b.err
	}
	return nil
}
