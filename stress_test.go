package lsg

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// Many concurrent clients exchange many frames; every response must match.
func TestConcurrentClients(t *testing.T) {
	cfg := DefaultConfig("")
	cfg.MaxQueuedBytes = 128 * 1024
	g, _ := startTestGW(t, cfg, func(c *Conn, p []byte) {
		_ = c.Send(p)
	})

	const clients = 25
	const rounds = 40
	var wg sync.WaitGroup
	var failures atomic.Int64
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cl, err := Dial(g.Config().SocketPath)
			if err != nil {
				failures.Add(1)
				return
			}
			defer cl.Close()
			for r := 0; r < rounds; r++ {
				msg := []byte(fmt.Sprintf("c%d-m%d", id, r))
				if err := cl.Send(msg); err != nil {
					failures.Add(1)
					return
				}
				got, err := cl.Recv()
				if err != nil || string(got) != string(msg) {
					failures.Add(1)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	if n := failures.Load(); n != 0 {
		t.Fatalf("%d client failures", n)
	}
}
