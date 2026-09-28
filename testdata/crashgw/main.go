// Command crashgw is a test-only gateway subprocess that is started and then
// SIGKILLed so it leaves a stale Unix socket behind.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	lsg "github.com/guoyyit/local-unix-socket-gateway-go"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: crashgw <socket-path>")
		os.Exit(2)
	}
	cfg := lsg.DefaultConfig(os.Args[1])
	cfg.DrainTimeout = 250 * time.Millisecond
	g, err := lsg.New(cfg, func(c *lsg.Conn, p []byte) { _ = c.Send(p) })
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	_ = g.Run(context.Background())
}
