// 示例：本地 Unix 域套接字网关的最小可运行用法。
//
// 运行：
//
//	go run ./example
//
// 它会在 /tmp/gateway-example.sock 上启动一个只允许当前用户访问的 echo 网关，
// 内部建立一个客户端完成一次往返，然后优雅停止并清理监听路径。
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"syscall"
	"time"

	"github.com/guoyyit/local-unix-socket-gateway-go/gateway"
)

func main() {
	path := "/tmp/gateway-example.sock"
	_ = os.Remove(path)

	cfg := gateway.Config{
		SocketPath:      path,
		SocketMode:      0o600,
		AllowedUIDs:     []uint32{uint32(syscall.Getuid())},
		AllowedGIDs:     []uint32{uint32(syscall.Getgid())},
		MaxFrameSize:    1 << 20,
		SendQueueFrames: 64,
		SendQueueBytes:  4 << 20,
		IdleTimeout:     30 * time.Second,
		DrainTimeout:    3 * time.Second,
	}

	echo := gateway.HandlerFunc(func(ctx context.Context, c *gateway.Conn, payload []byte) {
		log.Printf("recv from uid=%d: %q", c.Peer().UID, payload)
		if err := c.Send(ctx, append([]byte("echo: "), payload...)); err != nil {
			log.Printf("reply: %v", err)
		}
	})

	srv, err := gateway.NewServer(cfg, echo)
	if err != nil {
		log.Fatal(err)
	}
	if err := srv.Listen(); err != nil {
		log.Fatal(err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve() }()
	log.Printf("gateway listening on %s (boot=%s)", path, srv.Stats().BootID)

	// 内部客户端做一次往返。
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	cl, err := gateway.Dial(ctx, path)
	if err != nil {
		log.Fatal(err)
	}
	if err := cl.Send(ctx, []byte("hello gateway")); err != nil {
		log.Fatal(err)
	}
	reply, err := cl.Recv(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("client got: %s (server boot=%s)\n", reply, cl.BootID())
	cancel()
	_ = cl.Close()

	// 优雅停止：排空在途数据并清理监听路径。
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutCancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		log.Fatal(err)
	}
	if err := <-serveErr; err != nil {
		log.Fatal(err)
	}
	log.Println("gateway stopped, socket path removed")
}
