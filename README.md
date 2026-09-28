# local-unix-socket-gateway-go

面向本机进程间通信（IPC）的 **Unix 域套接字连接接入层**。多个本地客户端可以
建立受控连接、交换带长度边界与 CRC 校验的数据帧，并在进程异常、重启、
慢消费者等场景下保持可恢复、安全、资源有界。

- 纯 Go 实现，零第三方运行时依赖（仅 `golang.org/x/sys` 用于读取对端凭证）
- 支持 macOS（`LOCAL_PEERCRED`/`xucred`）与 Linux（`SO_PEERCRED`/`ucred`）
- 连接认证、长度前缀帧、CRC32 校验、每连接背压、空闲租约、优雅排空、重启代际标识

## 安装

```go
import "github.com/guoyyit/local-unix-socket-gateway-go/gateway"
```

## 快速开始

### 服务端

```go
cfg := gateway.Config{
    SocketPath:      "/var/run/myapp.sock",
    SocketMode:      0o600,                       // 仅属主可读写
    AllowedUIDs:     []uint32{501},              // UID 白名单
    AllowedGIDs:     []uint32{20},               // GID 白名单（匹配主组/附加组）
    MaxFrameSize:    1 << 20,                    // 单帧 1 MiB
    SendQueueFrames: 128,                        // 每连接发送队列帧数
    SendQueueBytes:  16 << 20,                   // 每连接在途字节上限
    IdleTimeout:     5 * time.Minute,            // 空闲租约
    DrainTimeout:    10 * time.Second,           // 优雅停止排空上限
}

handler := gateway.HandlerFunc(func(ctx context.Context, c *gateway.Conn, payload []byte) {
    // payload 是一条完整、已通过 CRC 校验的业务消息
    _ = c.Send(ctx, append([]byte("echo: "), payload...))
})

srv, err := gateway.NewServer(cfg, handler)
if err != nil { log.Fatal(err) }

if err := srv.Listen(); err != nil { log.Fatal(err) } // 同步建立监听
go func() { log.Fatal(srv.Serve()) }()               // 阻塞 accept 循环

// 优雅停止（通常在收到 SIGTERM/SIGINT 时）
ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
defer cancel()
_ = srv.Shutdown(ctx)
```

### 客户端

```go
cl, err := gateway.Dial(ctx, "/var/run/myapp.sock")
if err != nil { log.Fatal(err) }
defer cl.Close()

if err := cl.Send(ctx, []byte("hello")); err != nil { log.Fatal(err) }
msg, err := cl.Recv(ctx)                              // 自动处理半包/粘包
fmt.Printf("server boot=%s reply=%s\n", cl.BootID(), msg)
```

## 数据帧格式

所有帧小端序、自带边界与校验：

```
 0      4      8      12          12+length
 +------+------+------+-----------+
 | magic| length| crc32| payload   |
 +------+------+------+-----------+
```

- `magic = 0x47574631`：快速识别流错位/非协议数据
- `length`：payload 字节数，受 `MaxFrameSize` 与硬上限（64 MiB）双重约束
- `crc32`：payload 的 IEEE CRC-32
- 接收侧 `io.ReadFull` 循环天然处理**半包**；按 length 精确消费、剩余字节留给
  下一帧，天然处理**粘包**；魔数/长度/CRC 任一不符即判定帧损坏。

握手是协议控制帧，独立于业务 `MaxFrameSize`：连接通过认证后服务端发送第一帧
`{"proto":"gwf/v1","boot_id":"<32hex>"}`，客户端据此获得**服务端本次启动的
唯一代际标识**。

## 安全模型

1. **监听路径**
   - 启动时若路径已存在：
     - 能成功连接 → 已有服务在使用，**拒绝启动**（`ErrSocketInUse`）；
     - 连接被拒绝（`ECONNREFUSED`）→ 遗留的未使用套接字文件，**安全回收**；
     - 是普通文件/目录 → **拒绝启动且绝不删除**。
   - 监听建立后显式 `Chmod` 到配置权限（不依赖 umask），并记录设备号+inode；
     停止时只删除仍属于本实例的同名文件，避免误删他者抢占的路径。
2. **连接认证**：服务端在 accept 后立即通过内核提供的对端凭证
   （Linux `SO_PEERCRED`，macOS `LOCAL_PEERCRED`）读取调用方 UID/GID/PID，
   客户端**无法伪造**。白名单为空时默认拒绝所有连接（默认安全）。
   认证不通过时先半关闭写端发送 FIN，再关闭 fd，**不写入任何业务数据**。
3. **帧损坏隔离**：损坏帧只终止其所在连接，其他连接不受影响。

## 背压与空闲租约

- 每个连接有两级发送背压：**在途帧数**（`SendQueueFrames`）与
  **在途字节数**（`SendQueueBytes`，含帧头）。
- `Conn.Send` 在达到上限时按传入 `ctx` **阻塞等待**，绝不静默丢帧；
  `ctx` 取消可放弃排队。单帧超过字节上限立即返回 `ErrSendQueueFull`。
- 写操作以阻塞模式进行：慢消费者只会让发送方在有界队列上等待，不会因写超时
  被误杀；连接关闭（含排空超时强杀）时关闭底层 fd，阻塞写被立即打断。
- 超过 `IdleTimeout` 没有任何成功收发活动的连接会被自动回收并释放全部资源。

## 动态配置

```go
err := srv.UpdateConfig(func(c *gateway.Config) {
    c.IdleTimeout     = 2 * time.Minute
    c.SendQueueFrames = 256
})
```

**生效规则（刻意简单且明确）：**

- 每条连接在 **accept 时刻固化一份 Config 快照**，整个生命周期只使用该快照；
- 更新配置只影响**之后建立的新连接**，已建立连接的租约/背压/帧上限不变；
- 想立即收紧老连接，应显式关闭对应连接（下一次连接即采用新配置）；
- 更新函数在快照副本上修改，校验失败则整个更新被拒绝，旧配置继续生效。

**配置与运行态严格区分**：`srv.Config()` 返回纯声明式配置；`srv.Stats()`
返回运行态（`BootID`、`State`、`StartedAt`、`ActiveConns`、
`AcceptedTotal`、`RejectedTotal`）。服务重启后 BootID 必然改变。

## 优雅停止语义

`Shutdown(ctx)`：

1. 进入 `stopping`，先从文件系统摘除监听路径再关闭监听器，**停止接收新连接**；
2. 对所有现存连接进入排空：**新提交的 `Send` 立即得到 `ErrServerStopping`**
   （确定结果，不悬挂）；
3. 写循环继续排空队列中**已确认入队**的数据，等待上限取 `ctx` 剩余时间与
   该连接快照 `DrainTimeout` 的较小值；
4. 排空超时的连接被强制回收（关闭 fd 打断阻塞写），未送达帧得到
   `ErrDrainTimeout`——每个 `Send` 都有成功或失败的确定结果；
5. 空闲/等待中的连接同样被回收，最终确认并清理属于本实例的监听路径。

## 异常与恢复

- **客户端异常断开**：服务端读循环得到 EOF/错误后关闭并注销连接，
  `ActiveConns` 归零，向已消失对端的 `Send` 在有限时间内得到确定错误。
- **服务端崩溃/重启**：来不及清理时套接字文件残留；新实例启动时探测到
  “文件存在但无人监听”即安全回收。旧连接状态全部随进程消失，
  新实例拥有全新 `BootID`，客户端可据此识别重启、绝不跨代际复用旧会话假设。

## 配置参考

| 字段 | 说明 | 默认值 |
| --- | --- | --- |
| `SocketPath` | 监听路径（macOS ≤103、Linux ≤107 字节） | 必填 |
| `SocketMode` | 套接字文件权限 | `0600` |
| `AllowedUIDs` / `AllowedGIDs` | 本地用户/用户组白名单（皆空=拒绝所有） | 无 |
| `MaxFrameSize` | 单帧 payload 上限 | `1 MiB` |
| `SendQueueFrames` | 每连接在途帧数上限 | `128` |
| `SendQueueBytes` | 每连接在途字节上限 | `16 MiB` |
| `SendBufferBytes` | 每连接内核 `SO_SNDBUF`（0=系统默认） | `0` |
| `IdleTimeout` | 空闲租约 | `5m` |
| `WriteTimeout` | 握手等控制帧写超时 | `30s` |
| `DrainTimeout` | 优雅停止每连接排空上限 | `10s` |

## 测试

```bash
go test ./... -race -count=1
```

覆盖场景：权限拒绝（UID/GID/空表默认拒绝、拒绝不泄露数据）、监听路径在使用/
遗留回收/非套接字占位、半包（逐字节）与粘包、CRC/魔数/长度损坏与连接隔离、
帧/字节双背压与 ctx 取消、空闲租约到期与续租、动态配置新旧连接生效规则、
优雅停止排空与确定结果、客户端异常断开清理、服务端重启 BootID 与状态隔离、
多客户端并发与大帧往返。
