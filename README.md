# local-unix-socket-gateway-go

一个面向**本机进程间通信 (IPC)** 的受控 Unix 域套接字网关。多个本地客户端通过
Unix domain socket 接入，由操作系统内核提供的对端凭据（UID/GID，Linux 还含
PID）完成认证，交换**带长度边界与 CRC32 校验**的数据帧，并受到**按连接的发送
背压**与**空闲租约**约束。支持配置动态更新、优雅停止、异常/崩溃后的可恢复。

仅依赖 Go 标准库，支持 Linux 与 macOS（其他平台提供保守实现）。

## 安装

```sh
go get github.com/guoyyit/local-unix-socket-gateway-go
```

## 快速开始

### 服务端

```go
package main

import (
    "context"
    "log"
    "os/signal"
    "syscall"

    lsg "github.com/guoyyit/local-unix-socket-gateway-go"
)

func main() {
    cfg := lsg.DefaultConfig("/var/run/myapp/gw.sock")
    cfg.AllowUIDs = []int32{501}              // 仅允许这些 UID；空表示不限制
    cfg.AllowGIDs = []int32{20}               // 及这些用户组（含附属组）
    cfg.SocketMode = 0o660                    // 监听文件权限
    cfg.MaxPayload = 1 << 20                  // 单帧最大 1 MiB
    cfg.MaxQueuedBytes = 4 << 20              // 每连接发送背压预算 4 MiB
    cfg.IdleLease = 5 * time.Minute           // 空闲租约
    cfg.DrainTimeout = 10 * time.Second       // 优雅停止排空上限

    g, err := lsg.New(cfg, func(c *lsg.Conn, payload []byte) {
        // 每个完整、校验通过的帧都会串行调用。payload 仅在调用期间有效。
        _ = c.Send(append([]byte("echo:"), payload...)) // 阻塞直到送达
    })
    if err != nil {
        log.Fatal(err)
    }

    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    go func() {
        if err := g.Run(ctx); err != nil {
            log.Fatal(err)
        }
    }()

    <-ctx.Done()
    sd, cancel := context.WithTimeout(context.Background(), cfg.DrainTimeout)
    defer cancel()
    if err := g.Shutdown(sd); err != nil {
        log.Print(err)
    }
}
```

### 客户端

```go
cl, err := lsg.Dial("/var/run/myapp/gw.sock")
if err != nil {
    log.Fatal(err)
}
defer cl.Close()

if err := cl.Send([]byte("hello")); err != nil { // 自动处理短写
    log.Fatal(err)
}
msg, err := cl.Recv()                            // 自动处理短读/粘包/半包
if err != nil {
    log.Fatal(err)
}
log.Printf("got %q", msg)
```

## 数据帧格式

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         Payload Length (BE uint32)            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                    CRC32-IEEE of payload (BE uint32)          |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          Payload ...                           |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

- `Length` 声明载荷长度；超过 `MaxPayload` 立即返回 `ErrFrameTooLarge`。
- 读取使用 `io.ReadFull` 循环，天然处理**短读**；基于长度前缀切帧，天然处理
  **粘包（一次到达多帧）**与**半包（一帧跨多次到达）**。
- `CRC32` 不匹配返回 `ErrFrameCorrupt`，**只终止该连接**，其他连接不受影响，且
  损坏帧的载荷不会交给业务处理。

## 关键行为与保证

### 1. 监听路径的安全管理
- 自动创建父目录并按 `DirMode` 授权；监听文件按 `SocketMode` 授权（额外
  `chmod`，不依赖 umask）。
- 启动前若路径已存在：
  - 是**存活监听器**（connect 成功）→ 返回 `ErrSocketInUse`，拒绝启动；
  - 是**无监听器的遗留 socket**（connect 返回 `ECONNREFUSED`，常见于上次崩溃）
    → 安全识别并回收后再绑定；
  - 是**非套接字**（普通文件/符号链接等，可能是恶意占位）→ **绝不替换**，报错。
- 路径长度受平台 `sockaddr_un.sun_path` 限制，配置校验阶段即报明确错误
  （macOS 103、Linux 107 个可用字符）。

### 2. 基于内核凭据的认证
- 身份来自连接本身：macOS 用 `LOCAL_PEERCRED`（`xucred`），Linux 用
  `SO_PEERCRED`，**完全不来自网络数据**，客户端无法伪造。
- `AllowUIDs` 为空表示用户维度不限制；否则 UID 必须命中。
- `AllowGIDs` 为空表示组维度不限制；否则对端主 GID 与解析到的附属组至少一个
  命中。解析失败时**fail-closed**（拒绝）。
- 不满足策略：在读取任何业务字节前关闭连接，handler 绝不运行，不泄露业务数据。

### 3. 发送背压（按连接）
- 每个连接维护「队列中 + 在途」的发送字节计数，上限为 `MaxQueuedBytes`。
- accept 时将内核 `SO_SNDBUF` 限制到与预算匹配，使慢消费者真实地阻塞写。
- `TrySend`：预算不足**立即**返回 `ErrBackpressure`（准入即返回，交付异步进行）。
- `Send`：阻塞直到**完整送达**或连接终止/关停；返回结果是确定的。
- 已准入的数据由调度器持有并精确结算一次，**不会静默丢失**。
- 背压按连接隔离，一个慢消费者不影响其他连接。

### 4. 空闲租约
- `IdleLease` 内若该连接无任何读/写活动，回收器（约 250ms 一轮）关闭它，返回
  `ErrIdleLeaseExpired`，释放全部相关资源。持续收发的连接不会被回收。

### 5. 配置动态更新
`UpdateConfig` 为**部分覆盖**语义：标量零值表示沿用当前值；切片字段
（`AllowUIDs/AllowGIDs`）始终为显式值。生效规则明确：

| 字段 | 生效规则 |
| --- | --- |
| `SocketPath` | 运行期不可变（空=不变；改值报错，需重启迁移） |
| `SocketMode/DirMode` | 新路径使用；`SocketMode` 立即对现存 inode 重新 `chmod` |
| `AllowUIDs/AllowGIDs` | **仅对新连接**立即生效；已建立连接终身保持其认证结果 |
| `MaxPayload` | 仅对新连接生效（已建立连接保持 accept 时的帧上限，避免流中变帧） |
| `MaxQueuedBytes` | 对所有已存在连接的**下一次准入/出队决策**生效，不回溯丢帧 |
| `IdleLease` | 对所有已存在连接的**下一次回收扫描**生效 |
| `DrainTimeout` | 对下一次 `Shutdown` 生效 |

**配置与运行态严格分离**：`Config()` 返回期望配置快照；`Stats()/Conns()` 返回
运行时状态（连接数、帧/字节计数、拒绝数等）。重启后运行态从零开始，配置作为
独立输入重新加载——二者互不混淆。

### 6. 优雅停止
`Shutdown(ctx)`：
1. 状态切为 `shutting_down`，关闭监听器，**停止接受新连接**；
2. 向所有连接发起排空：已准入的发送继续送出，等待中的（未准入）请求立即得到
   `ErrShutdown`；
3. 排空不超过 `DrainTimeout`，超时强制回收；
4. 排空期间新提交的 `Send` 得到**确定结果**（`ErrShutdown`），不会悬挂；
5. 取消仍空闲等待的连接，最终删除监听路径。
- 幂等，可重复调用；也可在 handler 中调用。
- 仅取消 `Run` 的 context 而不调用 `Shutdown` 也会清理监听路径。

### 7. 异常退出与恢复
- 客户端异常断开（EOF/RST/崩溃）：读循环检测到错误，仅回收该连接及其队列资源，
  其他连接继续服务。
- 服务端崩溃（如 SIGKILL）：会遗留无主 socket。重启时 `Run` 通过
  `ECONNREFUSED` 探测将其安全回收；新实例的运行态计数从零开始，连接 ID 从新
  计数器分配，不会复用任何旧连接状态。
- 存活 socket 与遗留 socket 的区分避免了「误把正在运行的实例顶掉」。

## 错误哨兵

| 错误 | 含义 |
| --- | --- |
| `ErrSocketInUse` | 监听路径正被存活监听器占用 |
| `ErrPermissionDenied` | 对端凭据不满足访问策略 |
| `ErrFrameTooLarge` | 帧声明长度超过 `MaxPayload` |
| `ErrFrameCorrupt` | CRC 校验失败 |
| `ErrBackpressure` | 超过按连接背压预算（`TrySend`） |
| `ErrIdleLeaseExpired` | 连接因空闲租约到期被回收 |
| `ErrShutdown` | 网关关停中，操作被确定地拒绝/中止 |
| `ErrConnClosed` | 连接已关闭 |

## 可观测性

- `g.Stats()`：网关级快照（状态、启动时间、活跃连接、累计接受/拒绝、帧/字节）。
- `g.Conns()`：每个连接的快照（UID/GID/PID、创建时间、收发帧数与字节数、当前
  排队字节）。
- `g.Config()`：当前生效配置。
- `g.Ready()`：监听器已绑定并开始接受连接的信号通道。

## 测试

```sh
go test ./...            # 普通
go test -race ./...      # 竞态检测
```

覆盖：监听路径回收/占用拒绝/非套接字保护/权限位、UID 拒绝（含 root 切换到
nobody 的子进程用例）、粘包/半包、超长帧与损坏帧隔离、慢消费者背压、阻塞发送
排空、空闲租约到期与活跃保活、动态配置各类生效规则、优雅停止排空与确定错误、
客户端异常断开清理、以及 **SIGKILL 子进程后真实重启回收遗留 socket**。

> 提示：macOS 上 socket 路径可用长度仅 103 字符，请使用较短路径（如
> `/var/run/<app>/x.sock` 或 `/tmp/...`），不要放在名称很长的临时目录下。
