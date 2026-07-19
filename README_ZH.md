# Easy Proxies

[English](README.md) | 简体中文

Easy Proxies 是一个基于 sing-box 的代理池管理工具。

目标是把大量上游节点统一成稳定的本地 HTTP/SOCKS5 代理入口，同时支持按节点独立端口访问。

## 当前能力

- 运行模式：`pool`、`multi-port`、`hybrid`。
- 实际构建的上游协议：`vmess`、`vless`、`trojan`、`ss/shadowsocks`、`ssr/shadowsocksr`、`hysteria`、`hysteria2/hy2`、`socks5/socks5h/socks`、`http/https`、`anytls`、`tuic`。
- 节点来源：
  - `config.yaml` 的 `nodes`
  - `nodes_file`（每行一个 URI）
  - `subscriptions`（支持 Base64/纯文本/Clash YAML 解析）
- 有界并发且串行化批次的健康检查、探测超时、失败熔断、黑名单恢复与健康状态持久化。
- Web 管理面板 + API：
  - 节点状态/探测/导出
  - **手动拉黑/解封节点**
  - 动态设置（`external_ip`、`probe_target`、`probe_concurrency`、`skip_cert_verify`、`geoip`）
  - 节点配置增删改查 + 重载
  - 订阅状态查询 + 手动刷新 + 带回滚保护的事务化即时生效
  - **实时日志控制台**（最近 1000 行，WebSocket 流式传输）
- 节点级差异重载：未变化的监听器和现有连接保持运行，移除节点按超时排空。
- 中英文黑白主题 WebUI：正式图标、表格排序、搜索、地域筛选、分页、彩色日志和敏感字段遮罩。
- 新增可配置 DNS 解析器（对 VMess 域名节点非常关键）。
- 可选 GeoIP 标记（支持 JP/KR/US/HK/TW/SG 地域分区，可在 WebUI 中开关，支持自动更新和热重载）。
- **可配置日志轮转**，支持大小限制、备份数量和压缩。

## 快速开始

### 1）准备配置

```bash
cp config.example.yaml config.yaml
cp nodes.example nodes.txt
```

编辑 `config.yaml`，并配置节点来源（`nodes.txt` / `subscriptions` / `nodes`）。

### 2）启动

Docker：

```bash
./start.sh
# 或
docker compose up -d
```

本地完整协议构建（与 Docker 镜像使用相同的可选标签）：

```bash
go build -trimpath -tags "with_utls with_quic with_grpc with_wireguard with_gvisor with_clash_api" -o easy_proxies ./cmd/easy_proxies
./easy_proxies -config config.yaml
```

Windows 请将输出改为 `easy_proxies.exe`，然后执行 `.\easy_proxies.exe -config config.yaml`。

上面的标签会启用真实 Clash 订阅常见的可选协议实现。其中 Hysteria、Hysteria2 和 TUIC 必须使用 `with_quic`；无标签的普通 `go build` 虽然可以解析这些节点，但启动时会拒绝加载。仅使用非可选协议时，仍可执行精简构建：`go run ./cmd/easy_proxies -config config.yaml`。

## 最小配置示例（Pool）

```yaml
mode: pool

listener:
  address: 0.0.0.0
  port: 2323
  username: user
  password: pass

pool:
  mode: sequential    # sequential / random / balance / latency
  failure_threshold: 3
  blacklist_duration: 24h

management:
  enabled: true
  listen: 127.0.0.1:9091
  probe_target: http://cp.cloudflare.com/generate_204
  probe_concurrency: 32  # 全局批量探测并发数（1-1024）
  password: ""
  # 非回环监听地址必须同时配置强密码和原生 TLS：
  # tls_cert_file: ./certs/management.crt
  # tls_key_file: ./certs/management.key

dns:
  server: 223.5.5.5
  port: 53
  strategy: prefer_ipv4

nodes_file: nodes.txt
```

## DNS 配置说明

`dns` 会同时影响 sing-box DNS 客户端和 VMess 域名拨号解析：

```yaml
dns:
  server: 223.5.5.5
  fallback_servers:    # 备用 DNS 服务器（主 DNS 解析失败时使用）
    - 8.8.8.8
    - 1.1.1.1
  port: 53
  strategy: prefer_ipv4
```

`strategy` 可选值：

- `as_is`
- `prefer_ipv4`
- `prefer_ipv6`
- `ipv4_only`
- `ipv6_only`

如果日志中出现 `lookup <domain>: empty result`，请优先检查该 DNS 配置是否可达且策略合理。

## 运行模式

- `pool`：所有节点共享一个本地 HTTP/SOCKS5 入口。
- `multi-port`：每个节点一个独立本地 HTTP/SOCKS5 端口。
- `hybrid`：同时启用 pool + multi-port。

多端口模式会把节点规范化 URI 的哈希与端口持久化到配置目录下的 `port-map.yaml`。订阅改名、重排或进程重启不会改变已有节点端口；节点删除后，端口默认保留 24 小时再允许其他节点复用。可通过 `multi_port.port_map_file` 和 `multi_port.port_reuse_delay` 调整。

Pool 支持 `sequential`、`random`、`balance` 和有界采样的 `latency` 调度。短暂网络故障会进入独立冷却，不会立即累加长期拉黑计数；统一入口可以在拨号失败时切换其他节点重试，并可选启用有容量和 TTL 的会话保持。每节点独立端口始终只使用对应节点。

## 节点来源行为

- 配置了 `subscriptions` 时：
  - 会以有界并发抓取订阅节点（`subscription_refresh.fetch_concurrency`，默认 16，最大 32）
  - 默认拒绝回环、私网、链路本地和元数据地址，并对重定向目标重新校验；只有可信内网订阅才应开启 `subscription_refresh.allow_private_networks`
  - 单个响应严格限制为 10 MB，日志和错误不会暴露订阅凭据、路径或查询参数
  - URL 和节点会按稳定身份去重；运行期按 URL 独立回退到上次成功节点
  - `nodes_file` 作为订阅节点写入路径
  - 重启后若首次抓取不完整，会保守使用 `nodes_file` 中上次可用的全局缓存
- `nodes`（内联节点）以及 WebUI 手动新增的节点会保留在 `config.yaml`，订阅刷新不会覆盖。

订阅刷新会把配置、各来源缓存、聚合 `nodes_file` 和运行时切换作为一个事务处理。候选节点会在切换前完成构建和健康检查；抓取失败、不支持的节点、持久化失败或配置版本冲突都会回滚，不会替换正在服务的代理池。外部节点缓存中重复的稳定身份会确定性去重，显式内联节点始终优先，而重复的内联定义仍会报错。

管理面板密码为空时，`management.listen` 只允许绑定回环地址；如需对外监听，必须同时配置强密码和原生 TLS 证书/私钥。程序会拒绝启动不安全的远程管理监听。

## 协议支持注意事项

运行时真正支持的协议：

- `vmess`
- `vless`
- `trojan`
- `ss` / `shadowsocks`
- `ssr` / `shadowsocksr`
- `hysteria`
- `hysteria2` / `hy2`
- `socks5` / `socks5h` / `socks`
- `http` / `https`
- `anytls`
- `tuic`

Shadowsocks 支持 SIP002、旧式整段 Base64 和明文兼容形式，但不支持外部 plugin。单个格式错误的节点只会被跳过，不会让整批订阅节点加载失败。

## WebUI

- 仪表盘显示实时节点、地域/住宅节点可用率、延迟和流量图表。
- 节点监控、配置和诊断表格支持点击列排序、关键词搜索、地域筛选以及每页 25/50/100/200 条分页。
- 中文与英文可在系统设置即时切换，界面以黑白配色和 SVG 图标为主。
- 控制台按 info、warn、error 分色；密码和订阅地址默认遮罩，可通过眼睛按钮临时查看。
- 节点列表默认不返回代理凭据；编辑单个节点时才通过受保护接口读取完整配置。

## 管理 API（核心）

- `POST /api/auth`
- `GET|PUT /api/settings`
- `GET /api/nodes`
- `POST /api/nodes/{tag}/probe`
- `POST /api/nodes/{tag}/release`
- `POST /api/nodes/{tag}/blacklist`
- `POST /api/nodes/probe-all`（SSE）
- `GET /api/export`
- `GET|PUT /api/subscription/config`
- `GET|POST /api/subscription/status|refresh`
- `GET|POST|PUT|DELETE /api/nodes/config[...]`
- `POST /api/reload`

`management.password` 为空时，Web/API 不要求登录。

## 重要运行说明

- 节点和订阅变更使用节点级差异重载：未变化的监听器与连接保持运行，新候选在切换前测活，移除的出站按 `drain_timeout` 排空。只有不可变的全局监听或日志设置变更才需要经过短暂的完整实例交接。
- Settings API 会把配置写回 `config.yaml`；部分设置需要重载后才能完全生效。
- 省略项默认值可在 `internal/config/config.go` 中查看。
- 日志轮转通过 `log` 配置段设置；当 `output: file` 时，日志同时写入控制台和文件，并自动轮转。

## 更新日志

详见 [CHANGELOG.md](CHANGELOG.md)。

## 开发验证

```bash
go test ./...
go vet ./...

# 验证生产/完整协议构建
go build -trimpath -tags "with_utls with_quic with_grpc with_wireguard with_gvisor with_clash_api" -o easy_proxies ./cmd/easy_proxies
```

## 许可证

MIT License

