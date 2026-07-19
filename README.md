# Easy Proxies

[简体中文](README_ZH.md)

> A sing-box based proxy pool manager -- aggregate many upstream proxy nodes into one stable, health-checked, load-balanced local proxy endpoint.

## Features

- **Three runtime modes**: `pool` (single-port load balancing), `multi-port` (one port per node), and `hybrid` (both simultaneously)
- **Wide protocol support**: VLESS, VMess, Trojan, Shadowsocks, ShadowsocksR, Hysteria v1/v2, TUIC, AnyTLS, SOCKS5/SOCKS5H, HTTP/HTTPS
- **Bounded health checking** with serialized sweeps, probe deadlines, persistent health state, configurable failure thresholds, and manual blacklist/release
- **GeoIP region routing**: classify nodes by country and route traffic through a specific region via a dedicated HTTP proxy endpoint
- **Multiple node sources**: inline config, `nodes.txt` file, or subscription URLs (Base64, plain text, Clash YAML)
- **Transactional subscription refresh**: per-source fallback, stable-identity deduplication, candidate health checks, atomic persistence, and automatic rollback
- **Drain-safe node-level reloads**: unchanged listeners and connections stay alive while added/removed nodes are diffed and replaced
- **Bilingual WebUI dashboard**: Chinese/English, monochrome theme, sortable/searchable/paginated node tables, traffic charts, diagnostics, colored logs, and masked secrets
- **Management API**: RESTful endpoints for node CRUD, probing, blacklisting, subscription management, and config reload
- **Configurable DNS resolver** with fallback servers and IPv4/IPv6 strategy control
- **Log rotation**: size-based rotation with configurable backup count, age, and compression
- **Multi-platform Docker**: supports amd64 and arm64 with host networking

## Quick Start

### 1. Prepare Configuration

```bash
cp config.example.yaml config.yaml
touch nodes.txt
```

Edit `config.yaml` and add your proxy nodes (inline nodes, `nodes.txt` file, or subscription URLs).

> **Important**: `config.yaml` and `nodes.txt` MUST exist as files before starting the Docker container. If they don't exist, Docker will create them as directories, causing startup failure. Use `start.sh` to avoid this issue.

### 2. Run with Docker (Recommended)

```bash
./start.sh
# or manually:
docker compose up -d
```

### 3. Run Natively from Source

```bash
go build -trimpath -tags "with_utls with_quic with_grpc with_wireguard with_gvisor with_clash_api" -o easy_proxies ./cmd/easy_proxies
./easy_proxies -config config.yaml
```

On Windows, use `-o easy_proxies.exe` and start it with `.\easy_proxies.exe -config config.yaml`.

The tag set above matches the Docker build and enables the optional protocol implementations used by real-world Clash subscriptions. In particular, Hysteria, Hysteria2, and TUIC require `with_quic`; a plain untagged `go build` can parse those nodes but cannot start them. If you only need the non-optional protocols, the reduced build remains available with `go run ./cmd/easy_proxies -config config.yaml`.

### 4. Access WebUI

Open `http://localhost:9091` in your browser.

## Configuration

### Runtime Modes

| Mode | Description |
|------|-------------|
| `pool` | Single port proxy pool. All nodes share one port with load balancing |
| `multi-port` | One local port per node for direct access |
| `hybrid` | Both pool + multi-port simultaneously |

Dedicated ports keep their assignments in `port-map.yaml`. Per-node listener credentials edited through the WebUI are stored separately in `node-auth.yaml` (mode `0600`) and are automatically reapplied to matching `nodes_file` or subscription nodes by stable node identity.

Pool failure streaks, active blacklist deadlines, and the latest health/latency counters are coalesced into `health-state.yaml` (also mode `0600`). They are restored on restart; active connection counts remain process-local.

### Pool Scheduling

| Algorithm | Description |
|-----------|-------------|
| `sequential` | Round-robin through healthy nodes |
| `random` | Random node selection |
| `balance` | O(1) power-of-two-choices balancing using active connection counts |
| `latency` | Samples a bounded set, then balances connections among nodes inside the configured latency tolerance |

Transient network failures use a short cooldown instead of immediately increasing the long-term blacklist streak. The unified pool can retry a different node after a failed dial and can optionally keep bounded, expiring session affinity. Dedicated per-node ports never retry through another node.

### Minimal Config Example

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
  probe_concurrency: 32  # process-wide batch probe workers (1-1024)
  password: ""
  # Required together with a strong password for any non-loopback listen address:
  # tls_cert_file: ./certs/management.crt
  # tls_key_file: ./certs/management.key

dns:
  server: 223.5.5.5
  port: 53
  strategy: prefer_ipv4

nodes_file: nodes.txt
```

### Full Config Reference

See [config.example.yaml](config.example.yaml) for the full documented configuration with all available options.

## GeoIP Region Routing

### Overview

When GeoIP is enabled, Easy Proxies automatically classifies your proxy nodes by geographic region and provides a separate HTTP proxy endpoint that lets you route traffic through nodes in a specific country/region.

### Supported Regions

| Code | Region |
|------|--------|
| `jp` | Japan 🇯🇵 |
| `kr` | South Korea 🇰🇷 |
| `us` | United States 🇺🇸 |
| `hk` | Hong Kong 🇭🇰 |
| `tw` | Taiwan 🇹🇼 |
| `sg` | Singapore 🇸🇬 |
| `other` | All other regions |

### Configuration

```yaml
geoip:
  enabled: true
  database_path: "./GeoLite2-Country.mmdb"
  listen: "0.0.0.0"          # defaults to listener.address if omitted
  port: 1221                  # defaults to listener.port if omitted
  exit_ip_url: "https://api.ipify.org" # requested through every node
  exit_ip_timeout: 10s
  exit_ip_concurrency: 16
  auto_update_enabled: true   # auto-update the GeoIP database
  auto_update_interval: 24h   # check interval
```

The GeoIP router reuses the `listener.username` and `listener.password` for proxy authentication.

Key behaviors:
- The GeoIP database (MaxMind GeoLite2-Country) is **auto-downloaded** on first startup
- When auto-update is enabled, the MMDB is checked every 24h by default and hot-reloaded without restarting listeners
- Node region classification uses the public exit IP observed by requesting `exit_ip_url` through that exact outbound; it does not use the subscription server address
- Classification runs at startup and after node reloads. A transient probe failure keeps the node's last observed exit IP when available; otherwise it is placed in `other`
- After an MMDB update, saved exit IPs are immediately reclassified and the region pools/router are replaced in place; this does not repeat the external exit-IP probes or interrupt existing proxy connections

### How to Use

The GeoIP router is an HTTP proxy that listens on its own port. Region selection uses standard proxy credentials, so it works for both normal HTTP requests and HTTPS `CONNECT` tunnels without changing the destination URL.

When proxy authentication is configured, append `@<region>` to the proxy username (URL-encode the `@` as `%40`). Without authentication, use the region code as the proxy username. The normal configured username selects the global pool.

```bash
# Route through Japanese nodes
curl -x http://user%40jp:pass@localhost:1221 http://example.com

# Route through US nodes
curl -x http://user%40us:pass@localhost:1221 http://example.com

# Route through Hong Kong nodes
curl -x http://user%40hk:pass@localhost:1221 http://example.com

# Route through Singapore nodes
curl -x http://user%40sg:pass@localhost:1221 http://example.com

# Normal configured credentials = global pool (all nodes)
curl -x http://user:pass@localhost:1221 http://example.com
```

#### HTTPS Requests (CONNECT Tunnel)

```bash
# Route HTTPS through Japanese nodes
https_proxy=http://user%40jp:pass@localhost:1221 curl https://www.google.com

# Route HTTPS through US nodes
https_proxy=http://user%40us:pass@localhost:1221 curl https://www.google.com

# Normal configured credentials = global pool
https_proxy=http://user:pass@localhost:1221 curl https://www.google.com
```

#### Using with Applications

**Environment variables:**

```bash
# Use Japanese nodes for all traffic
export http_proxy=http://user%40jp:pass@your-server:1221
export https_proxy=http://user%40jp:pass@your-server:1221

# Use global pool (all nodes)
export http_proxy=http://user:pass@your-server:1221
export https_proxy=http://user:pass@your-server:1221
```

**Browser proxy extensions (SwitchyOmega, FoxyProxy, etc.):**

- Protocol: HTTP
- Server: your-server-ip
- Port: 1221
- Username/Password: use `<configured username>@<region>` with the configured password; use the unmodified username for the global pool

**Python requests:**

```python
import requests

proxies = {
    "http": "http://user%40jp:pass@your-server:1221",
    "https": "http://user%40jp:pass@your-server:1221",
}
r = requests.get("http://example.com", proxies=proxies)
```

**Go net/http:**

```go
proxyURL, _ := url.Parse("http://user%40jp:pass@your-server:1221")
client := &http.Client{
    Transport: &http.Transport{
        Proxy: http.ProxyURL(proxyURL),
    },
}
resp, err := client.Get("http://example.com")
```

### How It Works

1. After outbounds start, each node requests the configured IP-echo endpoint through that exact proxy
2. The observed public exit IP is looked up in MaxMind and nodes are grouped into per-region pools (`pool-jp`, `pool-kr`, `pool-us`, etc.)
3. The GeoIP router listens on its own port and reads the optional region suffix from the proxy username
4. Matching requests are routed through the corresponding region pool; unmatched requests use the global pool
5. Each region pool uses the same scheduling algorithm configured in the `pool` section
6. The last successfully observed exit IP is retained across a transient probe failure within the running process

## Supported Protocols

| Protocol | URI Schemes | Transport |
|----------|-------------|-----------|
| VLESS | `vless://` | TCP, WS, HTTP/2, gRPC, HTTPUpgrade; TLS/Reality/uTLS |
| VMess | `vmess://` | WS, HTTP/2, gRPC, HTTPUpgrade; TLS/uTLS |
| Trojan | `trojan://` | WS, HTTP/2, gRPC, HTTPUpgrade; TLS/Reality/uTLS |
| Shadowsocks | `ss://`, `shadowsocks://` | SIP002, legacy whole-payload Base64, plaintext-compatible forms; external plugins are rejected |
| ShadowsocksR | `ssr://`, `shadowsocksr://` | Protocol/obfuscation parameters and Unicode metadata |
| Hysteria | `hysteria://` | QUIC; auth, bandwidth, obfuscation, TLS/SNI, ALPN, windows and MTU |
| Hysteria2 | `hysteria2://`, `hy2://` | QUIC-based |
| TUIC | `tuic://` | QUIC-based |
| AnyTLS | `anytls://` | TLS |
| SOCKS5 | `socks5://`, `socks5h://`, `socks://` | Direct; `socks5h` is accepted for subscription compatibility |
| HTTP | `http://`, `https://` | Direct |

## Node Sources

### Inline Nodes

```yaml
nodes:
  - uri: "vless://uuid@server:443?security=tls&type=ws&path=/path#Name"
```

### Nodes File

```yaml
nodes_file: nodes.txt
```

One proxy URI per line. Lines starting with `#` are comments.

### Subscriptions

```yaml
subscriptions:
  - "https://provider.example/api?token=xxx"

subscription_refresh:
  enabled: true
  interval: 1h
  fetch_concurrency: 16 # default 16, capped at 32
  allow_private_networks: false # opt in only for trusted private subscription services
```

Supports Base64, plain text, and Clash YAML formats. Subscription URLs are fetched with bounded concurrency, responses are strictly limited to 10 MB, and URL credentials/query data are redacted from errors and logs. Loopback, private, link-local, and metadata destinations (including redirects) are blocked by default; set `allow_private_networks: true` only when a trusted subscription service is intentionally hosted on such a network. Duplicate URLs and nodes are removed by stable identity. Runtime refreshes cache each URL independently so one failed provider can reuse only its own last known-good nodes; after a restart, `nodes_file` is the conservative aggregate fallback until every provider has refreshed successfully. Inline and WebUI-added nodes remain explicit configuration and are never overwritten by a subscription refresh.

When subscriptions are configured, fetched nodes are written to `nodes_file`. A refresh is committed as one transaction across configuration, cache files, and runtime state. Candidate nodes are built and health-checked before cutover; a failed fetch, unsupported node, persistence error, or stale configuration revision rolls back without replacing the active pool. Unchanged nodes and listeners retain their connections, removed outbounds drain for the configured timeout, and dedicated ports are restored from `port-map.yaml`.

When the management password is empty, `management.listen` must use a loopback address. A non-loopback management listener requires both a strong non-empty password and native TLS certificate/key files; the service refuses an insecure remote-management configuration.

## WebUI Dashboard

Access at `http://your-server:9091` (configurable via the `management` section).

Features:

- **Dashboard**: Real-time node status, traffic charts, region/resident availability, and latency monitoring
- **Large node sets**: Click-to-sort columns, search, region filters, and configurable 25/50/100/200-row pagination
- **Node Config**: Add/edit/delete inline nodes and manage subscription URLs without exposing credentials in list responses
- **Diagnostics**: Sortable/searchable connectivity results, stability rankings, and node state export
- **Console**: Real-time application logs (last 1000 lines, WebSocket streaming) with separate info/warn/error styling
- **Settings**: Chinese/English switcher, system theme, masked passwords/subscriptions with reveal controls, and persistent configuration editing

When `management.password` is empty, authentication is bypassed.

## Management API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/auth` | POST | Login with password |
| `/api/settings` | GET, PUT | Read/update settings |
| `/api/nodes` | GET | List all nodes with status |
| `/api/nodes/{tag}/probe` | POST | Test node connectivity |
| `/api/nodes/{tag}/blacklist` | POST | Manually blacklist a node |
| `/api/nodes/{tag}/release` | POST | Release node from blacklist |
| `/api/nodes/probe-all` | POST | Probe all nodes (SSE stream) |
| `/api/export` | GET | Export node configuration |
| `/api/subscription/config` | GET, PUT | Manage subscription URLs |
| `/api/subscription/status` | GET | Check subscription status |
| `/api/subscription/refresh` | POST | Trigger manual refresh |
| `/api/nodes/config` | GET, POST, PUT, DELETE | CRUD for node config |
| `/api/reload` | POST | Reload sing-box instance |

## Docker Deployment

### docker-compose.yml

The default setup uses host networking (recommended for automatic port management). Volumes mount `config.yaml` and `nodes.txt`:

```yaml
services:
  easy_proxies:
    image: ghcr.io/jasonwong1991/easy_proxies:latest
    container_name: easy_proxies
    restart: unless-stopped
    network_mode: host
    volumes:
      - ./config.yaml:/etc/easy_proxies/config.yaml
      - ./nodes.txt:/etc/easy_proxies/nodes.txt
      - ./logs:/app/logs
```

### Important Notes

- **Create config files first**: `config.yaml` and `nodes.txt` must exist as files before running `docker compose up`. Use `./start.sh` which handles this automatically.
- **Permissions**: Files must be writable by the container user for WebUI settings to persist. Prefer correct ownership with `0600`/`0640` permissions; avoid world-writable configuration files.
- **Multi-platform**: Supports amd64 and arm64 architectures.
- **Reload**: node/subscription changes use a node-level diff. Unchanged listeners and active connections stay up; new candidates are health-checked before cutover when `min_available_nodes` is configured, and removed outbounds drain for `drain_timeout`. Immutable global listener/log changes still require a short validated full-instance handoff.

### Ports

| Port | Usage |
|------|-------|
| 2323 | Pool proxy entry (pool/hybrid mode) |
| 9091 | WebUI and Management API |
| 1221 | GeoIP region router (when enabled, configurable) |
| 24000+ | Multi-port mode (one per node) |

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for version history.

## Development

```bash
go test ./...
go vet ./...

# Verify the production/full-protocol build
go build -trimpath -tags "with_utls with_quic with_grpc with_wireguard with_gvisor with_clash_api" -o easy_proxies ./cmd/easy_proxies
```

## License

MIT License
