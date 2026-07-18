package config

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"easy_proxies/internal/probetarget"
	"easy_proxies/internal/ssruri"

	"gopkg.in/yaml.v3"
)

// Config describes the high level settings for the proxy pool server.
type Config struct {
	Mode                string                    `yaml:"mode"`
	Listener            ListenerConfig            `yaml:"listener"`
	MultiPort           MultiPortConfig           `yaml:"multi_port"`
	Pool                PoolConfig                `yaml:"pool"`
	Management          ManagementConfig          `yaml:"management"`
	SubscriptionRefresh SubscriptionRefreshConfig `yaml:"subscription_refresh"`
	GeoIP               GeoIPConfig               `yaml:"geoip"`
	Log                 LogConfig                 `yaml:"log"`
	Nodes               []NodeConfig              `yaml:"nodes"`
	NodesFile           string                    `yaml:"nodes_file"`    // 节点文件路径，每行一个 URI
	Subscriptions       []string                  `yaml:"subscriptions"` // 订阅链接列表
	ExternalIP          string                    `yaml:"external_ip"`   // 外部 IP 地址，用于导出时替换 0.0.0.0
	LogLevel            string                    `yaml:"log_level"`
	SkipCertVerify      bool                      `yaml:"skip_cert_verify"` // 全局跳过 SSL 证书验证

	filePath string `yaml:"-"` // 配置文件路径，用于保存
}

// LogConfig controls log output and rotation.
type LogConfig struct {
	Output     string `yaml:"output"`      // 日志输出: "stdout", "file", 默认 "stdout"
	File       string `yaml:"file"`        // 日志文件路径，默认 "logs/easy_proxies.log"
	MaxSize    int    `yaml:"max_size"`    // 单个日志文件最大 MB，默认 50
	MaxBackups int    `yaml:"max_backups"` // 保留旧日志文件个数，默认 3
	MaxAge     int    `yaml:"max_age"`     // 保留旧日志文件天数，默认 7
	Compress   bool   `yaml:"compress"`    // 是否压缩旧日志，默认 false
}

// GeoIPConfig controls GeoIP-based region routing.
type GeoIPConfig struct {
	Enabled            bool          `yaml:"enabled"`              // 是否启用 GeoIP 地域分区
	DatabasePath       string        `yaml:"database_path"`        // GeoLite2-Country.mmdb 文件路径
	Listen             string        `yaml:"listen"`               // GeoIP 路由监听地址，默认使用 listener 配置
	Port               uint16        `yaml:"port"`                 // GeoIP 路由监听端口，默认 1221
	ExitIPURL          string        `yaml:"exit_ip_url"`          // 通过每个节点请求的出口 IP 回显地址
	ExitIPTimeout      time.Duration `yaml:"exit_ip_timeout"`      // 单节点出口 IP 探测超时
	ExitIPConcurrency  int           `yaml:"exit_ip_concurrency"`  // 并发出口 IP 探测数
	AutoUpdateEnabled  bool          `yaml:"auto_update_enabled"`  // 是否启用自动更新数据库
	AutoUpdateInterval time.Duration `yaml:"auto_update_interval"` // 自动更新间隔，默认 24 小时
}

// ListenerConfig defines how the HTTP/SOCKS5 mixed proxy should listen for clients.
type ListenerConfig struct {
	Address  string `yaml:"address"`
	Port     uint16 `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// PoolConfig configures scheduling + failure handling.
type PoolConfig struct {
	Mode              string        `yaml:"mode"`
	FailureThreshold  int           `yaml:"failure_threshold"`
	BlacklistDuration time.Duration `yaml:"blacklist_duration"`
	FailOpen          bool          `yaml:"fail_open,omitempty"`
	HealthStateFile   string        `yaml:"health_state_file,omitempty"`
	RetryEnabled      *bool         `yaml:"retry_enabled,omitempty"`
	RetryAttempts     int           `yaml:"retry_attempts,omitempty"`
	TransientCooldown time.Duration `yaml:"transient_cooldown,omitempty"`
	LatencySampleSize int           `yaml:"latency_sample_size,omitempty"`
	LatencyTolerance  time.Duration `yaml:"latency_tolerance,omitempty"`
	Sticky            StickyConfig  `yaml:"sticky,omitempty"`
}

// StickyConfig controls bounded session affinity on the unified pool listener.
// Dedicated per-node listeners are already deterministic and ignore this setting.
type StickyConfig struct {
	Enabled    bool          `yaml:"enabled" json:"enabled"`
	TTL        time.Duration `yaml:"ttl" json:"ttl"`
	MaxEntries int           `yaml:"max_entries" json:"max_entries"`
}

// RetryEnabledValue returns the effective retry setting. Retries default to on
// for pooled traffic, while dedicated listeners suppress them at runtime.
func (c PoolConfig) RetryEnabledValue() bool {
	return c.RetryEnabled == nil || *c.RetryEnabled
}

// MultiPortConfig defines address/credential defaults for multi-port mode.
type MultiPortConfig struct {
	Address           string        `yaml:"address"`
	BasePort          uint16        `yaml:"base_port"`
	Username          string        `yaml:"username"`
	Password          string        `yaml:"password"`
	PortMapFile       string        `yaml:"port_map_file,omitempty"`
	AuthOverridesFile string        `yaml:"auth_overrides_file,omitempty"`
	PortReuseDelay    time.Duration `yaml:"port_reuse_delay,omitempty"`
}

// ManagementConfig controls the monitoring HTTP endpoint.
type ManagementConfig struct {
	Enabled          *bool  `yaml:"enabled"`
	Listen           string `yaml:"listen"`
	ProbeTarget      string `yaml:"probe_target"`
	ProbeConcurrency int    `yaml:"probe_concurrency"` // 全局批量探测并发数（1-1024，默认 32）
	Password         string `yaml:"password"`          // WebUI 访问密码，为空则不需要密码
	TLSCertFile      string `yaml:"tls_cert_file,omitempty"`
	TLSKeyFile       string `yaml:"tls_key_file,omitempty"`
}

// SubscriptionRefreshConfig controls subscription auto-refresh and reload settings.
type SubscriptionRefreshConfig struct {
	Enabled              bool          `yaml:"enabled"`                // 是否启用定时刷新
	Interval             time.Duration `yaml:"interval"`               // 刷新间隔，默认 1 小时
	Timeout              time.Duration `yaml:"timeout"`                // 获取订阅的超时时间
	HealthCheckTimeout   time.Duration `yaml:"health_check_timeout"`   // 新节点健康检查超时
	DrainTimeout         time.Duration `yaml:"drain_timeout"`          // 旧实例排空超时时间
	MinAvailableNodes    int           `yaml:"min_available_nodes"`    // 最少可用节点数，低于此值不切换
	FetchConcurrency     int           `yaml:"fetch_concurrency"`      // 订阅抓取并发数，默认 16，最大 32
	AllowPrivateNetworks bool          `yaml:"allow_private_networks"` // 显式允许订阅访问回环/私网/链路本地地址
}

// NodeSource indicates where a node configuration originated from.
type NodeSource string

const (
	NodeSourceInline       NodeSource = "inline"       // Defined directly in config.yaml nodes array
	NodeSourceFile         NodeSource = "nodes_file"   // Loaded from external nodes file
	NodeSourceSubscription NodeSource = "subscription" // Fetched from subscription URL
)

// NodeConfig describes a single upstream proxy endpoint expressed as URI.
type NodeConfig struct {
	Name     string     `yaml:"name" json:"name"`
	URI      string     `yaml:"uri" json:"uri"`
	Port     uint16     `yaml:"port,omitempty" json:"port,omitempty"`
	Username string     `yaml:"username,omitempty" json:"username,omitempty"`
	Password string     `yaml:"password,omitempty" json:"password,omitempty"`
	Source   NodeSource `yaml:"-" json:"source,omitempty"` // Runtime only, not persisted
}

// NodeKey returns a stable, non-secret identifier for a node. Display names and
// URL query ordering do not affect the key, so subscription reordering/renaming
// does not change a node's dedicated port.
func (n *NodeConfig) NodeKey() string {
	canonical := canonicalNodeURI(n.URI)
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])
}

func canonicalNodeURI(rawURI string) string {
	rawURI = strings.TrimSpace(rawURI)
	if rawURI == "" {
		return ""
	}
	lowerURI := strings.ToLower(rawURI)
	if strings.HasPrefix(lowerURI, "ssr://") || strings.HasPrefix(lowerURI, "shadowsocksr://") {
		if parsed, err := ssruri.Parse(rawURI); err == nil {
			parameters := url.Values{}
			parameters.Set("protocol", parsed.Protocol)
			parameters.Set("method", parsed.Method)
			parameters.Set("obfs", parsed.Obfs)
			parameters.Set("password", parsed.Password)
			parameters.Set("obfsparam", parsed.ObfsParam)
			parameters.Set("protoparam", parsed.ProtocolParam)
			return "ssr://" + net.JoinHostPort(strings.ToLower(parsed.Server), strconv.Itoa(parsed.Port)) + "?" + parameters.Encode()
		}
	}

	if strings.HasPrefix(lowerURI, "vmess://") {
		payload := rawURI[len("vmess://"):]
		if idx := strings.IndexByte(payload, '#'); idx >= 0 {
			payload = payload[:idx]
		}
		payload = strings.TrimSpace(payload)
		encodings := []*base64.Encoding{
			base64.StdEncoding,
			base64.RawStdEncoding,
			base64.URLEncoding,
			base64.RawURLEncoding,
		}
		for _, encoding := range encodings {
			decoded, err := encoding.DecodeString(payload)
			if err != nil {
				continue
			}
			var document map[string]any
			if json.Unmarshal(decoded, &document) != nil {
				continue
			}
			delete(document, "ps")
			canonical, err := json.Marshal(document)
			if err == nil {
				return "vmess://" + string(canonical)
			}
		}
	}

	parsed, err := url.Parse(rawURI)
	if err != nil {
		if fragment := strings.IndexByte(rawURI, '#'); fragment >= 0 {
			return rawURI[:fragment]
		}
		return rawURI
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	parsed.Host = strings.ToLower(parsed.Host)
	parsed.Fragment = ""
	parsed.RawFragment = ""
	if parsed.RawQuery != "" {
		parsed.RawQuery = parsed.Query().Encode()
	}
	return parsed.String()
}

// Load reads YAML config from disk and applies defaults/validation.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	cfg.filePath = path

	// Resolve nodes_file path relative to config file directory
	if cfg.NodesFile != "" && !filepath.IsAbs(cfg.NodesFile) {
		configDir := filepath.Dir(path)
		cfg.NodesFile = filepath.Join(configDir, cfg.NodesFile)
	}

	if err := cfg.normalize(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// ExtractNodeName extracts a human-readable name from a proxy URI.
// For standard URIs (vless://, ss://, trojan://), it extracts from the URL fragment (#name).
// For vmess:// URIs, it base64-decodes the payload and extracts the "ps" field.
func ExtractNodeName(uri string) string {
	uri = strings.TrimSpace(uri)
	lowerURI := strings.ToLower(uri)

	// Handle vmess:// specially - it's base64-encoded JSON, not a standard URL
	if strings.HasPrefix(lowerURI, "vmess://") {
		payload := uri[len("vmess://"):]
		// Remove any fragment that might be appended
		if idx := strings.Index(payload, "#"); idx != -1 {
			payload = payload[:idx]
		}
		payload = strings.TrimSpace(payload)
		// Try standard base64 first, then raw/URL-safe variants
		var decoded []byte
		var err error
		decoded, err = base64.StdEncoding.DecodeString(payload)
		if err != nil {
			decoded, err = base64.RawStdEncoding.DecodeString(payload)
		}
		if err != nil {
			decoded, err = base64.RawURLEncoding.DecodeString(payload)
		}
		if err == nil {
			var vmess struct {
				PS string `json:"ps"`
			}
			if json.Unmarshal(decoded, &vmess) == nil && vmess.PS != "" {
				return strings.TrimSpace(vmess.PS)
			}
		}
		return ""
	}
	if strings.HasPrefix(lowerURI, "ssr://") || strings.HasPrefix(lowerURI, "shadowsocksr://") {
		if parsed, err := ssruri.Parse(uri); err == nil {
			return strings.TrimSpace(parsed.Remarks)
		}
		return ""
	}

	// For standard URIs, extract from URL fragment (#name)
	if idx := strings.LastIndex(uri, "#"); idx != -1 && idx < len(uri)-1 {
		fragment := uri[idx+1:]
		if decoded, err := url.QueryUnescape(fragment); err == nil && decoded != "" {
			return strings.TrimSpace(decoded)
		}
		return strings.TrimSpace(fragment)
	}

	return ""
}

func (c *Config) normalize() error {
	if c.Mode == "" {
		c.Mode = "pool"
	}
	// Normalize mode name: support both multi-port and multi_port
	if c.Mode == "multi_port" {
		c.Mode = "multi-port"
	}
	switch c.Mode {
	case "pool", "multi-port", "hybrid":
	default:
		return fmt.Errorf("unsupported mode %q (use 'pool', 'multi-port', or 'hybrid')", c.Mode)
	}
	if c.Listener.Address == "" {
		c.Listener.Address = "0.0.0.0"
	}
	if c.Listener.Port == 0 {
		c.Listener.Port = 2323
	}
	if err := c.normalizePoolConfig(); err != nil {
		return err
	}
	if c.MultiPort.Address == "" {
		c.MultiPort.Address = "0.0.0.0"
	}
	if c.MultiPort.BasePort == 0 {
		c.MultiPort.BasePort = 24000
	}
	if c.MultiPort.PortReuseDelay <= 0 {
		c.MultiPort.PortReuseDelay = defaultPortReuseTTL
	}
	if c.Management.Listen == "" {
		c.Management.Listen = "127.0.0.1:9091"
	}
	if c.Management.ProbeTarget == "" {
		c.Management.ProbeTarget = "www.apple.com:80"
	}
	if _, ready, err := probetarget.Parse(c.Management.ProbeTarget); err != nil || !ready {
		if err == nil {
			err = errors.New("probe target is empty")
		}
		return fmt.Errorf("invalid management probe_target: %w", err)
	}
	if c.Management.Enabled == nil {
		defaultEnabled := true
		c.Management.Enabled = &defaultEnabled
	}
	if err := ValidateManagementConfig(c.Management); err != nil {
		return err
	}
	c.normalizeGeoIPConfig()

	// Subscription refresh defaults
	if c.SubscriptionRefresh.Interval <= 0 {
		c.SubscriptionRefresh.Interval = 1 * time.Hour
	}
	if c.SubscriptionRefresh.Timeout <= 0 {
		c.SubscriptionRefresh.Timeout = 30 * time.Second
	}
	if c.SubscriptionRefresh.HealthCheckTimeout <= 0 {
		c.SubscriptionRefresh.HealthCheckTimeout = 60 * time.Second
	}
	if c.SubscriptionRefresh.DrainTimeout <= 0 {
		c.SubscriptionRefresh.DrainTimeout = 30 * time.Second
	}
	if c.SubscriptionRefresh.MinAvailableNodes <= 0 {
		c.SubscriptionRefresh.MinAvailableNodes = 1
	}
	c.SubscriptionRefresh.FetchConcurrency = NormalizeSubscriptionFetchConcurrency(c.SubscriptionRefresh.FetchConcurrency)
	validatedSubscriptions, err := ValidateSubscriptionURLs(c.Subscriptions)
	if err != nil {
		return err
	}
	c.Subscriptions = validatedSubscriptions

	// Mark inline nodes with source
	for idx := range c.Nodes {
		c.Nodes[idx].Source = NodeSourceInline
	}

	// Load nodes from file if specified (but NOT if subscriptions exist - subscription takes priority)
	if c.NodesFile != "" && len(c.Subscriptions) == 0 {
		fileNodes, err := loadNodesFromFile(c.NodesFile)
		if err != nil {
			return fmt.Errorf("load nodes from file %q: %w", c.NodesFile, err)
		}
		for idx := range fileNodes {
			fileNodes[idx].Source = NodeSourceFile
		}
		c.Nodes = append(c.Nodes, fileNodes...)
	}

	// Load nodes from subscriptions concurrently (highest priority - writes to nodes.txt).
	// Startup cannot attribute a legacy nodes.txt entry to a specific URL, so a
	// partial failure keeps that last known-good aggregate. Runtime refreshes use
	// a finer per-URL cache in the subscription manager.
	if len(c.Subscriptions) > 0 {
		nodesFilePath := c.NodesFile
		if nodesFilePath == "" {
			nodesFilePath = filepath.Join(filepath.Dir(c.filePath), "nodes.txt")
			c.NodesFile = nodesFilePath
		}
		cachedNodes, cacheErr := loadNodesFromFile(nodesFilePath)

		subNodes, stats := FetchSubscriptionNodes(nil, c.Subscriptions, SubscriptionFetchOptions{
			Timeout:              c.SubscriptionRefresh.Timeout,
			Concurrency:          c.SubscriptionRefresh.FetchConcurrency,
			AllowPrivateNetworks: c.SubscriptionRefresh.AllowPrivateNetworks,
			Loggerf:              log.Printf,
		})
		if (stats.Failed > 0 || stats.Empty > 0 || len(subNodes) == 0) && cacheErr == nil && len(cachedNodes) > 0 {
			log.Printf("⚠️ Keeping %d cached subscription nodes after an incomplete startup refresh", len(cachedNodes))
			subNodes = cachedNodes
		}
		// Mark subscription nodes. The cache is committed only after BoxManager
		// successfully starts, so a parseable-but-unbootable subscription cannot
		// destroy the last known-good restart cache.
		for idx := range subNodes {
			subNodes[idx].Source = NodeSourceSubscription
		}
		c.Nodes = append(c.Nodes, subNodes...)
	}

	if len(c.Nodes) == 0 {
		return errors.New("config.nodes cannot be empty (configure nodes in config or use nodes_file)")
	}
	for idx := range c.Nodes {
		c.Nodes[idx].Name = sanitizeNodeName(c.Nodes[idx].Name)
		c.Nodes[idx].URI = strings.TrimSpace(c.Nodes[idx].URI)

		if c.Nodes[idx].URI == "" {
			return fmt.Errorf("node %d is missing uri", idx)
		}

		// Auto-extract name from URI if not provided
		if c.Nodes[idx].Name == "" {
			c.Nodes[idx].Name = sanitizeNodeName(ExtractNodeName(c.Nodes[idx].URI))
		}
		// Fallback to default name if still empty
		if c.Nodes[idx].Name == "" {
			c.Nodes[idx].Name = fmt.Sprintf("node-%d", idx)
		}

	}
	if err := validateUniqueNodeKeys(c.Nodes); err != nil {
		return err
	}
	if err := c.ApplyNodeAuthOverrides(); err != nil {
		return err
	}
	if err := c.validateInboundCredentials(); err != nil {
		return err
	}
	// Port assignments are part of the candidate configuration until the
	// runtime has started successfully.  Persisting here would let a failed
	// startup replace the last-known-good port map on disk.
	if err := c.assignNodePorts(nil, false); err != nil {
		return err
	}
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}

	// Log config defaults
	c.normalizeLogConfig()

	return nil
}

// BuildPortMap creates a mapping from stable node key to port for existing nodes.
// This is used to preserve port assignments when reloading configuration.
func (c *Config) BuildPortMap() map[string]uint16 {
	portMap := make(map[string]uint16)
	for _, node := range c.Nodes {
		if node.Port > 0 {
			portMap[node.NodeKey()] = node.Port
		}
	}
	return portMap
}

const (
	portMappingVersion  = 1
	defaultPortMapFile  = "port-map.yaml"
	defaultPortReuseTTL = 24 * time.Hour
	nodeAuthVersion     = 1
	defaultNodeAuthFile = "node-auth.yaml"
)

type nodeAuthOverride struct {
	Username string `yaml:"username,omitempty"`
	Password string `yaml:"password,omitempty"`
}

type nodeAuthState struct {
	Version   int                         `yaml:"version"`
	Overrides map[string]nodeAuthOverride `yaml:"overrides"`
}

func newNodeAuthState() *nodeAuthState {
	return &nodeAuthState{
		Version:   nodeAuthVersion,
		Overrides: make(map[string]nodeAuthOverride),
	}
}

func (c *Config) nodeAuthPath() string {
	path := strings.TrimSpace(c.MultiPort.AuthOverridesFile)
	if path == "" {
		path = defaultNodeAuthFile
	}
	if filepath.IsAbs(path) {
		return filepath.Clean(path)
	}
	if c.filePath == "" {
		return filepath.Clean(path)
	}
	return filepath.Join(filepath.Dir(c.filePath), path)
}

func (c *Config) loadNodeAuthState() (*nodeAuthState, error) {
	path := c.nodeAuthPath()
	var state *nodeAuthState
	err := withFileLock(path, func() error {
		var err error
		state, err = loadNodeAuthStateLocked(path)
		return err
	})
	return state, err
}

// loadNodeAuthStateLocked reads a state snapshot while the caller holds the
// path's sidecar lock.
func loadNodeAuthStateLocked(path string) (*nodeAuthState, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return newNodeAuthState(), nil
	}
	if err != nil {
		return nil, fmt.Errorf("read node auth overrides %q: %w", path, err)
	}
	state := newNodeAuthState()
	if err := yaml.Unmarshal(data, state); err != nil {
		return nil, fmt.Errorf("decode node auth overrides %q: %w", path, err)
	}
	if state.Version != 0 && state.Version != nodeAuthVersion {
		return nil, fmt.Errorf("unsupported node auth override version %d in %q", state.Version, path)
	}
	state.Version = nodeAuthVersion
	if state.Overrides == nil {
		state.Overrides = make(map[string]nodeAuthOverride)
	}
	for key := range state.Overrides {
		if strings.TrimSpace(key) == "" {
			delete(state.Overrides, key)
		}
	}
	return state, nil
}

func encodeNodeAuthState(state *nodeAuthState) ([]byte, error) {
	if state == nil {
		return nil, errors.New("node auth state is nil")
	}
	state.Version = nodeAuthVersion
	if state.Overrides == nil {
		state.Overrides = make(map[string]nodeAuthOverride)
	}
	data, err := yaml.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("encode node auth overrides: %w", err)
	}
	return data, nil
}

func (c *Config) updateNodeAuthState(update func(*nodeAuthState)) error {
	_, err := c.updateNodeAuthStateSnapshot(update)
	return err
}

func (c *Config) updateNodeAuthStateSnapshot(update func(*nodeAuthState)) (FileSnapshot, error) {
	if update == nil {
		return FileSnapshot{}, errors.New("node auth update is nil")
	}
	path := c.nodeAuthPath()
	var snapshot FileSnapshot
	err := withFileLock(path, func() error {
		var err error
		snapshot, err = c.updateNodeAuthStateLocked(update)
		return err
	})
	return snapshot, err
}

func (c *Config) updateNodeAuthStateLocked(update func(*nodeAuthState)) (FileSnapshot, error) {
	if update == nil {
		return FileSnapshot{}, errors.New("node auth update is nil")
	}
	path := c.nodeAuthPath()
	state, err := loadNodeAuthStateLocked(path)
	if err != nil {
		return FileSnapshot{}, err
	}
	update(state)
	data, err := encodeNodeAuthState(state)
	if err != nil {
		return FileSnapshot{}, err
	}
	snapshot, err := writeFileLockedSnapshot(path, data, 0o600)
	if err != nil {
		return FileSnapshot{}, fmt.Errorf("write node auth overrides: %w", err)
	}
	return snapshot, nil
}

// ApplyNodeAuthOverrides restores per-node listener credentials for nodes
// loaded from nodes_file/subscriptions. The stable NodeKey keeps overrides
// attached when the provider changes display names or URI query ordering.
func (c *Config) ApplyNodeAuthOverrides() error {
	if c == nil {
		return errors.New("config is nil")
	}
	state, err := c.loadNodeAuthState()
	if err != nil {
		return err
	}
	for idx := range c.Nodes {
		node := &c.Nodes[idx]
		if node.Source == NodeSourceInline {
			continue
		}
		if override, ok := state.Overrides[node.NodeKey()]; ok {
			node.Username = override.Username
			node.Password = override.Password
		}
	}
	return nil
}

func (c *Config) persistNodeAuthOverrides() error {
	_, err := c.updateNodeAuthStateSnapshot(c.applyNodeAuthOverridesToState)
	return err
}

func (c *Config) persistNodeAuthOverridesLocked() (FileSnapshot, error) {
	return c.updateNodeAuthStateLocked(c.applyNodeAuthOverridesToState)
}

func (c *Config) applyNodeAuthOverridesToState(state *nodeAuthState) {
	for _, node := range c.Nodes {
		key := node.NodeKey()
		if node.Source == NodeSourceInline {
			delete(state.Overrides, key)
			continue
		}
		if node.Username == c.MultiPort.Username && node.Password == c.MultiPort.Password {
			delete(state.Overrides, key)
			continue
		}
		state.Overrides[key] = nodeAuthOverride{
			Username: node.Username,
			Password: node.Password,
		}
	}
}

// RemoveNodeAuthOverride forgets an explicitly deleted external node without
// pruning overrides for nodes that are merely absent from one subscription
// refresh and may return later.
func (c *Config) RemoveNodeAuthOverride(node NodeConfig) error {
	if c == nil || node.Source == NodeSourceInline {
		return nil
	}
	return c.updateNodeAuthState(func(state *nodeAuthState) {
		delete(state.Overrides, node.NodeKey())
	})
}

type portLease struct {
	Port       uint16     `yaml:"port"`
	ReleasedAt *time.Time `yaml:"released_at,omitempty"`
}

type portMappingState struct {
	Version int                  `yaml:"version"`
	Leases  map[string]portLease `yaml:"leases"`
}

func newPortMappingState() *portMappingState {
	return &portMappingState{
		Version: portMappingVersion,
		Leases:  make(map[string]portLease),
	}
}

func (c *Config) portMapPath() string {
	path := strings.TrimSpace(c.MultiPort.PortMapFile)
	if path == "" {
		path = defaultPortMapFile
	}
	if filepath.IsAbs(path) {
		return filepath.Clean(path)
	}
	if c.filePath == "" {
		return filepath.Clean(path)
	}
	return filepath.Join(filepath.Dir(c.filePath), path)
}

func (c *Config) loadPortMappingState() (*portMappingState, error) {
	path := c.portMapPath()
	if path == "" {
		return newPortMappingState(), nil
	}
	var state *portMappingState
	err := withFileLock(path, func() error {
		var err error
		state, err = loadPortMappingStateLocked(path)
		return err
	})
	return state, err
}

// loadPortMappingStateLocked reads a state snapshot while the caller holds
// the path's sidecar lock.
func loadPortMappingStateLocked(path string) (*portMappingState, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return newPortMappingState(), nil
	}
	if err != nil {
		return nil, fmt.Errorf("read port map %q: %w", path, err)
	}

	state := newPortMappingState()
	if err := yaml.Unmarshal(data, state); err != nil {
		return nil, fmt.Errorf("decode port map %q: %w", path, err)
	}
	if state.Version != 0 && state.Version != portMappingVersion {
		return nil, fmt.Errorf("unsupported port map version %d in %q", state.Version, path)
	}
	state.Version = portMappingVersion
	if state.Leases == nil {
		state.Leases = make(map[string]portLease)
	}
	for key, lease := range state.Leases {
		if strings.TrimSpace(key) == "" || lease.Port == 0 {
			delete(state.Leases, key)
		}
	}
	return state, nil
}

func encodePortMappingState(state *portMappingState) ([]byte, error) {
	if state == nil {
		return nil, errors.New("port mapping state is nil")
	}
	state.Version = portMappingVersion
	if state.Leases == nil {
		state.Leases = make(map[string]portLease)
	}
	data, err := yaml.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("encode port map: %w", err)
	}
	return data, nil
}

func (c *Config) updatePortMappingState(update func(*portMappingState)) error {
	_, err := c.updatePortMappingStateSnapshot(update)
	return err
}

func (c *Config) updatePortMappingStateSnapshot(update func(*portMappingState)) (FileSnapshot, error) {
	if c.Mode != "multi-port" && c.Mode != "hybrid" {
		return FileSnapshot{}, nil
	}
	if update == nil {
		return FileSnapshot{}, errors.New("port mapping update is nil")
	}
	path := c.portMapPath()
	var snapshot FileSnapshot
	err := withFileLock(path, func() error {
		var err error
		snapshot, err = c.updatePortMappingStateLocked(update)
		return err
	})
	return snapshot, err
}

func (c *Config) updatePortMappingStateLocked(update func(*portMappingState)) (FileSnapshot, error) {
	if c.Mode != "multi-port" && c.Mode != "hybrid" {
		return FileSnapshot{}, nil
	}
	if update == nil {
		return FileSnapshot{}, errors.New("port mapping update is nil")
	}
	path := c.portMapPath()
	state, err := loadPortMappingStateLocked(path)
	if err != nil {
		return FileSnapshot{}, err
	}
	update(state)
	data, err := encodePortMappingState(state)
	if err != nil {
		return FileSnapshot{}, err
	}
	snapshot, err := writeFileLockedSnapshot(path, data, 0o600)
	if err != nil {
		return FileSnapshot{}, fmt.Errorf("write port map: %w", err)
	}
	return snapshot, nil
}

func (c *Config) pruneExpiredPortLeases(state *portMappingState, activeKeys map[string]struct{}, now time.Time) {
	reuseDelay := c.MultiPort.PortReuseDelay
	if reuseDelay <= 0 {
		reuseDelay = defaultPortReuseTTL
	}
	for key, lease := range state.Leases {
		if _, active := activeKeys[key]; active {
			continue
		}
		if lease.ReleasedAt == nil {
			releasedAt := now
			lease.ReleasedAt = &releasedAt
			state.Leases[key] = lease
			continue
		}
		if now.Sub(*lease.ReleasedAt) >= reuseDelay {
			delete(state.Leases, key)
		}
	}
}

func (c *Config) recordNodePortLeases(state *portMappingState, now time.Time) {
	activeKeys := make(map[string]struct{}, len(c.Nodes))
	for idx := range c.Nodes {
		node := c.Nodes[idx]
		key := node.NodeKey()
		activeKeys[key] = struct{}{}
		for otherKey, lease := range state.Leases {
			if otherKey != key && lease.Port == node.Port {
				delete(state.Leases, otherKey)
			}
		}
		state.Leases[key] = portLease{Port: node.Port}
	}
	c.pruneExpiredPortLeases(state, activeKeys, now)
}

func (c *Config) persistNodePortLeases(now time.Time) error {
	_, err := c.updatePortMappingStateSnapshot(func(state *portMappingState) {
		c.recordNodePortLeases(state, now)
	})
	return err
}

func (c *Config) persistNodePortLeasesLocked(now time.Time) (FileSnapshot, error) {
	return c.updatePortMappingStateLocked(func(state *portMappingState) {
		c.recordNodePortLeases(state, now)
	})
}

func (c *Config) assignNodePorts(runtimeMap map[string]uint16, persist bool) error {
	if c.Mode != "multi-port" && c.Mode != "hybrid" {
		portCursor := c.MultiPort.BasePort
		for idx := range c.Nodes {
			if c.Nodes[idx].Port == 0 {
				c.Nodes[idx].Port = portCursor
				portCursor++
			}
		}
		return nil
	}

	state, err := c.loadPortMappingState()
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	activeKeys := make(map[string]struct{}, len(c.Nodes))
	for idx := range c.Nodes {
		activeKeys[c.Nodes[idx].NodeKey()] = struct{}{}
	}
	c.pruneExpiredPortLeases(state, activeKeys, now)

	reserved := make(map[uint16]string)
	for key, lease := range state.Leases {
		if _, active := activeKeys[key]; !active {
			reserved[lease.Port] = key
		}
	}
	used := make(map[uint16]string, len(c.Nodes)+1)
	if c.Mode == "hybrid" {
		used[c.Listener.Port] = "pool-listener"
	}

	for idx := range c.Nodes {
		node := &c.Nodes[idx]
		key := node.NodeKey()
		explicitPort := node.Port > 0
		candidate := node.Port
		if candidate == 0 && runtimeMap != nil {
			candidate = runtimeMap[key]
		}
		if candidate == 0 {
			candidate = state.Leases[key].Port
		}

		if candidate > 0 {
			_, alreadyUsed := used[candidate]
			_, heldForRemovedNode := reserved[candidate]
			if !alreadyUsed && (explicitPort || !heldForRemovedNode) {
				node.Port = candidate
				used[candidate] = key
				continue
			}
			log.Printf("⚠️  Port %d for node %q is unavailable or reserved; assigning a new stable port", candidate, node.Name)
		}
		node.Port = 0
	}

	portCursor := int(c.MultiPort.BasePort)
	if portCursor <= 0 {
		portCursor = 24000
	}
	for idx := range c.Nodes {
		node := &c.Nodes[idx]
		if node.Port == 0 {
			for {
				if portCursor > 65535 {
					return fmt.Errorf("no available ports found starting from %d", c.MultiPort.BasePort)
				}
				candidate := uint16(portCursor)
				_, alreadyUsed := used[candidate]
				_, reservedForRemovedNode := reserved[candidate]
				if !alreadyUsed && !reservedForRemovedNode && IsPortAvailable(c.MultiPort.Address, candidate) {
					node.Port = candidate
					used[candidate] = node.NodeKey()
					portCursor++
					break
				}
				portCursor++
			}
			log.Printf("📌 Assigned stable port %d for node %q", node.Port, node.Name)
		}

		if node.Username == "" {
			node.Username = c.MultiPort.Username
			node.Password = c.MultiPort.Password
		}
	}

	if persist {
		// Availability probes intentionally run without the sidecar lock. The
		// short commit below re-reads and merges the latest state while locked,
		// so unrelated concurrent leases are retained without serializing OS
		// port probes behind file I/O.
		return c.persistNodePortLeases(now)
	}
	return nil
}

// PersistPortMap commits the current node-to-port assignments after a
// successful start/reload.
func (c *Config) PersistPortMap() error {
	if c == nil || (c.Mode != "multi-port" && c.Mode != "hybrid") {
		return nil
	}
	return c.persistNodePortLeases(time.Now().UTC())
}

// NormalizeWithPortMap applies defaults and validation, preserving port assignments
// for nodes that exist in the provided port map.
func (c *Config) NormalizeWithPortMap(portMap map[string]uint16) error {
	if c.Mode == "" {
		c.Mode = "pool"
	}
	if c.Mode == "multi_port" {
		c.Mode = "multi-port"
	}
	switch c.Mode {
	case "pool", "multi-port", "hybrid":
	default:
		return fmt.Errorf("unsupported mode %q (use 'pool', 'multi-port', or 'hybrid')", c.Mode)
	}
	if c.Listener.Address == "" {
		c.Listener.Address = "0.0.0.0"
	}
	if c.Listener.Port == 0 {
		c.Listener.Port = 2323
	}
	if err := c.normalizePoolConfig(); err != nil {
		return err
	}
	if c.MultiPort.Address == "" {
		c.MultiPort.Address = "0.0.0.0"
	}
	if c.MultiPort.BasePort == 0 {
		c.MultiPort.BasePort = 24000
	}
	if c.MultiPort.PortReuseDelay <= 0 {
		c.MultiPort.PortReuseDelay = defaultPortReuseTTL
	}
	if c.Management.Listen == "" {
		c.Management.Listen = "127.0.0.1:9091"
	}
	if c.Management.ProbeTarget == "" {
		c.Management.ProbeTarget = "www.apple.com:80"
	}
	if _, ready, err := probetarget.Parse(c.Management.ProbeTarget); err != nil || !ready {
		if err == nil {
			err = errors.New("probe target is empty")
		}
		return fmt.Errorf("invalid management probe_target: %w", err)
	}
	if c.Management.Enabled == nil {
		defaultEnabled := true
		c.Management.Enabled = &defaultEnabled
	}
	if err := ValidateManagementConfig(c.Management); err != nil {
		return err
	}
	c.normalizeGeoIPConfig()
	if c.SubscriptionRefresh.Interval <= 0 {
		c.SubscriptionRefresh.Interval = 1 * time.Hour
	}
	if c.SubscriptionRefresh.Timeout <= 0 {
		c.SubscriptionRefresh.Timeout = 30 * time.Second
	}
	if c.SubscriptionRefresh.HealthCheckTimeout <= 0 {
		c.SubscriptionRefresh.HealthCheckTimeout = 60 * time.Second
	}
	if c.SubscriptionRefresh.DrainTimeout <= 0 {
		c.SubscriptionRefresh.DrainTimeout = 30 * time.Second
	}
	if c.SubscriptionRefresh.MinAvailableNodes <= 0 {
		c.SubscriptionRefresh.MinAvailableNodes = 1
	}
	c.SubscriptionRefresh.FetchConcurrency = NormalizeSubscriptionFetchConcurrency(c.SubscriptionRefresh.FetchConcurrency)
	validatedSubscriptions, err := ValidateSubscriptionURLs(c.Subscriptions)
	if err != nil {
		return err
	}
	c.Subscriptions = validatedSubscriptions

	if len(c.Nodes) == 0 {
		return errors.New("config.nodes cannot be empty")
	}

	for idx := range c.Nodes {
		c.Nodes[idx].Name = sanitizeNodeName(c.Nodes[idx].Name)
		c.Nodes[idx].URI = strings.TrimSpace(c.Nodes[idx].URI)
		if c.Nodes[idx].URI == "" {
			return fmt.Errorf("node %d is missing uri", idx)
		}

		// Auto-extract name from URI if not provided
		if c.Nodes[idx].Name == "" {
			c.Nodes[idx].Name = sanitizeNodeName(ExtractNodeName(c.Nodes[idx].URI))
		}
		if c.Nodes[idx].Name == "" {
			c.Nodes[idx].Name = fmt.Sprintf("node-%d", idx)
		}
	}
	if err := validateUniqueNodeKeys(c.Nodes); err != nil {
		return err
	}
	if err := c.ApplyNodeAuthOverrides(); err != nil {
		return err
	}
	if err := c.validateInboundCredentials(); err != nil {
		return err
	}
	if err := c.assignNodePorts(portMap, false); err != nil {
		return err
	}

	if c.LogLevel == "" {
		c.LogLevel = "info"
	}

	c.normalizeLogConfig()

	return nil
}

func validateUniqueNodeKeys(nodes []NodeConfig) error {
	seen := make(map[string]int, len(nodes))
	for index := range nodes {
		key := nodes[index].NodeKey()
		if previous, exists := seen[key]; exists {
			return fmt.Errorf("nodes %d and %d resolve to the same proxy identity", previous, index)
		}
		seen[key] = index
	}
	return nil
}

func sanitizeNodeName(name string) string {
	name = strings.Map(func(character rune) rune {
		if unicode.IsControl(character) {
			return -1
		}
		return character
	}, name)
	return strings.TrimSpace(name)
}

func validateCredentialPair(label, username, password string) error {
	if (username == "") != (password == "") {
		return fmt.Errorf("%s username and password must either both be set or both be empty", label)
	}
	return nil
}

func (c *Config) validateInboundCredentials() error {
	if c == nil {
		return errors.New("config is nil")
	}
	if c.Mode == "pool" || c.Mode == "hybrid" {
		if err := validateCredentialPair("listener", c.Listener.Username, c.Listener.Password); err != nil {
			return err
		}
	}
	if c.Mode != "multi-port" && c.Mode != "hybrid" {
		return nil
	}
	if err := validateCredentialPair("multi_port", c.MultiPort.Username, c.MultiPort.Password); err != nil {
		return err
	}
	for index, node := range c.Nodes {
		if err := validateCredentialPair(fmt.Sprintf("node %d", index), node.Username, node.Password); err != nil {
			return err
		}
	}
	return nil
}

func (c *Config) normalizePoolConfig() error {
	mode := strings.ToLower(strings.TrimSpace(c.Pool.Mode))
	if mode == "" || mode == "round-robin" || mode == "round_robin" {
		mode = "sequential"
	}
	switch mode {
	case "sequential", "random", "balance", "latency":
		c.Pool.Mode = mode
	default:
		return fmt.Errorf("unsupported pool mode %q (use 'sequential', 'random', 'balance', or 'latency')", c.Pool.Mode)
	}
	if c.Pool.FailureThreshold <= 0 {
		c.Pool.FailureThreshold = 3
	}
	if c.Pool.BlacklistDuration <= 0 {
		c.Pool.BlacklistDuration = 24 * time.Hour
	}
	if c.Pool.RetryEnabled == nil {
		enabled := true
		c.Pool.RetryEnabled = &enabled
	}
	if c.Pool.RetryAttempts <= 0 {
		c.Pool.RetryAttempts = 3
	} else if c.Pool.RetryAttempts > 10 {
		c.Pool.RetryAttempts = 10
	}
	if c.Pool.TransientCooldown <= 0 {
		c.Pool.TransientCooldown = time.Minute
	}
	if c.Pool.LatencySampleSize <= 0 {
		c.Pool.LatencySampleSize = 4
	} else if c.Pool.LatencySampleSize > 32 {
		c.Pool.LatencySampleSize = 32
	}
	if c.Pool.LatencyTolerance <= 0 {
		c.Pool.LatencyTolerance = 50 * time.Millisecond
	}
	if c.Pool.Sticky.TTL <= 0 {
		c.Pool.Sticky.TTL = 30 * time.Minute
	}
	if c.Pool.Sticky.MaxEntries <= 0 {
		c.Pool.Sticky.MaxEntries = 4096
	} else if c.Pool.Sticky.MaxEntries > 1_000_000 {
		c.Pool.Sticky.MaxEntries = 1_000_000
	}
	return nil
}

func (c *Config) normalizeGeoIPConfig() {
	if c.GeoIP.AutoUpdateEnabled && c.GeoIP.AutoUpdateInterval <= 0 {
		c.GeoIP.AutoUpdateInterval = 24 * time.Hour
	}
	if c.GeoIP.ExitIPURL == "" {
		c.GeoIP.ExitIPURL = "https://api.ipify.org"
	}
	if c.GeoIP.ExitIPTimeout <= 0 {
		c.GeoIP.ExitIPTimeout = 10 * time.Second
	}
	if c.GeoIP.ExitIPConcurrency <= 0 {
		c.GeoIP.ExitIPConcurrency = 16
	}
}

// normalizeLogConfig applies defaults to the log config.
func (c *Config) normalizeLogConfig() {
	if c.Log.Output == "" {
		c.Log.Output = "stdout"
	}
	if c.Log.File == "" {
		c.Log.File = "logs/easy_proxies.log"
	}
	// Resolve relative log file path against config dir
	if c.filePath != "" && !filepath.IsAbs(c.Log.File) {
		c.Log.File = filepath.Join(filepath.Dir(c.filePath), c.Log.File)
	}
	if c.Log.MaxSize <= 0 {
		c.Log.MaxSize = 50
	}
	if c.Log.MaxBackups <= 0 {
		c.Log.MaxBackups = 3
	}
	if c.Log.MaxAge <= 0 {
		c.Log.MaxAge = 7
	}
}

// ManagementEnabled reports whether the monitoring endpoint should run.
func (c *Config) ManagementEnabled() bool {
	if c.Management.Enabled == nil {
		return true
	}
	return *c.Management.Enabled
}

// ValidateManagementConfig prevents accidentally exposing an unauthenticated
// administrative API beyond the local machine. Disabled management endpoints
// are permitted so a future listen address can be prepared before enabling it.
func ValidateManagementConfig(cfg ManagementConfig) error {
	if cfg.Enabled != nil && !*cfg.Enabled {
		return nil
	}
	listen := strings.TrimSpace(cfg.Listen)
	host, portText, err := net.SplitHostPort(listen)
	if err != nil {
		return fmt.Errorf("invalid management listen address %q: %w", listen, err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("invalid management listen port %q", portText)
	}
	certFile := strings.TrimSpace(cfg.TLSCertFile)
	keyFile := strings.TrimSpace(cfg.TLSKeyFile)
	if (certFile == "") != (keyFile == "") {
		return errors.New("management tls_cert_file and tls_key_file must be configured together")
	}
	host = strings.Trim(strings.TrimSpace(host), "[]")
	loopback := strings.EqualFold(host, "localhost")
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		loopback = true
	}
	if loopback {
		return nil
	}
	if strings.TrimSpace(cfg.Password) == "" {
		return errors.New("management password is required when listen is not loopback")
	}
	if certFile == "" {
		return errors.New("management TLS is required when listen is not loopback")
	}
	return nil
}

// ProbeConcurrencyOrDefault returns the process-wide health probe worker
// count. Explicit values are bounded so a typo cannot exhaust file descriptors
// or create an unbounded number of goroutines.
func (c *Config) ProbeConcurrencyOrDefault() int {
	if c == nil || c.Management.ProbeConcurrency <= 0 {
		return 32
	}
	if c.Management.ProbeConcurrency > 1024 {
		return 1024
	}
	return c.Management.ProbeConcurrency
}

// loadNodesFromFile reads a nodes file where each line is a proxy URI
// Lines starting with # are comments, empty lines are ignored
func loadNodesFromFile(path string) ([]NodeConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return parseNodesFromContent(string(data))
}

// parseSubscriptionContent tries to parse subscription content in various formats (optimized)
func parseSubscriptionContent(content string) ([]NodeConfig, error) {
	content = strings.TrimSpace(content)

	// Detect only a top-level Clash proxies key. A substring search misclassifies
	// ordinary links whose name/query happens to contain "proxies:".
	if isLikelyClashYAML(content) {
		return parseClashYAML(content)
	}

	// Check if it's base64 encoded (common for v2ray subscriptions)
	if isBase64(content) {
		if decoded, ok := decodeSubscriptionBase64(content); ok && utf8.Valid(decoded) {
			decodedContent := strings.TrimSpace(string(decoded))
			if isLikelyClashYAML(decodedContent) {
				return parseClashYAML(decodedContent)
			}
			if decodedNodes, err := parseNodesFromContent(decodedContent); err == nil && len(decodedNodes) > 0 {
				return decodedNodes, nil
			}
		}
	}

	// Parse as plain text (one URI per line)
	return parseNodesFromContent(content)
}

// ParseSubscriptionContent parses subscription content in various formats (base64, plain text, Clash YAML).
// This is exported for use by the subscription manager.
func ParseSubscriptionContent(content string) ([]NodeConfig, error) {
	return parseSubscriptionContent(content)
}

// parseNodesFromContent parses nodes from plain text content (one URI per line)
func parseNodesFromContent(content string) ([]NodeConfig, error) {
	var nodes []NodeConfig
	lines := strings.Split(content, "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Check if it's a valid proxy URI
		if IsProxyURI(line) {
			nodes = append(nodes, NodeConfig{
				URI: line,
			})
		}
	}

	return nodes, nil
}

// isBase64 checks if a string looks like base64 encoded content (optimized version)
func isBase64(s string) bool {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return false
	}
	if strings.Contains(s, "://") {
		return false
	}
	_, ok := decodeSubscriptionBase64(s)
	return ok
}

func decodeSubscriptionBase64(content string) ([]byte, bool) {
	content = strings.Join(strings.Fields(content), "")
	if content == "" {
		return nil, false
	}
	for _, encoding := range []*base64.Encoding{
		base64.StdEncoding,
		base64.RawStdEncoding,
		base64.URLEncoding,
		base64.RawURLEncoding,
	} {
		if decoded, err := encoding.DecodeString(content); err == nil {
			return decoded, true
		}
	}
	return nil, false
}

func isLikelyClashYAML(content string) bool {
	content = strings.TrimPrefix(content, "\ufeff")
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSuffix(line, "\r")
		if line == "" || line[0] == ' ' || line[0] == '\t' {
			continue
		}
		if !strings.HasPrefix(line, "proxies:") {
			continue
		}
		remainder := strings.TrimSpace(strings.TrimPrefix(line, "proxies:"))
		return remainder == "" || strings.HasPrefix(remainder, "[")
	}
	return false
}

// IsProxyURI checks if a string is a valid proxy URI
func IsProxyURI(s string) bool {
	schemes := []string{"vmess://", "vless://", "trojan://", "ss://", "shadowsocks://", "ssr://", "shadowsocksr://", "hysteria://", "hysteria2://", "hy2://", "tuic://", "socks5://", "socks5h://", "socks://", "http://", "https://", "anytls://"}
	lower := strings.ToLower(strings.TrimSpace(s))
	for _, scheme := range schemes {
		if strings.HasPrefix(lower, scheme) {
			return true
		}
	}
	return false
}

// clashConfig represents a minimal Clash configuration for parsing proxies
// flexInt handles YAML values that may be either int or quoted string.
type flexInt int

func (fi *flexInt) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var intVal int
	if err := unmarshal(&intVal); err == nil {
		*fi = flexInt(intVal)
		return nil
	}
	var strVal string
	if err := unmarshal(&strVal); err != nil {
		return fmt.Errorf("cannot unmarshal port: expected int or string")
	}
	parsed, err := strconv.Atoi(strVal)
	if err != nil {
		return fmt.Errorf("cannot parse port %q as int: %w", strVal, err)
	}
	*fi = flexInt(parsed)
	return nil
}

type clashConfig struct {
	Proxies []yaml.Node `yaml:"proxies"`
}

type clashProxy struct {
	Name              string                 `yaml:"name"`
	Type              string                 `yaml:"type"`
	Server            string                 `yaml:"server"`
	Port              flexInt                `yaml:"port"`
	Ports             string                 `yaml:"ports"`
	UUID              string                 `yaml:"uuid"`
	Password          string                 `yaml:"password"`
	Cipher            string                 `yaml:"cipher"`
	AlterId           int                    `yaml:"alterId"`
	Network           string                 `yaml:"network"`
	TLS               bool                   `yaml:"tls"`
	SkipCertVerify    bool                   `yaml:"skip-cert-verify"`
	ServerName        string                 `yaml:"servername"`
	SNI               string                 `yaml:"sni"`
	Flow              string                 `yaml:"flow"`
	UDP               bool                   `yaml:"udp"`
	WSOpts            *clashWSOptions        `yaml:"ws-opts"`
	GrpcOpts          *clashGrpcOptions      `yaml:"grpc-opts"`
	RealityOpts       *clashRealityOptions   `yaml:"reality-opts"`
	ClientFingerprint string                 `yaml:"client-fingerprint"`
	Obfs              string                 `yaml:"obfs"`
	ObfsPassword      string                 `yaml:"obfs-password"`
	Plugin            string                 `yaml:"plugin"`
	PluginOpts        map[string]interface{} `yaml:"plugin-opts"`
	// TUIC-specific fields
	ALPN                 []string `yaml:"alpn"`
	CongestionController string   `yaml:"congestion-controller"`
	UDPRelayMode         string   `yaml:"udp-relay-mode"`
	// ShadowsocksR-specific fields
	Protocol      string `yaml:"protocol"`
	ProtocolParam string `yaml:"protocol-param"`
	ObfsParam     string `yaml:"obfs-param"`
	// Hysteria v1-specific fields
	AuthStr             string  `yaml:"auth-str"`
	Auth                string  `yaml:"auth"`
	UpMbps              flexInt `yaml:"up"`
	DownMbps            flexInt `yaml:"down"`
	PeerSNI             string  `yaml:"peer"`
	RecvWindow          uint64  `yaml:"recv-window"`
	RecvWindowConn      uint64  `yaml:"recv-window-conn"`
	DisableMTUDiscovery bool    `yaml:"disable-mtu-discovery"`
}

type clashWSOptions struct {
	Path    string            `yaml:"path"`
	Headers map[string]string `yaml:"headers"`
}

type clashGrpcOptions struct {
	GrpcServiceName string `yaml:"grpc-service-name"`
}

type clashRealityOptions struct {
	PublicKey string `yaml:"public-key"`
	ShortID   string `yaml:"short-id"`
}

// parseClashYAML parses Clash YAML format and converts to NodeConfig
func parseClashYAML(content string) ([]NodeConfig, error) {
	var clash clashConfig
	if err := yaml.Unmarshal([]byte(content), &clash); err != nil {
		return nil, fmt.Errorf("parse clash yaml: %w", err)
	}

	var nodes []NodeConfig
	skipped := 0
	for index, rawProxy := range clash.Proxies {
		var proxy clashProxy
		if err := rawProxy.Decode(&proxy); err != nil {
			skipped++
			log.Printf("[subscription] WARN: skip Clash proxy #%d (malformed fields)", index)
			continue
		}
		uri := convertClashProxyToURI(proxy)
		if uri == "" {
			skipped++
			log.Printf("[subscription] WARN: skip Clash proxy #%d name=%q type=%q", index, proxy.Name, proxy.Type)
			continue
		}
		nodes = append(nodes, NodeConfig{Name: proxy.Name, URI: uri})
	}
	if skipped > 0 {
		log.Printf("[subscription] parsed %d Clash nodes and skipped %d malformed/unsupported entries", len(nodes), skipped)
	}

	return nodes, nil
}

// convertClashProxyToURI converts a Clash proxy config to a standard URI
func convertClashProxyToURI(p clashProxy) string {
	switch strings.ToLower(p.Type) {
	case "vmess":
		return buildVMessURI(p)
	case "vless":
		return buildVLESSURI(p)
	case "trojan":
		return buildTrojanURI(p)
	case "anytls":
		return buildAnyTLSURI(p)
	case "ss", "shadowsocks":
		return buildShadowsocksURI(p)
	case "hysteria2", "hy2":
		return buildHysteria2URI(p)
	case "tuic":
		return buildTUICURI(p)
	case "ssr", "shadowsocksr":
		return buildShadowsocksRURI(p)
	case "hysteria":
		return buildHysteriaURI(p)
	default:
		return ""
	}
}

func clashProxyEndpoint(p clashProxy) (string, string, bool) {
	host := strings.TrimSpace(p.Server)
	if len(host) >= 2 && strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = strings.TrimSpace(host[1 : len(host)-1])
	}
	port := int(p.Port)
	if host == "" || strings.ContainsAny(host, "\r\n\x00") || port < 1 || port > 65535 {
		return "", "", false
	}
	return host, net.JoinHostPort(host, strconv.Itoa(port)), true
}

func buildVMessURI(p clashProxy) string {
	_, endpoint, ok := clashProxyEndpoint(p)
	if !ok {
		return ""
	}
	params := url.Values{}
	if p.Network != "" && p.Network != "tcp" {
		params.Set("type", p.Network)
	}
	if p.TLS {
		params.Set("security", "tls")
		if p.ServerName != "" {
			params.Set("sni", p.ServerName)
		} else if p.SNI != "" {
			params.Set("sni", p.SNI)
		}
	}
	if p.WSOpts != nil {
		if p.WSOpts.Path != "" {
			params.Set("path", p.WSOpts.Path)
		}
		if host, ok := p.WSOpts.Headers["Host"]; ok {
			params.Set("host", host)
		}
	}
	if p.ClientFingerprint != "" {
		params.Set("fp", p.ClientFingerprint)
	}

	query := ""
	if len(params) > 0 {
		query = "?" + params.Encode()
	}

	return fmt.Sprintf("vmess://%s@%s%s#%s", url.User(p.UUID).String(), endpoint, query, url.QueryEscape(p.Name))
}

func buildVLESSURI(p clashProxy) string {
	_, endpoint, ok := clashProxyEndpoint(p)
	if !ok {
		return ""
	}
	params := url.Values{}
	params.Set("encryption", "none")

	if p.Network != "" && p.Network != "tcp" {
		params.Set("type", p.Network)
	}
	if p.Flow != "" {
		params.Set("flow", p.Flow)
	}
	if p.TLS {
		params.Set("security", "tls")
		if p.ServerName != "" {
			params.Set("sni", p.ServerName)
		} else if p.SNI != "" {
			params.Set("sni", p.SNI)
		}
	}
	if p.RealityOpts != nil {
		params.Set("security", "reality")
		if p.RealityOpts.PublicKey != "" {
			params.Set("pbk", p.RealityOpts.PublicKey)
		}
		if p.RealityOpts.ShortID != "" {
			params.Set("sid", p.RealityOpts.ShortID)
		}
		if p.ServerName != "" {
			params.Set("sni", p.ServerName)
		}
	}
	if p.WSOpts != nil {
		if p.WSOpts.Path != "" {
			params.Set("path", p.WSOpts.Path)
		}
		if host, ok := p.WSOpts.Headers["Host"]; ok {
			params.Set("host", host)
		}
	}
	if p.GrpcOpts != nil && p.GrpcOpts.GrpcServiceName != "" {
		params.Set("serviceName", p.GrpcOpts.GrpcServiceName)
	}
	if p.ClientFingerprint != "" {
		params.Set("fp", p.ClientFingerprint)
	}

	return fmt.Sprintf("vless://%s@%s?%s#%s", url.User(p.UUID).String(), endpoint, params.Encode(), url.QueryEscape(p.Name))
}

func buildTrojanURI(p clashProxy) string {
	_, endpoint, ok := clashProxyEndpoint(p)
	if !ok {
		return ""
	}
	params := url.Values{}
	if p.ServerName != "" {
		params.Set("sni", p.ServerName)
	} else if p.SNI != "" {
		params.Set("sni", p.SNI)
	}
	if p.SkipCertVerify {
		params.Set("allowInsecure", "1")
	}
	if p.Network != "" && p.Network != "tcp" {
		params.Set("type", p.Network)
	}
	if p.WSOpts != nil {
		if p.WSOpts.Path != "" {
			params.Set("path", p.WSOpts.Path)
		}
		if host, ok := p.WSOpts.Headers["Host"]; ok {
			params.Set("host", host)
		}
	}
	if p.ClientFingerprint != "" {
		params.Set("fp", p.ClientFingerprint)
	}

	query := ""
	if len(params) > 0 {
		query = "?" + params.Encode()
	}

	return fmt.Sprintf("trojan://%s@%s%s#%s", url.User(p.Password).String(), endpoint, query, url.QueryEscape(p.Name))
}

func buildAnyTLSURI(p clashProxy) string {
	_, endpoint, ok := clashProxyEndpoint(p)
	if !ok {
		return ""
	}
	params := url.Values{}
	if p.ServerName != "" {
		params.Set("sni", p.ServerName)
	} else if p.SNI != "" {
		params.Set("sni", p.SNI)
	}
	if p.SkipCertVerify {
		params.Set("allowInsecure", "1")
	}
	if p.ClientFingerprint != "" {
		params.Set("fp", p.ClientFingerprint)
	}

	query := ""
	if len(params) > 0 {
		query = "?" + params.Encode()
	}

	return fmt.Sprintf("anytls://%s@%s%s#%s", url.User(p.Password).String(), endpoint, query, url.QueryEscape(p.Name))
}

func buildShadowsocksURI(p clashProxy) string {
	_, endpoint, ok := clashProxyEndpoint(p)
	if !ok {
		return ""
	}
	// Encode method:password in base64
	userInfo := base64.RawURLEncoding.EncodeToString([]byte(p.Cipher + ":" + p.Password))
	return fmt.Sprintf("ss://%s@%s#%s", userInfo, endpoint, url.QueryEscape(p.Name))
}

func buildHysteria2URI(p clashProxy) string {
	_, endpoint, ok := clashProxyEndpoint(p)
	if !ok {
		return ""
	}
	params := url.Values{}
	if p.ServerName != "" {
		params.Set("sni", p.ServerName)
	} else if p.SNI != "" {
		params.Set("sni", p.SNI)
	}
	if p.SkipCertVerify {
		params.Set("insecure", "1")
	}
	if p.Obfs != "" {
		params.Set("obfs", p.Obfs)
		if p.ObfsPassword != "" {
			params.Set("obfs-password", p.ObfsPassword)
		}
	}
	if strings.TrimSpace(p.Ports) != "" {
		ports, valid := normalizeHysteria2PortsValue(strings.TrimSpace(p.Ports))
		if !valid {
			return ""
		}
		params.Set("ports", ports)
	}

	query := ""
	if len(params) > 0 {
		query = "?" + params.Encode()
	}

	return fmt.Sprintf("hysteria2://%s@%s%s#%s", url.User(p.Password).String(), endpoint, query, url.QueryEscape(p.Name))
}

func normalizeHysteria2PortsValue(value string) (string, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", true
	}

	parts := strings.Split(value, ",")
	normalized := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return "", false
		}
		separator := ""
		if strings.Count(part, ":") == 1 && !strings.Contains(part, "-") {
			separator = ":"
		} else if strings.Count(part, "-") == 1 && !strings.Contains(part, ":") {
			separator = "-"
		} else if strings.ContainsAny(part, ":-") {
			return "", false
		}

		if separator == "" {
			port, err := strconv.Atoi(part)
			if err != nil || port < 1 || port > 65535 {
				return "", false
			}
			normalized = append(normalized, strconv.Itoa(port))
			continue
		}

		startText, endText, _ := strings.Cut(part, separator)
		start, startErr := strconv.Atoi(strings.TrimSpace(startText))
		end, endErr := strconv.Atoi(strings.TrimSpace(endText))
		if startErr != nil || endErr != nil || start < 1 || start > 65535 || end < 1 || end > 65535 || start > end {
			return "", false
		}
		normalized = append(normalized, strconv.Itoa(start)+":"+strconv.Itoa(end))
	}

	return strings.Join(normalized, ","), true
}

func buildTUICURI(p clashProxy) string {
	_, endpoint, ok := clashProxyEndpoint(p)
	if !ok {
		return ""
	}
	params := url.Values{}
	if p.ServerName != "" {
		params.Set("sni", p.ServerName)
	} else if p.SNI != "" {
		params.Set("sni", p.SNI)
	}
	if p.SkipCertVerify {
		params.Set("allowInsecure", "1")
	}
	if p.CongestionController != "" {
		params.Set("congestion_control", p.CongestionController)
	}
	if p.UDPRelayMode != "" {
		params.Set("udp_relay_mode", p.UDPRelayMode)
	}
	if len(p.ALPN) > 0 {
		params.Set("alpn", strings.Join(p.ALPN, ","))
	}

	query := ""
	if len(params) > 0 {
		query = "?" + params.Encode()
	}

	// TUIC URI format: tuic://uuid:password@server:port?params#name
	return fmt.Sprintf("tuic://%s@%s%s#%s", url.UserPassword(p.UUID, p.Password).String(), endpoint, query, url.QueryEscape(p.Name))
}

func buildShadowsocksRURI(p clashProxy) string {
	host, _, ok := clashProxyEndpoint(p)
	if !ok || p.Password == "" {
		return ""
	}
	port := int(p.Port)
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		host = "[" + host + "]"
	}
	password := base64.RawURLEncoding.EncodeToString([]byte(p.Password))
	main := fmt.Sprintf("%s:%d:%s:%s:%s:%s",
		host,
		port,
		defaultString(p.Protocol, "origin"),
		defaultString(p.Cipher, "none"),
		defaultString(p.Obfs, "plain"),
		password,
	)
	params := url.Values{}
	if p.ObfsParam != "" {
		params.Set("obfsparam", base64.RawURLEncoding.EncodeToString([]byte(p.ObfsParam)))
	}
	if p.ProtocolParam != "" {
		params.Set("protoparam", base64.RawURLEncoding.EncodeToString([]byte(p.ProtocolParam)))
	}
	if p.Name != "" {
		params.Set("remarks", base64.RawURLEncoding.EncodeToString([]byte(p.Name)))
	}
	if len(params) > 0 {
		main += "/?" + params.Encode()
	}
	return "ssr://" + base64.RawURLEncoding.EncodeToString([]byte(main))
}

func buildHysteriaURI(p clashProxy) string {
	_, endpoint, ok := clashProxyEndpoint(p)
	if !ok {
		return ""
	}
	params := url.Values{}
	params.Set("protocol", "udp")
	auth := firstNonEmpty(p.AuthStr, p.Auth, p.Password)
	if auth != "" {
		params.Set("auth", auth)
	}
	if peer := firstNonEmpty(p.ServerName, p.SNI, p.PeerSNI); peer != "" {
		params.Set("peer", peer)
	}
	if p.SkipCertVerify {
		params.Set("insecure", "1")
	}
	if p.UpMbps > 0 {
		params.Set("upmbps", strconv.Itoa(int(p.UpMbps)))
	}
	if p.DownMbps > 0 {
		params.Set("downmbps", strconv.Itoa(int(p.DownMbps)))
	}
	if len(p.ALPN) > 0 {
		params.Set("alpn", strings.Join(p.ALPN, ","))
	}
	if p.Obfs != "" {
		params.Set("obfs", p.Obfs)
		if p.ObfsPassword != "" {
			params.Set("obfsParam", p.ObfsPassword)
		}
	}
	if p.RecvWindow > 0 {
		params.Set("recv_window", strconv.FormatUint(p.RecvWindow, 10))
	}
	if p.RecvWindowConn > 0 {
		params.Set("recv_window_conn", strconv.FormatUint(p.RecvWindowConn, 10))
	}
	if p.DisableMTUDiscovery {
		params.Set("disable_mtu_discovery", "1")
	}
	return "hysteria://" + endpoint + "?" + params.Encode() + "#" + url.PathEscape(p.Name)
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

// FilePath returns the config file path.
func (c *Config) FilePath() string {
	if c == nil {
		return ""
	}
	return c.filePath
}

// SetFilePath sets the config file path (used when creating config programmatically).
func (c *Config) SetFilePath(path string) {
	if c != nil {
		c.filePath = path
	}
}

// HealthStatePath resolves the restart-safe pool health sidecar.
func (c *Config) HealthStatePath() string {
	if c == nil {
		return ""
	}
	path := strings.TrimSpace(c.Pool.HealthStateFile)
	if path == "" {
		path = "health-state.yaml"
	}
	if filepath.IsAbs(path) {
		return filepath.Clean(path)
	}
	if c.filePath == "" {
		return filepath.Clean(path)
	}
	return filepath.Join(filepath.Dir(c.filePath), path)
}

// writeNodesToFile writes nodes to a file (one URI per line) with file locking.
func writeNodesToFile(path string, nodes []NodeConfig) error {
	_, err := writeNodesToFileSnapshot(path, nodes)
	return err
}

func writeNodesToFileSnapshot(path string, nodes []NodeConfig) (FileSnapshot, error) {
	var snapshot FileSnapshot
	err := withFileLock(path, func() error {
		var err error
		snapshot, err = writeNodesToFileLockedSnapshot(path, nodes)
		return err
	})
	return snapshot, err
}

func writeNodesToFileLockedSnapshot(path string, nodes []NodeConfig) (FileSnapshot, error) {
	var lines []string
	for _, node := range nodes {
		lines = append(lines, node.URI)
	}
	content := strings.Join(lines, "\n")
	if len(lines) > 0 {
		content += "\n"
	}
	return writeFileLockedSnapshot(path, []byte(content), 0o600)
}

// WriteNodesToFile atomically persists a URI-per-line node file.
func WriteNodesToFile(path string, nodes []NodeConfig) error {
	return writeNodesToFile(path, nodes)
}

// WriteFileAtomic writes a file under an inter-process sidecar lock and swaps
// it into place only after the complete contents have been synced.
func WriteFileAtomic(path string, data []byte, perm os.FileMode) error {
	return writeFileWithLock(path, data, perm)
}

// ReplaceFileAtomic swaps a fully-written source file over destination using
// the platform-specific atomic replacement primitive.
func ReplaceFileAtomic(source, destination string) error {
	return atomicReplaceFile(source, destination)
}

// SaveNodes persists nodes to their appropriate locations based on source.
// - subscription/nodes_file nodes → nodes.txt (or configured nodes_file)
// - inline nodes → config.yaml nodes array
// Config.yaml structure (subscriptions, nodes_file) is preserved.
func (c *Config) SaveNodes() error {
	if c == nil {
		return errors.New("config is nil")
	}
	if c.filePath == "" {
		return errors.New("config file path is unknown")
	}

	plan := c.buildNodeSavePlan()
	paths := orderedUniqueFilePaths(plan.lockPaths())
	return withFileLocks(paths, func() error {
		before, err := snapshotFilesLocked(paths)
		if err != nil {
			return err
		}
		_, err = c.saveNodesLocked(plan)
		if err != nil {
			return rollbackNodeFilesLocked(before, err)
		}
		return nil
	})
}

type nodeSavePlan struct {
	configPath    string
	nodesPath     string
	authPath      string
	writeNodes    bool
	inlineNodes   []NodeConfig
	externalNodes []NodeConfig
}

func (plan nodeSavePlan) lockPaths() []string {
	paths := []string{plan.configPath, plan.authPath}
	if plan.writeNodes {
		paths = append(paths, plan.nodesPath)
	}
	return paths
}

func (c *Config) buildNodeSavePlan() nodeSavePlan {
	plan := nodeSavePlan{
		configPath: c.filePath,
		nodesPath:  c.NodesFile,
		authPath:   c.nodeAuthPath(),
	}
	if plan.nodesPath == "" {
		plan.nodesPath = filepath.Join(filepath.Dir(c.filePath), "nodes.txt")
	}
	for _, node := range c.Nodes {
		cleanNode := NodeConfig{
			Name:     node.Name,
			URI:      node.URI,
			Port:     node.Port,
			Username: node.Username,
			Password: node.Password,
		}
		if node.Source == NodeSourceInline {
			plan.inlineNodes = append(plan.inlineNodes, cleanNode)
			continue
		}
		plan.externalNodes = append(plan.externalNodes, cleanNode)
	}
	plan.writeNodes = len(plan.externalNodes) > 0 || c.NodesFile != ""
	return plan
}

// saveNodesLocked performs every node-file mutation while the caller holds
// all paths from plan.lockPaths in their canonical order. Each successful
// write returns the exact post-write snapshot observed before releasing those
// locks, so a later rollback cannot adopt a concurrent writer as its expected
// state.
func (c *Config) saveNodesLocked(plan nodeSavePlan) (map[string]FileSnapshot, error) {
	written := make(map[string]FileSnapshot, 3)
	if plan.writeNodes {
		snapshot, err := writeNodesToFileLockedSnapshot(plan.nodesPath, plan.externalNodes)
		if err != nil {
			return written, fmt.Errorf("write nodes file %q: %w", plan.nodesPath, err)
		}
		written[snapshotPathKey(plan.nodesPath)] = snapshot
	}

	snapshot, err := transformFileLockedSnapshot(plan.configPath, 0o600, func(data []byte) ([]byte, error) {
		var saveCfg Config
		if err := yaml.Unmarshal(data, &saveCfg); err != nil {
			return nil, fmt.Errorf("decode config: %w", err)
		}
		saveCfg.Nodes = plan.inlineNodes

		newData, err := yaml.Marshal(&saveCfg)
		if err != nil {
			return nil, fmt.Errorf("encode config: %w", err)
		}
		return newData, nil
	})
	if err != nil {
		return written, fmt.Errorf("update config nodes: %w", err)
	}
	written[snapshotPathKey(plan.configPath)] = snapshot

	snapshot, err = c.persistNodeAuthOverridesLocked()
	if err != nil {
		return written, err
	}
	written[snapshotPathKey(plan.authPath)] = snapshot
	return written, nil
}

// SaveSubscriptionCache commits the startup-fetched subscription nodes after
// the runtime has demonstrated that the configuration can start.
func (c *Config) SaveSubscriptionCache() error {
	if c == nil || c.filePath == "" {
		return errors.New("config file path is unknown")
	}
	nodesFilePath := c.NodesFile
	if nodesFilePath == "" {
		nodesFilePath = filepath.Join(filepath.Dir(c.filePath), "nodes.txt")
	}
	nodes := make([]NodeConfig, 0)
	for _, node := range c.Nodes {
		if node.Source == NodeSourceSubscription {
			nodes = append(nodes, node)
		}
	}
	if len(nodes) == 0 {
		return nil
	}
	return writeNodesToFile(nodesFilePath, nodes)
}

// Save is deprecated, use SaveNodes instead.
// This method is kept for backward compatibility but now delegates to SaveNodes.
func (c *Config) Save() error {
	return c.SaveNodes()
}

// SaveSettings persists only config settings (external_ip, probe_target, skip_cert_verify)
// without touching nodes.txt. Use this for settings API updates.
func (c *Config) SaveSettings() error {
	if c == nil {
		return errors.New("config is nil")
	}
	if c.filePath == "" {
		return errors.New("config file path is unknown")
	}

	if err := transformFileWithLock(c.filePath, 0o600, c.transformSettingsData); err != nil {
		return fmt.Errorf("update config settings: %w", err)
	}
	return nil
}

// SaveSettingsTransaction persists settings and returns a compare-and-swap
// rollback whose expected image is captured before releasing the file lock.
// This closes the checkpoint window between SaveSettings and a second read.
func (c *Config) SaveSettingsTransaction() (func() error, error) {
	if c == nil || c.filePath == "" {
		return nil, errors.New("config file path is unknown")
	}
	path := c.filePath
	var before FileSnapshot
	var expected FileSnapshot
	err := withFileLock(path, func() error {
		var err error
		before, err = snapshotFileLocked(path)
		if err != nil {
			return err
		}
		expected, err = transformFileLockedSnapshot(path, 0o600, c.transformSettingsData)
		if err != nil {
			return rollbackNodeFilesLocked([]FileSnapshot{before}, err)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("update config settings: %w", err)
	}
	return func() error { return RestoreFileSnapshotCAS(before, expected) }, nil
}

func (c *Config) transformSettingsData(data []byte) ([]byte, error) {
	var saveCfg Config
	if err := yaml.Unmarshal(data, &saveCfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}

	saveCfg.ExternalIP = c.ExternalIP
	saveCfg.Management.ProbeTarget = c.Management.ProbeTarget
	saveCfg.SkipCertVerify = c.SkipCertVerify
	saveCfg.Log = c.Log
	saveCfg.Subscriptions = c.Subscriptions
	saveCfg.SubscriptionRefresh = c.SubscriptionRefresh
	saveCfg.GeoIP = c.GeoIP
	saveCfg.Mode = c.Mode
	saveCfg.Listener = c.Listener
	saveCfg.MultiPort = c.MultiPort
	saveCfg.Pool = c.Pool
	saveCfg.Management = c.Management

	newData, err := yaml.Marshal(&saveCfg)
	if err != nil {
		return nil, fmt.Errorf("encode config: %w", err)
	}
	return newData, nil
}

// IsPortAvailable checks if a port is available for binding.
func IsPortAvailable(address string, port uint16) bool {
	address = strings.Trim(strings.TrimSpace(address), "[]")
	addr := net.JoinHostPort(address, strconv.Itoa(int(port)))
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return false
	}
	_ = ln.Close()
	return true
}

// withFileLock holds the stable sidecar lock for the complete operation. RMW
// callers must do both their read and atomic replacement inside fn; locking
// only the final write still permits two writers to derive output from the
// same stale snapshot.
func withFileLock(path string, fn func() error) error {
	if strings.TrimSpace(path) == "" {
		return errors.New("file path is empty")
	}
	if fn == nil {
		return errors.New("file operation is nil")
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create parent directory: %w", err)
	}

	// Lock a stable sidecar rather than the destination. The destination can
	// then be atomically replaced without truncating the last good file first.
	lockPath := path + ".lock"
	lockHandle, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return fmt.Errorf("open lock file: %w", err)
	}
	defer lockHandle.Close()

	if err := lockFile(lockHandle); err != nil {
		return fmt.Errorf("lock file: %w", err)
	}
	defer unlockFile(lockHandle)

	return fn()
}

// withFileLocks acquires multiple sidecar locks in one canonical order. This
// makes multi-file transactions deadlock-free relative to every cooperating
// writer and lets their rollback validation cover all files before restoring
// any of them.
func withFileLocks(paths []string, fn func() error) error {
	if fn == nil {
		return errors.New("file operation is nil")
	}
	ordered := orderedUniqueFilePaths(paths)
	var acquire func(int) error
	acquire = func(index int) error {
		if index == len(ordered) {
			return fn()
		}
		return withFileLock(ordered[index], func() error {
			return acquire(index + 1)
		})
	}
	return acquire(0)
}

// transformFileWithLock serializes a read-transform-atomic-replace transaction
// against every other helper using the destination's stable sidecar lock.
func transformFileWithLock(path string, perm os.FileMode, transform func([]byte) ([]byte, error)) error {
	_, err := transformFileWithLockSnapshot(path, perm, transform)
	return err
}

func transformFileWithLockSnapshot(path string, perm os.FileMode, transform func([]byte) ([]byte, error)) (FileSnapshot, error) {
	if transform == nil {
		return FileSnapshot{}, errors.New("file transform is nil")
	}
	var snapshot FileSnapshot
	err := withFileLock(path, func() error {
		var err error
		snapshot, err = transformFileLockedSnapshot(path, perm, transform)
		return err
	})
	return snapshot, err
}

func transformFileLockedSnapshot(path string, perm os.FileMode, transform func([]byte) ([]byte, error)) (FileSnapshot, error) {
	if transform == nil {
		return FileSnapshot{}, errors.New("file transform is nil")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return FileSnapshot{}, err
	}
	updated, err := transform(data)
	if err != nil {
		return FileSnapshot{}, err
	}
	return writeFileLockedSnapshot(path, updated, perm)
}

// writeFileWithLock writes data to a file with exclusive locking.
func writeFileWithLock(path string, data []byte, perm os.FileMode) error {
	_, err := writeFileWithLockSnapshot(path, data, perm)
	return err
}

func writeFileWithLockSnapshot(path string, data []byte, perm os.FileMode) (FileSnapshot, error) {
	var snapshot FileSnapshot
	err := withFileLock(path, func() error {
		var err error
		snapshot, err = writeFileLockedSnapshot(path, data, perm)
		return err
	})
	return snapshot, err
}

func writeFileLockedSnapshot(path string, data []byte, perm os.FileMode) (FileSnapshot, error) {
	if err := writeFileLocked(path, data, perm); err != nil {
		return FileSnapshot{}, err
	}
	return snapshotFileLocked(path)
}

// writeFileLocked atomically replaces path. The caller must already hold the
// sidecar lock acquired by withFileLock.
func writeFileLocked(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)

	// Atomic replacement creates a new inode, so explicitly carry forward any
	// permission bits that are stricter than the requested mode. In particular,
	// credential-bearing config and node files must never become group/world
	// readable, and an existing read-only file must not be made writable merely
	// because it was saved through the WebUI.
	effectivePerm, err := restrictiveWritePerm(path, perm)
	if err != nil {
		return err
	}

	tempFile, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temporary file: %w", err)
	}
	tempPath := tempFile.Name()
	defer os.Remove(tempPath)

	if err := tempFile.Chmod(effectivePerm); err != nil {
		tempFile.Close()
		return fmt.Errorf("set temporary file permissions: %w", err)
	}
	if _, err := tempFile.Write(data); err != nil {
		tempFile.Close()
		return fmt.Errorf("write file: %w", err)
	}
	if err := tempFile.Sync(); err != nil {
		tempFile.Close()
		return fmt.Errorf("sync file: %w", err)
	}
	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("close temporary file: %w", err)
	}
	if err := atomicReplaceFile(tempPath, path); err != nil {
		return fmt.Errorf("replace file: %w", err)
	}

	return nil
}

func restrictiveWritePerm(path string, requested os.FileMode) (os.FileMode, error) {
	requested = requested.Perm()
	info, err := os.Stat(path)
	if err == nil {
		// Intersection can only remove permissions. This both tightens legacy
		// 0644 files to 0600 and preserves stricter modes such as 0400.
		return requested & info.Mode().Perm(), nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return requested, nil
	}
	return 0, fmt.Errorf("inspect existing file permissions: %w", err)
}
