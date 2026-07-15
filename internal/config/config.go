package config

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

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
	Enabled     *bool  `yaml:"enabled"`
	Listen      string `yaml:"listen"`
	ProbeTarget string `yaml:"probe_target"`
	Password    string `yaml:"password"` // WebUI 访问密码，为空则不需要密码
}

// SubscriptionRefreshConfig controls subscription auto-refresh and reload settings.
type SubscriptionRefreshConfig struct {
	Enabled            bool          `yaml:"enabled"`              // 是否启用定时刷新
	Interval           time.Duration `yaml:"interval"`             // 刷新间隔，默认 1 小时
	Timeout            time.Duration `yaml:"timeout"`              // 获取订阅的超时时间
	HealthCheckTimeout time.Duration `yaml:"health_check_timeout"` // 新节点健康检查超时
	DrainTimeout       time.Duration `yaml:"drain_timeout"`        // 旧实例排空超时时间
	MinAvailableNodes  int           `yaml:"min_available_nodes"`  // 最少可用节点数，低于此值不切换
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

	if strings.HasPrefix(strings.ToLower(rawURI), "vmess://") {
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

	// Handle vmess:// specially - it's base64-encoded JSON, not a standard URL
	if strings.HasPrefix(uri, "vmess://") {
		payload := strings.TrimPrefix(uri, "vmess://")
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
	if c.Pool.Mode == "" {
		c.Pool.Mode = "sequential"
	}
	if c.Pool.FailureThreshold <= 0 {
		c.Pool.FailureThreshold = 3
	}
	if c.Pool.BlacklistDuration <= 0 {
		c.Pool.BlacklistDuration = 24 * time.Hour
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
	if c.Management.Enabled == nil {
		defaultEnabled := true
		c.Management.Enabled = &defaultEnabled
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

	// Load nodes from subscriptions (highest priority - writes to nodes.txt)
	if len(c.Subscriptions) > 0 {
		var subNodes []NodeConfig
		subTimeout := c.SubscriptionRefresh.Timeout
		for _, subURL := range c.Subscriptions {
			nodes, err := loadNodesFromSubscription(subURL, subTimeout)
			if err != nil {
				log.Printf("⚠️ Failed to load subscription %q: %v (skipping)", subURL, err)
				continue
			}
			log.Printf("✅ Loaded %d nodes from subscription", len(nodes))
			subNodes = append(subNodes, nodes...)
		}
		// Mark subscription nodes and write to nodes.txt
		for idx := range subNodes {
			subNodes[idx].Source = NodeSourceSubscription
		}
		if len(subNodes) > 0 {
			// Determine nodes.txt path
			nodesFilePath := c.NodesFile
			if nodesFilePath == "" {
				nodesFilePath = filepath.Join(filepath.Dir(c.filePath), "nodes.txt")
				c.NodesFile = nodesFilePath
			}
			// Write subscription nodes to nodes.txt
			if err := writeNodesToFile(nodesFilePath, subNodes); err != nil {
				log.Printf("⚠️ Failed to write nodes to %q: %v", nodesFilePath, err)
			} else {
				log.Printf("✅ Written %d subscription nodes to %s", len(subNodes), nodesFilePath)
			}
		}
		c.Nodes = append(c.Nodes, subNodes...)
		// Fallback: if all subscriptions failed, try loading cached nodes.txt
		if len(subNodes) == 0 && c.NodesFile != "" {
			cachedNodes, err := loadNodesFromFile(c.NodesFile)
			if err == nil && len(cachedNodes) > 0 {
				log.Printf("⚠️  All subscriptions failed, using %d cached nodes from %s", len(cachedNodes), c.NodesFile)
				c.Nodes = append(c.Nodes, cachedNodes...)
			}
		}
	}

	if len(c.Nodes) == 0 {
		return errors.New("config.nodes cannot be empty (configure nodes in config or use nodes_file)")
	}
	for idx := range c.Nodes {
		c.Nodes[idx].Name = strings.TrimSpace(c.Nodes[idx].Name)
		c.Nodes[idx].URI = strings.TrimSpace(c.Nodes[idx].URI)

		if c.Nodes[idx].URI == "" {
			return fmt.Errorf("node %d is missing uri", idx)
		}

		// Auto-extract name from URI if not provided
		if c.Nodes[idx].Name == "" {
			c.Nodes[idx].Name = ExtractNodeName(c.Nodes[idx].URI)
		}
		// Fallback to default name if still empty
		if c.Nodes[idx].Name == "" {
			c.Nodes[idx].Name = fmt.Sprintf("node-%d", idx)
		}

	}
	if err := c.ApplyNodeAuthOverrides(); err != nil {
		return err
	}
	if err := c.assignNodePorts(nil, true); err != nil {
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

func (c *Config) saveNodeAuthState(state *nodeAuthState) error {
	if state == nil {
		return errors.New("node auth state is nil")
	}
	state.Version = nodeAuthVersion
	if state.Overrides == nil {
		state.Overrides = make(map[string]nodeAuthOverride)
	}
	data, err := yaml.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode node auth overrides: %w", err)
	}
	if err := writeFileWithLock(c.nodeAuthPath(), data, 0o600); err != nil {
		return fmt.Errorf("write node auth overrides: %w", err)
	}
	return nil
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
	state, err := c.loadNodeAuthState()
	if err != nil {
		return err
	}
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
	return c.saveNodeAuthState(state)
}

// RemoveNodeAuthOverride forgets an explicitly deleted external node without
// pruning overrides for nodes that are merely absent from one subscription
// refresh and may return later.
func (c *Config) RemoveNodeAuthOverride(node NodeConfig) error {
	if c == nil || node.Source == NodeSourceInline {
		return nil
	}
	state, err := c.loadNodeAuthState()
	if err != nil {
		return err
	}
	delete(state.Overrides, node.NodeKey())
	return c.saveNodeAuthState(state)
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

func (c *Config) savePortMappingState(state *portMappingState) error {
	if c.Mode != "multi-port" && c.Mode != "hybrid" {
		return nil
	}
	if state == nil {
		return errors.New("port mapping state is nil")
	}
	state.Version = portMappingVersion
	if state.Leases == nil {
		state.Leases = make(map[string]portLease)
	}
	data, err := yaml.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode port map: %w", err)
	}
	if err := writeFileWithLock(c.portMapPath(), data, 0o600); err != nil {
		return fmt.Errorf("write port map: %w", err)
	}
	return nil
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

	for idx := range c.Nodes {
		node := &c.Nodes[idx]
		key := node.NodeKey()
		for otherKey, lease := range state.Leases {
			if otherKey != key && lease.Port == node.Port {
				delete(state.Leases, otherKey)
			}
		}
		state.Leases[key] = portLease{Port: node.Port}
	}
	c.pruneExpiredPortLeases(state, activeKeys, now)

	if persist {
		return c.savePortMappingState(state)
	}
	return nil
}

// PersistPortMap commits the current node-to-port assignments after a
// successful start/reload.
func (c *Config) PersistPortMap() error {
	if c == nil || (c.Mode != "multi-port" && c.Mode != "hybrid") {
		return nil
	}
	state, err := c.loadPortMappingState()
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	activeKeys := make(map[string]struct{}, len(c.Nodes))
	for idx := range c.Nodes {
		node := &c.Nodes[idx]
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
	return c.savePortMappingState(state)
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
	if c.Pool.Mode == "" {
		c.Pool.Mode = "sequential"
	}
	if c.Pool.FailureThreshold <= 0 {
		c.Pool.FailureThreshold = 3
	}
	if c.Pool.BlacklistDuration <= 0 {
		c.Pool.BlacklistDuration = 24 * time.Hour
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
	if c.Management.Enabled == nil {
		defaultEnabled := true
		c.Management.Enabled = &defaultEnabled
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

	if len(c.Nodes) == 0 {
		return errors.New("config.nodes cannot be empty")
	}

	for idx := range c.Nodes {
		c.Nodes[idx].Name = strings.TrimSpace(c.Nodes[idx].Name)
		c.Nodes[idx].URI = strings.TrimSpace(c.Nodes[idx].URI)
		if c.Nodes[idx].URI == "" {
			return fmt.Errorf("node %d is missing uri", idx)
		}

		// Auto-extract name from URI if not provided
		if c.Nodes[idx].Name == "" {
			c.Nodes[idx].Name = ExtractNodeName(c.Nodes[idx].URI)
		}
		if c.Nodes[idx].Name == "" {
			c.Nodes[idx].Name = fmt.Sprintf("node-%d", idx)
		}
	}
	if err := c.ApplyNodeAuthOverrides(); err != nil {
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

func (c *Config) normalizeGeoIPConfig() {
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

// loadNodesFromFile reads a nodes file where each line is a proxy URI
// Lines starting with # are comments, empty lines are ignored
func loadNodesFromFile(path string) ([]NodeConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return parseNodesFromContent(string(data))
}

// loadNodesFromSubscription fetches and parses nodes from a subscription URL
// Supports multiple formats: base64 encoded, plain text, clash yaml, etc.
func loadNodesFromSubscription(subURL string, timeout time.Duration) ([]NodeConfig, error) {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	client := &http.Client{
		Timeout: timeout,
	}

	req, err := http.NewRequest("GET", subURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	// Use clash-compatible User-Agent to get Clash YAML format from subscription servers
	// This ensures we receive structured YAML with all proxy types (AnyTLS, TUIC, etc.)
	// instead of base64-encoded content that may only contain basic SS nodes
	req.Header.Set("User-Agent", "clash-verge/v2.2.3")
	req.Header.Set("Accept", "*/*")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch subscription: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("subscription returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	content := string(body)

	// Try to detect and parse different formats
	return parseSubscriptionContent(content)
}

// parseSubscriptionContent tries to parse subscription content in various formats (optimized)
func parseSubscriptionContent(content string) ([]NodeConfig, error) {
	content = strings.TrimSpace(content)

	// Quick check for YAML format (check first 16384 chars for "proxies:")
	sampleSize := 16384
	if len(content) < sampleSize {
		sampleSize = len(content)
	}
	if strings.Contains(content[:sampleSize], "proxies:") {
		return parseClashYAML(content)
	}

	// Check if it's base64 encoded (common for v2ray subscriptions)
	if isBase64(content) {
		decoded, err := base64.StdEncoding.DecodeString(content)
		if err != nil {
			// Try URL-safe base64
			decoded, err = base64.RawStdEncoding.DecodeString(content)
			if err != nil {
				// Not base64, try as plain text
				return parseNodesFromContent(content)
			}
		}
		content = string(decoded)
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
	// Remove whitespace
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return false
	}

	// Remove newlines for checking
	s = strings.ReplaceAll(s, "\n", "")
	s = strings.ReplaceAll(s, "\r", "")

	// Quick check: if it contains proxy URI schemes, it's not base64
	if strings.Contains(s, "://") {
		return false
	}

	// Check character set - base64 only contains A-Za-z0-9+/=
	// This is much faster than trying to decode
	for _, c := range s {
		if !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
			(c >= '0' && c <= '9') || c == '+' || c == '/' || c == '=') {
			return false
		}
	}

	// Length must be multiple of 4 (with padding)
	return len(s)%4 == 0
}

// IsProxyURI checks if a string is a valid proxy URI
func IsProxyURI(s string) bool {
	schemes := []string{"vmess://", "vless://", "trojan://", "ss://", "ssr://", "hysteria://", "hysteria2://", "hy2://", "tuic://", "socks5://", "socks://", "http://", "https://", "anytls://"}
	lower := strings.ToLower(s)
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
	Proxies []clashProxy `yaml:"proxies"`
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
	for _, proxy := range clash.Proxies {
		uri := convertClashProxyToURI(proxy)
		if uri != "" {
			nodes = append(nodes, NodeConfig{
				Name: proxy.Name,
				URI:  uri,
			})
		}
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
	default:
		return ""
	}
}

func buildVMessURI(p clashProxy) string {
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

	return fmt.Sprintf("vmess://%s@%s:%d%s#%s", p.UUID, p.Server, int(p.Port), query, url.QueryEscape(p.Name))
}

func buildVLESSURI(p clashProxy) string {
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

	return fmt.Sprintf("vless://%s@%s:%d?%s#%s", p.UUID, p.Server, int(p.Port), params.Encode(), url.QueryEscape(p.Name))
}

func buildTrojanURI(p clashProxy) string {
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

	return fmt.Sprintf("trojan://%s@%s:%d%s#%s", p.Password, p.Server, int(p.Port), query, url.QueryEscape(p.Name))
}

func buildAnyTLSURI(p clashProxy) string {
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

	return fmt.Sprintf("anytls://%s@%s:%d%s#%s", p.Password, p.Server, int(p.Port), query, url.QueryEscape(p.Name))
}

func buildShadowsocksURI(p clashProxy) string {
	// Encode method:password in base64
	userInfo := base64.StdEncoding.EncodeToString([]byte(p.Cipher + ":" + p.Password))
	return fmt.Sprintf("ss://%s@%s:%d#%s", userInfo, p.Server, int(p.Port), url.QueryEscape(p.Name))
}

func buildHysteria2URI(p clashProxy) string {
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
		params.Set("ports", normalizeHysteria2PortsValue(strings.TrimSpace(p.Ports)))
	}

	query := ""
	if len(params) > 0 {
		query = "?" + params.Encode()
	}

	port := int(p.Port)
	if port <= 0 {
		port = 443
	}

	return fmt.Sprintf("hysteria2://%s@%s:%d%s#%s", p.Password, p.Server, port, query, url.QueryEscape(p.Name))
}

func normalizeHysteria2PortsValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}

	parts := strings.Split(value, ",")
	normalized := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if strings.Contains(part, ":") {
			normalized = append(normalized, part)
			continue
		}
		if strings.Count(part, "-") == 1 {
			normalized = append(normalized, strings.Replace(part, "-", ":", 1))
			continue
		}
		normalized = append(normalized, part)
	}

	return strings.Join(normalized, ",")
}

func buildTUICURI(p clashProxy) string {
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
	return fmt.Sprintf("tuic://%s:%s@%s:%d%s#%s", p.UUID, p.Password, p.Server, int(p.Port), query, url.QueryEscape(p.Name))
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
	var lines []string
	for _, node := range nodes {
		lines = append(lines, node.URI)
	}
	content := strings.Join(lines, "\n")
	if len(lines) > 0 {
		content += "\n"
	}
	// Use file locking for safe concurrent writes
	return writeFileWithLock(path, []byte(content), 0o644)
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

	// Separate nodes by source
	var inlineNodes []NodeConfig
	var fileNodes []NodeConfig

	for _, node := range c.Nodes {
		// Create a clean copy without runtime fields for saving
		cleanNode := NodeConfig{
			Name:     node.Name,
			URI:      node.URI,
			Port:     node.Port,
			Username: node.Username,
			Password: node.Password,
		}
		switch node.Source {
		case NodeSourceInline:
			inlineNodes = append(inlineNodes, cleanNode)
		case NodeSourceFile, NodeSourceSubscription:
			fileNodes = append(fileNodes, cleanNode)
		default:
			// Default to file nodes for unknown source
			fileNodes = append(fileNodes, cleanNode)
		}
	}

	// Write file-based nodes to nodes.txt
	if len(fileNodes) > 0 || c.NodesFile != "" {
		nodesFilePath := c.NodesFile
		if nodesFilePath == "" {
			nodesFilePath = filepath.Join(filepath.Dir(c.filePath), "nodes.txt")
		}
		if err := writeNodesToFile(nodesFilePath, fileNodes); err != nil {
			return fmt.Errorf("write nodes file %q: %w", nodesFilePath, err)
		}
	}

	// Update config.yaml nodes array (including clearing it when all inline nodes are deleted)
	{
		// Read original config to preserve structure
		data, err := os.ReadFile(c.filePath)
		if err != nil {
			return fmt.Errorf("read config: %w", err)
		}
		var saveCfg Config
		if err := yaml.Unmarshal(data, &saveCfg); err != nil {
			return fmt.Errorf("decode config: %w", err)
		}
		// Update only the inline nodes
		saveCfg.Nodes = inlineNodes

		newData, err := yaml.Marshal(&saveCfg)
		if err != nil {
			return fmt.Errorf("encode config: %w", err)
		}
		// Use file locking for safe concurrent writes
		if err := writeFileWithLock(c.filePath, newData, 0o644); err != nil {
			return fmt.Errorf("write config: %w", err)
		}
	}
	if err := c.persistNodeAuthOverrides(); err != nil {
		return err
	}

	return nil
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

	data, err := os.ReadFile(c.filePath)
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}
	var saveCfg Config
	if err := yaml.Unmarshal(data, &saveCfg); err != nil {
		return fmt.Errorf("decode config: %w", err)
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
		return fmt.Errorf("encode config: %w", err)
	}

	// Use file locking for safe concurrent writes
	if err := writeFileWithLock(c.filePath, newData, 0o644); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	return nil
}

// IsPortAvailable checks if a port is available for binding.
func IsPortAvailable(address string, port uint16) bool {
	addr := fmt.Sprintf("%s:%d", address, port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return false
	}
	_ = ln.Close()
	return true
}

// writeFileWithLock writes data to a file with exclusive locking.
func writeFileWithLock(path string, data []byte, perm os.FileMode) error {
	if strings.TrimSpace(path) == "" {
		return errors.New("file path is empty")
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

	tempFile, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temporary file: %w", err)
	}
	tempPath := tempFile.Name()
	defer os.Remove(tempPath)

	if err := tempFile.Chmod(perm); err != nil {
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
