package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const defaultConfigYAML = `# Easy Proxies configuration.
# This first-run file starts the local WebUI without proxy nodes.
# Add a subscription in System settings, refresh it, and the proxy runtime
# will start automatically after usable nodes have been loaded.
mode: pool

listener:
  address: 127.0.0.1
  port: 2323

pool:
  mode: sequential

management:
  enabled: true
  listen: 127.0.0.1:9091
  probe_target: www.apple.com:80

subscription_refresh:
  enabled: true

log:
  output: stdout

subscriptions: []
nodes: []
`

// EnsureDefaultFile creates a safe first-run configuration when path does not
// exist. The existence check and write share the config sidecar lock so a
// concurrent user or process can never have its newly-created file replaced.
func EnsureDefaultFile(path string) (bool, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return false, errors.New("config file path is empty")
	}
	path = filepath.Clean(path)

	created := false
	err := withFileLock(path, func() error {
		if _, err := os.Stat(path); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("inspect config file: %w", err)
		}
		if _, err := writeFileLockedSnapshot(path, []byte(defaultConfigYAML), 0o600); err != nil {
			return fmt.Errorf("create default config: %w", err)
		}
		created = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return created, nil
}
