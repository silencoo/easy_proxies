package pool

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"easy_proxies/internal/config"
	"easy_proxies/internal/monitor"

	"gopkg.in/yaml.v3"
)

const (
	healthStateVersion = 3
	healthWriteDelay   = 250 * time.Millisecond
)

type persistedMemberHealth struct {
	Failures         int                          `yaml:"failures,omitempty"`
	BlacklistedUntil time.Time                    `yaml:"blacklisted_until,omitempty"`
	ManualBlacklist  bool                         `yaml:"manual_blacklist,omitempty"`
	CooldownUntil    time.Time                    `yaml:"cooldown_until,omitempty"`
	Monitor          monitor.PersistedHealthState `yaml:"monitor,omitempty"`
	UpdatedAt        time.Time                    `yaml:"updated_at"`
}

type persistedHealthFile struct {
	Version int                              `yaml:"version"`
	Nodes   map[string]persistedMemberHealth `yaml:"nodes"`
}

type healthPersistenceManager struct {
	writeMu sync.Mutex
	mu      sync.Mutex
	path    string
	records map[string]persistedMemberHealth
	dirty   bool
	timer   *time.Timer
}

var healthPersistence = healthPersistenceManager{
	records: make(map[string]persistedMemberHealth),
}

var readHealthStateFile = os.ReadFile
var writeHealthStateFile = config.WriteFileAtomic

// ConfigureHealthPersistence selects and loads the health-state sidecar. It is
// safe to call on every manager start/reload; an unchanged path is a no-op so
// live in-memory state always wins over an older disk snapshot.
func ConfigureHealthPersistence(path string) error {
	path = strings.TrimSpace(path)
	if path != "" {
		path = filepath.Clean(path)
	}

	// Serialize a path transition with every physical flush. Keep mu locked for
	// the full transition too: a store that arrives while the old file is being
	// flushed or the new file is being loaded will wait, then publish into the
	// selected state instead of being overwritten by the loaded snapshot.
	healthPersistence.writeMu.Lock()
	defer healthPersistence.writeMu.Unlock()

	healthPersistence.mu.Lock()
	if healthPersistence.path == path {
		healthPersistence.mu.Unlock()
		return nil
	}
	if healthPersistence.timer != nil {
		healthPersistence.timer.Stop()
		healthPersistence.timer = nil
	}

	// Commit all pending state to the old path before attempting to read or
	// activate the replacement. On failure, path and records remain untouched.
	if healthPersistence.dirty && healthPersistence.path != "" {
		data, err := yaml.Marshal(persistedHealthFile{
			Version: healthStateVersion,
			Nodes:   healthPersistence.records,
		})
		if err != nil {
			healthPersistence.mu.Unlock()
			return err
		}
		if err := writeHealthStateFile(healthPersistence.path, data, 0o600); err != nil {
			healthPersistence.mu.Unlock()
			return err
		}
		healthPersistence.dirty = false
	}

	records := make(map[string]persistedMemberHealth)
	if path != "" {
		data, err := readHealthStateFile(path)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			healthPersistence.mu.Unlock()
			return err
		}
		if err == nil {
			var state persistedHealthFile
			if err := yaml.Unmarshal(data, &state); err != nil {
				healthPersistence.mu.Unlock()
				return err
			}
			if state.Version != 0 && state.Version != 1 && state.Version != 2 && state.Version != healthStateVersion {
				healthPersistence.mu.Unlock()
				return errors.New("unsupported health state version")
			}
			now := time.Now()
			for tag, record := range state.Nodes {
				if strings.TrimSpace(tag) == "" {
					continue
				}
				if !record.BlacklistedUntil.After(now) {
					record.BlacklistedUntil = time.Time{}
					record.Monitor.BlacklistedUntil = time.Time{}
				}
				if !record.CooldownUntil.After(now) {
					record.CooldownUntil = time.Time{}
					record.Monitor.CooldownUntil = time.Time{}
				}
				records[tag] = record
			}
		}
	}

	healthPersistence.path = path
	healthPersistence.records = records
	healthPersistence.dirty = false
	healthPersistence.mu.Unlock()
	return nil
}

func restoredMemberHealth(tag string) (persistedMemberHealth, bool) {
	healthPersistence.mu.Lock()
	record, ok := healthPersistence.records[tag]
	healthPersistence.mu.Unlock()
	return record, ok
}

func storeMemberHealth(tag string, record persistedMemberHealth) {
	healthPersistence.mu.Lock()
	if healthPersistence.path == "" {
		healthPersistence.mu.Unlock()
		return
	}
	record.UpdatedAt = time.Now().UTC()
	healthPersistence.records[tag] = record
	healthPersistence.dirty = true
	if healthPersistence.timer == nil {
		healthPersistence.timer = time.AfterFunc(healthWriteDelay, func() {
			if err := FlushHealthState(); err != nil {
				log.Printf("[pool] persist health state: %v", err)
			}
		})
	}
	healthPersistence.mu.Unlock()
}

// FlushHealthState synchronously writes pending state, primarily for graceful
// shutdown and tests. Normal traffic mutations are coalesced by a short timer.
func FlushHealthState() error {
	// Keep the physical writes ordered as well as the in-memory snapshots. A
	// newer flush must never reach disk first and then be overwritten by an
	// older, slower write.
	healthPersistence.writeMu.Lock()
	defer healthPersistence.writeMu.Unlock()

	healthPersistence.mu.Lock()
	if healthPersistence.timer != nil {
		healthPersistence.timer.Stop()
		healthPersistence.timer = nil
	}
	if !healthPersistence.dirty || healthPersistence.path == "" {
		healthPersistence.mu.Unlock()
		return nil
	}
	path := healthPersistence.path
	records := make(map[string]persistedMemberHealth, len(healthPersistence.records))
	for tag, record := range healthPersistence.records {
		records[tag] = record
	}
	healthPersistence.dirty = false
	healthPersistence.mu.Unlock()

	data, err := yaml.Marshal(persistedHealthFile{Version: healthStateVersion, Nodes: records})
	if err != nil {
		return err
	}
	if err := writeHealthStateFile(path, data, 0o600); err != nil {
		healthPersistence.mu.Lock()
		healthPersistence.dirty = true
		healthPersistence.mu.Unlock()
		return err
	}
	return nil
}

// PersistHealthStateNow snapshots all live shared members and flushes them.
// It is used on graceful shutdown and when the configured sidecar path moves.
func PersistHealthStateNow() error {
	sharedStateStore.Range(func(_, value any) bool {
		value.(*sharedMemberState).persist()
		return true
	})
	return FlushHealthState()
}

func resetHealthPersistenceForTest() {
	healthPersistence.writeMu.Lock()
	defer healthPersistence.writeMu.Unlock()
	healthPersistence.mu.Lock()
	if healthPersistence.timer != nil {
		healthPersistence.timer.Stop()
	}
	healthPersistence.path = ""
	healthPersistence.records = make(map[string]persistedMemberHealth)
	healthPersistence.dirty = false
	healthPersistence.timer = nil
	healthPersistence.mu.Unlock()
}
