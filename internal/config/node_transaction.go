package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

// ErrRollbackConflict means a rollback was deliberately skipped because the
// destination changed after this transaction wrote it. Overwriting that newer
// content would lose a concurrent process's successful update.
var ErrRollbackConflict = errors.New("rollback target changed after transaction")

// FileSnapshot is an opaque, lock-consistent image used by rollback-capable
// persistence in other internal packages.
type FileSnapshot struct {
	path    string
	data    []byte
	perm    os.FileMode
	existed bool
}

// The hook remains replaceable by tests so they can force a failure after the
// node files were written. The default implementation assumes the node-auth
// sidecar lock is already held by SaveNodesTransaction.
var removeNodeAuthOverridesForTransaction = func(cfg *Config, nodes []NodeConfig) (FileSnapshot, bool, error) {
	keys := make(map[string]struct{})
	for _, node := range nodes {
		if node.Source != NodeSourceInline {
			keys[node.NodeKey()] = struct{}{}
		}
	}
	if len(keys) == 0 {
		return FileSnapshot{}, false, nil
	}
	snapshot, err := cfg.updateNodeAuthStateLocked(func(state *nodeAuthState) {
		for key := range keys {
			delete(state.Overrides, key)
		}
	})
	return snapshot, true, err
}

// SaveNodesTransaction updates config.yaml, nodes_file, the node-auth sidecar,
// and the port map as one rollback-capable operation. All participating
// sidecars are locked in canonical order for the initial snapshots and writes.
// This prevents a cooperating process from being interleaved between files and
// gives the rollback exact post-write snapshots without a checkpoint TOCTOU.
func (c *Config) SaveNodesTransaction(removedAuth []NodeConfig) (func() error, error) {
	if c == nil || c.filePath == "" {
		return nil, errors.New("config file path is unknown")
	}
	plan := c.buildNodeSavePlan()
	paths := plan.lockPaths()
	if c.Mode == "multi-port" || c.Mode == "hybrid" {
		paths = append(paths, c.portMapPath())
	}
	paths = orderedUniqueFilePaths(paths)

	var before []FileSnapshot
	var expected []FileSnapshot
	err := withFileLocks(paths, func() error {
		var err error
		before, err = snapshotFilesLocked(paths)
		if err != nil {
			return err
		}
		expected = cloneFileSnapshots(before)

		written, writeErr := c.saveNodesLocked(plan)
		if mergeErr := mergeExpectedSnapshots(expected, written); mergeErr != nil {
			return rollbackNodeFilesLocked(before, mergeErr)
		}
		if writeErr != nil {
			return rollbackNodeFilesLocked(before, writeErr)
		}

		authSnapshot, changed, authErr := removeNodeAuthOverridesForTransaction(c, removedAuth)
		if authErr != nil {
			return rollbackNodeFilesLocked(before, authErr)
		}
		if changed {
			if mergeErr := mergeExpectedSnapshot(expected, authSnapshot); mergeErr != nil {
				return rollbackNodeFilesLocked(before, mergeErr)
			}
		}

		if c.Mode == "multi-port" || c.Mode == "hybrid" {
			portSnapshot, portErr := c.persistNodePortLeasesLocked(time.Now().UTC())
			if portErr == nil {
				if mergeErr := mergeExpectedSnapshot(expected, portSnapshot); mergeErr != nil {
					return rollbackNodeFilesLocked(before, mergeErr)
				}
			}
			if portErr != nil {
				return rollbackNodeFilesLocked(before, portErr)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return func() error { return RestoreFileSnapshotsCAS(before, expected) }, nil
}

// PersistPortMapTransaction makes a port-lease update rollback-capable so a
// failed runtime cutover cannot leave disk describing an uncommitted mapping.
func (c *Config) PersistPortMapTransaction() (func() error, error) {
	if c == nil || (c.Mode != "multi-port" && c.Mode != "hybrid") {
		return func() error { return nil }, nil
	}
	path := c.portMapPath()
	var before FileSnapshot
	var expected FileSnapshot
	err := withFileLock(path, func() error {
		var err error
		before, err = snapshotFileLocked(path)
		if err != nil {
			return err
		}
		expected, err = c.persistNodePortLeasesLocked(time.Now().UTC())
		if err != nil {
			return rollbackNodeFilesLocked([]FileSnapshot{before}, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return func() error { return RestoreFileSnapshotCAS(before, expected) }, nil
}

// SaveSubscriptionStateTransaction commits subscription settings, the
// restart node cache, and (when subscriptions are cleared) the external-node
// authentication sidecar under one canonically ordered lock set. Its rollback
// validates every post-write image before restoring any file.
func (c *Config) SaveSubscriptionStateTransaction(subscriptionNodes []NodeConfig, clearExternalAuth bool) (func() error, error) {
	if c == nil || c.filePath == "" {
		return nil, errors.New("config file path is unknown")
	}
	nodesPath := c.NodesFile
	if strings.TrimSpace(nodesPath) == "" {
		nodesPath = filepath.Join(filepath.Dir(c.filePath), "nodes.txt")
	}
	if filepath.Clean(c.filePath) == filepath.Clean(nodesPath) {
		return nil, errors.New("config file and nodes file must be different")
	}
	paths := []string{c.filePath, nodesPath}
	if clearExternalAuth {
		paths = append(paths, c.nodeAuthPath())
	}
	paths = orderedUniqueFilePaths(paths)

	var before []FileSnapshot
	var expected []FileSnapshot
	err := withFileLocks(paths, func() error {
		var err error
		before, err = snapshotFilesLocked(paths)
		if err != nil {
			return err
		}
		expected = cloneFileSnapshots(before)

		settingsSnapshot, err := transformFileLockedSnapshot(c.filePath, 0o600, c.transformSettingsData)
		if err != nil {
			return rollbackNodeFilesLocked(before, fmt.Errorf("update subscription settings: %w", err))
		}
		if err := mergeExpectedSnapshot(expected, settingsSnapshot); err != nil {
			return rollbackNodeFilesLocked(before, err)
		}

		nodesSnapshot, err := writeNodesToFileLockedSnapshot(nodesPath, subscriptionNodes)
		if err != nil {
			return rollbackNodeFilesLocked(before, fmt.Errorf("write subscription nodes: %w", err))
		}
		if err := mergeExpectedSnapshot(expected, nodesSnapshot); err != nil {
			return rollbackNodeFilesLocked(before, err)
		}

		if clearExternalAuth {
			authSnapshot, err := c.updateNodeAuthStateLocked(func(state *nodeAuthState) {
				state.Overrides = make(map[string]nodeAuthOverride)
			})
			if err != nil {
				return rollbackNodeFilesLocked(before, fmt.Errorf("clear external node authentication: %w", err))
			}
			if err := mergeExpectedSnapshot(expected, authSnapshot); err != nil {
				return rollbackNodeFilesLocked(before, err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return func() error { return RestoreFileSnapshotsCAS(before, expected) }, nil
}

// CaptureFileSnapshot reads path while holding the same inter-process sidecar
// lock used by atomic writers.
func CaptureFileSnapshot(path string) (FileSnapshot, error) {
	var snapshot FileSnapshot
	err := withFileLock(path, func() error {
		var err error
		snapshot, err = snapshotFileLocked(path)
		return err
	})
	return snapshot, err
}

// snapshotFileLocked captures path while its withFileLock sidecar is held.
func snapshotFileLocked(path string) (FileSnapshot, error) {
	snapshot := FileSnapshot{path: filepath.Clean(path), perm: 0o600}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return snapshot, nil
	}
	if err != nil {
		return FileSnapshot{}, fmt.Errorf("snapshot %q: %w", path, err)
	}
	snapshot.data = data
	snapshot.existed = true
	info, err := os.Stat(path)
	if err != nil {
		return FileSnapshot{}, fmt.Errorf("inspect snapshot %q: %w", path, err)
	}
	snapshot.perm = info.Mode().Perm()
	return snapshot, nil
}

// RestoreFileSnapshotCAS restores one file only when it still has the exact
// state observed after the transaction's write.
func RestoreFileSnapshotCAS(before, expected FileSnapshot) error {
	return RestoreFileSnapshotsCAS([]FileSnapshot{before}, []FileSnapshot{expected})
}

// RestoreFileSnapshotsCAS validates every target while holding all sidecar
// locks before restoring any target. A conflict therefore produces no partial
// rollback; only I/O failures during the restore itself can leave a partial
// result, and those failures are all reported.
func RestoreFileSnapshotsCAS(before, expected []FileSnapshot) error {
	pairs, paths, err := prepareSnapshotPairs(before, expected)
	if err != nil {
		return err
	}
	return withFileLocks(paths, func() error {
		restore := make([]bool, len(pairs))
		var conflictErr error
		for index, pair := range pairs {
			current, snapshotErr := snapshotFileLocked(pair.before.path)
			if snapshotErr != nil {
				conflictErr = errors.Join(conflictErr, snapshotErr)
				continue
			}
			if sameFileSnapshot(current, pair.before) {
				continue
			}
			if !sameFileSnapshot(current, pair.expected) {
				conflictErr = errors.Join(conflictErr, fmt.Errorf("%w: %q", ErrRollbackConflict, pair.before.path))
				continue
			}
			restore[index] = true
		}
		if conflictErr != nil {
			return conflictErr
		}

		var restoreErr error
		for index := len(pairs) - 1; index >= 0; index-- {
			if !restore[index] {
				continue
			}
			restoreErr = errors.Join(restoreErr, restoreFileSnapshotLocked(pairs[index].before))
		}
		return restoreErr
	})
}

type fileSnapshotPair struct {
	before   FileSnapshot
	expected FileSnapshot
}

func prepareSnapshotPairs(before, expected []FileSnapshot) ([]fileSnapshotPair, []string, error) {
	if len(before) != len(expected) {
		return nil, nil, errors.New("rollback snapshot counts do not match")
	}
	pairs := make([]fileSnapshotPair, 0, len(before))
	seen := make(map[string]struct{}, len(before))
	for index := range before {
		if snapshotPathKey(before[index].path) != snapshotPathKey(expected[index].path) {
			return nil, nil, errors.New("rollback snapshot paths do not match")
		}
		key := snapshotPathKey(before[index].path)
		if _, duplicate := seen[key]; duplicate {
			return nil, nil, fmt.Errorf("duplicate rollback path %q", before[index].path)
		}
		seen[key] = struct{}{}
		pairs = append(pairs, fileSnapshotPair{before: before[index], expected: expected[index]})
	}
	sort.Slice(pairs, func(i, j int) bool {
		return snapshotPathKey(pairs[i].before.path) < snapshotPathKey(pairs[j].before.path)
	})
	paths := make([]string, len(pairs))
	for index := range pairs {
		paths[index] = pairs[index].before.path
	}
	return pairs, paths, nil
}

func snapshotFilesLocked(paths []string) ([]FileSnapshot, error) {
	snapshots := make([]FileSnapshot, 0, len(paths))
	for _, path := range paths {
		snapshot, err := snapshotFileLocked(path)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots, nil
}

func cloneFileSnapshots(snapshots []FileSnapshot) []FileSnapshot {
	cloned := make([]FileSnapshot, len(snapshots))
	for index, snapshot := range snapshots {
		cloned[index] = snapshot
		cloned[index].data = append([]byte(nil), snapshot.data...)
	}
	return cloned
}

func mergeExpectedSnapshots(expected []FileSnapshot, updates map[string]FileSnapshot) error {
	for _, snapshot := range updates {
		if err := mergeExpectedSnapshot(expected, snapshot); err != nil {
			return err
		}
	}
	return nil
}

func mergeExpectedSnapshot(expected []FileSnapshot, update FileSnapshot) error {
	key := snapshotPathKey(update.path)
	for index := range expected {
		if snapshotPathKey(expected[index].path) == key {
			expected[index] = update
			return nil
		}
	}
	return fmt.Errorf("rollback path %q was not snapshotted", update.path)
}

func restoreFileSnapshotLocked(snapshot FileSnapshot) error {
	if snapshot.existed {
		return writeFileLocked(snapshot.path, snapshot.data, snapshot.perm)
	}
	err := os.Remove(snapshot.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return syncParentDirectory(snapshot.path)
}

func restoreFileSnapshotsLocked(snapshots []FileSnapshot) error {
	var restoreErr error
	for index := len(snapshots) - 1; index >= 0; index-- {
		restoreErr = errors.Join(restoreErr, restoreFileSnapshotLocked(snapshots[index]))
	}
	return restoreErr
}

func rollbackNodeFilesLocked(before []FileSnapshot, cause error) error {
	if rollbackErr := restoreFileSnapshotsLocked(before); rollbackErr != nil {
		return fmt.Errorf("%w; rollback failed: %v", cause, rollbackErr)
	}
	return cause
}

func sameFileSnapshot(left, right FileSnapshot) bool {
	return left.existed == right.existed &&
		(!left.existed || (left.perm == right.perm && bytes.Equal(left.data, right.data)))
}

func orderedUniqueFilePaths(paths []string) []string {
	unique := make(map[string]string, len(paths))
	for _, path := range paths {
		if strings.TrimSpace(path) == "" {
			continue
		}
		cleaned := filepath.Clean(path)
		if absolute, err := filepath.Abs(cleaned); err == nil {
			cleaned = absolute
		}
		key := snapshotPathKey(cleaned)
		if _, exists := unique[key]; !exists {
			unique[key] = cleaned
		}
	}
	keys := make([]string, 0, len(unique))
	for key := range unique {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	ordered := make([]string, len(keys))
	for index, key := range keys {
		ordered[index] = unique[key]
	}
	return ordered
}

func snapshotPathKey(path string) string {
	cleaned := filepath.Clean(path)
	if absolute, err := filepath.Abs(cleaned); err == nil {
		cleaned = absolute
	}
	if runtime.GOOS == "windows" {
		cleaned = strings.ToLower(cleaned)
	}
	return cleaned
}
