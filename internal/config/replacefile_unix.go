//go:build unix

package config

import (
	"fmt"
	"os"
	"path/filepath"
)

func atomicReplaceFile(source, destination string) error {
	if err := os.Rename(source, destination); err != nil {
		return err
	}
	return syncParentDirectory(destination)
}

// Syncing the file before rename is not enough for crash durability: the
// directory entry itself must also reach stable storage.
func syncParentDirectory(path string) error {
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("open parent directory: %w", err)
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return fmt.Errorf("sync parent directory: %w", err)
	}
	if err := directory.Close(); err != nil {
		return fmt.Errorf("close parent directory: %w", err)
	}
	return nil
}
