//go:build unix

package config

import "os"

func atomicReplaceFile(source, destination string) error {
	return os.Rename(source, destination)
}
