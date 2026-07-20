package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPrepareConfigFileCreatesAndPreservesDefault(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "config.yaml")

	resolved, created, err := prepareConfigFile(path)
	if err != nil {
		t.Fatalf("prepare config: %v", err)
	}
	if !filepath.IsAbs(resolved) {
		t.Fatalf("resolved path is not absolute: %q", resolved)
	}
	if !created {
		t.Fatal("first prepare did not report creation")
	}
	first, err := os.ReadFile(resolved)
	if err != nil {
		t.Fatalf("read generated config: %v", err)
	}

	resolvedAgain, created, err := prepareConfigFile(path)
	if err != nil {
		t.Fatalf("prepare existing config: %v", err)
	}
	if resolvedAgain != resolved {
		t.Fatalf("resolved path changed: %q != %q", resolvedAgain, resolved)
	}
	if created {
		t.Fatal("existing config was reported as newly created")
	}
	second, err := os.ReadFile(resolved)
	if err != nil {
		t.Fatalf("read preserved config: %v", err)
	}
	if string(first) != string(second) {
		t.Fatal("existing config was replaced")
	}
}
