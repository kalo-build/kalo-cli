package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDefaultPluginMemoryMiB(t *testing.T) {
	for _, tc := range []struct {
		available uint64
		want      uint32
	}{
		{available: 200, want: 128},
		{available: 1100, want: 588},
		{available: 64 * 1024, want: wasm32MaxMiB},
	} {
		if got := defaultPluginMemoryMiB(tc.available); got != tc.want {
			t.Errorf("defaultPluginMemoryMiB(%d) = %d, want %d", tc.available, got, tc.want)
		}
	}
}

func TestMemAvailableMiB(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meminfo")
	if err := os.WriteFile(path, []byte("MemTotal:        2018304 kB\nMemFree:          65536 kB\nMemAvailable:    1126400 kB\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got, ok := memAvailableMiB(path); !ok || got != 1100 {
		t.Fatalf("memAvailableMiB = %d, %v; want 1100, true", got, ok)
	}
	if _, ok := memAvailableMiB(filepath.Join(t.TempDir(), "missing")); ok {
		t.Fatal("a missing meminfo must give no default limit")
	}
}

func TestPluginMemoryLimitPrefersFlag(t *testing.T) {
	if got := pluginMemoryLimit(256); got.MiB != 256 || got.Source != "--plugin-memory-mib" {
		t.Fatalf("pluginMemoryLimit(256) = %+v", got)
	}
}

func TestOutOfMemoryIsOneLine(t *testing.T) {
	exit := errors.New("failed to instantiate plugin module: module closed with exit_code(2)")
	if !pluginOutOfMemory(exit, "runtime: out of memory: cannot allocate 71303168-byte block\nfatal error: out of memory\n") {
		t.Fatal("Go runtime out-of-memory output not recognised")
	}
	if !pluginOutOfMemory(exit, "memory allocation of 1048576 bytes failed\n") {
		t.Fatal("Rust allocation failure not recognised")
	}
	if pluginOutOfMemory(exit, "circular reference detected\n") {
		t.Fatal("an ordinary plugin failure must not be reported as out of memory")
	}
	message := outOfMemoryError("KALOX:OPENAPI-TO-TS-SDK@1", "/tmp/in/spec3.json", 8_300_000, memoryLimit{MiB: 256, Source: "--plugin-memory-mib"}, true).Error()
	for _, want := range []string{"ran out of memory at the 256 MiB plugin limit (set from --plugin-memory-mib)", "spec3.json (8.3 MB)", "--verbose"} {
		if !strings.Contains(message, want) {
			t.Errorf("message %q lacks %q", message, want)
		}
	}
	if strings.Contains(message, "\n") {
		t.Errorf("message must be one line: %q", message)
	}
}

func TestTailWriterKeepsCauseBeforeLongDump(t *testing.T) {
	log := &tailWriter{max: 4096}
	_, _ = log.Write([]byte("fatal error: out of memory\n"))
	_, _ = log.Write([]byte(strings.Repeat("goroutine dump line\n", 1000)))
	if strings.Contains(log.Tail(), "out of memory") {
		t.Fatal("test premise: the cause should have scrolled out of the tail")
	}
	if !pluginOutOfMemory(errors.New("exit_code(2)"), log.Head()+log.Tail()) {
		t.Fatal("the cause at the start of a long dump must still be recognised")
	}
}
