package main

import (
	"context"
	"os"
	"path/filepath"

	"github.com/tetratelabs/wazero"
)

// wasmCompileCacheDir is where spec run keeps compiled plugin machine code
// between runs, next to the recipe cache. Compiling a ~20 MB Go plugin takes
// most of a cold run; without this every run paid it again. An empty result
// disables the cache (KALO_NO_COMPILE_CACHE=1, or no cache directory).
func wasmCompileCacheDir() string {
	if value := os.Getenv("KALO_NO_COMPILE_CACHE"); value != "" && value != "0" {
		return ""
	}
	root := os.Getenv("KALO_CACHE_DIR")
	if root == "" {
		userCache, err := os.UserCacheDir()
		if err != nil {
			return ""
		}
		root = filepath.Join(userCache, "kalo")
	}
	return filepath.Join(root, "wazero")
}

// withCompileCache adds the on-disk compilation cache to config. wazero keys
// entries by the module bytes, its own version and the CPU, and spec run only
// compiles bytes whose digest it has already checked. A cache that can't be
// opened only costs time, so the run goes on without it. Call close after the
// runtime is closed.
func withCompileCache(ctx context.Context, config wazero.RuntimeConfig) (wazero.RuntimeConfig, func()) {
	dir := wasmCompileCacheDir()
	if dir == "" {
		return config, func() {}
	}
	cache, err := wazero.NewCompilationCacheWithDir(dir)
	if err != nil {
		return config, func() {}
	}
	return config.WithCompilationCache(cache), func() { _ = cache.Close(ctx) }
}
