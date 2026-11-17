package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"

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
// runtime is closed. The returned dir is "" when no cache is in use.
func withCompileCache(ctx context.Context, config wazero.RuntimeConfig) (wazero.RuntimeConfig, string, func()) {
	dir := wasmCompileCacheDir()
	if dir == "" {
		return config, "", func() {}
	}
	cache, err := wazero.NewCompilationCacheWithDir(dir)
	if err != nil {
		return config, "", func() {}
	}
	return config.WithCompilationCache(cache), dir, func() { _ = cache.Close(ctx) }
}

// isCompileCacheError reports a compile that failed on a damaged cache entry;
// wazero returns these instead of recompiling.
func isCompileCacheError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "compilationcache:")
}

// clearCompileCache deletes the cached files but keeps the directories, which
// the open cache expects to exist when it writes the next entry.
func clearCompileCache(dir string) {
	_ = filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err == nil && !entry.IsDir() {
			_ = os.Remove(path)
		}
		return nil
	})
}
