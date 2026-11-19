package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"

	"github.com/tetratelabs/wazero"
)

// kaloCacheRoot is the folder that holds everything spec run caches: fetched
// recipes (recipes/) and compiled plugin code (compiled/).
func kaloCacheRoot() (string, error) {
	if root := os.Getenv("KALO_CACHE_DIR"); root != "" {
		return root, nil
	}
	userCache, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("find cache directory (set KALO_CACHE_DIR): %w", err)
	}
	return filepath.Join(userCache, "kalo"), nil
}

// compileCacheOff reports KALO_NO_COMPILE_CACHE set to anything but "" or "0".
func compileCacheOff() bool {
	value := os.Getenv("KALO_NO_COMPILE_CACHE")
	return value != "" && value != "0"
}

// recipeCompileCacheSubdir is where a fetched recipe's compiled plugins live,
// relative to the cache root, next to recipes/<name>/<version>. One directory
// per recipe version lets pruneRecipeVersions drop a version's compiled code
// with its plugins, without knowing wazero's file names.
func recipeCompileCacheSubdir(name, version string) string {
	return filepath.Join("compiled", "recipe", name, version)
}

// routeCompileCacheSubdir holds compiled code for route runs and recipes run
// from a local folder.
const routeCompileCacheSubdir = "compiled/route"

// legacyCompileCacheSubdir is where builds up to v0.2.0-agent.4 kept every
// compiled plugin in one directory; nothing reads it any more.
const legacyCompileCacheSubdir = "wazero"

// wasmCompileCacheDir is where spec run keeps compiled plugin machine code
// between runs. Compiling a ~20 MB Go plugin takes most of a cold run; without
// this every run paid it again. subdir is relative to the cache root ("" for
// the route directory). An empty result disables the cache
// (KALO_NO_COMPILE_CACHE=1, or no cache directory).
func wasmCompileCacheDir(subdir string) string {
	if compileCacheOff() {
		return ""
	}
	root, err := kaloCacheRoot()
	if err != nil {
		return ""
	}
	if subdir == "" {
		subdir = routeCompileCacheSubdir
	}
	return filepath.Join(root, filepath.FromSlash(subdir))
}

// withCompileCache adds the on-disk compilation cache to config. wazero keys
// entries by the module bytes, its own version and the CPU, and spec run only
// compiles bytes whose digest it has already checked. A cache that can't be
// opened only costs time, so the run goes on without it. Call close after the
// runtime is closed. The returned dir is "" when no cache is in use.
func withCompileCache(ctx context.Context, config wazero.RuntimeConfig, subdir string) (wazero.RuntimeConfig, string, func()) {
	dir := wasmCompileCacheDir(subdir)
	if dir == "" {
		return config, "", func() {}
	}
	cache, err := wazero.NewCompilationCacheWithDir(dir)
	if err != nil {
		return config, "", func() {}
	}
	pruneOtherWazeroVersions(dir)
	if root, err := kaloCacheRoot(); err == nil {
		_ = os.RemoveAll(filepath.Join(root, legacyCompileCacheSubdir))
	}
	return config.WithCompilationCache(cache), dir, func() { _ = cache.Close(ctx) }
}

// pruneRecipeVersions keeps one version of a fetched recipe: it deletes the
// other versions' plugins (recipes/<name>/<v>) and compiled code
// (compiled/recipe/<name>/<v>). Without it every recipe bump left ~195 MB
// behind. Fetch staging dirs (".fetch-*") and other recipes are left alone.
// Errors only leave bytes on disk.
func pruneRecipeVersions(root, name, keep string) {
	for _, parent := range []string{filepath.Join(root, "recipes", name), filepath.Join(root, "compiled", "recipe", name)} {
		entries, err := os.ReadDir(parent)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if !entry.IsDir() || entry.Name() == keep || strings.HasPrefix(entry.Name(), ".") {
				continue
			}
			_ = os.RemoveAll(filepath.Join(parent, entry.Name()))
		}
	}
}

// treeBytes sums the sizes of the regular files under root (0 if missing).
func treeBytes(root string) int64 {
	var total int64
	_ = filepath.WalkDir(root, func(_ string, entry os.DirEntry, err error) error {
		if err == nil && entry.Type().IsRegular() {
			if info, err := entry.Info(); err == nil {
				total += info.Size()
			}
		}
		return nil
	})
	return total
}

// formatMB prints bytes as decimal megabytes: "153 MB", "4.2 MB", "<0.1 MB".
func formatMB(bytes int64) string {
	mb := float64(bytes) / 1e6
	switch {
	case mb < 0.1:
		return "<0.1 MB"
	case mb < 10:
		return fmt.Sprintf("%.1f MB", mb)
	default:
		return fmt.Sprintf("%.0f MB", mb)
	}
}

// displayPath shortens a path under the home directory to ~/...
func displayPath(path string) string {
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		if rel, err := filepath.Rel(home, path); err == nil && rel != "." && !strings.HasPrefix(rel, "..") {
			return "~" + string(filepath.Separator) + rel
		}
	}
	return path
}

// printCacheLine tells, once after a run that added bytes to the cache, what
// the cache holds and how to delete it. Sizes are read from disk.
func printCacheLine(w io.Writer, root string) {
	recipes := treeBytes(filepath.Join(root, "recipes"))
	compiled := treeBytes(filepath.Join(root, "compiled"))
	if compileCacheOff() || compiled == 0 {
		fmt.Fprintf(w, "kalo: cached %s of plugins in %s so later runs skip the download. Delete the folder any time.\n",
			formatMB(recipes), displayPath(root))
		return
	}
	fmt.Fprintf(w, "kalo: cached %s in %s (plugins %s, compiled code %s) so later runs skip compiling. Delete the folder any time; KALO_NO_COMPILE_CACHE=1 keeps compiled code off disk (every run compiles).\n",
		formatMB(recipes+compiled), displayPath(root), formatMB(recipes), formatMB(compiled))
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

// currentWazeroCacheDirName is the subdirectory wazero writes into inside the
// cache dir: "wazero-<version>-<GOARCH>-<GOOS>" (wazero cache.go). The version
// is read the way wazero reads it, from the build info. "" when unknown.
func currentWazeroCacheDirName() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	version := ""
	for _, dep := range info.Deps {
		if dep.Path == "github.com/tetratelabs/wazero" {
			version = dep.Version
			if dep.Replace != nil && dep.Replace.Version != "" {
				version = dep.Replace.Version
			}
		}
	}
	if version == "" || version == "(devel)" {
		return ""
	}
	return "wazero-" + version + "-" + runtime.GOARCH + "-" + runtime.GOOS
}

// pruneOtherWazeroVersions deletes the cache subdirectories that older or newer
// wazero builds (other kalo versions) left for this CPU and OS, so upgrading
// kalo doesn't keep ~150 MB per old version. Directories for other platforms
// and anything not named like a wazero cache dir are left alone. Does nothing
// when the current version can't be told.
func pruneOtherWazeroVersions(dir string) {
	current := currentWazeroCacheDirName()
	if current == "" {
		return
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	suffix := "-" + runtime.GOARCH + "-" + runtime.GOOS
	for _, entry := range entries {
		name := entry.Name()
		if !entry.IsDir() || name == current || !strings.HasPrefix(name, "wazero-") || !strings.HasSuffix(name, suffix) {
			continue
		}
		_ = os.RemoveAll(filepath.Join(dir, name))
	}
}

// compiledMarkerPath is an empty file spec run leaves once a plugin with this
// digest has compiled and run with the cache on. It sits inside wazero's
// version directory, so pruning that directory or clearing a damaged cache
// removes it with the compiled code. "" when the cache is off or the wazero
// version can't be told; callers then assume a compile.
func compiledMarkerPath(dir, digest string) string {
	current := currentWazeroCacheDirName()
	if dir == "" || current == "" || digest == "" {
		return ""
	}
	name := strings.NewReplacer(":", "-", "/", "-", "\\", "-").Replace(digest)
	return filepath.Join(dir, current, "kalo-compiled", name)
}

// compiledBefore reports whether this digest already has compiled code in the
// cache, so the progress line only warns about a wait when there will be one.
func compiledBefore(dir, digest string) bool {
	path := compiledMarkerPath(dir, digest)
	if path == "" {
		return false
	}
	_, err := os.Stat(path)
	return err == nil
}

// markCompiled records that digest compiled into the cache. Errors only mean
// the next run warns about a compile again.
func markCompiled(dir, digest string) {
	path := compiledMarkerPath(dir, digest)
	if path == "" {
		return
	}
	if os.MkdirAll(filepath.Dir(path), 0o755) == nil {
		_ = os.WriteFile(path, nil, 0o644)
	}
}
