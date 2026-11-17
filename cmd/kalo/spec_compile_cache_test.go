package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tetratelabs/wazero"
)

// TestMain keeps tests that don't care about the compile cache from writing
// compiled plugins into the developer's cache directory.
func TestMain(m *testing.M) {
	if _, set := os.LookupEnv("KALO_NO_COMPILE_CACHE"); !set {
		os.Setenv("KALO_NO_COMPILE_CACHE", "1")
	}
	os.Exit(m.Run())
}

func countFiles(t *testing.T, root string) int {
	t.Helper()
	count := 0
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if os.IsNotExist(err) {
			return filepath.SkipDir
		}
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			count++
		}
		return nil
	})
	require.NoError(t, err)
	return count
}

func TestSpecRunKeepsCompiledPluginsBetweenRunsAndSaysWhatItRuns(t *testing.T) {
	cacheRoot := t.TempDir()
	t.Setenv("KALO_CACHE_DIR", cacheRoot)
	t.Setenv("KALO_NO_COMPILE_CACHE", "")
	root := buildFixtureCatalog(t, nil)
	compiled := filepath.Join(cacheRoot, "wazero")

	var progress bytes.Buffer
	first := fixtureRunOptions(root, filepath.Join(t.TempDir(), "out"))
	first.Progress = &progress
	firstResult, err := runSPEC(first)
	require.NoError(t, err)
	require.Equal(t, "kalo: running KALOX:OPENAPI-TO-TS-SDK@1; the first run compiles the plugin and can take a minute\n", progress.String())
	entries := countFiles(t, compiled)
	require.Greater(t, entries, 0, "first run must leave compiled code in %s", compiled)

	second := fixtureRunOptions(root, filepath.Join(t.TempDir(), "out"))
	secondResult, err := runSPEC(second)
	require.NoError(t, err)
	require.Equal(t, entries, countFiles(t, compiled), "second run must reuse the entry, not add one")
	firstOut, err := os.ReadFile(filepath.Join(first.Output, filepath.FromSlash(firstResult.Output)))
	require.NoError(t, err)
	secondOut, err := os.ReadFile(filepath.Join(second.Output, filepath.FromSlash(secondResult.Output)))
	require.NoError(t, err)
	require.Equal(t, string(firstOut), string(secondOut), "a cached compile must not change the output")
}

func TestSpecRunCompileCacheCanBeTurnedOff(t *testing.T) {
	cacheRoot := t.TempDir()
	t.Setenv("KALO_CACHE_DIR", cacheRoot)
	t.Setenv("KALO_NO_COMPILE_CACHE", "1")
	root := buildFixtureCatalog(t, nil)

	_, err := runSPEC(fixtureRunOptions(root, filepath.Join(t.TempDir(), "out")))
	require.NoError(t, err)
	require.Equal(t, 0, countFiles(t, filepath.Join(cacheRoot, "wazero")))
}

func TestSpecRunWithoutCacheWhenCacheDirIsReadOnly(t *testing.T) {
	cacheRoot := t.TempDir()
	t.Setenv("KALO_CACHE_DIR", cacheRoot)
	t.Setenv("KALO_NO_COMPILE_CACHE", "")
	compiled := filepath.Join(cacheRoot, "wazero")
	require.NoError(t, os.MkdirAll(compiled, 0o555))
	t.Cleanup(func() { os.Chmod(compiled, 0o755) })
	root := buildFixtureCatalog(t, nil)

	_, err := runSPEC(fixtureRunOptions(root, filepath.Join(t.TempDir(), "out")))
	require.NoError(t, err)
	require.Equal(t, 0, countFiles(t, compiled))
}

func TestSpecRunRecompilesOverACorruptCache(t *testing.T) {
	cacheRoot := t.TempDir()
	t.Setenv("KALO_CACHE_DIR", cacheRoot)
	t.Setenv("KALO_NO_COMPILE_CACHE", "")
	root := buildFixtureCatalog(t, nil)
	first := fixtureRunOptions(root, filepath.Join(t.TempDir(), "out"))
	firstResult, err := runSPEC(first)
	require.NoError(t, err)

	corrupted := 0
	require.NoError(t, filepath.WalkDir(filepath.Join(cacheRoot, "wazero"), func(path string, entry os.DirEntry, err error) error {
		if err == nil && !entry.IsDir() {
			corrupted++
			return os.WriteFile(path, []byte("not a compiled module"), 0o644)
		}
		return err
	}))
	require.Greater(t, corrupted, 0)

	second := fixtureRunOptions(root, filepath.Join(t.TempDir(), "out"))
	secondResult, err := runSPEC(second)
	require.NoError(t, err)
	firstOut, err := os.ReadFile(filepath.Join(first.Output, filepath.FromSlash(firstResult.Output)))
	require.NoError(t, err)
	secondOut, err := os.ReadFile(filepath.Join(second.Output, filepath.FromSlash(secondResult.Output)))
	require.NoError(t, err)
	require.Equal(t, string(firstOut), string(secondOut))
}

func TestCompileCacheDropsOtherWazeroVersionsForThisPlatform(t *testing.T) {
	cacheRoot := t.TempDir()
	t.Setenv("KALO_CACHE_DIR", cacheRoot)
	t.Setenv("KALO_NO_COMPILE_CACHE", "")
	compiled := filepath.Join(cacheRoot, "wazero")
	current := currentWazeroCacheDirName()
	require.NotEmpty(t, current, "wazero version not found in build info")

	otherArch := "arm64"
	if runtime.GOARCH == "arm64" {
		otherArch = "amd64"
	}
	old := filepath.Join(compiled, "wazero-v0.0.1-"+runtime.GOARCH+"-"+runtime.GOOS)
	foreign := filepath.Join(compiled, "wazero-v0.0.1-"+otherArch+"-"+runtime.GOOS)
	unrelated := filepath.Join(compiled, "notes")
	for _, dir := range []string{old, foreign, unrelated} {
		require.NoError(t, os.MkdirAll(dir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "entry"), []byte("x"), 0o644))
	}

	_, dir, closeCache := withCompileCache(context.Background(), wazero.NewRuntimeConfig())
	closeCache()
	require.Equal(t, compiled, dir)

	require.DirExists(t, filepath.Join(compiled, current), "our name must match the dir wazero creates")
	require.NoDirExists(t, old)
	require.DirExists(t, foreign)
	require.DirExists(t, unrelated)
}
