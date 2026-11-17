package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
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
