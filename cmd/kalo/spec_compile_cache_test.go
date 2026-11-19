package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
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
	compiled := filepath.Join(cacheRoot, "compiled", "route")

	var progress bytes.Buffer
	first := fixtureRunOptions(root, filepath.Join(t.TempDir(), "out"))
	first.Progress = &progress
	firstResult, err := runSPEC(first)
	require.NoError(t, err)
	require.Equal(t, "kalo: running KALOX:OPENAPI-TO-TS-SDK@1; compiling the plugin first, this can take a minute\n", progress.String())
	entries := countFiles(t, compiled)
	require.Greater(t, entries, 0, "first run must leave compiled code in %s", compiled)

	second := fixtureRunOptions(root, filepath.Join(t.TempDir(), "out"))
	var warmProgress bytes.Buffer
	second.Progress = &warmProgress
	secondResult, err := runSPEC(second)
	require.NoError(t, err)
	require.Equal(t, "kalo: running KALOX:OPENAPI-TO-TS-SDK@1\n", warmProgress.String(), "a warm run must not promise a compile")
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
	require.Equal(t, 0, countFiles(t, filepath.Join(cacheRoot, "compiled", "route")))
}

func TestSpecRunWithoutCacheWhenCacheDirIsReadOnly(t *testing.T) {
	cacheRoot := t.TempDir()
	t.Setenv("KALO_CACHE_DIR", cacheRoot)
	t.Setenv("KALO_NO_COMPILE_CACHE", "")
	compiled := filepath.Join(cacheRoot, "compiled", "route")
	require.NoError(t, os.MkdirAll(filepath.Dir(compiled), 0o755))
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
	require.NoError(t, filepath.WalkDir(filepath.Join(cacheRoot, "compiled", "route"), func(path string, entry os.DirEntry, err error) error {
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
	compiled := filepath.Join(cacheRoot, "compiled", "route")
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

	_, dir, closeCache := withCompileCache(context.Background(), wazero.NewRuntimeConfig(), "")
	closeCache()
	require.Equal(t, compiled, dir)

	require.DirExists(t, filepath.Join(compiled, current), "our name must match the dir wazero creates")
	require.NoDirExists(t, old)
	require.DirExists(t, foreign)
	require.DirExists(t, unrelated)
}

func TestRecipeRunKeepsOneVersionDropsOldCacheAndPrintsCacheLineOnce(t *testing.T) {
	root := buildFixtureRecipe(t)
	archive := tarGzDir(t, root, nil)
	serveRecipe(t, archive, fmt.Sprintf("%x", sha256.Sum256(archive)))
	t.Setenv("KALO_NO_COMPILE_CACHE", "")
	cacheRoot := os.Getenv("KALO_CACHE_DIR")

	// Left by an older recipe version, by agent.4's shared compile cache, and
	// by another recipe (must stay).
	stale := []string{
		filepath.Join(cacheRoot, "recipes", "fixture", "0.0.9"),
		filepath.Join(cacheRoot, "compiled", "recipe", "fixture", "0.0.9"),
		filepath.Join(cacheRoot, "wazero", "wazero-v1.9.0-amd64-linux"),
	}
	kept := []string{
		filepath.Join(cacheRoot, "recipes", "other", "1.0.0"),
		filepath.Join(cacheRoot, "recipes", "fixture", ".fetch-123"),
	}
	for _, dir := range append(append([]string{}, stale...), kept...) {
		require.NoError(t, os.MkdirAll(dir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "entry"), []byte("x"), 0o644))
	}

	var progress bytes.Buffer
	options := recipeRunOptions{Name: "fixture", Input: userInput(t, root), Out: filepath.Join(t.TempDir(), "sdk")}
	options.Run.Progress = &progress
	_, err := runRecipe(options)
	require.NoError(t, err)
	for _, dir := range stale {
		require.NoDirExists(t, dir)
	}
	for _, dir := range kept {
		require.DirExists(t, dir)
	}
	compiled := filepath.Join(cacheRoot, "compiled", "recipe", "fixture", "0.1.0")
	require.Greater(t, countFiles(t, compiled), 0, "a recipe compiles into its own version dir")
	line := fmt.Sprintf("kalo: cached %s in %s (plugins %s, compiled code %s) so later runs skip compiling.",
		formatMB(treeBytes(filepath.Join(cacheRoot, "recipes"))+treeBytes(filepath.Join(cacheRoot, "compiled"))), cacheRoot,
		formatMB(treeBytes(filepath.Join(cacheRoot, "recipes"))), formatMB(treeBytes(filepath.Join(cacheRoot, "compiled"))))
	require.Contains(t, progress.String(), line, "sizes come from disk")
	require.Contains(t, progress.String(), "Delete the folder any time")
	require.NotContains(t, progress.String(), " s ", "no seconds in the line")

	// Nothing new cached: no line.
	var warm bytes.Buffer
	again := recipeRunOptions{Name: "fixture", Version: "0.1.0", Input: userInput(t, root), Out: filepath.Join(t.TempDir(), "sdk")}
	again.Run.Progress = &warm
	_, err = runRecipe(again)
	require.NoError(t, err)
	require.NotContains(t, warm.String(), "kalo: cached")

	// A bump where the pruned version holds more bytes than this run adds
	// (eng1's repro on #21): the line still prints.
	bigStale := filepath.Join(cacheRoot, "recipes", "fixture", "0.0.9")
	require.NoError(t, os.MkdirAll(bigStale, 0o755))
	sparse, err := os.Create(filepath.Join(bigStale, "big"))
	require.NoError(t, err)
	require.NoError(t, sparse.Truncate(300<<20))
	require.NoError(t, sparse.Close())
	require.NoError(t, os.RemoveAll(compiled))
	var bump bytes.Buffer
	third := recipeRunOptions{Name: "fixture", Version: "0.1.0", Input: userInput(t, root), Out: filepath.Join(t.TempDir(), "sdk")}
	third.Run.Progress = &bump
	_, err = runRecipe(third)
	require.NoError(t, err)
	require.NoDirExists(t, bigStale)
	require.Greater(t, countFiles(t, compiled), 0)
	require.Contains(t, bump.String(), "kalo: cached", "freed bytes from a pruned version must not hide added bytes")
}

func TestFormatMB(t *testing.T) {
	require.Equal(t, "<0.1 MB", formatMB(99_999))
	require.Equal(t, "4.2 MB", formatMB(4_200_000))
	require.Equal(t, "153 MB", formatMB(153_307_006))
}
