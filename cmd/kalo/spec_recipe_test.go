package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

const fixtureRecipe = `name: fixture
version: 0.1.0
description: test recipe
catalog: descriptors
input:
  from: KALOX:OPENAPI@1:JSON@1
  accept: ["*.yaml", "*.yml"]
outputs:
  - to: KALOX:TS-SDK@1:JSON@1
    dir: ts
`

func buildFixtureRecipe(t *testing.T) string {
	root := buildFixtureCatalog(t, nil)
	require.NoError(t, os.WriteFile(filepath.Join(root, recipeFileName), []byte(fixtureRecipe), 0o644))
	return root
}

func tarGzDir(t *testing.T, root string, extra map[string]string) []byte {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	require.NoError(t, filepath.WalkDir(root, func(current string, entry fs.DirEntry, err error) error {
		require.NoError(t, err)
		rel, _ := filepath.Rel(root, current)
		if rel == "." || entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(current)
		require.NoError(t, err)
		require.NoError(t, tw.WriteHeader(&tar.Header{Name: filepath.ToSlash(rel), Mode: 0o644, Size: int64(len(data)), Typeflag: tar.TypeReg}))
		_, err = tw.Write(data)
		return err
	}))
	for name, content := range extra {
		require.NoError(t, tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(content)), Typeflag: tar.TypeReg}))
		_, err := tw.Write([]byte(content))
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())
	require.NoError(t, gz.Close())
	return buf.Bytes()
}

// serveRecipe publishes archive like a kalo-plugins release and counts asset GETs.
func serveRecipe(t *testing.T, archive []byte, sum string) *int32 {
	var assetGets int32
	mux := http.NewServeMux()
	mux.HandleFunc("/releases", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `[{"tag_name":"recipe-fixture-v9.0.0","draft":true},{"tag_name":"recipe-fixture-v0.1.0"},{"tag_name":"recipe-fixture-v0.0.9"},{"tag_name":"recipe-other-v1.0.0"}]`)
	})
	mux.HandleFunc("/dl/recipe-fixture-v0.1.0/recipe-fixture-0.1.0.tar.gz", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&assetGets, 1)
		_, _ = w.Write(archive)
	})
	mux.HandleFunc("/dl/recipe-fixture-v0.1.0/recipe-fixture-0.1.0.tar.gz.sha256", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "%s  recipe-fixture-0.1.0.tar.gz\n", sum)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	t.Setenv("KALO_RECIPE_RELEASES_API", server.URL+"/releases")
	t.Setenv("KALO_RECIPE_DOWNLOAD_BASE", server.URL+"/dl")
	t.Setenv("KALO_CACHE_DIR", t.TempDir())
	return &assetGets
}

func userInput(t *testing.T, root string) string {
	path := filepath.Join(t.TempDir(), "my-api.yml")
	data, err := os.ReadFile(filepath.Join(root, "openapi.yaml"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o644))
	return path
}

func listTree(t *testing.T, root string) []string {
	var paths []string
	require.NoError(t, filepath.WalkDir(root, func(current string, entry fs.DirEntry, err error) error {
		require.NoError(t, err)
		if !entry.IsDir() {
			rel, _ := filepath.Rel(root, current)
			paths = append(paths, filepath.ToSlash(rel))
		}
		return nil
	}))
	return paths
}

func TestRecipeRunFromLocalFolderLeavesOnlyOutput(t *testing.T) {
	root := buildFixtureRecipe(t)
	parent := t.TempDir()
	out := filepath.Join(parent, "sdk")
	keep := filepath.Join(parent, "kept")
	receipts := filepath.Join(parent, "receipts")
	result, err := runRecipe(recipeRunOptions{LocalCatalog: root, Input: userInput(t, root), Out: out, KeepSpecsDir: keep, ReceiptsDir: receipts})
	require.NoError(t, err)
	require.Equal(t, []recipeRunOutput{{Dir: "ts", Files: []string{"index.ts", "src/client.ts"}}}, result.Outputs)
	require.Equal(t, []string{"ts/index.ts", "ts/src/client.ts"}, listTree(t, out))
	entries, err := os.ReadDir(parent)
	require.NoError(t, err)
	require.Len(t, entries, 3, "only sdk, kept and receipts may remain next to --out")
	require.FileExists(t, filepath.Join(keep, "README.md"))
	require.FileExists(t, filepath.Join(keep, "recipe", recipeFileName))
	require.FileExists(t, filepath.Join(keep, "runs", "ts", "artifacts", "00-my-api.yml"))
	require.FileExists(t, filepath.Join(receipts, "ts", "execution.json"))

	// A second run into the same, now non-empty, --out needs --force.
	_, err = runRecipe(recipeRunOptions{LocalCatalog: root, Input: userInput(t, root), Out: out})
	require.ErrorContains(t, err, "not empty")
	_, err = runRecipe(recipeRunOptions{LocalCatalog: root, Input: userInput(t, root), Out: out, Force: true})
	require.NoError(t, err)
}

func TestRecipeRunRejectsUnacceptedInputAndCleansUpOnFailure(t *testing.T) {
	root := buildFixtureRecipe(t)
	parent := t.TempDir()
	bad := filepath.Join(t.TempDir(), "api.json")
	require.NoError(t, os.WriteFile(bad, []byte("{}"), 0o644))
	_, err := runRecipe(recipeRunOptions{LocalCatalog: root, Input: bad, Out: filepath.Join(parent, "sdk")})
	require.ErrorContains(t, err, "accepts *.yaml, *.yml")

	broken := filepath.Join(t.TempDir(), "broken.yaml")
	require.NoError(t, os.WriteFile(broken, []byte("a: [1\n"), 0o644))
	_, err = runRecipe(recipeRunOptions{LocalCatalog: root, Input: broken, Out: filepath.Join(parent, "sdk")})
	require.Error(t, err)
	entries, err := os.ReadDir(parent)
	require.NoError(t, err)
	require.Empty(t, entries, "a failed run leaves nothing behind")
}

func TestRecipeFetchVerifiesCachesAndPicksLatest(t *testing.T) {
	root := buildFixtureRecipe(t)
	archive := tarGzDir(t, root, nil)
	gets := serveRecipe(t, archive, fmt.Sprintf("%x", sha256.Sum256(archive)))

	out := filepath.Join(t.TempDir(), "sdk")
	result, err := runRecipe(recipeRunOptions{Name: "fixture", Input: userInput(t, root), Out: out})
	require.NoError(t, err)
	require.Equal(t, "0.1.0", result.Version)
	require.Equal(t, int32(1), atomic.LoadInt32(gets))
	require.FileExists(t, filepath.Join(out, "ts", "index.ts"))

	// Cached: a pinned second run, even offline, does not fetch again.
	offline := recipeRunOptions{Name: "fixture", Version: "v0.1.0", Input: userInput(t, root), Out: filepath.Join(t.TempDir(), "sdk")}
	offline.Run.Policy.Offline = true
	_, err = runRecipe(offline)
	require.NoError(t, err)
	require.Equal(t, int32(1), atomic.LoadInt32(gets))

	_, err = runRecipe(recipeRunOptions{Name: "missing", Version: "1.0.0", Input: userInput(t, root), Out: filepath.Join(t.TempDir(), "sdk")})
	require.ErrorContains(t, err, "404")
}

func TestRecipeFetchRejectsBadDigestAndUnsafeArchive(t *testing.T) {
	root := buildFixtureRecipe(t)
	archive := tarGzDir(t, root, nil)
	serveRecipe(t, archive, fmt.Sprintf("%064x", 0))
	_, err := runRecipe(recipeRunOptions{Name: "fixture", Version: "0.1.0", Input: userInput(t, root), Out: filepath.Join(t.TempDir(), "sdk")})
	require.ErrorContains(t, err, "sha256 is")
	_, statErr := os.Stat(filepath.Join(os.Getenv("KALO_CACHE_DIR"), "recipes", "fixture", "0.1.0"))
	require.True(t, os.IsNotExist(statErr), "a rejected asset must not be cached")

	evil := tarGzDir(t, root, map[string]string{"../escape.txt": "x"})
	serveRecipe(t, evil, fmt.Sprintf("%x", sha256.Sum256(evil)))
	_, err = runRecipe(recipeRunOptions{Name: "fixture", Version: "0.1.0", Input: userInput(t, root), Out: filepath.Join(t.TempDir(), "sdk")})
	require.ErrorContains(t, err, "escapes the recipe folder")
}

func TestRecipeLatestVersionFollowsPagesAndExplainsRateLimit(t *testing.T) {
	var server *httptest.Server
	mux := http.NewServeMux()
	mux.HandleFunc("/releases", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("page") == "2" {
			fmt.Fprint(w, `[{"tag_name":"recipe-fixture-v0.3.0"}]`)
			return
		}
		w.Header().Set("Link", fmt.Sprintf(`<%s/releases?page=2>; rel="next", <%s/releases?page=2>; rel="last"`, server.URL, server.URL))
		fmt.Fprint(w, `[{"tag_name":"plugin-x-v9.0.0"},{"tag_name":"recipe-fixture-v0.2.0"}]`)
	})
	mux.HandleFunc("/limited", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "rate limit exceeded", http.StatusForbidden)
	})
	server = httptest.NewServer(mux)
	t.Cleanup(server.Close)

	t.Setenv("KALO_RECIPE_RELEASES_API", server.URL+"/releases")
	latest, err := latestRecipeVersion(server.Client(), "fixture")
	require.NoError(t, err)
	require.Equal(t, "0.3.0", latest, "a version on page 2 must be found")

	t.Setenv("KALO_RECIPE_RELEASES_API", server.URL+"/limited")
	_, err = latestRecipeVersion(server.Client(), "fixture")
	require.ErrorContains(t, err, "rate limit")
	require.ErrorContains(t, err, "--recipe-version")
}

func TestRecipeExtractCapsEntries(t *testing.T) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for i := 0; i <= maxRecipeEntries; i++ {
		require.NoError(t, tw.WriteHeader(&tar.Header{Name: fmt.Sprintf("d%d/", i), Typeflag: tar.TypeDir, Mode: 0o755}))
	}
	require.NoError(t, tw.Close())
	require.NoError(t, gz.Close())
	err := extractTarGz(buf.Bytes(), t.TempDir())
	require.ErrorContains(t, err, "more than")
}

func TestRecipeRejectsOverlappingDestinationsBeforeWriting(t *testing.T) {
	root := buildFixtureRecipe(t)
	parent := t.TempDir()
	out := filepath.Join(parent, "sdk")
	cases := []recipeRunOptions{
		{LocalCatalog: root, Input: userInput(t, root), Out: out, ReceiptsDir: out},
		{LocalCatalog: root, Input: userInput(t, root), Out: out, KeepSpecsDir: filepath.Join(out, "ts")},
		{LocalCatalog: root, Input: userInput(t, root), Out: out, KeepSpecsDir: filepath.Join(root, "kept")},
		{LocalCatalog: root, Input: userInput(t, root), Out: out, ReceiptsDir: filepath.Join(parent, "ev"), KeepSpecsDir: filepath.Join(parent, "ev", "k")},
	}
	for _, options := range cases {
		_, err := runRecipe(options)
		require.ErrorContains(t, err, "overlap")
	}
	entries, err := os.ReadDir(parent)
	require.NoError(t, err)
	require.Empty(t, entries, "nothing is written when destinations overlap")
}

func TestShellQuote(t *testing.T) {
	require.Equal(t, "runs/ts/artifacts/00-api.yml", shellQuote("runs/ts/artifacts/00-api.yml"))
	require.Equal(t, "'runs/ts/artifacts/00-my api.yml'", shellQuote("runs/ts/artifacts/00-my api.yml"))
	require.Equal(t, `'it'\''s;rm'`, shellQuote("it's;rm"))
	require.Equal(t, "''", shellQuote(""))
}
