package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kalo-build/kalo-cli/internal/specv0"
	"github.com/stretchr/testify/require"
)

// buildFixtureCatalog copies testdata/specrun into a temp dir, builds the
// fixture plugin for wasip1 and writes the implementation descriptor with the
// real digest. implementationEdit can change the descriptor text.
func buildFixtureCatalog(t *testing.T, implementationEdit func(string) string) string {
	t.Helper()
	if testing.Short() {
		t.Skip("builds a wasip1 plugin")
	}
	root := t.TempDir()
	require.NoError(t, os.CopyFS(root, os.DirFS("testdata/specrun")))
	wasm := filepath.Join(root, "artifacts", "plugin.wasm")
	build := exec.Command("go", "build", "-o", wasm, "./testdata/specrun/plugin")
	build.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm", "CGO_ENABLED=0")
	out, err := build.CombinedOutput()
	require.NoError(t, err, string(out))
	data, err := os.ReadFile(wasm)
	require.NoError(t, err)
	tmplPath := filepath.Join(root, "descriptors", "implementations", "sdk.yaml.tmpl")
	tmpl, err := os.ReadFile(tmplPath)
	require.NoError(t, err)
	descriptor := strings.ReplaceAll(string(tmpl), "{{DIGEST}}", fmt.Sprintf("sha256:%x", sha256.Sum256(data)))
	if implementationEdit != nil {
		descriptor = implementationEdit(descriptor)
	}
	require.NoError(t, os.WriteFile(filepath.Join(root, "descriptors", "implementations", "sdk.yaml"), []byte(descriptor), 0o644))
	require.NoError(t, os.Remove(tmplPath))
	require.NoError(t, os.RemoveAll(filepath.Join(root, "plugin")))
	return root
}

func fixtureRunOptions(root, output string) specRunOptions {
	return specRunOptions{
		CatalogPath: filepath.Join(root, "descriptors"),
		From:        "KALOX:OPENAPI@1:JSON@1",
		To:          "KALOX:TS-SDK@1:JSON@1",
		Input:       filepath.Join(root, "openapi.yaml"),
		Output:      output,
	}
}

func TestSpecRunYAMLInputPluginIOAndFileSetOutput(t *testing.T) {
	root := buildFixtureCatalog(t, nil)
	// The user's file name differs from what the plugin expects: KALO:PLUGIN-IO
	// input.filename and config.specFileName bridge it.
	userInput := filepath.Join(t.TempDir(), "my-api.yml")
	data, err := os.ReadFile(filepath.Join(root, "openapi.yaml"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(userInput, data, 0o644))
	options := fixtureRunOptions(root, filepath.Join(t.TempDir(), "out"))
	options.Input = userInput

	result, err := runSPEC(options)
	require.NoError(t, err)
	require.Equal(t, "artifacts/01-sdk.fileset.json", result.Output)

	packed, err := os.ReadFile(filepath.Join(options.Output, filepath.FromSlash(result.Output)))
	require.NoError(t, err)
	var set specv0.FileSet
	require.NoError(t, json.Unmarshal(packed, &set))
	require.Len(t, set.Files, 2)
	require.Equal(t, "index.ts", set.Files[0].Path)
	require.Equal(t, "src/client.ts", set.Files[1].Path)
	require.Equal(t, "// generated from 5 lines\n", set.Files[1].Content)

	_, err = os.Stat(filepath.Join(options.Output, "work"))
	require.True(t, os.IsNotExist(err), "work dir must be removed")
}

func TestSpecRunFailureShowsPluginOutputAndCleansUp(t *testing.T) {
	root := buildFixtureCatalog(t, func(descriptor string) string {
		return strings.Replace(descriptor, "config: {specFileName: openapi.yaml}", "config: {specFileName: openapi.yaml, fail: true}", 1)
	})
	options := fixtureRunOptions(root, filepath.Join(t.TempDir(), "out"))
	_, err := runSPEC(options)
	require.Error(t, err)
	require.Contains(t, err.Error(), "fixture plugin: failing on purpose")
	_, statErr := os.Stat(options.Output)
	require.True(t, os.IsNotExist(statErr), "output dir must be removed on failure")
}

func TestSpecRunRejectsPackWithoutFileSetBinding(t *testing.T) {
	root := buildFixtureCatalog(t, func(descriptor string) string {
		return strings.Replace(descriptor, "output: {pack: fileset}", "output: {pack: single}", 1)
	})
	_, err := runSPEC(fixtureRunOptions(root, filepath.Join(t.TempDir(), "out")))
	require.ErrorContains(t, err, "output.pack=fileset requires output binding")
}
