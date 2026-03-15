package registry

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestPluginLockInfo_PluginIdentity(t *testing.T) {
	t.Run("returns Plugin when set", func(t *testing.T) {
		info := PluginLockInfo{Plugin: "@kalo-build/other"}
		got := info.PluginIdentity("@kalo-build/alias")
		assert.Equal(t, PluginIdentifier("@kalo-build/other"), got)
	})
	t.Run("returns key when Plugin empty", func(t *testing.T) {
		info := PluginLockInfo{}
		got := info.PluginIdentity("@kalo-build/plugin-name")
		assert.Equal(t, PluginIdentifier("@kalo-build/plugin-name"), got)
	})
}

func TestCalculateSHA256(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "foo.txt")
	err := os.WriteFile(f, []byte("hello"), 0644)
	require.NoError(t, err)

	hash, err := CalculateSHA256(f)
	require.NoError(t, err)
	assert.True(t, len(hash) > 0)
	assert.Equal(t, "sha256:", hash[:7])
}

func TestCalculateSHA256_NoFile(t *testing.T) {
	_, err := CalculateSHA256(filepath.Join(t.TempDir(), "nonexistent"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read file")
}

func TestLockFile_YAML(t *testing.T) {
	lf := LockFile{
		GeneratedAt: time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC),
		Plugins: map[PluginIdentifier]PluginLockInfo{
			"@kalo-build/foo": {
				Version:      "v1.0.0",
				ResolvedHash: "sha256:abc",
				Location:     ".kalo/plugins/foo.wasm",
				DownloadedAt: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			"@kalo-build/alias": {
				Plugin:       "@kalo-build/bar",
				Version:      "v2.0.0",
				ResolvedHash: "sha256:def",
				Location:     ".kalo/plugins/bar.wasm",
				DownloadedAt: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		},
	}
	out, err := yaml.Marshal(lf)
	require.NoError(t, err)

	var decoded LockFile
	err = yaml.Unmarshal(out, &decoded)
	require.NoError(t, err)
	assert.Equal(t, lf.GeneratedAt.Unix(), decoded.GeneratedAt.Unix())
	assert.Len(t, decoded.Plugins, 2)
	assert.Equal(t, "v1.0.0", string(decoded.Plugins["@kalo-build/foo"].Version))
	assert.Equal(t, "@kalo-build/bar", decoded.Plugins["@kalo-build/alias"].Plugin)
}

func TestNewRegistryClient_Defaults(t *testing.T) {
	c := NewRegistryClient(nil)
	require.NotNil(t, c)
	assert.Equal(t, DefaultRegistryURL, c.options.RegistryURL)
	assert.Equal(t, DefaultCacheDir, c.options.CacheDir)
	assert.False(t, c.options.OfflineMode)
	assert.NotNil(t, c.httpClient)
}

func TestNewRegistryClient_Options(t *testing.T) {
	c := NewRegistryClient(&RegistryClientOptions{
		RegistryURL: "https://custom.registry",
		CacheDir:    "/tmp/plugins",
		OfflineMode: true,
	})
	require.NotNil(t, c)
	assert.Equal(t, "https://custom.registry", c.options.RegistryURL)
	assert.Equal(t, "/tmp/plugins", c.options.CacheDir)
	assert.True(t, c.options.OfflineMode)
}
