package specv0

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeValueYAMLMatchesJSON(t *testing.T) {
	fromYAML, err := DecodeValue("application/openapi+yaml", []byte("openapi: 3.0.3\ninfo:\n  version: 1\n  title: x\n"))
	require.NoError(t, err)
	fromJSON, err := DecodeValue("application/json", []byte(`{"openapi":"3.0.3","info":{"version":1,"title":"x"}}`))
	require.NoError(t, err)
	require.Equal(t, fromJSON, fromYAML)

	_, err = DecodeValue("application/json", []byte("openapi: 3.0.3\n"))
	require.Error(t, err, "a JSON binding must not silently accept YAML")
}

func TestIsYAMLMediaType(t *testing.T) {
	for _, mediaType := range []string{"application/yaml", "text/yaml", "application/openapi+yaml", "application/x-yaml; charset=utf-8"} {
		require.True(t, IsYAMLMediaType(mediaType), mediaType)
	}
	for _, mediaType := range []string{"", "application/json", MediaTypeFileSet, "application/yamlish"} {
		require.False(t, IsYAMLMediaType(mediaType), mediaType)
	}
}

func TestFileSetRoundTrip(t *testing.T) {
	src := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(src, "src"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "index.ts"), []byte("export {};\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(src, "src", "logo.bin"), []byte{0xff, 0x00, 0xfe}, 0o644))

	packed, err := PackDir(src)
	require.NoError(t, err)
	again, err := PackDir(src)
	require.NoError(t, err)
	require.Equal(t, packed, again, "packing must be deterministic")
	require.Contains(t, string(packed), `"path": "index.ts"`)
	require.Contains(t, string(packed), `"encoding": "base64"`)

	dst := t.TempDir()
	written, err := UnpackFileSet(packed, dst)
	require.NoError(t, err)
	require.Equal(t, []string{"index.ts", "src/logo.bin"}, written)
	data, err := os.ReadFile(filepath.Join(dst, "src", "logo.bin"))
	require.NoError(t, err)
	require.Equal(t, []byte{0xff, 0x00, 0xfe}, data)
}

func TestUnpackFileSetRejectsUnsafeEntries(t *testing.T) {
	const okSum = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" // sha256("")
	cases := map[string]string{
		"parent":    `{"files":[{"path":"../x","sha256":"` + okSum + `","encoding":"utf8","content":""}]}`,
		"absolute":  `{"files":[{"path":"/etc/x","sha256":"` + okSum + `","encoding":"utf8","content":""}]}`,
		"unclean":   `{"files":[{"path":"a/./b","sha256":"` + okSum + `","encoding":"utf8","content":""}]}`,
		"backslash": `{"files":[{"path":"a\\b","sha256":"` + okSum + `","encoding":"utf8","content":""}]}`,
		"duplicate": `{"files":[{"path":"a","sha256":"` + okSum + `","content":""},{"path":"a","sha256":"` + okSum + `","content":""}]}`,
		"digest":    `{"files":[{"path":"a","sha256":"00","encoding":"utf8","content":"x"}]}`,
		"encoding":  `{"files":[{"path":"a","sha256":"` + okSum + `","encoding":"hex","content":""}]}`,
		"unknown":   `{"files":[],"extra":1}`,
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			dst := t.TempDir()
			_, err := UnpackFileSet([]byte(data), dst)
			require.Error(t, err)
		})
	}
}

func TestPluginIOFor(t *testing.T) {
	implementation := func(ext any) *Implementation {
		impl := &Implementation{ImplementationID: "impl"}
		if ext != nil {
			impl.Extensions = map[string]any{ExtensionPluginIO: ext}
		}
		return impl
	}
	io, err := PluginIOFor(implementation(nil))
	require.NoError(t, err)
	require.Nil(t, io)

	io, err = PluginIOFor(implementation(map[string]any{"abi": "kalo-plugin-v1", "input": map[string]any{"filename": "openapi.yaml"}, "config": map[string]any{"specFileName": "openapi.yaml"}}))
	require.NoError(t, err)
	require.Equal(t, "single", io.Output.Pack)
	require.Equal(t, "openapi.yaml", io.Input.Filename)
	require.Equal(t, "openapi.yaml", io.Config["specFileName"])

	for name, ext := range map[string]any{
		"abi":      map[string]any{"abi": "kalo-plugin-v2"},
		"pack":     map[string]any{"abi": "kalo-plugin-v1", "output": map[string]any{"pack": "zip"}},
		"filename": map[string]any{"abi": "kalo-plugin-v1", "input": map[string]any{"filename": "../openapi.yaml"}},
		"subdir":   map[string]any{"abi": "kalo-plugin-v1", "input": map[string]any{"filename": "a/openapi.yaml"}},
		"unknown":  map[string]any{"abi": "kalo-plugin-v1", "inputs": map[string]any{}},
	} {
		_, err := PluginIOFor(implementation(ext))
		require.Error(t, err, name)
	}
}

func TestDecodeValueRejectsTrailingYAMLDocuments(t *testing.T) {
	if _, err := DecodeValue("application/yaml", []byte("ok: true\n---\nok: false\n")); err == nil {
		t.Fatal("expected a second YAML document to be rejected")
	}
	if _, err := DecodeValue("application/yaml", []byte("ok: true\n---\n: : bad\n  - x\n")); err == nil {
		t.Fatal("expected a malformed trailing YAML document to be rejected")
	}
	if _, err := DecodeValue("application/yaml", []byte("---\nok: true\n")); err != nil {
		t.Fatalf("single document with leading marker: %v", err)
	}
}

func TestFileSetMediaTypeIgnoresCaseAndParameters(t *testing.T) {
	for _, mediaType := range []string{MediaTypeFileSet, "Application/Vnd.Kalo.Fileset+Json", "application/vnd.kalo.fileset+json; charset=utf-8"} {
		if !IsFileSetMediaType(mediaType) {
			t.Fatalf("%q should be a file set", mediaType)
		}
	}
	if IsFileSetMediaType("application/json") {
		t.Fatal("application/json is not a file set")
	}
}

func TestPackDirRejectsPathsUnpackWouldRefuse(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("backslash is a separator on Windows")
	}
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, `a\b`), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := PackDir(src); err == nil {
		t.Fatal(`expected a file named a\b to be refused`)
	}
}
