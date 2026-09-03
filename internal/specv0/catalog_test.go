package specv0

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArtifactFragmentHashesWholeResourceAndValidatesSelectedSchema(t *testing.T) {
	root := t.TempDir()
	wrapperPath := filepath.Join(root, "wrapper.json")
	wrapper := []byte(`{"schema":{"type":"object","required":["ok"],"properties":{"ok":{"const":true}},"additionalProperties":false},"metadata":"outside-fragment"}`)
	require.NoError(t, os.WriteFile(wrapperPath, wrapper, 0o644))
	digest := fmt.Sprintf("sha256:%x", sha256.Sum256(wrapper))
	descriptorPath := filepath.Join(root, "binding.yaml")
	require.NoError(t, os.WriteFile(descriptorPath, []byte("descriptor"), 0o644))

	resolved, fragment, err := ResolveArtifact(descriptorPath, "wrapper.json#/schema")
	require.NoError(t, err)
	require.Equal(t, wrapperPath, resolved)
	require.Equal(t, "/schema", fragment)
	require.NoError(t, VerifyDigest(resolved, digest))

	bindingAddress := "TEST:VALUE@1:JSON@1"
	catalog := &Catalog{Bindings: map[string]*Binding{
		bindingAddress: {
			Resource:       Resource{Address: bindingAddress, SourcePath: descriptorPath},
			Representation: Representation{Standard: "JSON Schema 2020-12", Artifact: &Artifact{Ref: "wrapper.json#/schema", Digest: digest}},
		},
	}}
	validPath := filepath.Join(root, "valid.json")
	invalidPath := filepath.Join(root, "invalid.json")
	require.NoError(t, os.WriteFile(validPath, []byte(`{"ok":true}`), 0o644))
	require.NoError(t, os.WriteFile(invalidPath, []byte(`{"ok":false}`), 0o644))
	require.NoError(t, catalog.ValidateValue(bindingAddress, validPath))
	require.Error(t, catalog.ValidateValue(bindingAddress, invalidPath))
}

func TestReceiptSourceIsGenericAndUsesValueDigest(t *testing.T) {
	bindingAddress := "TEST:VALUE@1:JSON@1"
	catalog := &Catalog{Bindings: map[string]*Binding{
		bindingAddress: {
			Resource: Resource{Extensions: map[string]any{
				"ACME:PROVENANCE": map[string]any{
					"receipt_source": map[string]any{"system": "Acme", "resource": "orders", "version": "42"},
				},
			}},
		},
	}}
	source, err := catalog.ReceiptSource(bindingAddress, "sha256:abc")
	require.NoError(t, err)
	require.Equal(t, map[string]string{"system": "Acme", "resource": "orders", "version": "42", "digest": "sha256:abc"}, source)
}
