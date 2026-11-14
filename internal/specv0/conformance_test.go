package specv0

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type addressVector struct {
	Address        string `json:"address"`
	Form           string `json:"form"`
	Namespace      string `json:"namespace"`
	Name           string `json:"name"`
	Version        string `json:"version"`
	Binding        string `json:"binding"`
	BindingVersion string `json:"binding_version"`
}

func conformanceRoot(t *testing.T) string {
	t.Helper()
	root := os.Getenv("SPEC_CONFORMANCE_ROOT")
	if root == "" {
		t.Skip("SPEC_CONFORMANCE_ROOT is not set")
	}
	absolute, err := filepath.Abs(root)
	require.NoError(t, err)
	return absolute
}

func readAddressVectors(t *testing.T, name string) []addressVector {
	t.Helper()
	path := filepath.Join(conformanceRoot(t), "fixtures", "addresses", name)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var vectors []addressVector
	require.NoError(t, json.Unmarshal(data, &vectors))
	return vectors
}

func TestConformanceAddressVectors(t *testing.T) {
	for _, vector := range readAddressVectors(t, "valid.json") {
		parsed, err := ParseAddress(vector.Address)
		require.NoError(t, err, vector.Address)
		require.Equal(t, vector.Namespace, parsed.Namespace, vector.Address)
		require.Equal(t, vector.Name, parsed.Name, vector.Address)
		require.Equal(t, vector.Version, parsed.Version, vector.Address)
		require.Equal(t, vector.Binding, parsed.Binding, vector.Address)
		require.Equal(t, vector.BindingVersion, parsed.BindingVersion, vector.Address)
		require.Equal(t, vector.Form == "binding", parsed.IsBinding(), vector.Address)
	}
	for _, vector := range readAddressVectors(t, "invalid.json") {
		_, err := ParseAddress(vector.Address)
		require.Error(t, err, vector.Address)
	}
}

func TestConformanceOptionalExtensionRoundTrip(t *testing.T) {
	original := []byte(`
spec: "0.1-alpha"
kind: RepresentationBinding
address: TEST:VALUE@1:JSON@1
parent: TEST:VALUE@1
binding_id: binding-id
binding_version_id: binding-version-id
binding: JSON
binding_version: "1"
representation:
  standard: JSON Schema
  inline: {type: object}
extensions:
  ACME:OPTIONAL:
    nested:
      value: preserved
`)
	var before Binding
	require.NoError(t, yaml.Unmarshal(original, &before))
	encoded, err := yaml.Marshal(before)
	require.NoError(t, err)
	var after Binding
	require.NoError(t, yaml.Unmarshal(encoded, &after))
	require.Equal(t, before.Extensions, after.Extensions)
	require.Equal(t, "preserved", after.Extensions["ACME:OPTIONAL"].(map[string]any)["nested"].(map[string]any)["value"])
}

func TestConformanceDigestMismatchRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "artifact.bin")
	content := []byte("exact artifact bytes")
	require.NoError(t, os.WriteFile(path, content, 0o600))
	correct := fmt.Sprintf("sha256:%x", sha256.Sum256(content))
	require.NoError(t, VerifyDigest(path, correct))
	require.ErrorContains(t, VerifyDigest(path, "sha256:"+strings.Repeat("0", 64)), "digest mismatch")
}

func TestConformanceReceiptArtifactLink(t *testing.T) {
	output := ReceiptArtifact{
		Contract: "TEST:VALUE@1", ContractVersionID: "contract-version-id",
		Binding: "TEST:VALUE@1:JSON@1", BindingVersionID: "binding-version-id",
		ArtifactDigest: "sha256:" + strings.Repeat("a", 64),
	}
	input := output
	require.True(t, receiptArtifactsLink([]ReceiptArtifact{output}, []ReceiptArtifact{input}))
	input.ArtifactDigest = "sha256:" + strings.Repeat("b", 64)
	require.False(t, receiptArtifactsLink([]ReceiptArtifact{output}, []ReceiptArtifact{input}))
}
