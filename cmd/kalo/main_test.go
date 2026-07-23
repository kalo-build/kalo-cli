package main

import (
	"crypto/sha256"
	"fmt"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCLI_RootHelp verifies the CLI runs and responds to --help (interface smoke test).
func TestCLI_RootHelp(t *testing.T) {
	cmd := exec.Command("go", "run", ".", "--help")
	cmd.Dir = "."
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "kalo --help should succeed: %s", string(out))
	assert.Contains(t, string(out), "Usage", "help output should contain Usage")
}

// TestCLI_RunHelp verifies the run subcommand exists and shows help.
func TestCLI_RunHelp(t *testing.T) {
	cmd := exec.Command("go", "run", ".", "run", "--help")
	cmd.Dir = "."
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "kalo run --help should succeed: %s", string(out))
	assert.Contains(t, string(out), "run", "run help should mention run")
	assert.Contains(t, string(out), "--offline")
	assert.Contains(t, string(out), "--deny-network")
	assert.Contains(t, string(out), "--deterministic")
	assert.Contains(t, string(out), "--read-only-inputs")
	assert.Contains(t, string(out), "--plugin-timeout")
	assert.Contains(t, string(out), "--plugin-memory-mib")
}

func TestVerifyLockedPluginBytes(t *testing.T) {
	wasmBytes := []byte("\x00asm")
	expected := fmt.Sprintf("sha256:%x", sha256.Sum256(wasmBytes))

	require.NoError(t, verifyLockedPluginBytes(wasmBytes, expected))
	require.ErrorContains(t, verifyLockedPluginBytes(wasmBytes, ""), "no resolvedHash")
	require.ErrorContains(t, verifyLockedPluginBytes(wasmBytes, "sha256:deadbeef"), "artifact hash mismatch")
}

func TestRunTargetRejectsImpossibleMemoryLimitBeforeReadingProject(t *testing.T) {
	err := runTarget("compile", executionPolicy{PluginMemoryMiB: 4097})

	require.ErrorContains(t, err, "exceeds the WebAssembly maximum")
}
