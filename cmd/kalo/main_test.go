package main

import (
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
}
