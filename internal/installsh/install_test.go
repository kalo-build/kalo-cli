// Package installsh holds the check for install.sh, so `go test ./...` (and CI) runs it.
package installsh

import (
	"os/exec"
	"path/filepath"
	"testing"
)

func TestInstallScript(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("no sh on this system")
	}
	script, err := filepath.Abs(filepath.Join("..", "..", "scripts", "test-install.sh"))
	if err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("sh", script).CombinedOutput()
	t.Logf("%s", out)
	if err != nil {
		t.Fatalf("scripts/test-install.sh failed: %v", err)
	}
}
