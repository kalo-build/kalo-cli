package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/kalo-build/kalo-cli/internal/specv0"
)

// stageInputValue places one SPEC value into a plugin's /input directory.
// A file-set value is unpacked; any other value is copied as one file, named
// by KALO:PLUGIN-IO input.filename when the implementation declares one.
func stageInputValue(valuePath string, binding *specv0.Binding, pluginIO *specv0.PluginIO, inputDir string) error {
	if binding != nil && binding.MediaType == specv0.MediaTypeFileSet {
		data, err := os.ReadFile(valuePath)
		if err != nil {
			return err
		}
		_, err = specv0.UnpackFileSet(data, inputDir)
		return err
	}
	name := filepath.Base(valuePath)
	if pluginIO != nil && pluginIO.Input.Filename != "" {
		name = pluginIO.Input.Filename
	}
	return copyFile(valuePath, filepath.Join(inputDir, name))
}

// packStageOutput packs everything a plugin wrote below outputDir into one
// file-set value at target.
func packStageOutput(outputDir, target string) (string, error) {
	data, err := specv0.PackDir(outputDir)
	if err != nil {
		return "", err
	}
	if string(data) == "{\n  \"files\": []\n}\n" {
		return "", fmt.Errorf("plugin wrote no files")
	}
	if err := os.WriteFile(target, data, 0o644); err != nil {
		return "", err
	}
	return target, nil
}

// tailWriter forwards everything to dst and keeps the last max bytes.
type tailWriter struct {
	dst io.Writer
	max int
	mu  sync.Mutex
	buf []byte
}

func (w *tailWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	w.buf = append(w.buf, p...)
	if len(w.buf) > w.max {
		w.buf = append([]byte(nil), w.buf[len(w.buf)-w.max:]...)
	}
	w.mu.Unlock()
	if w.dst != nil {
		_, _ = w.dst.Write(p)
	}
	return len(p), nil
}

func (w *tailWriter) Tail() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return string(w.buf)
}
